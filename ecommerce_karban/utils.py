import frappe
from ecommerce_integrations.unicommerce.constants import (
	ADDRESS_JSON_FIELD,
	CUSTOMER_CODE_FIELD,
	SETTINGS_DOCTYPE,
	UNICOMMERCE_COUNTRY_MAPPING,
	UNICOMMERCE_INDIAN_STATES_MAPPING,
	CHANNEL_ID_FIELD,
	FACILITY_CODE_FIELD,
	IS_COD_CHECKBOX,
	ORDER_CODE_FIELD,
	ORDER_STATUS_FIELD,
	SETTINGS_DOCTYPE,
)
from typing import Any
from frappe import _
import json
from frappe.utils.nestedset import get_root_of
from typing import Any, NewType

from ecommerce_integrations.controllers.scheduling import need_to_run
from ecommerce_integrations.unicommerce.api_client import UnicommerceAPIClient
from ecommerce_integrations.unicommerce.constants import (
	ORDER_CODE_FIELD,
	SETTINGS_DOCTYPE,
)
from ecommerce_integrations.unicommerce.order import _create_sales_invoices, _sync_order_items,_get_line_items,_get_facility_code,get_taxes,_get_batch_no
from ecommerce_integrations.unicommerce.utils import create_unicommerce_log, get_unicommerce_date
from ecommerce_integrations.utils.taxation import get_dummy_tax_category
from collections.abc import Iterator
from frappe.utils import add_to_date, flt
from ecommerce_integrations.ecommerce_integrations.doctype.ecommerce_item import ecommerce_item
from ecommerce_integrations.unicommerce.constants import (
	CHANNEL_ID_FIELD,
	CHANNEL_TAX_ACCOUNT_FIELD_MAP,
	FACILITY_CODE_FIELD,
	INVOICE_CODE_FIELD,
	IS_COD_CHECKBOX,
	MODULE_NAME,
	ORDER_CODE_FIELD,
	ORDER_ITEM_BATCH_NO,
	ORDER_ITEM_CODE_FIELD,
	ORDER_STATUS_FIELD,
	PACKAGE_TYPE_FIELD,
	SETTINGS_DOCTYPE,
	TAX_FIELDS_MAPPING,
	TAX_RATE_FIELDS_MAPPING,
)


UnicommerceOrder = NewType("UnicommerceOrder", dict[str, Any])
def sync_customer(order):
	"""Using order create a new customer.

	Note: Unicommerce doesn't deduplicate customer."""
	customer, status = _create_new_customer(order)
	_create_customer_addresses(order.get("addresses") or [], customer, order.get("customerGSTIN"), status)
	return customer

def _check_if_customer_exists(address, customer_code):
	"""Very crude method to determine if same customer exists.

	If ALL address fields match then new customer is not created"""

	customer_name = None

	# if customer_code:
	# 	customer_name = frappe.db.get_value("Customer", {CUSTOMER_CODE_FIELD: customer_code})

	if not customer_name:
		customer_name = frappe.db.get_value("Customer", {'customer_name': address.get("name")})

	if customer_name:
		return frappe.get_doc("Customer", customer_name)


def _create_new_customer(order):
	"""Create a new customer from Sales Order address data"""

	address = order.get("billingAddress") or (order.get("addresses") and order.get("addresses")[0])
	address.pop("id", None)  # this is not important and can be different for same address
	customer_code = order.get("customerCode")

	customer = _check_if_customer_exists(address, customer_code)
	if customer:
		if order.get("customerGSTIN"):
			frappe.db.set_value("Customer", customer.name, "gstin", order.get("customerGSTIN"))
			frappe.db.set_value("Customer", customer.name, "gst_category", "Registered Regular")
		else:
			frappe.db.set_value("Customer", customer.name, "gstin", "")
			frappe.db.set_value("Customer", customer.name, "gst_category", "Unregistered")
		frappe.db.set_value("Customer", customer.name, "default_currency", order.get("currencyCode"))
		return customer, 'Existing'

	setting = frappe.get_cached_doc(SETTINGS_DOCTYPE)
	customer_group = (
		frappe.db.get_value(
			"Unicommerce Channel", {"channel_id": order["channel"]}, fieldname="customer_group"
		)
		or setting.default_customer_group
	)

	name = address.get("name") or order["channel"] + " customer"
	customer = frappe.get_doc(
		{
			"doctype": "Customer",
			"customer_name": name,
			"customer_group": customer_group,
			"territory": get_root_of("Territory"),
			"customer_type": "Individual",
			"gstin": order.get("customerGSTIN") if order.get("customerGSTIN") else "",
			"gst_category": "Registered Regular" if order.get("customerGSTIN") else "Unregistered",
			"default_currency": order.get("currencyCode"),
			ADDRESS_JSON_FIELD: json.dumps(address),
			CUSTOMER_CODE_FIELD: customer_code,
		}
	)

	customer.flags.ignore_mandatory = True
	customer.insert(ignore_permissions=True)

	return customer, 'New'


def _create_customer_addresses(addresses: list[dict[str, Any]], customer, gstin, status) -> None:
	"""Create address from dictionary containing fields used in Address doctype of ERPNext.

	Unicommerce orders contain address list,
	if there is only one address it's both shipping and billing,
	else first is billing and second is shipping"""

	if len(addresses) == 1:
		_create_customer_address(addresses[0], "Billing", customer, gstin,status, also_shipping=True)
	elif len(addresses) >= 2:
		_create_customer_address(addresses[0], "Billing", customer, gstin, status)
		_create_customer_address(addresses[1], "Shipping", customer, gstin, status)


def _create_customer_address(uni_address, address_type, customer, gstin, status, also_shipping=False):
	country_code = uni_address.get("country")
	country = UNICOMMERCE_COUNTRY_MAPPING.get(country_code)

	state = uni_address.get("state")
	if country_code == "IN" and state in UNICOMMERCE_INDIAN_STATES_MAPPING:
		state = UNICOMMERCE_INDIAN_STATES_MAPPING.get(state)
		
	address_line1 = uni_address.get("addressLine1") or "Not provided"
	address_line2 = uni_address.get("addressLine2")
	city = uni_address.get("city")
	state = state
	country = country
	pincode = uni_address.get("pincode")
	phone = uni_address.get("phone")

	filters = {
		"address_type": address_type,
		"address_line1": address_line1,
		"address_line2": address_line2,
		"city": city,
		"state": state,
		"country": country,
		"pincode": pincode
	}

	filters = {k: (v[0] if isinstance(v, tuple) else v) for k, v in filters.items() if v}


	existing_address_name = frappe.db.get_all("Address",filters=filters,fields=["name"],order_by="creation desc",limit=1)

	check_contact = filters.copy()
	check_contact.update({"phone": phone})

	address_with_same_phone = frappe.db.get_all("Address",filters=check_contact,fields=["name"],limit=1)


	if existing_address_name and address_with_same_phone and status=='Existing':
		existing_address = frappe.get_doc("Address", existing_address_name[0].name)

		if not any(l.link_doctype == "Customer" and l.link_name == customer.name for l in existing_address.links):
			existing_address.append("links", {"link_doctype": "Customer", "link_name": customer.name})
			existing_address.save(ignore_permissions=True)

		return existing_address.name
	
	# elif existing_address_name and not address_with_same_phone:

	# if status=='Existing' and existing_address_name and address_with_same_phone:
	# 	return
		
	new_address = frappe.get_doc({
		"doctype": "Address",
		"address_type": address_type,
		"address_line1": address_line1,
		"address_line2": address_line2,
		"city": city,
		"state": state,
		"country": country,
		"pincode": pincode,
		"email_id": uni_address.get("email"),
		"phone": phone,
		"is_primary_address": int(address_type == "Billing"),
		"is_shipping_address": int(also_shipping or address_type == "Shipping"),
		"links": [{"link_doctype": "Customer", "link_name": customer.name}],
		"gstin": gstin if gstin else "",
		"gst_category": "Registered Regular" if gstin else "Unregistered",
	})
	new_address.insert(ignore_permissions=True)
	return new_address.name

SYNC_METHODS = {
	"Items": "ecommerce_integrations.unicommerce.product.upload_new_items",
	"Orders": "ecommerce_karban.utils.sync_new_orders",
	"Inventory": "ecommerce_integrations.unicommerce.inventory.update_inventory_on_unicommerce",
}


@frappe.whitelist()
def force_sync(document) -> None:
	frappe.only_for("System Manager")

	method = SYNC_METHODS.get(document)
	if not method:
		frappe.throw(frappe._("Unknown method"))
	frappe.enqueue(method, queue="long", is_async=True, **{"force": True})


def sync_new_orders(client: UnicommerceAPIClient = None, force=False):
	"""This is called from a scheduled job and syncs all new orders from last synced time."""
	settings = frappe.get_cached_doc(SETTINGS_DOCTYPE)

	if not settings.is_enabled():
		return

	# check if need to run based on configured sync frequency.
	# Note: This also updates last_order_sync if function runs.
	if not force and not need_to_run(SETTINGS_DOCTYPE, "order_sync_frequency", "last_order_sync"):
		return

	if client is None:
		client = UnicommerceAPIClient()

	status = "COMPLETE" if settings.only_sync_completed_orders else None

	new_orders = _get_new_orders(client, status=status)

	if new_orders is None:
		return

	for order in new_orders:
		try:
			sales_order = create_order(order, client=client)

			# if settings.only_sync_completed_orders:
			# 	_create_sales_invoices(order, sales_order, client)
		except Exception as e:
			create_unicommerce_log(
				method="ecommerce_integrations.unicommerce.order.sync_new_orders",
				request_data=order,
				status="Error",
				exception=e,
			)
			continue


def _get_new_orders(client: UnicommerceAPIClient, status: str | None) -> Iterator[UnicommerceOrder] | None:
	"""Search new sales order from unicommerce."""

	updated_since = 24 * 60  # minutes
	val_from_date = frappe.db.get_single_value("Unicommerce Settings", "custom_from_date")
	val_to_date = frappe.db.get_single_value("Unicommerce Settings", "custom_to_date")
	
	uni_orders = client.search_sales_order(from_date=str(val_from_date), to_date=str(val_to_date), status=status)

	configured_channels = {
		c.channel_id
		for c in frappe.get_all("Unicommerce Channel", filters={"enabled": 1}, fields="channel_id")
	}
	if uni_orders is None:
		return

	for order in uni_orders:
		if order["channel"] not in configured_channels:
			continue

		# In case a sales invoice is not generated for some reason and is skipped, we need to create it manually. Therefore, I have commented out this line of code.
		order = client.get_sales_order(order_code=order["code"])
		if order:
			yield order

def create_order(payload: UnicommerceOrder, request_id: str | None = None, client=None) -> None:
	order = payload

	existing_so = frappe.db.get_value("Sales Order", {ORDER_CODE_FIELD: order["code"]})
	if existing_so:
		so = frappe.get_doc("Sales Order", existing_so)
		return so

	# If a sales order already exists, then every time it's executed
	if request_id is None:
		log = create_unicommerce_log(
			method="ecommerce_integrations.unicommerce.order.create_order", request_data=payload
		)
		request_id = log.name

	if client is None:
		client = UnicommerceAPIClient()

	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id
	try:
		_sync_order_items(order, client=client)
		customer = sync_customer(order)
		order = _create_order(order, customer)
	except Exception as e:
		create_unicommerce_log(status="Error", exception=e, rollback=True)
		frappe.flags.request_id = None
	else:
		create_unicommerce_log(status="Success")
		frappe.flags.request_id = None
		return order

def _create_order(order: UnicommerceOrder, customer) -> None:
	channel_config = frappe.get_doc("Unicommerce Channel", order["channel"])
	settings = frappe.get_cached_doc(SETTINGS_DOCTYPE)

	is_cancelled = order["status"] == "CANCELLED"

	facility_code = _get_facility_code(order["saleOrderItems"])
	company_address, dispatch_address = settings.get_company_addresses(facility_code)

	so = frappe.get_doc(
		{
			"doctype": "Sales Order",
			"customer": customer.name,
			"naming_series": channel_config.sales_order_series or settings.sales_order_series,
			ORDER_CODE_FIELD: order["code"],
			ORDER_STATUS_FIELD: order["status"],
			CHANNEL_ID_FIELD: order["channel"],
			FACILITY_CODE_FIELD: facility_code,
			IS_COD_CHECKBOX: bool(order["cod"]),
			"transaction_date": get_unicommerce_date(order["displayOrderDateTime"]),
			"delivery_date": get_unicommerce_date(order["fulfillmentTat"]),
			"ignore_pricing_rule": 1,
			"items": _get_line_items(
				order["saleOrderItems"], default_warehouse=channel_config.warehouse, is_cancelled=is_cancelled
			),
			"company": channel_config.company,
			"taxes": get_taxes(order["saleOrderItems"], channel_config),
			"tax_category": get_dummy_tax_category(),
			"company_address": company_address,
			"dispatch_address_name": dispatch_address,
			"currency": order.get("currencyCode"),
		}
	)

	so.flags.raw_data = order
	taxes = get_taxes_so(order["saleOrderItems"], channel_config)

	for tax in taxes:
		so.append("taxes", tax)
	so.save()

	# so.submit()
	if is_cancelled:
		so.cancel()

	return so


def _get_line_items(
	line_items, default_warehouse: str | None = None, is_cancelled: bool = False
) -> list[dict[str, Any]]:
	settings = frappe.get_cached_doc(SETTINGS_DOCTYPE)
	wh_map = settings.get_integration_to_erpnext_wh_mapping(all_wh=True)
	so_items = []

	for item in line_items:
		# if not is_cancelled and item.get("statusCode") == "CANCELLED":
		# 	continue

		item_code = ecommerce_item.get_erpnext_item_code(
			integration=MODULE_NAME, integration_item_code=item["itemSku"]
		)
		warehouse = wh_map.get(item["facilityCode"]) or default_warehouse
		so_items.append(
			{
				"item_code": item_code,
				"distributed_discount_amount": item.get("discount"),
				"rate": item["sellingPriceWithoutTaxesAndDiscount"] - item.get("discount", 0),
				"qty": 1,
				"stock_uom": "Nos",
				"warehouse": warehouse,
				ORDER_ITEM_CODE_FIELD: item.get("code"),
				ORDER_ITEM_BATCH_NO: _get_batch_no(item),
			}
		)
	return so_items

def get_taxes_so(line_items, channel_config) -> list:
	taxes = []
	TAX_FIELDS_MAPPINGs = {
		"igst": "totalIntegratedGst",
		"cgst": "totalCentralGst",
		"sgst": "totalStateGst",
		"ugst": "totalUnionTerritoryGst"
	}


	tax_map = {tax_head: 0.0 for tax_head in TAX_FIELDS_MAPPING.keys()}
	item_wise_tax_map = {tax_head: {} for tax_head in TAX_FIELDS_MAPPING.keys()}

	tax_account_map = {
		tax_head: channel_config.get(account_field)
		for tax_head, account_field in CHANNEL_TAX_ACCOUNT_FIELD_MAP.items()
	}


	for item in line_items:
		item_code = ecommerce_item.get_erpnext_item_code(
			integration=MODULE_NAME, integration_item_code=item["itemSku"]
		)
		for tax_head, unicommerce_field in TAX_FIELDS_MAPPINGs.items():
			tax_amount = 0.0
			tax_rate_field = TAX_RATE_FIELDS_MAPPING.get(tax_head, "")
			tax_rate = item.get(tax_rate_field, 0.0)
			tax_amount = flt(item.get(unicommerce_field)) or 0.0
			tax_map[tax_head] += tax_amount

			existing = item_wise_tax_map[tax_head].get(item_code, [tax_rate, 0.0])
			item_wise_tax_map[tax_head][item_code] = [
				tax_rate,
				flt(existing[1]) + tax_amount
			]

	taxes = []

	for tax_head, value in tax_map.items():
		if not value:
			continue
		taxes.append(
			{
				"charge_type": "Actual",
				"account_head": tax_account_map[tax_head],
				"tax_amount": value,
				"description": tax_head.replace("_", " ").upper(),
				"item_wise_tax_detail": json.dumps(item_wise_tax_map[tax_head]),
				"dont_recompute_tax": 1,
			}
		)

	return taxes
