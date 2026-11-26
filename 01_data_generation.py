# Databricks notebook source
from faker import Faker
from datetime import datetime, timezone
import random
import json

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


# CONFIGURATION

fake = Faker("en_IN")

BASE_PATH = "/Volumes/ecommerce_pipeline/default/landing"
STATE_PATH = f"{BASE_PATH}/_state/generator_state.json"

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
BATCH_TIMESTAMP = datetime.now(timezone.utc).isoformat()

NUM_CUSTOMERS = random.randint(100, 150)
NUM_PRODUCTS = random.randint(30, 80)
NUM_ORDERS = random.randint(100, 200)

MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 5

print(f"Starting batch generation: {BATCH_ID}")
print(f"Target counts: {NUM_CUSTOMERS} customers, {NUM_PRODUCTS} products, {NUM_ORDERS} orders")

# COMMAND ----------


# PATHS


CUSTOMERS_PATH = f"{BASE_PATH}/customers/batch_{BATCH_ID}"
PRODUCTS_PATH = f"{BASE_PATH}/products/batch_{BATCH_ID}"
ORDERS_PATH = f"{BASE_PATH}/orders/batch_{BATCH_ID}"
ORDER_ITEMS_PATH = f"{BASE_PATH}/order_items/batch_{BATCH_ID}"
PAYMENTS_PATH = f"{BASE_PATH}/payments/batch_{BATCH_ID}"

# COMMAND ----------


# GENERATOR STATE


DEFAULT_STATE = {
    "last_customer_id": 0,
    "last_product_id": 0,
    "last_order_id": 0,
    "last_order_item_id": 0,
    "last_payment_id": 0,
    "active_customers": {},
    "deleted_customer_ids": [],
    "active_products": {},
    "deleted_product_ids": [],
    "active_orders": {},
    "deleted_order_ids": []
}


def load_state():
    try:
        state_df = spark.read.text(STATE_PATH)
        state_json = "\n".join(row["value"] for row in state_df.collect())
        loaded = json.loads(state_json)

        for k, v in DEFAULT_STATE.items():
            if k not in loaded:
                loaded[k] = v.copy() if isinstance(v, (dict, list)) else v

        print(f"Loaded existing state: last_cid={loaded['last_customer_id']}, active_custs={len(loaded['active_customers'])}")
        return loaded
    except Exception:
        print("No existing generator state found. Initializing new state.")
        return DEFAULT_STATE.copy()


state = load_state()

active_customers = state["active_customers"]
deleted_customer_ids = state["deleted_customer_ids"]
active_products = state["active_products"]
deleted_product_ids = state["deleted_product_ids"]
active_orders = state["active_orders"]
deleted_order_ids = state["deleted_order_ids"]

# COMMAND ----------


# ID HELPERS

def generate_customer_id(idx):
    return f"CUST_{idx:07d}"

def generate_product_id(idx):
    return f"PROD_{idx:06d}"

def generate_order_id(idx):
    return f"ORD_{idx:08d}"

def generate_order_item_id(idx):
    return f"ITEM_{idx:09d}"

def generate_payment_id(idx):
    return f"PAY_{idx:08d}"

# COMMAND ----------


# CUSTOMER GENERATION (NEW + UPDATES/DELETES FOR SCD2 CDC)


cities = [
    ("Hyderabad", "Telangana"),
    ("Bangalore", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Kochi", "Kerala")
]

customers = []
customer_start = state["last_customer_id"] + 1
customer_end = customer_start + NUM_CUSTOMERS

# 1. New Customers (INSERT)
for i in range(customer_start, customer_end):
    city, state_name = random.choice(cities)
    cid = generate_customer_id(i)
    signup_dt = fake.date_between(start_date="-2y", end_date="today").isoformat()
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.unique.email()
    phone = fake.phone_number()

    customer = {
        "customer_id": cid,
        "operation": "INSERT",
        "event_timestamp": BATCH_TIMESTAMP,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "city": city,
        "state": state_name,
        "country": "India",
        "signup_date": signup_dt,
        "created_at": BATCH_TIMESTAMP,
        "updated_at": BATCH_TIMESTAMP,
        "batch_id": BATCH_ID
    }
    customers.append(customer)

    active_customers[cid] = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "city": city,
        "state": state_name,
        "signup_date": signup_dt,
        "created_at": BATCH_TIMESTAMP
    }

# 2. Existing Customer Updates & Deletes (UPDATE / DELETE)
available_cids = [cid for cid in active_customers.keys() if cid not in {c["customer_id"] for c in customers}]
num_customer_updates = 0
num_customer_deletes = 0

if available_cids:
    sample_count = min(random.randint(10, 15), len(available_cids))
    sampled_cids = random.sample(available_cids, sample_count)
    delete_count = min(random.randint(1, 2), max(1, len(sampled_cids) // 5)) if len(sampled_cids) >= 5 else 0
    delete_ids = set(sampled_cids[:delete_count])
    update_ids = [cid for cid in sampled_cids if cid not in delete_ids]

    for cid in update_ids:
        if cid not in active_customers:
            raise ValueError(f"State error: Customer {cid} missing from active_customers.")
        orig = active_customers[cid]
        available_cities = [c for c in cities if c[0] != orig["city"]]
        new_city, new_state = random.choice(available_cities if available_cities else cities)

        customers.append({
            "customer_id": cid,
            "operation": "UPDATE",
            "event_timestamp": BATCH_TIMESTAMP,
            "first_name": orig["first_name"],
            "last_name": orig["last_name"],
            "email": orig["email"],
            "phone": orig["phone"],
            "city": new_city,
            "state": new_state,
            "country": "India",
            "signup_date": orig["signup_date"],
            "created_at": orig["created_at"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })
        active_customers[cid]["city"] = new_city
        active_customers[cid]["state"] = new_state

    num_customer_updates = len(update_ids)

    for cid in delete_ids:
        if cid not in active_customers:
            raise ValueError(f"State error: Customer {cid} missing from active_customers.")
        orig = active_customers.pop(cid)
        deleted_customer_ids.append(cid)

        customers.append({
            "customer_id": cid,
            "operation": "DELETE",
            "event_timestamp": BATCH_TIMESTAMP,
            "first_name": orig["first_name"],
            "last_name": orig["last_name"],
            "email": orig["email"],
            "phone": orig["phone"],
            "city": orig["city"],
            "state": orig["state"],
            "country": "India",
            "signup_date": orig["signup_date"],
            "created_at": orig["created_at"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })

    num_customer_deletes = len(delete_ids)

print(f"Customers generated: {NUM_CUSTOMERS} new, {num_customer_updates} updated, {num_customer_deletes} deleted")

# COMMAND ----------


# PRODUCT GENERATION (NEW + UPDATES/DELETES FOR SCD2 CDC)

categories = [
    "Electronics", "Fashion", "Home & Kitchen", "Books",
    "Sports", "Beauty", "Grocery"
]

brands = [
    "Nova", "UrbanX", "TechPro", "Apex", "Zenith",
    "Prime", "Vertex", "Orion", "Nexon", "Pulse"
]

products = []
product_start = state["last_product_id"] + 1
product_end = product_start + NUM_PRODUCTS

# 1. New Products (INSERT)
for i in range(product_start, product_end):
    pid = generate_product_id(i)
    price = round(random.uniform(199, 75000), 2)
    name = fake.catch_phrase()
    category = random.choice(categories)
    brand = random.choice(brands)

    product = {
        "product_id": pid,
        "operation": "INSERT",
        "event_timestamp": BATCH_TIMESTAMP,
        "product_name": name,
        "category": category,
        "brand": brand,
        "price": price,
        "stock_quantity": random.randint(10, 500),
        "product_status": "ACTIVE",
        "created_at": BATCH_TIMESTAMP,
        "updated_at": BATCH_TIMESTAMP,
        "batch_id": BATCH_ID
    }
    products.append(product)

    active_products[pid] = {
        "product_name": name,
        "category": category,
        "brand": brand,
        "price": price,
        "created_at": BATCH_TIMESTAMP
    }

# 2. Existing Product Updates & Deletes (UPDATE / DELETE)
available_pids = [pid for pid in active_products.keys() if pid not in {p["product_id"] for p in products}]
num_product_updates = 0
num_product_deletes = 0

if available_pids:
    sample_count = min(random.randint(5, 10), len(available_pids))
    sampled_pids = random.sample(available_pids, sample_count)
    delete_count = min(random.randint(1, 2), max(1, len(sampled_pids) // 5)) if len(sampled_pids) >= 5 else 0
    delete_pids = set(sampled_pids[:delete_count])
    update_pids = [pid for pid in sampled_pids if pid not in delete_pids]

    for pid in update_pids:
        if pid not in active_products:
            raise ValueError(f"State error: Product {pid} missing from active_products.")
        orig = active_products[pid]
        new_price = round(orig["price"] * random.choice([0.85, 0.9, 0.95, 1.05, 1.1, 1.15]), 2)
        new_stock = random.randint(10, 500)
        new_status = random.choice(["ACTIVE", "ACTIVE", "INACTIVE"])

        products.append({
            "product_id": pid,
            "operation": "UPDATE",
            "event_timestamp": BATCH_TIMESTAMP,
            "product_name": orig["product_name"],
            "category": orig["category"],
            "brand": orig["brand"],
            "price": new_price,
            "stock_quantity": new_stock,
            "product_status": new_status,
            "created_at": orig["created_at"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })
        active_products[pid]["price"] = new_price

    num_product_updates = len(update_pids)

    for pid in delete_pids:
        if pid not in active_products:
            raise ValueError(f"State error: Product {pid} missing from active_products.")
        orig = active_products.pop(pid)
        deleted_product_ids.append(pid)

        products.append({
            "product_id": pid,
            "operation": "DELETE",
            "event_timestamp": BATCH_TIMESTAMP,
            "product_name": orig["product_name"],
            "category": orig["category"],
            "brand": orig["brand"],
            "price": orig["price"],
            "stock_quantity": 0,
            "product_status": "INACTIVE",
            "created_at": orig["created_at"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })

    num_product_deletes = len(delete_pids)

print(f"Products generated: {NUM_PRODUCTS} new, {num_product_updates} updated, {num_product_deletes} deleted")

# COMMAND ----------


# ORDER & ORDER ITEM GENERATION

customer_pool = list(active_customers.keys())
product_pool = list(active_products.keys())

orders = []
order_items = []

order_start = state["last_order_id"] + 1
order_end = order_start + NUM_ORDERS
order_item_counter = state["last_order_item_id"] + 1

order_statuses = ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]

# 1. New Orders & Items (INSERT)
for order_number in range(order_start, order_end):
    cid = random.choice(customer_pool)
    cust_info = active_customers[cid]
    current_order_id = generate_order_id(order_number)
    order_date = BATCH_TIMESTAMP

    item_count = random.randint(MIN_ITEMS_PER_ORDER, min(MAX_ITEMS_PER_ORDER, len(product_pool)))
    selected_pids = random.sample(product_pool, item_count)

    order_total = 0.0

    for pid in selected_pids:
        prod_info = active_products[pid]
        quantity = random.randint(1, 5)
        unit_price = prod_info["price"]
        discount = round(unit_price * quantity * random.uniform(0, 0.20), 2)
        line_total = round((unit_price * quantity) - discount, 2)

        order_items.append({
            "order_item_id": generate_order_item_id(order_item_counter),
            "operation": "INSERT",
            "event_timestamp": order_date,
            "order_id": current_order_id,
            "product_id": pid,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "line_total": line_total,
            "batch_id": BATCH_ID
        })
        order_total += line_total
        order_item_counter += 1

    order_total = round(order_total, 2)
    order_status = random.choice(order_statuses)

    order = {
        "order_id": current_order_id,
        "operation": "INSERT",
        "event_timestamp": order_date,
        "customer_id": cid,
        "order_date": order_date,
        "order_status": order_status,
        "shipping_city": cust_info["city"],
        "shipping_state": cust_info["state"],
        "total_amount": order_total,
        "updated_at": order_date,
        "batch_id": BATCH_ID
    }
    orders.append(order)

    active_orders[current_order_id] = {
        "customer_id": cid,
        "order_date": order["order_date"],
        "order_status": order_status,
        "shipping_city": cust_info["city"],
        "shipping_state": cust_info["state"],
        "total_amount": order_total
    }

# 2. Existing Order Updates & Deletes (UPDATE / DELETE for SCD1 CDC)
active_order_candidates = [
    oid for oid, o in active_orders.items()
    if o["order_status"] in ["PLACED", "CONFIRMED", "SHIPPED"] and oid not in {o["order_id"] for o in orders}
    and o["customer_id"] in active_customers
]
num_order_updates = 0
num_order_deletes = 0

if active_order_candidates:
    sample_size = min(random.randint(5, 10), len(active_order_candidates))
    sampled_oids = random.sample(active_order_candidates, sample_size)
    delete_count = min(1, len(sampled_oids)) if len(sampled_oids) >= 5 else 0
    delete_oids = set(sampled_oids[:delete_count])
    update_oids = [oid for oid in sampled_oids if oid not in delete_oids]

    status_next = {"PLACED": "CONFIRMED", "CONFIRMED": "SHIPPED", "SHIPPED": "DELIVERED"}

    for oid in update_oids:
        if oid not in active_orders:
            raise ValueError(f"State error: Order {oid} missing from active_orders.")
        orig = active_orders[oid]
        new_status = status_next.get(orig["order_status"], "DELIVERED")

        orders.append({
            "order_id": oid,
            "operation": "UPDATE",
            "event_timestamp": BATCH_TIMESTAMP,
            "customer_id": orig["customer_id"],
            "order_date": orig["order_date"],
            "order_status": new_status,
            "shipping_city": orig["shipping_city"],
            "shipping_state": orig["shipping_state"],
            "total_amount": orig["total_amount"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })
        active_orders[oid]["order_status"] = new_status

    num_order_updates = len(update_oids)

    for oid in delete_oids:
        if oid not in active_orders:
            raise ValueError(f"State error: Order {oid} missing from active_orders.")
        orig = active_orders.pop(oid)
        deleted_order_ids.append(oid)

        orders.append({
            "order_id": oid,
            "operation": "DELETE",
            "event_timestamp": BATCH_TIMESTAMP,
            "customer_id": orig["customer_id"],
            "order_date": orig["order_date"],
            "order_status": "CANCELLED",
            "shipping_city": orig["shipping_city"],
            "shipping_state": orig["shipping_state"],
            "total_amount": orig["total_amount"],
            "updated_at": BATCH_TIMESTAMP,
            "batch_id": BATCH_ID
        })

    num_order_deletes = len(delete_oids)

print(f"Orders generated: {NUM_ORDERS} new, {num_order_updates} updated, {num_order_deletes} deleted ({len(order_items)} items)")

# COMMAND ----------


# PAYMENT GENERATION

payment_methods = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET"]
payment_statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "REFUNDED"]

payments = []
payment_start = state["last_payment_id"] + 1

new_orders = [o for o in orders if o["operation"] == "INSERT"]

for payment_number, order in enumerate(new_orders, start=payment_start):
    payments.append({
        "payment_id": generate_payment_id(payment_number),
        "operation": "INSERT",
        "event_timestamp": order["order_date"],
        "order_id": order["order_id"],
        "payment_date": order["order_date"],
        "payment_method": random.choice(payment_methods),
        "payment_status": random.choice(payment_statuses),
        "amount": order["total_amount"],
        "transaction_reference": f"TXN_{order['order_id']}_{payment_number}",
        "batch_id": BATCH_ID
    })

print(f"Payments generated: {len(payments)}")

# COMMAND ----------


# RELATIONSHIP & INTEGRITY VALIDATIONS

# 1. Uniqueness
customer_pks = [c["customer_id"] for c in customers]
assert len(customer_pks) == len(set(customer_pks)), "Duplicate customer IDs detected"

product_pks = [p["product_id"] for p in products]
assert len(product_pks) == len(set(product_pks)), "Duplicate product IDs detected"

# 2. Foreign keys
all_known_custs = set(active_customers.keys()) | {c["customer_id"] for c in customers} | set(deleted_customer_ids)
for o in orders:
    assert o["customer_id"] in all_known_custs, f"Invalid customer reference in order {o['order_id']}"

all_batch_order_ids = {o["order_id"] for o in orders}
for item in order_items:
    assert item["order_id"] in all_batch_order_ids, f"Invalid order reference in item {item['order_item_id']}"
    assert item["product_id"] in active_products, f"Invalid product reference in item {item['order_item_id']}"

for pmt in payments:
    assert pmt["order_id"] in all_batch_order_ids, f"Invalid order reference in payment {pmt['payment_id']}"

# 3. Order totals calculation match
item_totals = {}
for item in order_items:
    item_totals[item["order_id"]] = round(item_totals.get(item["order_id"], 0.0) + item["line_total"], 2)

for o in new_orders:
    calc_total = item_totals.get(o["order_id"], 0.0)
    assert abs(calc_total - float(o["total_amount"])) < 0.01, f"Mismatch in order total for {o['order_id']}: calculated {calc_total} vs stored {o['total_amount']}"

print("All entity relationships and order total validations PASSED.")

# COMMAND ----------


# WRITE JSON LANDING DATA

def write_json_file(records, path, filename):
    file_path = f"{path}/{filename}"
    json_content = "\n".join(json.dumps(record) for record in records)
    dbutils.fs.put(file_path, json_content, overwrite=True)

write_json_file(customers, CUSTOMERS_PATH, "customers.json")
write_json_file(products, PRODUCTS_PATH, "products.json")
write_json_file(orders, ORDERS_PATH, "orders.json")
write_json_file(order_items, ORDER_ITEMS_PATH, "order_items.json")
write_json_file(payments, PAYMENTS_PATH, "payments.json")

# COMMAND ----------


# UPDATE GENERATOR STATE

new_state = {
    "last_customer_id": customer_end - 1,
    "last_product_id": product_end - 1,
    "last_order_id": order_end - 1,
    "last_order_item_id": order_item_counter - 1,
    "last_payment_id": payment_start + len(payments) - 1,
    "active_customers": dict(list(active_customers.items())[-1000:]),
    "deleted_customer_ids": deleted_customer_ids[-500:],
    "active_products": dict(list(active_products.items())[-1000:]),
    "deleted_product_ids": deleted_product_ids[-500:],
    "active_orders": dict(list(active_orders.items())[-1000:]),
    "deleted_order_ids": deleted_order_ids[-500:]
}

dbutils.fs.put(STATE_PATH, json.dumps(new_state, indent=2), overwrite=True)

print(f"Batch {BATCH_ID} generation complete. State successfully persisted.")

# COMMAND ----------

# DBTITLE 1,Export batch_id for downstream tasks

# EXPORT BATCH ID FOR DOWNSTREAM TASKS

dbutils.jobs.taskValues.set(key="batch_id", value=BATCH_ID)
print(f"Exported batch_id for downstream tasks: {BATCH_ID}")