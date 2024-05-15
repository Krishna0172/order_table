from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import sql
import json
from pincode import get_address_by_postal_code

csv_file_path = 'Evenflow.csv'
csv_file_path_refund_data = 'Evenflow_Refund_Data.csv'
csv_file_path_penalty_data = 'Evenflow_Penalty_Data.csv'


def fetch_inventory_data(cursor, orgid, channel, sku):
    try:
        select_query = """
            SELECT costperunittax, costperunit, sku FROM inventory WHERE orgid in %s AND channel in %s AND sku in %s;
        """
        cursor.execute(select_query, (tuple(orgid.tolist()), tuple(channel.tolist()), tuple(sku.tolist())))
        inventory_results = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in
                             cursor.fetchall()]

        df = pd.DataFrame(inventory_results)
        return df
    except psycopg2.Error as e:
        print("Error fetching", e)


def lambda_handler(events):
    for event in events:
        csv_file_path = event['csv_file_path']

    try:
        conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        evenflow_columns = ['order_item_id', 'Order Date Time', 'Merchant SKU', 'Product ID', 'Destination Pincode',
                            'Warehouse Location Code',
                            'Category', 'PG Cost Base', 'PG Cost Includig Tax', 'Order Status', 'PG Cost Base',
                            'PG Cost Includig Tax',
                            'TCS on Net GMV', 'TDS on Net GMV', 'Estimated Shipment Cost Including GST',
                            'Payout After Adjustments']

        evenflow_col_mapping = {'Merchant SKU': 'sku', 'order_item_id': 'orderid', 'Product ID': 'productid',
                                'Destination Pincode': 'shippostalcode',
                                'Warehouse Location Code': 'fulfillmentcenterid', 'Category': 'subcategory',
                                'Order Status': 'fsorderstatus',
                                'Order Date Time': 'orderdate', 'PG Cost Base': 'invoiceprice',
                                'PG Cost Includig Tax': 'taxdetails'
                                }

        evenflow_df = pd.read_csv(csv_file_path, usecols=evenflow_columns).rename(columns=evenflow_col_mapping).assign(
            quantityshipped='1', orgid='10', profitfulfillmentchannel='Merchant', channel='CRED')

        evenflow_df['shipstate'] = evenflow_df['shippostalcode'].apply(
            lambda postal_code: get_address_by_postal_code(evenflow_df['orgid'], str(postal_code))['state'])

        evenflow_df['shipdistrict'] = evenflow_df['shippostalcode'].apply(
            lambda postal_code: get_address_by_postal_code(evenflow_df['orgid'], str(postal_code))['district'])

        evenflow_df['orderitemid'] = evenflow_df['orderid'] + '-' + evenflow_df['sku']

        evenflow_df['id'] = evenflow_df['orgid'] + '-' + evenflow_df['orderitemid']

        evenflow_df['returnquantity'] = evenflow_df['fsorderstatus'].map({'RETURNED': 1, 'RTO': 1}).fillna(0).astype(
            int)

        evenflow_df['fsorderstatus'] = evenflow_df['fsorderstatus'].replace(
            {'DELIVERED': 'SHIPPED', 'PACKED': 'SHIPPED', 'SELLER_CONFIRMED': 'SHIPPED', 'RTO': 'RTO RECEIVED',
             'RETURN_REQUESTED': 'C RETURN RECEIVED',
             'RETURNED': 'C RETURN RECEIVED', 'CANCELLED': 'CANCELLED'})

        evenflow_df['returnfees'] = evenflow_df.apply(
            lambda row: json.dumps({"total": row['invoiceprice'] * -1, "breakup": []}) if row['returnquantity'] == 1 and
                                                                                          row[
                                                                                              'fsorderstatus'] != 'CANCELLED'
            else json.dumps({"total": 0, "breakup": []}), axis=1)

        evenflow_df['tcs'] = evenflow_df.apply(
            lambda x: json.dumps({"total": round(sum(x[col] * -1 for col in ['TCS on Net GMV', 'TDS on Net GMV']), 2),
                                  "breakup": [
                                      {"TCS": {"CurrencyCode": "INR",
                                               "CurrencyAmount": round(x['TCS on Net GMV'] * -1, 2)}},
                                      {"TDS": {"CurrencyCode": "INR",
                                               "CurrencyAmount": round(x['TDS on Net GMV'] * -1, 2)}}]})
            if x['fsorderstatus'] != 'CANCELLED' else json.dumps({"total": 0, "breakup": []}), axis=1)

        evenflow_df['taxdetails'] = evenflow_df['taxdetails'].apply(lambda x: json.dumps({"igst_amount": x}))

        evenflow_columns_2 = ['cred_order_item_id', 'merchant_sku', 'estimated_reverse_shipment_cost_including_gst']

        evenflow_refunddata = pd.read_csv(csv_file_path_refund_data, usecols=evenflow_columns_2) \
            .assign(orderitemid=lambda x: x['cred_order_item_id'] + '-' + x['merchant_sku']) \
            .drop(['cred_order_item_id', 'merchant_sku'], axis=1) \
            .drop_duplicates(subset='orderitemid', keep='first')

        evenflow_df = pd.merge(evenflow_df, evenflow_refunddata, on='orderitemid', how='left')

        print(evenflow_df.info())

        evenflow_df['estimated_reverse_shipment_cost_including_gst'] = evenflow_df[
            'estimated_reverse_shipment_cost_including_gst'].fillna(0).astype(int)

        evenflow_df['shippingfees'] = evenflow_df.apply(
            lambda row: json.dumps({
                "total": (
                             (row['Estimated Shipment Cost Including GST'] + row[
                                 'estimated_reverse_shipment_cost_including_gst']) * -1
                             if row['fsorderstatus'] not in ['CANCELLED', 'RTO']
                             else 0
                         ) + row['estimated_reverse_shipment_cost_including_gst'] * -1,
                "breakup": [
                    {
                        "Shipping Fee": {
                            "CurrencyCode": "INR",
                            "CurrencyAmount": (
                                (row['Estimated Shipment Cost Including GST'] + row[
                                    'estimated_reverse_shipment_cost_including_gst']) * -1
                                if row['fsorderstatus'] not in ['CANCELLED', 'RTO']
                                else 0
                            )
                        }
                    },
                    {
                        "Reverse Shipping Fee": {
                            "CurrencyCode": "INR",
                            "CurrencyAmount": row['estimated_reverse_shipment_cost_including_gst'] * -1
                        }
                    }
                ]
            }), axis=1
        )

        evenflow_columns_3 = ['Penalty', 'cred_order_item_id', 'merchant_sku']

        evenflow_penalty_data = pd.read_csv(csv_file_path_penalty_data,
                                            usecols=evenflow_columns_3) \
            .assign(orderitemid=lambda x: x['cred_order_item_id'] + '-' + x['merchant_sku']) \
            .drop(['cred_order_item_id', 'merchant_sku'], axis=1)

        evenflow_df = pd.merge(evenflow_df, evenflow_penalty_data, on='orderitemid', how='left')

        evenflow_df['marketplacefees'] = round(
            (evenflow_df['invoiceprice'] - evenflow_df['TCS on Net GMV'] - evenflow_df['TDS on Net GMV'] -
             evenflow_df['Estimated Shipment Cost Including GST'] - evenflow_df['Payout After Adjustments']), 2)

        evenflow_df['marketplacefees'] = evenflow_df.apply(
            lambda x: json.dumps({
                "total": (x['marketplacefees'] * -1 if x['fsorderstatus'] != 'CANCELLED' else 0) + (
                    x['Penalty'] * -1 if pd.notna(x['Penalty']) else 0),
                "breakup": [
                    {"Commision": {"Percentage": round((x['marketplacefees'] / x['invoiceprice'] * 100), 2) if x[
                                                                                                                   'marketplacefees'] != 0 else 0,
                                   "CurrencyCode": "INR", "CurrencyAmount": x['marketplacefees'] * -1 if x[
                                                                                                             'fsorderstatus'] != 'CANCELLED' else 0}},
                    {"penalty": {"CurrencyCode": "INR",
                                 "CurrencyAmount": x['Penalty'] * -1 if pd.notna(x['Penalty']) else 0}}
                ]
            }), axis=1)

        evenflow_df['invoiceprice'] = evenflow_df.apply(
            lambda x: json.dumps({"total": x['invoiceprice'], "breakup": []}) if x[
                                                                                     'fsorderstatus'] != 'CANCELLED' else json.dumps(
                {"total": 0, "breakup": []}), axis=1)

        json_cols = ['invoiceprice', 'marketplacefees', 'shippingfees', 'tcs', 'returnfees']

        for col in json_cols:
            evenflow_df[col] = evenflow_df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

        settled_amount_expr = sum(evenflow_df[col].apply(
            lambda x: x.get('total', 0) if isinstance(x.get('total', 0), (int, float)) else 0) * (
                                      -1 if col in ['marketplacefees', 'shippingfees', 'tcs', 'returnfees'] else 1)
                                  for col in json_cols)

        evenflow_df['settledamount'] = round(settled_amount_expr, 2)

        for col in json_cols:
            evenflow_df[col] = evenflow_df[col].apply(json.dumps)

        evenflow_df['updatedat'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cost_perunit_cost_perunit_tax = fetch_inventory_data(cursor, evenflow_df['orgid'], evenflow_df['channel'],
                                                             evenflow_df['sku'])

        evenflow_df = pd.merge(evenflow_df, cost_perunit_cost_perunit_tax, on='sku', how='left')

        print("Number of rows in CSV:", len(evenflow_df))

        column_order = ['id', 'channel', 'orgid', 'orderid', 'orderdate', 'sku', 'quantityshipped', 'returnquantity',
                        'productid', 'fsorderstatus', 'fulfillmentcenterid', 'profitfulfillmentchannel', 'shipdistrict',
                        'shipstate', 'shippostalcode', 'marketplacefees', 'shippingfees', 'tcs', 'returnfees',
                        'orderitemid', 'subcategory', 'taxdetails', 'costperunit', 'costperunittax', 'updatedat',
                        'invoiceprice',
                        'settledamount'
                        ]

        evenflow_df = evenflow_df.reindex(columns=column_order)

        insert_query = sql.SQL("""
            INSERT INTO orders (id, channel, orgid, orderid, orderdate, sku, quantityshipped, returnquantity, productid, fsorderstatus, fulfillmentcenterid, profitfulfillmentchannel, shipdistrict, shipstate, shippostalcode, marketplacefees, shippingfees, tcs, returnfees, orderitemid, subcategory, taxdetails, costperunit, costperunittax, updatedat, invoiceprice, settledamount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (orgid, id) DO UPDATE SET
                id = EXCLUDED.id, 
                channel = EXCLUDED.channel, 
                orgid = EXCLUDED.orgid, 
                orderid = EXCLUDED.orderid,
                orderdate = EXCLUDED.orderdate,
                sku = EXCLUDED.sku,
                quantityshipped = EXCLUDED.quantityshipped,
                returnquantity = EXCLUDED.returnquantity,
                productid = EXCLUDED.productid,
                fsorderstatus = EXCLUDED.fsorderstatus,
                fulfillmentcenterid = EXCLUDED.fulfillmentcenterid, 
                profitfulfillmentchannel = EXCLUDED.profitfulfillmentchannel, 
                shipdistrict = EXCLUDED.shipdistrict, 
                shipstate = EXCLUDED.shipstate, 
                shippostalcode = EXCLUDED.shippostalcode, 
                marketplacefees = EXCLUDED.marketplacefees, 
                shippingfees = EXCLUDED.shippingfees, 
                tcs = EXCLUDED.tcs, 
                returnfees = EXCLUDED.returnfees, 
                orderitemid = EXCLUDED.orderitemid, 
                subcategory = EXCLUDED.subcategory, 
                taxdetails = EXCLUDED.taxdetails, 
                costperunit = EXCLUDED.costperunit, 
                costperunittax = EXCLUDED.costperunittax, 
                updatedat = EXCLUDED.updatedat, 
                invoiceprice = EXCLUDED.invoiceprice, 
                settledamount = EXCLUDED.settledamount 
        """)

        with conn.cursor() as cursor:
            data = evenflow_df.to_records(index=False).tolist()
            cursor.executemany(insert_query, data)

        conn.commit()
        print("CSV data successfully inserted into PostgreSQL table.")

    except psycopg2.Error as error:
        print("An error occurred", error)

    finally:
        conn.close()


events = [
    {
        'csv_file_path': csv_file_path
    }
]

lambda_handler(events)
