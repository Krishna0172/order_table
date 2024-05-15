import psycopg2

def create_orders_table():
    try:
        conn = psycopg2.connect(
            database="postgres",
            user='postgres',
            password='postgres',
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        create_table_query = """CREATE TABLE IF NOT EXISTS orders (
order_id bigserial,
id varchar(255) NOT NULL,
channel	varchar(255) NOT NULL,
orgid integer NOT NULL,
orderId	varchar(255),
typeSellerSKU varchar(255),
orderDate TIMESTAMPTZ,
sku varchar(255),
quantityShipped	integer,
returnQuantity	integer,
orderTotal real,
productID varchar(255),
orderStatus varchar(255),
fsOrderStatus varchar(255),
fsReturnStatus varchar(255),
fsReturnShipmentStatus varchar(255),
fulfillmentChannel varchar(255),
fulfillmentCenterId varchar(255),
profitFulfillmentChannel varchar(255),
subCategory varchar(255),
tier varchar(255),
shipDistrict varchar(255),
shipState varchar(255),
shipPostalCode integer,
fwdTrackingId varchar(255),
isPickedUp boolean,
inYourHand boolean,
isReturnSolved boolean,
productDetails jsonb,
isTechnologyFee	boolean,
isReplacement boolean,
isClaim	boolean,
claimReceived boolean,
settlementDetails jsonb,
bankSettleAmount real,
commisionRate real,
commisionVar real,
shippingVar real,
fbaWeightBaseFeeVar real,
technologyFeeVar real,
fixedCloseFeeVar real,
fixedFeeVar real,
collectionVar real,
pickAndPackFeeVar real,
refundCommisionVar real,
refundFeeVar real,
refundCheckVar real,
bankVariance real,
invoicePrice jsonb,
promotionFees jsonb,
marketplaceFees jsonb,
shippingFees jsonb,
tcs jsonb,
returnFees jsonb,
settledAmount real,
adsSpent real,
wtInvoice real,
invOut real,
returnOut real,
costPerUnit real,
costPerUnitTax real,
estimation jsonb,
returnDate TIMESTAMPTZ,
returnReason varchar(255),
returnSubReason	varchar(255),
returnType varchar(255),
returnSubType varchar(255),
returnTrackingId varchar(255),
AdsReportLastSyncAt TIMESTAMPTZ,
adsSpentSBCampaigns real,
adsSpentSBCampaignsVideo real,
adsSpentSDProductAdsT00020 real,
adsSpentSDProductAdsT00030 real,
adsSpentSPProductAds real,
sellerAdsSpentSBCampaigns real,
sellerAdsSpentSBCampaignsVideo	real,
sellerAdsSpentSDProductAdsT00020 real,
sellerAdsSpentSDProductAdsT00030 real,
sellerAdsSpentSPProductAds real,
vendorAdsSpentSBCampaigns real,
vendorAdsSpentSBCampaignsVideo real,
vendorAdsSpentSDProductAdsT00020 real,
vendorAdsSpentSDProductAdsT00030 real,
vendorAdsSpentSPProductAds real,
automatedEmailCampaign boolean,
emailCampaignDetails integer,
bankDetails jsonb,
buyerNames text[],
AFShipmentsReportLastSyncAt TIMESTAMPTZ,
CancellationsDashboardLastSyncAt TIMESTAMPTZ,
dashboardCancellationsReportLastSyncAt TIMESTAMPTZ,
settledmentIds	text[],
dashboardCancelledOrdersLastSyncAt TIMESTAMPTZ,
DashboardOrdersReportLastSyncAt TIMESTAMPTZ,
dashboardPenaltiesLastSyncAt TIMESTAMPTZ,
dashboardPendingOrdersLastSyncAt TIMESTAMPTZ,
dashboardPickupsReportLastSyncAt TIMESTAMPTZ,
dashboardReadyToShipOrdersLastSyncAt TIMESTAMPTZ,
dashboardReturnsReportLastSyncAt TIMESTAMPTZ,
updatedAt TIMESTAMPTZ,
dashboardShippedOrdersLastSyncAt TIMESTAMPTZ,
easyshipPickedUpReportLastSyncAt TIMESTAMPTZ,
FBACSSReportLastSyncAt TIMESTAMPTZ,
FBAReturnsReportLastSyncAt TIMESTAMPTZ,
financeEventsLastSyncAt	TIMESTAMPTZ,
reconcillation	jsonb,
isOrderApiSynced boolean,
isTransactionApiSynced boolean,
isShopsy boolean,
TaxDetails jsonb,
MFNPReturnsReportLastSyncAt TIMESTAMPTZ,
ordersAPILastSyncAt TIMESTAMPTZ,
OrdersByOrdersLastSyncAt TIMESTAMPTZ,
OrdersDashboardLastSyncAt TIMESTAMPTZ,
ordersGeneralReportLastSyncAt TIMESTAMPTZ,
isReplacementUpdatedAt TIMESTAMPTZ,
ordersReportLastSyncAt TIMESTAMPTZ,
returnsDashboardLastSyncAt TIMESTAMPTZ,
catalogId varchar(255),
ReturnsLastSyncAt TIMESTAMPTZ,
returnsReportLastSyncAt	TIMESTAMPTZ,
sellerFlexFileLastSyncAt TIMESTAMPTZ,
ShipmentsLastSyncAt TIMESTAMPTZ,
syncDashboardOrdersByOrders TIMESTAMPTZ,
TransactionsByOrdersLastSyncAt	TIMESTAMPTZ,
TransactionsReportLastSyncAt TIMESTAMPTZ,
ordersAPIByOrdersLastSyncAt TIMESTAMPTZ,
transactionsAPIByOrdersLastSyncAt TIMESTAMPTZ,
shipCountry varchar(255),
itemPrice real,
itemTax real,
shipServiceLevel varchar(255),
fulfilledBy varchar(255),
asin varchar(255),
isSelfShip boolean,
stepProgram varchar(255),
amazonIXDStatus varchar(255),
deliveryDate TIMESTAMPTZ,
buyerEmail varchar(255),
merchantListingsReportLastSyncAt TIMESTAMPTZ,
wtMarket varchar(255),
marketIn varchar(255),
financialEventsAPILastSyncAt TIMESTAMPTZ,
refundDate TIMESTAMPTZ,
wtShipping varchar(255),
shippingIn varchar(255),
wtReturn varchar(255),
returnIn varchar(255),
smartOrdersReportLastSyncAt TIMESTAMPTZ,
orderCancellationCharge real,
orderCancellationChargeGst real,
profitQuantityShipped integer,
profitReturnQuantity integer,
profitRevenue real,
profitSettledAmount real,
profitCogs real,
profitCostUnit real,
profitAdSpend real,
profitTcsClaim real,
profitShippingFees real,
profitFees real,
profitReturnPrice real,
profitClaimReceived real,
profitOrderCancellationCharge real,
profitSaleGst real,
profitReturnGst real,
profitShipGst real,
profitFeesGst real,
profitAdsGst real,
profitGstOut real,
profitGstIn real,
orderItemId varchar(255),
shipCity varchar(255),
buyerName varchar(255),
deliveryAddress jsonb,
trackingId varchar(255),
courier varchar(255),
deliveredDate TIMESTAMPTZ,
isDelivered boolean,
shippedDate TIMESTAMPTZ,
pickupTrackingId varchar(255),
fwdCourier varchar(255),
cancellationDate TIMESTAMPTZ,
returnDetails jsonb,
returnApprovedDate TIMESTAMPTZ,
settledDate TIMESTAMPTZ,
shipmentDate TIMESTAMPTZ,
fulfillmentType varchar(255),
flipkartDiscount real,
serviceProfile varchar(255),
returnId varchar(255),
revTrackingId varchar(255),
revCourier varchar(255),
settlementids text[],
replacedquantity integer,
returnMarketFees real,
returnTcsFees real,
claimsReceived real,
returnCarrier varchar(255),
trackingLink varchar(255),
promotionsnotincommissionamount real,
promotionsincommissionamount real,
sellerpostalcode integer,
isprime boolean,
ordertype varchar(255),
packageDimensions jsonb,
UNIQUE(orgid,id),
constraint orderid_id unique(orgid,id)
)
        """
        cursor.execute(create_table_query)
        conn.commit()

        print('order table created!')

    except psycopg2.Error as error:
        print('An error occurs in a table',error)
    finally:
        cursor.close()
        conn.close()

create_orders_table()