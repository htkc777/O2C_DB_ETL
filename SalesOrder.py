# Databricks notebook source
#! pip install pandas

# COMMAND ----------

#! pip install pandas-profiling

# COMMAND ----------

#from pandas_profiling import ProfileReport

# COMMAND ----------

#! pip install --upgrade pip

# COMMAND ----------

# Read Sales Header file from local
#df_SalesHeader = pd.read_csv("C:\\Users\\15447\\Documents\\O2C Data\\O2C QDI Data\\Sales Header.csv", encoding='latin1')

# COMMAND ----------

spark.conf.set(
"fs.azure.account.key.o2cstorage.dfs.core.windows.net",
""
)

# COMMAND ----------

dbutils.fs.mount(source= 'wasbs://rawdata@o2cstorage.blob.core.windows.net',
                mount_point = '/mnt/blobstorage',
                extra_configs ={'fs.azure.account.key.o2cstorage.blob.core.windows.net':'NmbEymyyVC2iSFf4D0m4kyJwFdVp6nt0Iim3iEXXDUQrP3n/jiLSDivexZ1Spi9zIqbMSi+LwIqF+AStaEKsLw=='})

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstorage')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/blobstorage/Sales Header.csv'

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df_SalesHeader = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load("dbfs:/mnt/blobstorage/Sales Header.csv")

# COMMAND ----------

df_SalesLine = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load("dbfs:/mnt/blobstorage/Sales Line.csv")

# COMMAND ----------

df_SalesLine_main = df_SalesLine.toPandas()

# COMMAND ----------

df_SalesHeader_main = df_SalesHeader.toPandas()

# COMMAND ----------

df_SalesHeader_main.info()

# COMMAND ----------

# Replace null values with None
df_SalesHeader_main['State'] = df_SalesHeader_main['State'].where(pd.notnull(df_SalesHeader_main['State']), None)

# COMMAND ----------

df_SalesHeader_main['Responsibility Center'] = df_SalesHeader_main['Responsibility Center'].where(pd.notnull(df_SalesHeader_main['Responsibility Center']), None)

# COMMAND ----------

# Rename column name 
df_SalesHeader_main.rename(columns = {'No_':'Document_No'}, inplace = True)

# COMMAND ----------

df_SalesHeader_main["Order Date"] = pd.to_datetime(df_SalesHeader_main["Order Date"])

# COMMAND ----------

df_SalesHeader_main["Posting Date"] = pd.to_datetime(df_SalesHeader_main["Posting Date"])

# COMMAND ----------

df_SalesHeader_main["Shipment Date"] = pd.to_datetime(df_SalesHeader_main["Shipment Date"])

# COMMAND ----------

df_SalesHeader_main["Promised Delivery Date"] = pd.to_datetime(df_SalesHeader_main["Promised Delivery Date"])

# COMMAND ----------

df_SalesHeader_main

# COMMAND ----------

df_SalesHeader_main.info()

# COMMAND ----------

# df_SalesLine = pd.read_csv("C:\\Users\\15447\\Documents\\O2C Data\\O2C QDI Data\\Sales Line.csv", encoding='latin1')

# COMMAND ----------

df_SalesLine_main.info()

# COMMAND ----------

df_SalesLine_main.rename(columns = {'Document No_':'Document_No'}, inplace = True)

# COMMAND ----------

df_SalesLine_main.drop(['Planned Delivery Date','Planned Shipment Date'], axis=1,inplace = True)

# COMMAND ----------

df_SalesLine_main

# COMMAND ----------

df_SalesLine_main.info()

# COMMAND ----------

Left_Join_SL_SH = df_SalesLine_main.merge(df_SalesHeader_main, on='Document_No', how='left')

# COMMAND ----------

Left_Join_SL_SH

# COMMAND ----------

pd.set_option('display.max_columns',None)

# COMMAND ----------

pd.set_option('display.max_rows',None)

# COMMAND ----------

Left_Join_SL_SH['Responsibility Center'] = Left_Join_SL_SH['Responsibility Center'].where(pd.notnull(Left_Join_SL_SH['Responsibility Center']), None)

# COMMAND ----------

Left_Join_SL_SH.drop(['Requested Delivery Date'], axis=1,inplace = True)

# COMMAND ----------

Left_Join_SL_SH.drop(['Document Type_x'], axis=1,inplace = True)

# COMMAND ----------

# Rename Column name
Left_Join_SL_SH.rename(columns = {'Document Type_y':'Document_Type',
'Line No_':'Line_No',
'No_':'No',
'Amount':'Sales_Amount',
'Location Code':'Location_Code',
'Outstanding Quantity':'Outstanding_Quantity',
'Qty_ to Invoice': 'Qty_To_Invoice',
'Qty_ to Ship': 'Qty_To_Ship',
'Unit Price': 'Unit_Price',
'Unit Cost (LCY)':'Unit_Cost_LCY',
'Line Discount _': 'Line_Discount_%',
'Line Discount Amount':'Line_Discount_Amount',
'Profit _':'Profit_%',
'Outstanding Amount (LCY)':'Outstanding_Amount_LCY',
'Shipped Not Invoiced (LCY)':'Shipped_Not_Invoiced_LCY',
'Line Amount':'Line_Amount',
'Sell-to Customer No_':'Sell_To_Customer_No',
'Ship-to Name':'Ship_To_Name',
'Ship-to City':'Ship_To_City',
'Order Date':'Order_Date',
'Posting Date':'Posting_Date',
'Shipment Date':'Shipment_Date',
'Payment Terms Code':'Payment_Terms_Code',
'Ship':'Ship_Info',
'Invoice':'Invoice_Info',
'Promised Delivery Date':'Promised_Delivery_Date',
'Sell-to Customer Name':'Sell_To_Customer_Name',
'Currency Factor':'Currency_Factor',
'Salesperson Code':'Salesperson_Code',
'Responsibility Center':'Responsibility_Center'}, inplace = True)



# COMMAND ----------

Left_Join_SL_SH['Type'] = Left_Join_SL_SH['Type'].astype('str')

# COMMAND ----------

Left_Join_SL_SH.info()

# COMMAND ----------

# def condition(type, no):
#     if '2' in type:
#         return no
#     else:
#         return None
#if condition for link item
Left_Join_SL_SH['Link_Item'] = Left_Join_SL_SH.apply(lambda x: x['No'] if x['Type'] == '2' else None, axis=1)

# Left_Join_SL_SH['Link_Item'] = Left_Join_SL_SH.lookup(Left_Join_SL_SH['Type'] == '2', Left_Join_SL_SH['No'])

# COMMAND ----------

Left_Join_SL_SH['Pending_SO'] = Left_Join_SL_SH.apply(lambda x: x['No'] if x['Document_Type'] == 1 else None, axis=1)

# COMMAND ----------

Left_Join_SL_SH['Cost_Amount'] = Left_Join_SL_SH['Qty_To_Invoice'] * Left_Join_SL_SH['Unit_Cost_LCY']

# COMMAND ----------

#Filter
# Left_Join_SL_SH.loc[Left_Join_SL_SH['Document_No'] == 101011]

# COMMAND ----------

Left_Join_SL_SH['Flag'] = 'SalesOrder'

# COMMAND ----------

# Nested if condition
def Shipment_Flag(Shipment_Date,Promised_Delivery_Date):
    if Shipment_Date > Promised_Delivery_Date:
        return 'Late'
    elif Shipment_Date < Promised_Delivery_Date:
        return 'Early'
    elif Shipment_Date == Promised_Delivery_Date:
        return 'On_Time'

Left_Join_SL_SH['Shipment_Flag'] = Left_Join_SL_SH.apply(lambda x: Shipment_Flag(x['Shipment_Date'],x['Promised_Delivery_Date']), axis=1)


# COMMAND ----------

Left_Join_SL_SH.drop(['Ship_To_Name','Ship_To_City','Sell_To_Customer_Name','State'], axis=1,inplace = True)

# COMMAND ----------

Left_Join_SL_SH['Link_Customer'] = Left_Join_SL_SH.loc[: , 'Sell_To_Customer_No']

# COMMAND ----------

Left_Join_SL_SH.info()

# COMMAND ----------

#Left_Join_SL_SH.to_csv('C:\\Users\\15447\\Documents\\O2C Data\\O2C ETL\\Sales_Order.csv',index= False)

# COMMAND ----------

Sales_orderdf= spark.createDataFrame(Left_Join_SL_SH)

# COMMAND ----------

Server_name = "dummysaleserver.database.windows.net"
Database = "o2cdatabase"
Port = "1433"
user_name = "dummysaleserver"
Password = "Team@1234"
jdbc_Url = "jdbc:sqlserver://{0}:{1};database={2}".format(Server_name, Port,Database)
conProp = {
  "user" : user_name,
  "password" : Password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

Sales_orderdf.write.jdbc(url=jdbc_Url,table="Sales_Order",mode="overwrite",properties=conProp)
