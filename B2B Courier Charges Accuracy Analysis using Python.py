#!/usr/bin/env python
# coding: utf-8

# # B2B Courier Charges Accuracy Analysis using Python

# Data Cleaning & Manipulation

# DataSet: https://statso.io/b2b-ecommerce-fraud-case-study/
# 

# Let’s start this task by importing the necessary Python libraries and the dataset:

# In[1]:


import pandas as pd


# In[2]:


order_report = pd.read_csv('C:\\Users\\BHS7BAN\\Desktop\\Python\\Order Report.csv')
sku_master = pd.read_csv('C:\\Users\\BHS7BAN\\Desktop\\Python\\SKU Master.csv')
pincode_mapping = pd.read_csv('C:\\Users\\BHS7BAN\\Desktop\\Python\\pincodes.csv')
courier_invoice = pd.read_csv('C:\\Users\\BHS7BAN\\Desktop\\Python\\Invoice.csv')
courier_company_rates = pd.read_csv('C:\\Users\\BHS7BAN\\Desktop\\Python\\Courier Company - Rates.csv')


# In[3]:


print("Order Report:")
print(order_report.head(3))
print("\nSKU Master:")
print(sku_master.head(3))
print("\nPincode Mapping:")
print(pincode_mapping.head(3))
print("\nCourier Invoice:")
print(courier_invoice.head(3))
print("\nCourier Company rates:")
print(courier_company_rates.head(3))


# Now let’s have a look if any of the data contains missing values:

# In[14]:


print("\nMissing values in Website Order Report:")
order_report.isnull().sum()


# In[15]:


print("\nMissing values in SKU Master:")
sku_master.isnull().sum()


# In[16]:


print("\nMissing values in Pincode Mapping:")
pincode_mapping.isnull().sum()


# In[17]:


print("\nMissing values in Courier Invoice:")
courier_invoice.isnull().sum()


# In[18]:


print("\nMissing values in courier company rates:")
courier_company_rates.isnull().sum()


# Now let’s clean the data:

# In[6]:


# Remove unnamed columns from the Website Order Report DataFrame
order_report = order_report.drop(columns=['Unnamed: 3', 'Unnamed: 4'])

# Remove unnamed columns from the SKU Master DataFrame
sku_master = sku_master.drop(columns=['Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4'])

# Remove unnamed columns from the Pincode Mapping DataFrame
pincode_mapping = pincode_mapping.drop(columns=['Unnamed: 3', 'Unnamed: 4'])


# Now let’s merge the order report and SKU master datasets according to the common SKU column:

# In[11]:


# Merge the Order Report and SKU Master based on SKU
merged_data = pd.merge(order_report, sku_master, on='SKU')
merged_data.head()


# The ‘ExternOrderNo’ is nothing but ‘Order Id’ in other datasets. Let’s rename this column:

# In[39]:


# Rename the "ExternOrderNo" column to "Order ID" in the merged_data DataFrame
merged_data = merged_data.rename(columns={'ExternOrderNo': 'Order ID'})


# In[40]:


merged_data.head()  # main table


# Now let’s merge the courier invoice and pincode mapping dataset:

# In[41]:


#We first extract the unique customer pin codes from the courier_invoice dataset and 
#create a new DataFrame called “abc_courier” to store this information
abc_courier = pincode_mapping.drop_duplicates(subset=['Customer Pincode'])
abc_courier


# In[42]:


#We then select specific columns (“Order ID”, “Customer Pincode”, “Type of Shipment”) 
#from the courier_invoice dataset and create a new DataFrame called “courier_abc” to store this subset of data.
courier_abc= courier_invoice[['Order ID', 'Customer Pincode','Type of Shipment']]
courier_abc.head()


# In[43]:


#We then merge the ‘courier_abc’ DataFrame with the ‘abc_courier’ DataFrame 
#based on the ‘Customer Pincode’ column. 
#This merge operation helps us associate customer pin codes with their respective orders and shipping types. 
#The resulting DataFrame is named ‘pincodes’.
pincodes= courier_abc.merge(abc_courier,on='Customer Pincode')
pincodes


# 
# Now let’s merge the pin codes with the main dataframe:

# In[46]:


merged2 = merged_data.merge(pincodes, on='Order ID')
merged2.head()
#now merged2 is the main table


# Now let’s calculate the weight in kilograms by dividing the ‘Weight (g)’ column in the ‘merged2’ DataFrame by 1000:

# In[47]:


merged2['Weights (Kgs)'] = merged2['Weight (g)'] / 1000


# In[48]:


merged2.head()


# Now let’s calculate the weight slabs:

# In[53]:


def weight_slab(weight):
    i = round(weight % 1, 1)
    if i == 0.0:
        return weight
    elif i > 0.5:
        return int(weight) + 1.0
    else:
        return int(weight) + 0.5


# The function first calculates the remainder of the weight divided by 1 and rounds it to one decimal place. 
# - If the remainder is 0.0, it means the weight is a multiple of 1 KG, and the function returns the weight as it is.
# - If the remainder is greater than 0.5, it means that the weight exceeds the next half KG slab. In this case, the function rounds the weight to the nearest integer and adds 1.0 to it, which represents the next heavier slab.
# - If the remainder is less than or equal to 0.5, it means the weight falls into the current half-KG bracket. In this case, the function rounds the weight to the nearest integer and adds 0.5 to it, which represents the current weight slab.

# In[54]:


merged2['Weight Slab (KG)'] = merged2['Weights (Kgs)'].apply(weight_slab)
merged2


# In[52]:


courier_invoice['Weight Slab Charged by Courier Company']=(courier_invoice['Charged Weight']).apply(weight_slab)
courier_invoice


# Now let’s rename the columns to prepare the desired dataframe:

# In[55]:


courier_invoice = courier_invoice.rename(columns={'Zone': 'Delivery Zone Charged by Courier Company'})
merged2 = merged2.rename(columns={'Zone': 'Delivery Zone As Per ABC'})
merged2 = merged2.rename(columns={'Weight Slab (KG)': 'Weight Slab As Per ABC'})


# Now let’s calculate the expected charges:

# In[57]:


total_expected_charge = []

for _, row in merged2.iterrows():
    fwd_category = 'fwd_' + row['Delivery Zone As Per ABC']
    fwd_fixed = courier_company_rates.at[0, fwd_category + '_fixed']
    fwd_additional = courier_company_rates.at[0, fwd_category + '_additional']
    rto_category = 'rto_' + row['Delivery Zone As Per ABC']
    rto_fixed = courier_company_rates.at[0, rto_category + '_fixed']
    rto_additional = courier_company_rates.at[0, rto_category + '_additional']

    weight_slab = row['Weight Slab As Per ABC']

    if row['Type of Shipment'] == 'Forward charges':
        additional_weight = max(0, (weight_slab - 0.5) / 0.5)
        total_expected_charge.append(fwd_fixed + additional_weight * fwd_additional)
    elif row['Type of Shipment'] == 'Forward and RTO charges':
        additional_weight = max(0, (weight_slab - 0.5) / 0.5)
        total_expected_charge.append(fwd_fixed + additional_weight * (fwd_additional + rto_additional))
    else:
        total_expected_charge.append(0)


# In this code, we loop through each row of the ‘merged2’ DataFrame to calculate the expected charges based on ABC’s tariffs. 
# We retrieve the necessary rates and parameters, such as fixed charges and surcharges per weight tier for forward and RTO shipments, based on the delivery area.
# We then determine the weight of the slab for each row. 
# If the shipment type is ‘Forward Charges’, we calculate the additional weight beyond the basic weight slab (0.5 KG) and apply the corresponding additional charges. For “Forward and RTO Charges” shipments, we consider additional charges for term and RTO components.
# Finally, we store the calculated expected charges in the “Expected charges according to ABC” column of the “merged2” DataFrame. This allows us to compare the expected charges with the charges billed to analyze the accuracy of the courier company’s charges.

# In[58]:


merged2['Expected Charge as per ABC'] = total_expected_charge
merged2.head()


# Now let’s merge it with the courier invoice to display the final dataframe:

# In[59]:


merged_output = merged2.merge(courier_invoice, on='Order ID')
merged_output.head()


# Now let’s calculate the differences in charges and expected charges for each order:

# In[60]:


df_diff = merged_output
df_diff['Difference (Rs.)'] = df_diff['Billing Amount (Rs.)'] - df_diff['Expected Charge as per ABC']

df_new = df_diff[['Order ID', 'Difference (Rs.)', 'Expected Charge as per ABC']]

df_new.head()


# Now let’s summarize the accuracy of B2B courier charges based on the charged prices and expected prices:

# In[61]:


# Calculate the total orders in each category
total_correctly_charged = len(df_new[df_new['Difference (Rs.)'] == 0])
total_overcharged = len(df_new[df_new['Difference (Rs.)'] > 0])
total_undercharged = len(df_new[df_new['Difference (Rs.)'] < 0])


# In[62]:


# Calculate the total amount in each category
amount_overcharged = abs(df_new[df_new['Difference (Rs.)'] > 0]['Difference (Rs.)'].sum())
amount_undercharged = df_new[df_new['Difference (Rs.)'] < 0]['Difference (Rs.)'].sum()
amount_correctly_charged = df_new[df_new['Difference (Rs.)'] == 0]['Expected Charge as per ABC'].sum()


# In[63]:


# Create a new DataFrame for the summary
summary_data = {'Description': ['Total Orders where ABC has been correctly charged',
                                'Total Orders where ABC has been overcharged',
                                'Total Orders where ABC has been undercharged'],
                'Count': [total_correctly_charged, total_overcharged, total_undercharged],
                'Amount (Rs.)': [amount_correctly_charged, amount_overcharged, amount_undercharged]}


# In[64]:


df_summary = pd.DataFrame(summary_data)

df_summary


# We can also visualize the proportion of errors as shown below:

# In[67]:


import plotly.graph_objects as go
fig = go.Figure(data=go.Pie(labels=df_summary['Description'],
                            values=df_summary['Count'],
                            textinfo='label+percent',
                            hole=0.6))
fig.update_layout(title='Proportion')

fig.show()


# In[ ]:




