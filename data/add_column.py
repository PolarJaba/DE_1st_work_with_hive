import pandas as pd

df_p = pd.read_csv('raw_data/people.csv')
df_p['v'] = df_p['User Id'].apply(lambda x: int(x, 16))
df_p['cust_group'] = pd.qcut(df_p['v'], q=10, labels=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])
df_p.drop(columns=['v'], axis=1, inplace=True)
df_p.to_csv('people_groups.csv', index=False, sep=';')

df_o = pd.read_csv('raw_data/organizations.csv')
df_o['v'] = df_o['Organization Id'].apply(lambda x: int(x, 16))
df_o['cust_group'] = pd.qcut(df_o['v'], q=10, labels=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])
df_o.drop(columns=['v'], axis=1, inplace=True)
df_o.to_csv('orgs_groups.csv', index=False, sep=';')

df_c = pd.read_csv('raw_data/customers.csv')
df_c['Subscription year'] = df_c['Subscription Date'].astype('datetime64[ns]').dt.to_period('Y')
df_c['v'] = df_c['Customer Id'].apply(lambda x: int(x, 16))
df_c['cust_group'] = pd.qcut(df_c['v'], q=10, labels=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10'])
df_c.drop(columns=['v'], axis=1, inplace=True)
df_c.to_csv('customers_groups.csv', index=False, sep=';')

df_g = pd.DataFrame([[0, 18, '0-18'],
                    [19, 25, '19-25'],
                    [26, 35, '26-35'],
                    [36, 45, '36-45'],
                    [46, 55, '46-55'],
                    [56, 65, '56-65'],
                    [66, 200, '66+']])
df_g.to_csv('age_groups.csv', index=False, sep=';')









