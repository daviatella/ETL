import psycopg
import pandas as pd


conn = psycopg.connect(dbname = "postgres", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "admin",
                        port = 5432)
query = "SELECT  * FROM life_expect_data WHERE country='Brazil';"
df = pd.read_sql(query, conn)
df.sort_values(by=['year'], ascending=False)
df = df[::-1]
df.reset_index(inplace=True, drop=True)
print(df)
porcent_inc = list()
for i in df.index:
    if i != 0:
        porcent_inc.append(round(100*(df['lifeexpectancy'][i] - df['lifeexpectancy'][i-1])/df['lifeexpectancy'][i-1],2))
    else:
        porcent_inc.append(0)

print(porcent_inc)
df["porcentincrease"] = porcent_inc
print(df)
conn.commit()
conn.close()

#inserir no cloud