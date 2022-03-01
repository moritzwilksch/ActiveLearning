#%%
from django.db import connection
import pandas as pd
import sqlite3

conn = sqlite3.connect("db/label-studio-db-stuff/label_studio.sqlite3")

df = pd.read_sql("SELECT * FROM task;", con=conn)
print(df)
