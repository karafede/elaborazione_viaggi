
import psycopg2
import db_connect
from sklearn.metrics import silhouette_score
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
import math
import pandas as pd
import csv
import datetime

__autore__ = "Massimo Mancini & Federico Karagulian"
__versione__ = "$Revision: 1.0 $"
__data__ = "$Date: 2019/11/06 $"


#Connect to an existing database
conn=db_connect.connect_Octo2013()
cur = conn.cursor()

# erase existing table
cur.execute("DROP TABLE IF EXISTS DatiRawOrigine01 CASCADE")
cur.execute("DROP TABLE IF EXISTS ElencoTerm01 CASCADE")
conn.commit()

# create input data TABLE with ID_raw generated from the "prepareRaw.py" script

##############################
### CreaDatiRawOrigine.sql ###
##############################

cur.execute("""
     CREATE  TABLE DatiRawOrigine01(
	 Id_record_raw integer,
	 Id_terminale integer,
	 DataOra timestamp,
	 LonWGS84 numeric,
	 LatWGS84 numeric,
	 Velocita integer,
	 Direzione integer,
	 Qualita integer,
	 Pannello integer,
	 Distanza integer,
	 Strada text)
     """)

conn.commit()

# Creato indice non clustered su campo Id_terminale:

cur.execute("""
    CREATE INDEX IX_DatiRawOrigine01 
    ON DatiRawOrigine01(Id_terminale)
    """)

conn.commit()

###################################
## BulkInsertDatiRawOrigine.sql ###
###################################
# Con BulkInsertDatiRawOrigine.sql sono state popolate le 12 tabelle DatiRawOrigine<mm> using
# the input files ("DatiRaw01_1-10000")

cur.execute("""
COPY DatiRawOrigine01
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\OctoPerFederico\DatiRaw01_1-10000.csv' DELIMITER ';' CSV HEADER
    """)

conn.commit()

# Per ogni mese contiamo il numero dei terminali presenti con la query and we create the table
# for each month let's count the number of terminal while creating a new table ElencoTerm01

# elenco terminali (auto)
cur.execute("""SELECT DISTINCT Id_terminale
               INTO ElencoTerm01
               FROM DatiRawOrigine01
               ORDER BY Id_terminale
            """)
conn.commit()

# Ora mettiamo tutti i terminali in una unica tabella relativa all’intero anno 2013 mediante la query:
# let's put all the terminals in an unique tables for the all year 2013

# cur.execute("""
# SELECT *
# INTO ElencoTermTemp
# FROM  ElencoTerm01
# UNION
# SELECT *
# FROM  ElencoTerm01
# UNION
# SELECT *
# FROM  ElencoTerm03
# UNION
# SELECT *
# FROM  ElencoTerm04
# UNION
# SELECT *
# FROM  ElencoTerm05
# UNION
# SELECT *
# FROM  ElencoTerm06
# UNION
# SELECT *
# FROM  ElencoTerm07
# UNION
# SELECT *
# FROM  ElencoTerm08
# UNION
# SELECT *
# FROM  ElencoTerm09
# UNION
# SELECT *
# FROM  ElencoTerm10
# UNION
# SELECT *
# FROM  ElencoTerm11
# UNION
# SELECT *
# FROM  ElencoTerm12
# """)


#Connect to an existing database
conn=db_connect.connect_Octo2013()
cur = conn.cursor()

# Sostituiamo la tabella con una nuova tabella ordinata in cui compare anche il numero d’ordine del terminale

cur.execute("""
ALTER TABLE ElencoTerm01 
ADD COLUMN nTerm SERIAL PRIMARY KEY
""")

conn.commit()

# Nella tabella ottenuta i records non risultano ordinati:
# Per ottenere una tabella fisicamente ordinate creiamo un indice Clustered sul campo nTerm:

cur.execute("""
CREATE INDEX ix_term
 ON ElencoTerm01(nTerm)
""")
conn.commit()
cur.execute(""" CLUSTER VERBOSE ElencoTerm01 USING ix_term;""")
conn.commit()


# Per ogni gruppo creiamo una tabella dove per ogni terminale calcoliamo il numero totale
# di acquisizioni e  la distanza complessiva annua:

cur.execute("""
SELECT Id_terminale, count(*) as nPuntiAnno, sum(Distanza) as DistAnno
INTO TermDistanzeRawAnno
FROM DatiRawOrigine01
GROUP BY Id_terminale
ORDER BY Id_terminale
""")
conn.commit()

# disconnect from the server
conn.close()
cur.close()



