
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
__data__ = "$Date: 2019/12/03 $"


#Connect to an existing database
conn=db_connect.connect_Octo2015()
cur = conn.cursor()

# erase existing table
cur.execute("DROP TABLE IF EXISTS DatiRawOrigine01 CASCADE")
cur.execute("DROP TABLE IF EXISTS ElencoTerm01 CASCADE")
cur.execute("DROP TABLE IF EXISTS TermDistanzeRawMese CASCADE")
cur.execute("DROP TABLE IF EXISTS GPSpointsDelta CASCADE")
cur.execute("DROP TABLE IF EXISTS nPuntiTerm CASCADE")
cur.execute("DROP TABLE IF EXISTS PuntiViaggio CASCADE")
cur.execute("DROP TABLE IF EXISTS Viaggi CASCADE")

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
	 LonWGS84 float,
	 LatWGS84 float,
	 Velocita integer,
	 Direzione integer,
	 Qualita integer,
	 Pannello integer,
	 Distanza integer)
     """)

# Strada text)
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

# load all data into the DatiRawOrigine table in the PostgreSQL server

cur.execute("""
COPY DatiRawOrigine01
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\DatiRaw_Octo_11_2015_20000001-31919514.csv' DELIMITER ';' CSV HEADER
    """)
conn.commit()

# FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\DatiRaw_Octo_11_2015_1-10000000.csv' DELIMITER ';'  CSV HEADER
# FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\DatiRaw_Octo_11_2015_10000001-20000000.csv' DELIMITER ';'  CSV HEADER
# FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\DatiRaw_Octo_11_2015_20000001-31919514.csv' DELIMITER ';'  CSV HEADER


# Contiamo il numero dei terminali presenti con la query and we create the table
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

#####################################################
#### connect to postgresql with pandas ##############
#####################################################

# grafico della distribuzione cumulativa dei codici dei terminali (Id_terminale)

ID_terminali_distr = pd.read_sql_query(
'''select *
from ElencoTerm01''', conn)

ID_terminali_distr.plot(x='id_terminale', y='nterm', marker='.',
                        figsize=(15,8), linewidth=5, fontsize=20)
plt.xlabel('id_terminale', fontsize=20)
plt.ylabel('numero terminali', fontsize=20)
plt.show()

# max number of terminals
print(max(ID_terminali_distr["nterm"]))


# Per ogni gruppo creiamo una tabella dove per ogni terminale calcoliamo il numero totale
# di acquisizioni e  la distanza complessiva per il mese di November 2015:

cur.execute("""
SELECT Id_terminale, count(*) as nPuntiMese, sum(Distanza) as DistMese
INTO TermDistanzeRawMese
FROM DatiRawOrigine01
GROUP BY Id_terminale
ORDER BY Id_terminale
""")
conn.commit()


conn=db_connect.connect_Octo2015()
cur = conn.cursor()


# Individuiamo i terminali che risultano guasti durante l’intero il mese con la query:

terminali_guasti = pd.read_sql_query(
''' SELECT Id_terminale,
      nPuntiMese,
      DistMese
    FROM TermDistanzeRawMese
    WHERE DistMese = 0''', conn)


#  contiamo i relativi records/terminali sbagliati/guasti:

count_records_guasti = pd.read_sql_query(
''' SELECT count (*)
    FROM DatiRawOrigine01
    WHERE Id_terminale in (SELECT Id_terminale FROM TermDistanzeRawMese
    WHERE DistMese = 0)''', conn)

# troviamo 18364 records guasti !!!
##### procedi con la procedura add.delta.py che controlla che la sequenza dei valori pannello sia corretta,
# corregge eventuali errori e calcola i valori delta tra acquisizioni consecutive.
# Calcola inoltre le coordinate metriche XUTM32, YUTM32 e l’ora locale DataOraLoc
# che tiene conto anche dell’ora legale

# copy table GPSTrackDelta.csv into the DB

#Connect to an existing database
conn=db_connect.connect_Octo2015()
cur = conn.cursor()

cur.execute("""
     CREATE  TABLE GPSpointsDelta(
	 Id_record_raw integer,
	 Id_terminale integer,
	 nPunto integer,
	 DataOraUTC timestamp,
	 DataOraLoc timestamp,
	 LonWGS84 float,
	 LatWGS84 float,
	 XUTM32 float,
	 YUTM32 float,
	 Velocita integer,
	 Direzione integer,
	 Qualita integer,
	 PanState integer,
	 VPanState integer,
	 TrckType char(2) ,
	 PanSeq char(2),
	 VPanSeq char(2),
	 Distanza integer,
	 DistEucl integer,
	 DeltaSec integer,
	 Vmedia integer,
	 DeltaDir integer)
     """)

# Strada text)
conn.commit()

# populate table

cur.execute("""
COPY GPSpointsDelta
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\GPSTrackDelta.csv' DELIMITER ';' CSV HEADER
    """)

conn.commit()


# VERIFICARE che VPanState del primo punto dovrebbe essere sempre pari a 0.
# C’è un errore nel programma AddDelta, infatti se effettuiamo le query seguenti troviamo i records errati:

df = pd.read_sql_query(
''' SELECT *
    FROM GPSpointsDelta
    WHERE nPunto = 1 and VPanState <> 0''', conn)



# Controlliamo ora l’ultimo punto dei terminali che deve avere VPanState = 2:
cur.execute(
    ''' SELECT Id_terminale, MAX(nPunto) AS nPunti 
        INTO nPuntiTerm
        FROM GPSpointsDelta
        GROUP BY Id_terminale
        ORDER BY Id_terminale
        ''')

conn.commit()


df = pd.read_sql_query(
    ''' SELECT  GPSpointsDelta.Id_record_raw, GPSpointsDelta.Id_terminale, GPSpointsDelta.nPunto, GPSpointsDelta.DataOraUTC, GPSpointsDelta.DataOraLoc, 
                           GPSpointsDelta.LonWGS84, GPSpointsDelta.LatWGS84, GPSpointsDelta.XUTM32, GPSpointsDelta.YUTM32, GPSpointsDelta.Velocita, GPSpointsDelta.Direzione, 
                           GPSpointsDelta.Qualita, GPSpointsDelta.PanState, GPSpointsDelta.VPanState, GPSpointsDelta.TrckType, GPSpointsDelta.PanSeq, GPSpointsDelta.VPanSeq, 
                           GPSpointsDelta.Distanza, GPSpointsDelta.DistEucl, GPSpointsDelta.DeltaSec, GPSpointsDelta.Vmedia, GPSpointsDelta.DeltaDir,
                           FROM GPSpointsDelta INNER JOIN nPuntiTerm ON GPSpointsDelta.Id_terminale = nPuntiTerm.Id_terminale AND GPSpointsDelta.nPunto = nPuntiTerm.nPunti
                           WHERE VPanState <> 2''', conn)

# disconnect from the server
conn.close()
cur.close()

######################################################

# Modificato programma ElaboraViaggi.py per associare alle origini e destinazioni  il codice Provincia
# (fuori provincia di Roma) od il codice Comune (entro la provincia di Roma) o l’appartenenza all’interno del GRA.
# La procedura è la seguente: si individua la provincia di appartenenza, se è Roma (58) si individua il comune di
# appartenenza, se è Roma (58091) si verifica l’appartenenza all’interno del GRA. Se il punto è interno al GRA si
# attribuisce il codice 58000. Sono stati utilizzati gli shapefiles: Com2011_WGS84geoProvRoma, GraWGS84geo,
# prov2011_WGS84geo.Lo shapefile Com2011_WGS84geoProvRoma e stato editato con QGis per eliminare i buchi corrisponenti
# alla Città del Vaticano ed a Castelgandolfo.

# Creare tabella PuntiViaggio:

conn=db_connect.connect_Octo2015()
cur = conn.cursor()

cur.execute("""
     CREATE  TABLE PuntiViaggio(        
	 Id_viaggio float,
	 Id_terminale integer,
	 Id_puntoTerm integer,
	 nViaggio integer,
	 nPuntoV integer, 
	 DataOraUTC timestamp,
	 DataOraLoc timestamp,
	 LonWGS84 float,
	 LatWGS84 float,
	 XUTM32 float,
	 YUTM32 float,
	 TrckType char(2),
	 Anomalia char(6), 
	 Velocita integer,
	 Direzione integer,
	 DeltaDir integer,
	 Qualita integer,
	 PanState integer,
	 VPanState integer,
	 VPanSeq char(2),
	 DistGps integer,
	 Distanza integer,
	 DistEucl integer,
	 Tortuosita float,
	 Progressiva integer,
	 DeltaSec integer,
	 Vmedia integer)
     """)

conn.commit()

# populate table

cur.execute("""
COPY PuntiViaggio
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\PuntiViaggio.csv' DELIMITER ';' CSV HEADER
    """)

conn.commit()

# create tabella Viaggi
cur.execute("""
     CREATE  TABLE Viaggi(        
	 Id_viaggio float,
	 Id_terminale integer,
	 nViaggio integer,
	 Id_puntoOrig integer,
	 Id_puntoDest integer,
	 O_PRO_COM integer,
	 D_PRO_COM integer,
	 OTrckType char(2),
	 DTrckType char(2),
	 ODataOra timestamp,
	 DDataOra timestamp,
	 Distanza integer,
	 DurataSec integer,
	 Vmedia float,
	 nPunti integer)
     """)

conn.commit()

# populate table

cur.execute("""
COPY Viaggi
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\Viaggi.csv' DELIMITER ';' CSV HEADER
    """)

conn.commit()


# create tabella Soste
cur.execute("""
     CREATE  TABLE Soste(  
     Id_sosta float,
     Id_terminale integer,
     nSosta integer,
     Id_viaggioPrec float,
     Id_puntoArrivo integer,
     Id_puntoPartenza integer,
     A_PRO_COM integer,
     P_PRO_COM integer,
     ATrckType char(2),
     PTrckType char(2),
     ADataOra timestamp,
     PDataOra timestamp,
     DistAP integer,
     DurataSec integer)
     """)

conn.commit()

# populate table

cur.execute("""
COPY Soste
FROM 'C:\ENEA_CAS_WORK\Mancini_stuff\Octo2015\extracted_files\Soste.csv' DELIMITER ';' CSV HEADER
    """)

conn.commit()

############################################################
# Build SOSTE NOTTE from queries in the DB ###
############################################################

'''
SELECT Id_sosta,
       Id_terminale,
       nSosta,
       Id_viaggioPrec,
       Id_puntoArrivo,
       Id_puntoPartenza,
       A_PRO_COM,
       P_PRO_COM,
       ATrckType,
       PTrckType,
       ADataOra,
       PDataOra,
       DistAP,
       DurataSec
  INTO SosteNotte
  FROM Soste
  WHERE (ATrckType = '2n' AND PTrckType = '0n') AND 
        (DISTAP <= 500) AND   /* Distanza Arrivo-Partenza minore o uguale a 500 metri */
        (DurataSec >= 18000) /* 5 ore */
        AND       
        (
			(DATE_PART('day', ADataOra::timestamp - PDataOra::timestamp) * 24 + 
			DATE_PART('hour', ADataOra::timestamp - PDataOra::timestamp)) >= 24
         )
  ORDER BY Id_terminale, nSosta
'''


cur.execute('''
SELECT Id_sosta,
       Id_terminale,
       nSosta,
       Id_viaggioPrec,
       Id_puntoArrivo,
       Id_puntoPartenza,
       A_PRO_COM,
       P_PRO_COM,
       ATrckType,
       PTrckType,
       ADataOra,
       PDataOra,
       DistAP,
       DurataSec
  INTO SosteNotte
  FROM Soste
  WHERE (ATrckType = '2n' AND PTrckType = '0n') AND 
        (DISTAP <= 500) AND   /* Distanza Arrivo-Partenza minore o uguale a 500 metri */
        (DurataSec >= 18000) /* 5 ore */
        AND       
        (
           (DATE_PART('day', ADataOra::timestamp - PDataOra::timestamp) * 24 + 
			DATE_PART('hour', ADataOra::timestamp - PDataOra::timestamp)) >= 24  /* Sosta superiore o uguale alle 24h */
          OR 
          /* Sosta a cavallo delle ore 3 di mattina (con durata di almeno 5 ore */
          (ADataOra < DATEADD(hh,3,cast(CONVERT(varchar(8), PDataOra, 112) AS datetime)) AND DATEPART(hour,PDataOra) >= 3)
        )
  ORDER BY Id_terminale, nSosta
  
  /* prove varie */
  SELECT mydate, TO_CHAR(mydate,'YYYY-MM-DD HH-MI-SS') AS mydate_text FROM mytable;
  
  SELECT TO_CHAR( NOW(),'YYYY-MM-DD HH-MI-SS' )
  
  
  ''')
# https://stackoverflow.com/questions/38379918/how-to-convert-to-datetime-yyyy-mm-dd-hhmiss







