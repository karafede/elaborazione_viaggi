"""assegnaClusters.py

Elaborazione dei dati relativi alle soste notturne
per il calcolo della posizione delle case tramite il
metodo Central Feature

Uso: python trovaCase.py [opzioni] [shapefile]

Opzioni:
  -h, --help              show this help
 
Esempio:
  trovaCase.py outFile.shp     genera lo shapefile outFile.shp 

"""

__autore__ = "Massimo Mancini"
__versione__ = "$Revision: 1.0 $"
__data__ = "$Date: 2011/04/15 $"

import sys, os
import getopt
import csv
import math
import time, datetime
import numpy as np
from scipy.spatial import distance
from sklearn.cluster import DBSCAN
from sklearn import metrics

class SosteNotteClusters(object):
    csvHeading= ["Id_terminale",
                 "Id_sosta",
                 "OraIni",
                 "DurataOre",
                 "Gsett",
                 "Id_cluster",
                 "Silhouette"]
    def __init__(self,
                 Id_terminale="",
                 Id_sosta="",
                 OraIni="",
                 DurataOre="",
                 Gsett="",
                 Id_cluster="",
                 Silhouette=""):
        self.Id_terminale=Id_terminale
        self.Id_sosta=Id_sosta
        self.OraIni=OraIni
        self.DurataOre=DurataOre
        self.Gsett=Gsett
        self.Id_cluster=Id_cluster        
        self.Silhouette=Silhouette
    def csvRow( self ):
        return [ self.Id_terminale,
        self.Id_sosta,
        self.OraIni,
        self.DurataOre,
        self.Gsett,
        self.Id_cluster,                 
        self.Silhouette]

class Home(object):
    csvHeading= ["Id_terminale",
                 "Id_sosta",
                 "Id_cluster",
                 "nSosteCluster",
                 "DurataTotCluster",
                 "PercUtilizzo",
                 "XHome",
                 "YHome",
                 "LatHome",
                 "LonHome",
                 "Strada",
                 "ProCom",
                 "NomeZona",
                 "XMean",
                 "YMean",
                 "DistCF_Mean",
                 "StdDist"]
    def __init__(self,
                 Id_terminale="",
                 Id_sosta="",
                 Id_cluster="",
                 nSosteCluster="",
                 DurataTotCluster="",
                 PercUtilizzo="",
                 XHome="",
                 YHome="",
                 LatHome="",
                 LonHome="",
                 Strada="",
                 ProCom="",
                 NomeZona="",
                 XMean="",
                 YMean="",                 
                 DistCF_Mean="",
                 StdDist=""):
        self.Id_terminale=Id_terminale
        self.Id_sosta=int(Id_sosta)
        self.Id_cluster=int(Id_cluster)
        self.nSosteCluster=nSosteCluster        
        self.DurataTotCluster=DurataTotCluster  
        self.PercUtilizzo=int(PercUtilizzo)
        self.XHome=XHome
        self.YHome=YHome
        self.LatHome=LatHome
        self.LonHome=LonHome
        self.Strada=Strada
        self.ProCom=ProCom
        self.NomeZona=NomeZona
        self.XMean=XMean
        self.YMean=YMean
        self.DistCF_Mean=DistCF_Mean        
        self.StdDist=StdDist
    def csvRow( self ):
        return [ self.Id_terminale,
        self.Id_sosta,
        self.Id_cluster,  
        self.nSosteCluster,                 
        self.DurataTotCluster,
        self.PercUtilizzo,
        self.XHome,
        self.YHome,
        self.LatHome,
        self.LonHome,
        self.Strada,
        self.ProCom,
        self.NomeZona,
        self.XMean,
        self.YMean,
        self.DistCF_Mean,
        self.StdDist]

def usage():
    print "for help use -h or --help"
    sys.exit(0)

def elapsed_time(seconds, suffixes=['y','w','d','h','m','s'], add_s=False, separator=' '):
	"""
	Takes an amount of seconds and turns it into a human-readable amount of time.
	"""
	# the formatted time string to be returned
	time = []
	
	# the pieces of time to iterate over (days, hours, minutes, etc)
	# - the first piece in each tuple is the suffix (d, h, w)
	# - the second piece is the length in seconds (a day is 60s * 60m * 24h)
	parts = [(suffixes[0], 60 * 60 * 24 * 7 * 52),
		  (suffixes[1], 60 * 60 * 24 * 7),
		  (suffixes[2], 60 * 60 * 24),
		  (suffixes[3], 60 * 60),
		  (suffixes[4], 60),
		  (suffixes[5], 1)]
	
	# for each time piece, grab the value and remaining seconds, and add it to
	# the time string
	for suffix, length in parts:
		value = seconds / length
		if value > 0:
			seconds = seconds % length
			time.append('%s%s' % (str(value),
					       (suffix, (suffix, suffix + 's')[value > 1])[add_s]))
		if seconds < 1:
			break
	
	return separator.join(time)
	

def DistanzaTraDuePunti(x1, y1, x2, y2): 
  dx = x2 - x1 
  dy = y2 - y1 
  DistQuadrata = dx**2 + dy**2 
  Risultato = math.sqrt(DistQuadrata) 
  return Risultato 
   
def process(daTerm, aTerm):
    # import moduli
    from arcpy import env
    import adodbapi, arcgisscripting 
    import locale
    from array import array 
 
    global lang, enc
    lang, enc = locale.getdefaultlocale()

    # Create the Geoprocesser
    gp = arcgisscripting.create()
    
    gp.OverWriteOutput = 1
    
    # Load required toolboxes...
    gp.AddToolbox("C:/Program Files (x86)/ArcGIS/Desktop10.0/ArcToolbox/Toolboxes/Spatial Statistics Tools.tbx")
    gp.AddToolbox("C:/Program Files (x86)/ArcGIS/Desktop10.0/ArcToolbox/Toolboxes/Analysis Tools.tbx")
    
    outFolder = r"N:\PHEV-V2G\Procedure\TrovaCase\shp"
    
    outFileCsv = r"N:\PHEV-V2G\Procedure\TrovaCase\csv\SosteNotteClusters"+daTerm+"-"+aTerm+".csv"
    outFileCaseCsv = r"N:\PHEV-V2G\Procedure\TrovaCase\csv\HomeLocations"+daTerm+"-"+aTerm+".csv"

    global csvWriter
    fcsv=open(outFileCsv, "w")
    csvWriter = csv.writer(fcsv, dialect='excel', delimiter=';', lineterminator='\n')
    csvWriter.writerow( SosteNotteClusters.csvHeading )

    global csvCaseWriter
    fCcsv=open(outFileCaseCsv, "w")
    csvCaseWriter = csv.writer(fCcsv, dialect='excel', delimiter=';', lineterminator='\n')
    csvCaseWriter.writerow( Home.csvHeading )
    
    gp.Workspace = outFolder
    gp.toolbox = "management"
    
    #Define Coordinate System
##    coordsys = "C:\\Program Files\\ArcGIS\\Coordinate Systems\\Geographic Coordinate Systems\\World\\WGS 1984.prj"
    coordsys = r"C:\Program Files (x86)\ArcGIS\Desktop10.0\Coordinate Systems\Projected Coordinate Systems\UTM\WGS 1984\Northern Hemisphere\WGS 1984 UTM Zone 32N.prj"

    #Create a Point object.
    pnt = gp.CreateObject("Point")
    
    # connection string for an SQL server
    _computername="127.0.0.1" #or name of computer with SQL Server
    _databasename="Octo2013G1"
    _tableTermName="ElencoTerminali"
    _tableSosteName= 'SostePerCercaCase'
    # this will open a MS-SQL table with Windows authentication
    connStr = r"Initial Catalog=%s; Data Source=%s; Provider=SQLOLEDB.1; Integrated Security=SSPI" \
             %(_databasename, _computername)
    #create the connection
    dbConn = adodbapi.connect(connStr)
    #make a cursor on the connection
    dbCur = dbConn.cursor()
    
    dbCur.execute("SELECT nTerm, Id_terminale FROM %s where (nTerm >= %s and nTerm <= %s) Order by Id_terminale" % (_tableTermName, daTerm, aTerm))    

    terminali = dbCur.fetchall()

    for terminale in terminali:

        print ""
        print terminale.nTerm," Term: ",terminale.Id_terminale
        
        dbCur.execute("SELECT * FROM %s WHERE Id_terminale = %d" % (_tableSosteName, terminale.Id_terminale))

        sosteNotte = dbCur.fetchall()

        nPunti = dbCur.rowcount
        print "nPunti:", nPunti

        if nPunti < 1:
            continue

        gp.OverWriteOutput = 1            
        snotteFile = str(terminale.Id_terminale)+"sosteNotteTerm"+".shp"
        gp.CreateFeatureclass(outFolder, snotteFile, "POINT")
        gp.defineprojection(snotteFile, coordsys)

        gp.AddField_management(snotteFile, "Id_term", "LONG", 10, 0)
        gp.AddField_management(snotteFile, "Id_sosta", "LONG", 10, 0)
        gp.AddField_management(snotteFile, "Lat", "DOUBLE", 9, 6)
        gp.AddField_management(snotteFile, "Lon", "DOUBLE", 9, 6)
        gp.AddField_management(snotteFile, "X", "LONG", 7, 0)
        gp.AddField_management(snotteFile, "Y", "LONG", 7, 0)
        gp.AddField_management(snotteFile, "AData", "TEXT", 8)
        gp.AddField_management(snotteFile, "AOra", "TEXT", 8)
        gp.AddField_management(snotteFile, "AGSett", "TEXT", 3)
        gp.AddField_management(snotteFile, "PData", "TEXT", 8)
        gp.AddField_management(snotteFile, "POra", "TEXT", 8)
        gp.AddField_management(snotteFile, "PGSett", "TEXT", 3)
        gp.AddField_management(snotteFile, "DurataOre", "FLOAT", 10, 2)
        gp.AddField_management(snotteFile, "Strada", "TEXT", 1, 0)
        gp.AddField_management(snotteFile, "ProCom", "LONG", 7, 0)
        gp.AddField_management(snotteFile, "NomeZona", "TEXT", 50, 10, 50)
        gp.AddField_management(snotteFile, "Cluster", "SHORT")

        #Open a cursor to insert rows into the shapefile.
        shpCur = gp.InsertCursor(snotteFile)
        
        XYdbscan = np.zeros((nPunti,4))  ## Array [n_samples, n_features]  x,y,durata,IdClust
        
        nSosta = 0            
        for sostaNotte in sosteNotte:

            nSosta = nSosta+1                
            
            pnt.x = sostaNotte.XUTM32
            pnt.y = sostaNotte.YUTM32

            #Insert the new point into the feature class.
            fcRow = shpCur.NewRow()
            fcRow.shape = pnt
            fcRow.Id = nSosta-1
            fcRow.Id_term = terminale.Id_terminale
            fcRow.Id_sosta = sostaNotte.Id_sosta
            fcRow.Lat = sostaNotte.LatWGS84
            fcRow.Lon = sostaNotte.LonWGS84
            fcRow.X = sostaNotte.XUTM32
            fcRow.Y = sostaNotte.YUTM32
            fcRow.AData = sostaNotte.A_Data
            fcRow.AOra = sostaNotte.A_Ora
            fcRow.AGSett = sostaNotte.A_GSett
            fcRow.PData = sostaNotte.P_Data
            fcRow.POra = sostaNotte.P_Ora
            fcRow.PGSett = sostaNotte.P_GSett
            fcRow.DurataOre = float(sostaNotte.DurataOre)
            fcRow.Strada = sostaNotte.Strada
            fcRow.ProCom = sostaNotte.PRO_COM
            fcRow.NomeZona = str(sostaNotte.NomeZona.strip().encode(enc))
            shpCur.InsertRow(fcRow)

            XYdbscan[nSosta-1][0] = sostaNotte.XUTM32
            XYdbscan[nSosta-1][1] = sostaNotte.YUTM32
            XYdbscan[nSosta-1][2] = sostaNotte.DurataOre

        ##############################################################################
        # Compute DBSCAN

        db = DBSCAN().fit(XYdbscan, eps=500.0, metric="euclidean", min_samples=3)
        core_samples = db.core_sample_indices_
        labels = db.labels_


        # Number of clusters in labels, ignoring noise if present.
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

        print 'N clusters: %d' % n_clusters_

        if n_clusters_ > 1:
##            print ("Silhouette Coefficient: %0.3f" %
##            metrics.silhouette_score(XYdbscan, labels, metric='euclidean'))
            SilhoSamples = metrics.cluster.silhouette_samples(XYdbscan, labels, metric='euclidean')
##            print ("Silhouette Samples: ", SilhoSamples)

        durateTotClusters = []
        
        nSosta = 0
        for sostaNotte in sosteNotte:
            nSosta = nSosta + 1
            if n_clusters_ > 1:
                if np.isnan(SilhoSamples[nSosta-1]):
                    Silhouette=None
                else:
                    Silhouette="%.4f" % SilhoSamples[nSosta-1]
            else:
                Silhouette=None
            sosta = SosteNotteClusters(Id_terminale=str(sostaNotte.Id_terminale),
                                       Id_sosta=str(sostaNotte.Id_sosta),
                                       OraIni=sostaNotte.A_Ora,
                                       DurataOre='%.2f' %sostaNotte.DurataOre,
                                       Gsett=sostaNotte.A_Gsett,
                                       Id_cluster="%d" %labels[nSosta-1],
                                       Silhouette=Silhouette
                                       )

            csvWriter.writerow(sosta.csvRow())
            XYdbscan[nSosta-1][3] = labels[nSosta-1]

        rows = gp.UpdateCursor(snotteFile)
        for row in rows:
            row.Cluster = labels[row.Id]
            rows.updateRow(row)

        del rows
        del row


##        if nPunti <= 2 or n_clusters_ == 0:
##            print "\n0 clusters"
##            print XYdbscan
#            continue
        
        # Process: Central Feature...
        print "Process: Central Feature..."
        gp.CentralFeature_stats(snotteFile, str(terminale.Id_terminale)+"CFterm"+".shp", "Euclidean Distance", "", "DurataOre", "Cluster")

        # Process: Standard Distance...
        print "Process: Standard Distance..."
        if nPunti >= 2:
            gp.StandardDistance_stats(snotteFile, str(terminale.Id_terminale)+"SDterm"+".shp", "1 Standard Deviation", "", "Cluster")
        else:
            print "nPunti <= 2 - non processo"

#        print labels
#        print XYdbscan
        DurataTotTerm = 0
        nTotSoste = 0
        for x in XYdbscan:
            if x[3] >= 0:
                DurataTotTerm = DurataTotTerm + x[2]
                nTotSoste = nTotSoste + 1
        for k in set(labels):
            print "k:", k
            class_members = [index[0] for index in np.argwhere(labels == k)]
            nSosteCluster = len(class_members)
            print "nSosteCluster:", nSosteCluster
            DurataTotCluster = 0
            for index in class_members:
                x = XYdbscan[index]
                DurataTotCluster = DurataTotCluster + x[2]

            if n_clusters_ > 0:
                percNSoste = nSosteCluster/float(nTotSoste)
                percDurata = DurataTotCluster/DurataTotTerm
                percMedia = (percNSoste+percDurata)/2.0
    #            print "perc:", '%.2f' %percNSoste, '%.2f' %percDurata, '%.2f' %percMedia

            rows = gp.SearchCursor(str(terminale.Id_terminale)+"CFterm"+".shp", "Cluster = %d" %k )
            row = rows.Next()
            pnt = row.shape.getpart()
            fcIdSosta = row.Id_sosta
            fcLat = row.Lat
            fcLon = row.Lon
            fcX = row.X
            fcY = row.Y
            fcStrada = row.Strada
            fcProCom = row.ProCom
            fcNomeZona = row.NomeZona

##            del row
##            del rows

            if nPunti >= 2:
                rows = gp.SearchCursor(str(terminale.Id_terminale)+"SDterm"+".shp", "Cluster = %d" %k )
                row = rows.Next()
                if row:
                    mcX = int(round(row.CenterX))
                    mcY = int(round(row.CenterY))
                    stdDist = int(round(row.StdDist))
                    dist = int(round(DistanzaTraDuePunti(fcX, fcY, mcX, mcY)))
                else:
                    mcX = None
                    mcY = None
                    stdDist = None
                    dist = None
            else:
                mcX = None
                mcY = None
                stdDist = None
                dist = None

            if k >= 0:
                home = Home(Id_terminale=terminale.Id_terminale,
                     Id_sosta=fcIdSosta,
                     Id_cluster=k,
                     nSosteCluster=nSosteCluster,
                     DurataTotCluster='%.2f' %DurataTotCluster,
                     PercUtilizzo='%.2f' %percMedia,
                     XHome=fcX,
                     YHome=fcY,
                     LatHome='%.6f' %fcLat,
                     LonHome='%.6f' %fcLon,
                     Strada=fcStrada,
                     ProCom=fcProCom,
                     NomeZona=fcNomeZona,
                     XMean=mcX,
                     YMean=mcY,                 
                     DistCF_Mean=dist,
                     StdDist=stdDist)
            else:
                if n_clusters_ == 0:
                    home = Home(Id_terminale=terminale.Id_terminale,
                         Id_sosta=fcIdSosta,
                         Id_cluster=k,
                         nSosteCluster=nSosteCluster,
                         DurataTotCluster='%.2f' %DurataTotCluster,
                         XHome=fcX,
                         YHome=fcY,
                         LatHome='%.6f' %fcLat,
                         LonHome='%.6f' %fcLon,
                         Strada=fcStrada,
                         ProCom=fcProCom,
                         NomeZona=fcNomeZona,
                         XMean=mcX,
                         YMean=mcY,                 
                         DistCF_Mean=dist,
                         StdDist=stdDist)
                else:
                    home = Home(Id_terminale=terminale.Id_terminale,
                         Id_sosta=fcIdSosta,
                         Id_cluster=k,
                         nSosteCluster=nSosteCluster,
                         DurataTotCluster='%.2f' %DurataTotCluster)

            csvCaseWriter.writerow(home.csvRow())
        
##        PlotResult(XYdbscan, n_clusters_, labels, core_samples)

        del row
        del rows
                
        del shpCur
        del sosteNotte
        
    fcsv.close()
    fCcsv.close()      
    dbCur.close()
    dbConn.close()
    del dbCur, dbConn

def PlotResult(XYdbscan, n_clusters_, labels, core_samples):    
    ##############################################################################
    # Plot result
    import pylab as pl
    from itertools import cycle

    pl.close('all')
    pl.figure(1)
    pl.clf()

    print "XYdbscan:", XYdbscan
    print "core_samples:", core_samples
    print "labels:", labels
    
    # Black removed and is used for noise instead.
    colors = cycle('bgrcmybgrcmybgrcmybgrcmy')
    for k, col in zip(set(labels), colors):
        print "k:", k
        print "argwhere:", np.argwhere(labels == k)
        for index in np.argwhere(labels == k):
            print index, "----------", index[0]
        if k == -1:
            # Black used for noise.
            col = 'k'
            markersize = 6
        class_members = [index[0] for index in np.argwhere(labels == k)]
        print "class_members:", class_members
        cluster_core_samples = [index for index in core_samples
                                if labels[index] == k]
        print "cluster_core_samples:", cluster_core_samples
        for index in class_members:
            x = XYdbscan[index]
            if index in core_samples and k != -1:
                markersize = 14
            else:
                markersize = 6
            pl.plot(x[0], x[1], 'o', markerfacecolor=col,
                    markeredgecolor='k', markersize=markersize)

    pl.title('Estimated number of clusters: %d' % n_clusters_)
    pl.show()


def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use -h or --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            sys.exit(0)
    # process arguments
    if len(args) == 0:
        usage()

    start_time = time.time()
    start_time_str = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime())
        
#    for arg in args:
    process(args[0], args[1]) # process() is defined elsewhere


    print "-" * 70
    print "START TIME:", start_time_str
    print "END TIME  :", time.strftime("%d/%m/%Y %H:%M:%S", time.localtime())
    
    elapsed_sec = round((time.time() - start_time))   
    print "Elapsed time:"
    print elapsed_time(int(elapsed_sec), [' year',' week',' day',' hour',' minute',' second'], add_s=True)

if __name__ == "__main__":
    main()

