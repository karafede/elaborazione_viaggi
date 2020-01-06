# -*- coding: iso-8859-15 -*-

"""addDelta.py

// ----------------------- addDelta -------------------------
//-------------------------------------------------------------


Opzioni:
  -h, --help              show this help


Controlla che la sequenza dei valori pannello sia corretta, corregge eventuali errori e calcola
i valori delta tra acquisizioni consecutive. Calcola inoltre le coordinate metriche XUTM32, YUTM32 e
l'ora locale DataOraLoc che tiene conto anche dell'ora legale

REM addDelta <Id_term> (0: elabora tutti)

######################################################
######################################################
# to execute run: (0 --> all terminals)
> python addDelta_Octo2015.py 0
######################################################
######################################################

$ python addDelta.py

"""

__autore__ =  "Massimo Mancini & Federico Karagulian"
__versione__ = "$Revision: 1.0 $"
__data__ = "$Date: 2019/12/15 $"

import sys
import locale
import math
import time, datetime
from dateutil import tz
import getopt
import db_connect
# import adodbapi.ado_consts as adc
import codecs
import csv
import unicodedata
#import ogr
import os
import pandas as pd


cwd = os.getcwd()
# change working directoy
os.chdir('C:\\ENEA_CAS_WORK\\Mancini_stuff\\OctoPerFederico')
cwd = os.getcwd()


import ogr

def paramDefinition():
	global timeBoundBuff, leftTimeBound, rightTimeBound

	timeBoundBuff = 900 # buffer time inizio e fine dataset: 900 sec = 15 min
	
	leftTimeBound = datetime.datetime(2013, 1, 1)
	rightTimeBound = datetime.datetime(2014, 1, 1)

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


class GPSTrack(object):
    csvHeading= ["Id_record_raw",
                 "Id_terminale",
                 "nPunto",
                 "DataOraUTC",
                 "DataOraLoc",
                 "LonWGS84",
                 "LatWGS84",
                 "XUTM32",
                 "YUTM32",
#                 "PRO_COM",
                 "Velocita",
                 "Direzione",
                 "Qualita",
                 "PanState",
                 "VPanState",
                 "TrckType",
                 "PanSeq",
                 "VPanSeq",
                 "Distanza",
                 "DistEucl",
                 "DeltaSec",
                 "Vmedia",
                 "DeltaDir"]
                # "Strada"]
    def __init__(self,
                 Id_record_raw="",
                 Id_terminale="",
                 nPunto="",
                 DataOraUTC="",
                 DataOraLoc="",
                 LonWGS84="",
                 LatWGS84="",
                 XUTM32="",
                 YUTM32="",
#                 PRO_COM="",
                 Velocita="",
                 Direzione="",
                 Qualita="",
                 PanState="",
                 VPanState="",
                 TrckType="",
                 PanSeq="",
                 VPanSeq="",
                 Distanza="",
                 DistEucl="",
                 DeltaSec="",
                 Vmedia="",
                 DeltaDir=""):
               #  Strada=""):
        self.Id_record_raw=Id_record_raw
        self.Id_terminale=Id_terminale
        self.nPunto=nPunto
        self.DataOraUTC=DataOraUTC
        self.DataOraLoc=DataOraLoc
        self.LonWGS84=LonWGS84
        self.LatWGS84=LatWGS84
        self.XUTM32=XUTM32
        self.YUTM32=YUTM32
#        self.PRO_COM=PRO_COM
        self.Velocita=Velocita
        self.Direzione=Direzione
        self.Qualita=Qualita
        self.PanState=PanState
        self.VPanState=VPanState
        self.TrckType=TrckType
        self.PanSeq=PanSeq
        self.VPanSeq=VPanSeq
        self.Distanza=Distanza 
        self.DistEucl=DistEucl
        self.DeltaSec=DeltaSec
        self.Vmedia=Vmedia
        self.DeltaDir=DeltaDir
     #   self.Strada=Strada
    def csvRow( self ):
        return [self.Id_record_raw,
        self.Id_terminale,
        self.nPunto,
        self.DataOraUTC,
        self.DataOraLoc,
        self.LonWGS84,
        self.LatWGS84,
        self.XUTM32,
        self.YUTM32,
#        self.PRO_COM,
        self.Velocita,
        self.Direzione,
        self.Qualita,
        self.PanState,
        self.VPanState,
        self.TrckType,
        self.PanSeq,
        self.VPanSeq,
        self.Distanza,
        self.DistEucl,
        self.DeltaSec,
        self.Vmedia,
        self.DeltaDir]
     #   self.Strada]

"""Define a geometric point and a few common manipulations."""
class Point( object ):
    """A 2-D geometric point."""
    def __init__( self, x, y ):
        """Create a point at (x,y)."""
        self.x, self.y = x, y
    def offset( self, xo, yo ):
        """Offset the point by xo parallel to the x-axis
        and yo parallel to the y-axis."""
        self.x += xo
        self.y += yo
    def offset2( self, val ):
        """Offset the point by val parallel to both axes."""
        self.offset( val, val )
    def __str__( self ):
        """Return a pleasant representation."""
        return "(%g,%g)" % ( self.x, self.y )
    def transform( self, fromEPSG, toEPSG ):
        ## EPSG4326: WGS84 geo - EPSG31468: UTM32N datum WGS84

        #original SRS
        oSRS=ogr.osr.SpatialReference()
        oSRS.ImportFromEPSG(fromEPSG)

        #target SRS
        tSRS=ogr.osr.SpatialReference()
        tSRS.ImportFromEPSG(toEPSG)

        coordTrans=ogr.osr.CoordinateTransformation(oSRS,tSRS)
        res=coordTrans.TransformPoint(self.x, self.y, 0.)
        self.x = res[0]
        self.y = res[1]

def DistanzaTraDuePunti(x1, y1, x2, y2): 
    dx = x2 - x1 
    dy = y2 - y1 
    DistQuadrata = dx**2 + dy**2 
    Risultato = math.sqrt(DistQuadrata) 
    return Risultato 
   


def usage():
    print("for help use -h or --help")
    sys.exit(0)

def appendGPSTrack(punto, VPanState, TrckType):

    track = GPSTrack(Id_record_raw=punto[0],
        Id_terminale=punto[1],
        DataOraUTC=punto[2],
#        DataOraLoc=DataOraLoc, 
        LonWGS84=punto[3],
        LatWGS84=punto[4],
#       XUTM32=XUTM32,
#       YUTM32=YUTM32,
#       PRO_COM=punto.PRO_COM,
        Velocita=punto[5],
        Direzione=punto[6],
        Qualita=punto[7],
        PanState=punto[8],
        VPanState=VPanState,
        TrckType=TrckType,
        Distanza=punto[9])
      #  Strada=punto[10])
                                    
    termTracksList.append(track)

def checkInizioFineTerm():
        nTracksTerm = len(termTracksList)
        timeDiffPrimo = termTracksList[0].DataOraUTC - leftTimeBound
        timeDiffUltimo = rightTimeBound - termTracksList[nTracksTerm-1].DataOraUTC
        if (termTracksList[0].PanState == 1):
            termTracksList[0].VPanState = 0
            if ((timeDiffPrimo.days*86400+timeDiffPrimo.seconds) < timeBoundBuff):
                termTracksList[0].TrckType = '0t'
            else:
                termTracksList[0].TrckType = '0m'

        if (termTracksList[nTracksTerm-1].PanState == 1):
            termTracksList[nTracksTerm-1].VPanState = 2
            if ((timeDiffUltimo.days*86400+timeDiffUltimo.seconds) < timeBoundBuff):
                termTracksList[nTracksTerm-1].TrckType = '2t'
            else:
                termTracksList[nTracksTerm-1].TrckType = '2m'

        ## se primo punto con Pannello 2 viene rimosso dalla lista
        if (termTracksList[0].VPanState == 2):
            fLog.write('Terminale: '+str(termTracksList[0].Id_terminale)+' primo punto VPan 2 rimosso\n')
            fRecDel.write(str(termTracksList[0].Id_record_raw)+' term: '+str(termTracksList[0].Id_terminale)+'\n')
            del termTracksList[0]
        ## se ultimo punto con Pannello 0 viene rimosso dalla lista
        nTracksTerm = len(termTracksList)
        if (termTracksList[nTracksTerm-1].VPanState == 0):
            fLog.write('Terminale: '+str(termTracksList[nTracksTerm-1].Id_terminale)+' ultimo punto VPan 0 rimosso\n')
            fRecDel.write(str(termTracksList[nTracksTerm-1].Id_record_raw)+' term: '+str(termTracksList[nTracksTerm-1].Id_terminale)+'\n')
            del termTracksList[nTracksTerm-1]
        aggiornaSeq(1, len(termTracksList))
        
def check02EqTS():
    nTrack = 0
#    inverto eventuali 02 in 20 se stesso timestamp e sequenza 002 102 (trasformo in quelle corrette 020 120)
    for track in termTracksList:
        nTrack = nTrack + 1
        if nTrack > 1:
            if ((termTracksList[nTrack-1].PanSeq == '02') and (termTracksList[nTrack-2].DataOraUTC==termTracksList[nTrack-1].DataOraUTC)):
                if (nTrack > 2):
                    if (termTracksList[nTrack-3].PanState in (0, 1)):
                        fLogSeq.write(str(track.Id_record_raw)+'- term: '+str(track.Id_terminale)+' - 02 stesso Timestamp: inverto 02 in 20\n')
                        tmp = termTracksList[nTrack-2]
                        termTracksList[nTrack-2] = termTracksList[nTrack-1]
                        termTracksList[nTrack-1] = tmp
                        aggiornaSeq(nTrack, nTrack+2)
                    else:
                        fLogSeq.write(str(track.Id_record_raw)+'- term: '+str(track.Id_terminale)+' - 02 stesso Timestamp: non inverto 02 in 20\n')

def checkVPanSeq():
    nTrack = 0
    nDeleted = 0
    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        track=termTracksList[nTrack-1]
        if nTrack > 1:
            if termTracksList[nTrack-1].PanSeq not in ('02', '01', '11', '12', '20'):
                fLogSeq.write(str(track.Id_record_raw)+'- term: '+str(track.Id_terminale)+'- npunto: '+str(nTrack+nDeleted)+' - PanSeq '\
                              +termTracksList[nTrack-1].PanSeq+' Inconsistent\n')
            if termTracksList[nTrack-1].VPanSeq not in ('02', '01', '11', '12', '20'):
                fLogSeq.write(str(track.Id_record_raw)+'- term: '+str(track.Id_terminale)+'- npunto: '+str(nTrack+nDeleted)+' - VPanSeq '\
                              +termTracksList[nTrack-1].VPanSeq+' Inconsistent\n')
#    return
                if track.VPanSeq == '00':
                    # elimino il primo
                    iTracksToRemove.append(nTrack-2)
                elif track.VPanSeq == '10':
                    termTracksList[nTrack-2].VPanState = 2
                    termTracksList[nTrack-2].TrckType = '2m'
                elif track.VPanSeq == '21':
                    termTracksList[nTrack-1].VPanState = 0
                    termTracksList[nTrack-1].TrckType = '0m'
                elif track.VPanSeq == '22':
                    # elimino il secondo
                    iTracksToRemove.append(nTrack-1)
                nDeleted = nDeleted + len(iTracksToRemove)
                nTrack = nTrack - len(iTracksToRemove)
                updateTrackList(nTrack-1)   

def check02Zero():
    nTrack = 0
#    controllo se la distanza 02 è zero
    for track in termTracksList:
        nTrack = nTrack + 1
        if nTrack > 1:
            if ((termTracksList[nTrack-1].PanSeq == '02') and (termTracksList[nTrack-1].VPanSeq == '02')):
                if (track.Distanza == 0):
                    f1Zero02.write(str(trackPrec.Id_record_raw)+';'+str(trackPrec.Id_terminale)+';'+str(nTrack-1)
                                +';'+str(trackPrec.Distanza)
                                +';'+str(trackPrec.PanState)+';'+str(trackPrec.VPanState)
                                +';'+str(trackPrec.VPanSeq)+';\n')
                    iTracksToRemove.append(nTrack-2)
                    f1Zero02.write(str(track.Id_record_raw)+';'+str(track.Id_terminale)+';'+str(nTrack)
                                +';'+str(track.Distanza)
                                +';'+str(track.PanState)+';'+str(track.VPanState)
                                +';'+str(track.VPanSeq)+';\n')
                    iTracksToRemove.append(nTrack-1)
                else:
                    timeDiff = track.DataOraUTC - trackPrec.DataOraUTC
                    DeltaSec=(timeDiff.days*86400+timeDiff.seconds)
                    track.DeltaSec=DeltaSec
                    if (track.DeltaSec == 0):
                        f2Zero02.write(str(trackPrec.Id_record_raw)+';'+str(trackPrec.Id_terminale)+';'+str(nTrack-1)
                                    +';'+str(trackPrec.Distanza)+';'+str(trackPrec.DeltaSec)
                                    +';'+str(trackPrec.PanState)+';'+str(trackPrec.VPanState)
                                    +';'+str(trackPrec.VPanSeq)+';\n')
                        iTracksToRemove.append(nTrack-2)
                        f2Zero02.write(str(track.Id_record_raw)+';'+str(track.Id_terminale)+';'+str(nTrack)
                                    +';'+str(track.Distanza)+';'+str(track.DeltaSec)
                                    +';'+str(track.PanState)+';'+str(track.VPanState)
                                    +';'+str(track.VPanSeq)+';\n')
                        iTracksToRemove.append(nTrack-1)
        trackPrec = track
    updateTrackList(1)
    
        
def aggiungiDelta():
    x=float(termTracksList[0].LonWGS84)
    x=x+0
    y=float(termTracksList[0].LatWGS84)
    y=y+0
            #original SRS
    oSRS=ogr.osr.SpatialReference()
    oSRS.ImportFromEPSG(4326)

        #target SRS
    tSRS=ogr.osr.SpatialReference()
    tSRS.ImportFromEPSG(32632)

    coordTrans=ogr.osr.CoordinateTransformation(oSRS,tSRS)
    res=coordTrans.TransformPoint(x, y, 0.)
    P1 = Point(x, y)
    P1.transform(4326, 32632)
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    nTrack = 0
    for track in termTracksList:
        nTrack = nTrack + 1
        # print("nTrack:", nTrack)
        utc = track.DataOraUTC
        utc = utc.replace(tzinfo=from_zone)
        local = utc.astimezone(to_zone)
        DataOraLoc = local.strftime ("%Y-%m-%d %H:%M:%S")
        punto = Point(float(track.LonWGS84), float(track.LatWGS84))
        P1 = Point(float(track.LonWGS84), float(track.LatWGS84))
        P1.transform(4326, 32632)

        punto.transform(4326, 32632)

        XUTM32 = int(round(punto.x))
        YUTM32 = int(round(punto.y))   
        if nTrack > 1:
            timeDiff = track.DataOraUTC - trackPrec.DataOraUTC
            DistEucl=int(round(DistanzaTraDuePunti(trackPrec.XUTM32, trackPrec.YUTM32, XUTM32, YUTM32)))
            DeltaSec=(timeDiff.days*86400+timeDiff.seconds)
            if track.VPanState > 0:
                if DeltaSec > 0.1:
                    Vmedia = int(round(3.6 * (float(track.Distanza)/float(DeltaSec))))
                else:
                    Vmedia = -1
            else:
                Vmedia = None
            DeltaDir = track.Direzione - trackPrec.Direzione
            if (DeltaDir < -180):
                DeltaDir = DeltaDir + 360
            elif (DeltaDir > 180):
                DeltaDir = DeltaDir - 360
        else:
            DeltaSec = None
            DistEucl = None
            Vmedia = None
            DeltaDir = None

        termTracksList[nTrack-1].nPunto = nTrack
        termTracksList[nTrack-1].DataOraLoc = DataOraLoc
        termTracksList[nTrack-1].DeltaSec = DeltaSec
        termTracksList[nTrack-1].XUTM32 = XUTM32        
        termTracksList[nTrack-1].YUTM32 = YUTM32        
        termTracksList[nTrack-1].DistEucl = DistEucl
        termTracksList[nTrack-1].Vmedia = Vmedia
        termTracksList[nTrack-1].DeltaDir = DeltaDir

        trackPrec = track
        
 
        
def updateTrackList(fromNTrack):

    # aggiorno solo da FromNTrack in poi
    
    removeTracks()

    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return

    aggiornaSeq(fromNTrack, nTracksTerm)
    

def lastUpdateTrackList():

#    removeTracks()

    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return
        

    ## se primo punto con Pannello 2 viene rimosso dalla lista
    if (termTracksList[0].VPanState == 2):
        fLog.write('Terminale: '+str(termTracksList[0].Id_terminale)+' primo punto VPan 2 rimosso\n')
        fRecDel.write(str(termTracksList[0].Id_record_raw)+' term: '+str(termTracksList[0].Id_terminale)+'\n')
        del termTracksList[0]
        aggiornaSeq(1, len(termTracksList))
    ## se ultimo punto con Pannello 0 viene rimosso dalla lista
    nTracksTerm = len(termTracksList)
    if (termTracksList[nTracksTerm-1].VPanState == 0):
        fLog.write('Terminale: '+str(termTracksList[nTracksTerm-1].Id_terminale)+' ultimo punto VPan 0 rimosso\n')
        fRecDel.write(str(termTracksList[nTracksTerm-1].Id_record_raw)+' term: '+str(termTracksList[nTracksTerm-1].Id_terminale)+'\n')
        del termTracksList[nTracksTerm-1]
        aggiornaSeq(1, len(termTracksList))

    if len(termTracksList) > 0:
        ## se primo punto con Pannello 2 viene rimosso dalla lista
        if (termTracksList[0].VPanState == 2):
            fLog.write('Terminale: '+str(termTracksList[0].Id_terminale)+' primo punto VPan 2 rimosso\n')
            del termTracksList[0]
        ## se ultimo punto con Pannello 0 viene rimosso dalla lista
        if (termTracksList[len(termTracksList)-1].VPanState == 0):
            fLog.write('Terminale: '+str(termTracksList[len(termTracksList)-1].Id_terminale)+' ultimo punto VPan 0 rimosso\n')
            del termTracksList[len(termTracksList)-1]
        
    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return
    
##  Controllo finale corretta sequenza Virtual Panel State

    termTracksList[0].VPanSeq = None
    for nTrack in range(1, nTracksTerm + 1):                ## nella funzione range il secondo estremo è escluso
        if nTrack > 1:
            VPanSeq = str(10 * termTracksList[nTrack-2].VPanState + termTracksList[nTrack-1].VPanState).zfill(2)
            termTracksList[nTrack-1].VPanSeq = VPanSeq
            if VPanSeq not in ('02', '01', '11', '12', '20'):
                fLog.write('=== Finale ==='+str(termTracksList[nTrack-1].Id_terminale)+'- npunto: '+str(termTracksList[nTrack-1].Id_puntoTerm)+' - '+VPanSeq+' Inconsistent\n')
                print('sequenza VPan errata - programma interrotto')
                chiudiFiles()
                sys.exit(2)

    aggiungiDelta()
    
## se la sequenza è corretta procedi ad aggiornare i valori delta
##    for nTrack in range(1, nTracksTerm + 1):
##        if nTrack > 1:
##            timeDiff = termTracksList[nTrack-1].DataOraUTC - termTracksList[nTrack-2].DataOraUTC
##            DistEucl=int(round(DistanzaTraDuePunti(termTracksList[nTrack-2].XUTM32, termTracksList[nTrack-2].YUTM32, termTracksList[nTrack-1].XUTM32, termTracksList[nTrack-1].YUTM32)))
##            if termTracksList[nTrack-1].VPanState in (1, 2):
##                ## Controllo se Distanza < DistEucl allora pongo Distanza = DistEucl
##                if termTracksList[nTrack-1].Distanza < termTracksList[nTrack-1].DistEucl:
##                    if (termTracksList[nTrack-1].DistEucl - termTracksList[nTrack-1].Distanza) > 30:
##                        t = termTracksList[nTrack-1]
##                        fLog.write('%7d - npunto: %4d VPanState: %d == Distanza: %6d < DistEucl: %6d ==\n' %(t.Id_terminale, t.Id_puntoTerm, t.VPanState, t.Distanza, t.DistEucl))
##                    termTracksList[nTrack-1].Distanza = termTracksList[nTrack-1].DistEucl
##            DeltaSec=(timeDiff.days*86400+timeDiff.seconds)
##            if termTracksList[nTrack-1].VPanState > 0:
##                if DeltaSec > 0.1:
##                   Vmedia = int(round(3.6 * (float(termTracksList[nTrack-1].Distanza)/float(DeltaSec))))
##                else:
##                    Vmedia = -1
##            else:
##                Vmedia = None
##            DeltaDir = termTracksList[nTrack-1].Direzione - termTracksList[nTrack-2].Direzione
##            if (DeltaDir < -180):
##                DeltaDir = DeltaDir + 360
##            elif (DeltaDir > 180):
##                DeltaDir = DeltaDir - 360
##        else:
##            DeltaSec = None
##            DistEucl = None
##            Vmedia = None
##            DeltaDir = None
##
##        termTracksList[nTrack-1].DeltaSec = DeltaSec
##        termTracksList[nTrack-1].DistEucl = DistEucl
##        if Vmedia > 250:
##            Vmedia = 250
##        termTracksList[nTrack-1].Vmedia = Vmedia
##        termTracksList[nTrack-1].DeltaDir = DeltaDir

def LogTermTracks(fromNTrack, toNTrack):
    if termToElab == '0':
        return
    fLog.write("nTrack Id_term Id_record_raw DataOraUTC Velocita Direzione Qualita PanState PanSeq VPanState VPanSeq TrckType Anomalia DistGps Distanza DistEucl DeltaSec Vmedia DeltaDir\n")
    nTrack = 0
    for track in termTracksList:
        nTrack = nTrack + 1
        if nTrack > toNTrack:
            break
        if nTrack >= fromNTrack:
            fLog.write('%4d %8s' % (nTrack, str(track.Id_terminale))) 
            fLog.write('%12s' %str(track.Id_record_raw))
            fLog.write('%20s' %str(track.DataOraUTC))
#            fLog.write('%3s' %str(track.Bordo))
#            fLog.write('%4s' %str(track.BordoSeq))        
            fLog.write('%6s' %str(track.Velocita))
            fLog.write('%4s' %str(track.Direzione))
            fLog.write('%3s' %str(track.Qualita))
            fLog.write('%3s' %str(track.PanState))
            fLog.write('%4s' %str(track.PanSeq))
            fLog.write('%3s' %str(track.VPanState))
            fLog.write('%4s' %str(track.VPanSeq))
            fLog.write('%4s' %str(track.TrckType))
#            fLog.write('%6s' %str(track.Anomalia))
#            fLog.write('%7s' %str(track.DistGps))
            fLog.write('%7s' %str(track.Distanza))
            fLog.write('%7s' %str(track.DistEucl))
            fLog.write('%8s' %str(track.DeltaSec))        
            fLog.write('%6s' %str(track.Vmedia))
            fLog.write('%5s' %str(track.DeltaDir))
            fLog.write('\n')

def scriviTermTracks():
    for track in termTracksList:
        csvWriter.writerow(track.csvRow())

def aggiornaSeq(fromNTrack, toNTrack):
    nTracksTerm = len(termTracksList)
    if toNTrack > nTracksTerm:
        toNTrack = nTracksTerm
    termTracksList[0].PanSeq = None
    termTracksList[0].VPanSeq = None
    for nTrack in range(fromNTrack, nTracksTerm + 1):          ## nella funzione range il secondo estremo è escluso
        if nTrack > 1:
            PanSeq = str(10 * termTracksList[nTrack-2].PanState + termTracksList[nTrack-1].PanState).zfill(2)
            termTracksList[nTrack-1].PanSeq = PanSeq
            VPanSeq = str(10 * termTracksList[nTrack-2].VPanState + termTracksList[nTrack-1].VPanState).zfill(2)
            termTracksList[nTrack-1].VPanSeq = VPanSeq
   

def process(terminale):

    # import adodbapi
    import db_connect
    global termToElab
    global termTracksList
    global iTracksToRemove
    
    termToElab = terminale
    
    paramDefinition()

    outFileLog = r"Log\addDelta.log"
    global fLog
    fLog = open(outFileLog, "w")

    outFileRecDel = r"Log\recDeleted.log"
    global fRecDel
    fRecDel = open(outFileRecDel, "w")

    outFileLog5p = r"Log\addDelta5p.log"
    global fLog5p
    fLog5p = open(outFileLog5p, "w")

    outFileLogSeq = r"Log\addDeltaSeq.log"
    global fLogSeq
    fLogSeq = open(outFileLogSeq, "w")
    
    out1File02zero = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\DistZero_02.csv"
    global f1Zero02
    f1Zero02 = open(out1File02zero, "w")
    f1Zero02.write('Id_record_raw; Id_terminale; np; Distanza;PanState; VPanState; VPanSeq\n')

    out2File02zero = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\DeltaSecZero_02.csv"
    global f2Zero02
    f2Zero02 = open(out2File02zero, "w")
    f2Zero02.write('Id_record_raw; Id_terminale; np; Distanza; DeltaSec; PanState; VPanState; VPanSeq\n')

    outFileCsv = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\GPSTrackDelta.csv"

    global csvWriter
    fcsv=open(outFileCsv, "w")
    csvWriter = csv.writer(fcsv, dialect='excel', delimiter=';', lineterminator='\n')
    csvWriter.writerow( GPSTrack.csvHeading )

    # connection string for an SQL server
    # _computername="127.0.0.1" #or name of computer with SQL Server
    # _databasename="Octo2014G1"
    _tableTermName="DatiRawOrigine01"
    # this will open a MS-SQL table with Windows authentication
    # connStr = r"Initial Catalog=%s; Data Source=%s; Provider=SQLOLEDB.1; Integrated Security=SSPI" \
    #          %(_databasename, _computername)
    #create the connection
    # dbConn = adodbapi.connect(connStr)
    #make a cursor on the connection
    # dbCur = dbConn.cursor()

    conn = db_connect.connect_Octo2015()
    cur = conn.cursor()
    
    if termToElab == '0':
        cur.execute("SELECT Distinct Id_terminale FROM %s Order by Id_terminale" % _tableTermName)
    else:
        cur.execute("SELECT Distinct Id_terminale FROM %s Where Id_terminale = " % _tableTermName +termToElab)

    # terminali = dbCur.fetchall()
    terminali= cur.fetchall()

    # print(terminali , "\n")

    nTerm = 0
    for terminale in terminali:
        nTerm = nTerm+1
##        if nTerm > 100:
##            break
        
        termTracksList=[]
        iTracksToRemove=[]
        
        cur.execute("SELECT Id_record_raw, Id_terminale, DataOra As DataOraUTC, \
        LonWGS84, LatWGS84,\
        Velocita,Direzione,Qualita,Pannello,Distanza\
        FROM DatiRawOrigine01 Where Id_terminale = %d Order By DataOra, Id_record_raw" % terminale)
     # FROM DatiRawOrigine01 Where Id_terminale = %d Order By DataOra, Id_record_raw" % terminale.Id_terminale)

        rows = cur.fetchall()
        # need to add column names
        # If you just want the COLUMN NAMES
        colnames_db = [desc[0] for desc in cur.description]
        # rows = pd.DataFrame(cur.fetchall(), columns=colnames_db)
        # nPuntiAnno = dbCur.rowcount
        nPuntiAnno = cur.rowcount

        # print(nPuntiAnno, "\n")

        if (nPuntiAnno < 10):
            fLog5p.write('Terminale: '+str(terminale)+' '+str(nPuntiAnno)+' < 5\n')
            print(nTerm,' Term: ',terminale,' n.punti: ',nPuntiAnno,'*** non elaboro')
            continue
        else:
            print(nTerm,' Term: ',terminale,' n.punti: ',nPuntiAnno)

        for punto in rows:
            VPanState = punto[8]  # Pannello
            TrckType = str(VPanState)+'n'
            appendGPSTrack(punto, VPanState, TrckType)

        aggiornaSeq(1, len(termTracksList))
        
        LogTermTracks(1, len(termTracksList))
        
        checkInizioFineTerm()

        check02EqTS()
        
        check02Zero()
        
        checkVPanSeq()        

        LogTermTracks(1, len(termTracksList))

        lastUpdateTrackList()
        
        scriviTermTracks()

    # dbCur.close()
    # dbConn.close()

    conn.close()
    cur.close()

    # del dbCur, dbConn, rows
    del cur, conn, rows

    chiudiFiles()
    
    
def removeTracks():  
    global iTracksToRemove
    ## ordino gli indici in ordine decrescente, perché il n. di elementi si riduce ad ogni del
    if len(iTracksToRemove) > 0:
        iTracksToRemove.sort(reverse = True)
##        print 'remove Tracks - index iTracksToRemove sorted: ', iTracksToRemove
        for iTrack in iTracksToRemove:
##            print 'numero di tracks nella Lista: ', len(termTracksList)
##            print 'Elimino track index n.: ', iTrack, 'nPunto: ', termTracksList[iTrack].nPunto
            fRecDel.write(str(termTracksList[iTrack].Id_record_raw)+' term: '+str(termTracksList[iTrack].Id_terminale)+'\n')
            del termTracksList[iTrack]            

    iTracksToRemove = []


def chiudiFiles():
#    fcsv.close()
    fLog.close()
    fRecDel.close()
    fLog5p.close()
    fLogSeq.close()
    f1Zero02.close()    
    f2Zero02.close()
    
def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error as msg:
        print(msg)
        print("for help use -h or --help")
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print( __doc__)
            sys.exit(0)
    # process arguments
#    if len(args) == 0:
        ## usage()

    start_time = time.time()
    start_time_str = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime())

    print("Processing data....")

    process(args[0]) # process() is defined elsewhere

    print("-" * 70)
    print("START TIME:", start_time_str)
    print("END TIME  :", time.strftime("%d/%m/%Y %H:%M:%S", time.localtime()))
    
    elapsed_sec = round((time.time() - start_time))   
    print("Elapsed time:")
    print(elapsed_time(int(elapsed_sec), [' year',' week',' day',' hour',' minute',' second'], add_s=True))


    
if __name__ == "__main__":
    main()
