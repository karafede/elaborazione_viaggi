# -*- coding: iso-8859-15 -*-

"""ElaboraViaggi.py

// ----------------------- ElaboraViaggi -------------------------
//-------------------------------------------------------------

//
// uso: ElaboraViaggi
//

Opzioni:
  -h, --help              show this help


Esempio:
  ElaboraViaggi.py <Id_terminale>

  se Id_terminale = 0 elabora tutti

# Modificato programma ElaboraViaggi.py per associare alle origini e destinazioni  il codice Provincia
# (fuori provincia di Roma) od il codice Comune (entro la provincia di Roma) o l?appartenenza all?interno del GRA.
# La procedura è la seguente: si individua la provincia di appartenenza, se è Roma (58) si individua il comune di
# appartenenza, se è Roma (58091) si verifica l?appartenenza all?interno del GRA. Se il punto è interno al GRA si
# attribuisce il codice 58000. Sono stati utilizzati gli shapefiles: Com2011_WGS84geoProvRoma, GraWGS84geo,
# prov2011_WGS84geo.Lo shapefile Com2011_WGS84geoProvRoma e stato editato con QGis per eliminare i buchi corrisponenti
# alla Città del Vaticano ed a Castelgandolfo.

"""

_autore__ =  "Massimo Mancini & Federico Karagulian"
__versione__ = "$Revision: 1.0 $"
__data__ = "$Date: 2019/12/24 $"

import sys
import locale
import math, random
import time, datetime
from dateutil import tz
import getopt
# import adodbapi.ado_consts as adc
import db_connect
import codecs
import csv
import unicodedata
import ogr
import os

cwd = os.getcwd()
# change working directoy
os.chdir('C:\\ENEA_CAS_WORK\\Mancini_stuff\\OctoPerFederico')
cwd = os.getcwd()

def elapsed_time(seconds, suffixes=['y', 'w', 'd', 'h', 'm', 's'], add_s=False, separator=' '):
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


class PuntoViaggio(object):
    csvHeading = ["Id_viaggio",
                  "Id_terminale",
                  "Id_puntoTerm",
                  "nViaggio",
                  "nPuntoV",
                  "DataOraUTC",
                  "DataOraLoc",
                  "LonWGS84",
                  "LatWGS84",
                  "XUTM32",
                  "YUTM32",
                  "TrckType",
                  "Anomalia",
                  ##                 "PRO_COM",
                  ##                 "Bordo",
                  ##                 "BordoSeq",
                  "Velocita",
                  "Direzione",
                  "DeltaDir",
                  "Qualita",
                  "PanState",
                  "VPanState",
                  "VPanSeq",
                  "DistGps",
                  "Distanza",
                  "DistEucl",
                  "Tortuosita",
                  "Progressiva",
                  "DeltaSec",
                  "Vmedia"]
                  # "Strada"]

    def __init__(self,
                 Id_viaggio="",
                 Id_terminale="",
                 Id_puntoTerm="",
                 nViaggio="",
                 nPuntoV="",
                 DataOraUTC="",
                 DataOraLoc="",
                 LonWGS84="",
                 LatWGS84="",
                 XUTM32="",
                 YUTM32="",
                 TrckType="",
                 Anomalia="",
                 ##                 PRO_COM="",
                 ##                 Bordo="",
                 ##                 BordoSeq="",
                 Velocita="",
                 Direzione="",
                 DeltaDir="",
                 Qualita="",
                 PanState="",
                 VPanState="",
                 VPanSeq="",
                 Distanza="",
                 DistGps="",
                 DistEucl="",
                 Tortuosita="",
                 Progressiva="",
                 DeltaSec="",
                 Vmedia=""):
                # Strada=""):
        self.Id_viaggio = Id_viaggio
        self.Id_terminale = Id_terminale
        self.Id_puntoTerm = Id_puntoTerm
        self.nViaggio = nViaggio
        self.nPuntoV = nPuntoV
        self.DataOraUTC = DataOraUTC
        self.DataOraLoc = DataOraLoc
        self.LonWGS84 = LonWGS84
        self.LatWGS84 = LatWGS84
        self.XUTM32 = XUTM32
        self.YUTM32 = YUTM32
        self.TrckType = TrckType
        self.Anomalia = Anomalia
        ##        self.PRO_COM=PRO_COM
        ##        self.Bordo=Bordo
        ##        self.BordoSeq=BordoSeq
        self.Velocita = Velocita
        self.Direzione = Direzione
        self.DeltaDir = DeltaDir
        self.Qualita = Qualita
        self.PanState = PanState
        self.VPanState = VPanState
        self.VPanSeq = VPanSeq
        self.Distanza = Distanza
        self.DistGps = DistGps
        self.DistEucl = DistEucl
        self.Tortuosita = Tortuosita
        self.Progressiva = Progressiva
        self.DeltaSec = DeltaSec
        self.Vmedia = Vmedia
        # self.Strada = Strada

    def csvRow(self):
        return [self.Id_viaggio,
                self.Id_terminale,
                self.Id_puntoTerm,
                self.nViaggio,
                self.nPuntoV,
                self.DataOraUTC,
                self.DataOraLoc,
                self.LonWGS84,
                self.LatWGS84,
                self.XUTM32,
                self.YUTM32,
                self.TrckType,
                self.Anomalia,
                ##        self.PRO_COM,
                ##        self.Bordo,
                ##        self.BordoSeq,
                self.Velocita,
                self.Direzione,
                self.DeltaDir,
                self.Qualita,
                self.PanState,
                self.VPanState,
                self.VPanSeq,
                self.DistGps,
                self.Distanza,
                self.DistEucl,
                self.Tortuosita,
                self.Progressiva,
                self.DeltaSec,
                self.Vmedia]
                # self.Strada]


class Viaggio(object):
    csvHeading = ["Id_viaggio",
                  "Id_terminale",
                  "nViaggio",
                  "Id_puntoOrig",
                  "Id_puntoDest",
                  "O_PRO_COM",
                  "D_PRO_COM",
                  "OTrckType",
                  "DTrckType",
                  "ODataOra",
                  "DDataOra",
                  "Distanza",
                  "DurataSec",
                  "Vmedia",
                  "nPunti"]

    def __init__(self,
                 Id_viaggio="",
                 Id_terminale="",
                 nViaggio="",
                 Id_puntoOrig="",
                 Id_puntoDest="",
                 O_PRO_COM="",
                 D_PRO_COM="",
                 OTrckType="",
                 DTrckType="",
                 ODataOra="",
                 DDataOra="",
                 Distanza="",
                 DurataSec="",
                 Vmedia="",
                 nPunti=""):
        self.Id_viaggio = Id_viaggio
        self.Id_terminale = Id_terminale
        self.nViaggio = nViaggio
        self.Id_puntoOrig = Id_puntoOrig
        self.Id_puntoDest = Id_puntoDest
        self.O_PRO_COM = O_PRO_COM
        self.D_PRO_COM = D_PRO_COM
        self.OTrckType = OTrckType
        self.DTrckType = DTrckType
        self.ODataOra = ODataOra
        self.DDataOra = DDataOra
        self.Distanza = Distanza
        self.DurataSec = DurataSec
        self.Vmedia = Vmedia
        self.nPunti = nPunti

    def csvRow(self):
        return [self.Id_viaggio,
                self.Id_terminale,
                self.nViaggio,
                self.Id_puntoOrig,
                self.Id_puntoDest,
                self.O_PRO_COM,
                self.D_PRO_COM,
                self.OTrckType,
                self.DTrckType,
                self.ODataOra,
                self.DDataOra,
                self.Distanza,
                self.DurataSec,
                self.Vmedia,
                self.nPunti]


class Sosta(object):
    csvHeading = ["Id_sosta",
                  "Id_terminale",
                  "nSosta",
                  "Id_viaggioPrec",
                  "Id_puntoArrivo",
                  "Id_puntoPartenza",
                  "A_PRO_COM",
                  "P_PRO_COM",
                  "ATrckType",
                  "PTrckType",
                  "ADataOra",
                  "PDataOra",
                  "DistAP",
                  "DurataSec"]

    def __init__(self,
                 Id_sosta="",
                 Id_terminale="",
                 nSosta="",
                 Id_viaggioPrec="",
                 Id_puntoArrivo="",
                 Id_puntoPartenza="",
                 A_PRO_COM="",
                 P_PRO_COM="",
                 ATrckType="",
                 PTrckType="",
                 ADataOra="",
                 PDataOra="",
                 DistAP="",
                 DurataSec=""):
        self.Id_sosta = Id_sosta
        self.Id_terminale = Id_terminale
        self.nSosta = nSosta
        self.Id_viaggioPrec = Id_viaggioPrec
        self.Id_puntoArrivo = Id_puntoArrivo
        self.Id_puntoPartenza = Id_puntoPartenza
        self.A_PRO_COM = A_PRO_COM
        self.P_PRO_COM = P_PRO_COM
        self.ATrckType = ATrckType
        self.PTrckType = PTrckType
        self.ADataOra = ADataOra
        self.PDataOra = PDataOra
        self.DistAP = DistAP
        self.DurataSec = DurataSec

    def csvRow(self):
        return [self.Id_sosta,
                self.Id_terminale,
                self.nSosta,
                self.Id_viaggioPrec,
                self.Id_puntoArrivo,
                self.Id_puntoPartenza,
                self.A_PRO_COM,
                self.P_PRO_COM,
                self.ATrckType,
                self.PTrckType,
                self.ADataOra,
                self.PDataOra,
                self.DistAP,
                self.DurataSec]


class GPSTrack(object):
    csvHeading = ["Id_terminale",
                  "Id_puntoTerm",
                  "DataOraUTC",
                  "DataOraLoc",
                  "LonWGS84",
                  "LatWGS84",
                  "XUTM32",
                  "YUTM32",
                  ##                 "PRO_COM",
                  ##                 "Bordo",
                  ##                 "BordoSeq",
                  "Velocita",
                  "Direzione",
                  "Qualita",
                  "PanState",
                  "VPanState",
                  "TrckType",
                  "Anomalia",
                  "PanSeq",
                  "VPanSeq",
                  "DistGps",
                  "Distanza",
                  "DistEucl",
                  "DeltaSec",
                  "Vmedia",
                  "DeltaDir"]
                  # "Strada"]

    def __init__(self,
                 Id_terminale="",
                 Id_puntoTerm="",
                 DataOraUTC="",
                 DataOraLoc="",
                 LonWGS84="",
                 LatWGS84="",
                 XUTM32="",
                 YUTM32="",
                 ##                 PRO_COM="",
                 ##                 Bordo="",
                 ##                 BordoSeq="",
                 Velocita="",
                 Direzione="",
                 Qualita="",
                 PanState="",
                 VPanState="",
                 TrckType="",
                 Anomalia="",
                 PanSeq="",
                 VPanSeq="",
                 DistGps="",
                 Distanza="",
                 DistEucl="",
                 DeltaSec="",
                 Vmedia="",
                 DeltaDir=""):
                 # Strada=""):
        self.Id_terminale = Id_terminale
        self.Id_puntoTerm = Id_puntoTerm
        self.DataOraUTC = DataOraUTC
        self.DataOraLoc = DataOraLoc
        self.LonWGS84 = LonWGS84
        self.LatWGS84 = LatWGS84
        self.XUTM32 = XUTM32
        self.YUTM32 = YUTM32
        ##        self.PRO_COM=PRO_COM
        ##        self.Bordo=Bordo
        ##        self.BordoSeq=BordoSeq
        self.Velocita = Velocita
        self.Direzione = Direzione
        self.Qualita = Qualita
        self.PanState = PanState
        self.VPanState = VPanState
        self.TrckType = TrckType
        self.Anomalia = Anomalia
        self.PanSeq = PanSeq
        self.VPanSeq = VPanSeq
        self.DistGps = DistGps
        self.Distanza = Distanza
        self.DistEucl = DistEucl
        self.DeltaSec = DeltaSec
        self.Vmedia = Vmedia
        self.DeltaDir = DeltaDir
        # self.Strada = Strada

    def csvRow(self):
        return [self.Id_terminale,
                self.Id_puntoTerm,
                self.DataOraUTC,
                self.DataOraLoc,
                self.LonWGS84,
                self.LatWGS84,
                self.XUTM32,
                self.YUTM32,
                ##        self.PRO_COM,
                ##        self.Bordo,
                ##        self.BordoSeq,
                self.Velocita,
                self.Direzione,
                self.Qualita,
                self.PanState,
                self.VPanState,
                self.TrckType,
                self.Anomalia,
                self.PanSeq,
                self.VPanSeq,
                self.DistGps,
                self.Distanza,
                self.DistEucl,
                self.DeltaSec,
                self.Vmedia,
                self.DeltaDir]
                # self.Strada]


def DistanzaTraDuePunti(x1, y1, x2, y2):
    dx = x2 - x1
    dy = y2 - y1
    DistQuadrata = dx ** 2 + dy ** 2
    Risultato = math.sqrt(DistQuadrata)
    return Risultato


def usage():
    print( "for help use -h or --help")
    sys.exit(0)


def paramDefinition():
    global tollDistEucl, minTrackSpace, min02Space, minStopTime, maxDeltaSec, DeltaSecLimite01_02, maxDurataExit, maxDistanza, maxVel

    tollDistEucl = 30  # m
    minTrackSpace = 10  # m
    min02Space = 30  # m
    minStopTime = 30  # sec
    maxDeltaSec = 1800  # sec
    DeltaSecLimite01_02 = 1800  # 30 minuti
    maxDistanza = 7000  # m
    maxDurataExit = 300  # sec
    maxVel = 250  # km/h

    fLog.write('tollDistEucl = 30 m\n')
    fLog.write('minTrackSpace = 10 m\n')
    fLog.write('min02Space = 30 m\n')
    fLog.write('minStopTime = 30 sec\n')
    fLog.write('maxDeltaSec = 1800 sec\n')
    fLog.write('DeltaSecLimite01_02 = 1800 sec\n')
    fLog.write('maxDistanza = 7000 m\n')
    fLog.write('maxDurataExit = 300 sec\n')
    fLog.write('maxVel = 250 km/h\n')

'''
def appendGPSTrack(punto):
    track = GPSTrack(Id_terminale=punto.Id_terminale,
                     Id_puntoTerm=punto.nPunto,
                     DataOraUTC=punto.DataOraUTC,
                     DataOraLoc=punto.DataOraLoc,
                     LonWGS84=punto.LonWGS84,
                     LatWGS84=punto.LatWGS84,
                     XUTM32=punto.XUTM32,
                     YUTM32=punto.YUTM32,
                     ##                    PRO_COM=punto.PRO_COM,
                     ##                    Bordo=punto.Bordo,
                     ##                    BordoSeq=punto.BordoSeq,
                     Velocita=punto.Velocita,
                     Direzione=punto.Direzione,
                     Qualita=punto.Qualita,
                     PanState=punto.PanState,
                     VPanState=punto.VPanState,
                     TrckType=punto.TrckType,
                     PanSeq=punto.PanSeq,
                     VPanSeq=punto.VPanSeq,
                     DistGps=punto.Distanza,
                     Distanza=punto.Distanza,
                     DistEucl=punto.DistEucl,
                     DeltaSec=punto.DeltaSec,
                     Vmedia=punto.Vmedia,
                     DeltaDir=punto.DeltaDir)
                     # Strada=punto.Strada)
'''

def appendGPSTrack(punto):
    track = GPSTrack(Id_terminale=punto[1],
                     Id_puntoTerm=punto[2],
                     DataOraUTC=punto[3],
                     DataOraLoc=punto[4],
                     LonWGS84=punto[5],
                     LatWGS84=punto[6],
                     XUTM32=punto[7],
                     YUTM32=punto[8],
                     Velocita=punto[9],
                     Direzione=punto[10],
                     Qualita=punto[11],
                     PanState=punto[12],
                     VPanState=punto[13],
                     TrckType=punto[14],
                     PanSeq=punto[15],
                     VPanSeq=punto[16],
                     DistGps=punto[17],
                     Distanza=punto[17],
                     DistEucl=punto[18],
                     DeltaSec=punto[19],
                     Vmedia=punto[20],
                     DeltaDir=punto[21])

    termTracksList.append(track)


def appendPuntoViaggio(track, Id_viaggio, nViaggio, nPuntoV, Progressiva):
    if track.VPanState > 0:
        if track.DistEucl > 0:
            Tortuosita = track.Distanza / float(track.DistEucl)
            Tortuosita = '%.2f' % Tortuosita
        else:
            Tortuosita = -1
    else:
        Tortuosita = None
    Punto = PuntoViaggio(Id_viaggio=Id_viaggio,
                         Id_terminale=track.Id_terminale,
                         Id_puntoTerm=track.Id_puntoTerm,
                         nViaggio=nViaggio,
                         nPuntoV=nPuntoV,
                         DataOraUTC=track.DataOraUTC,
                         DataOraLoc=track.DataOraLoc,
                         LonWGS84=track.LonWGS84,
                         LatWGS84=track.LatWGS84,
                         XUTM32=track.XUTM32,
                         YUTM32=track.YUTM32,
                         TrckType=track.TrckType,
                         Anomalia=track.Anomalia,
                         ##                 PRO_COM=track.PRO_COM,
                         ##                 Bordo=track.Bordo,
                         ##                 BordoSeq=track.BordoSeq,
                         Velocita=track.Velocita,
                         Direzione=track.Direzione,
                         DeltaDir=track.DeltaDir,
                         Qualita=track.Qualita,
                         PanState=track.PanState,
                         VPanState=track.VPanState,
                         VPanSeq=track.VPanSeq,
                         DistGps=track.DistGps,
                         Distanza=track.Distanza,
                         DistEucl=track.DistEucl,
                         Tortuosita=Tortuosita,
                         Progressiva=Progressiva,
                         DeltaSec=track.DeltaSec,
                         Vmedia=track.Vmedia)
                         # Strada=track.Strada)

    termPuntiViaggioList.append(Punto)


def appendViaggio(O_track, O_ProCom, D_track, D_ProCom, Id_viaggio, nViaggio, Distanza, nPunti):
    timeDiff = D_track.DataOraUTC - O_track.DataOraUTC
    DurataSec = (timeDiff.days * 86400 + timeDiff.seconds)
    Vmedia = 3.6 * (Distanza / float(DurataSec))
    if Vmedia > 250:
        Vmedia = 250
    V = Viaggio(Id_viaggio=Id_viaggio,
                Id_terminale=O_track.Id_terminale,
                nViaggio=nViaggio,
                Id_puntoOrig=O_track.Id_puntoTerm,
                Id_puntoDest=D_track.Id_puntoTerm,
                O_PRO_COM=O_ProCom,
                D_PRO_COM=D_ProCom,
                OTrckType=O_track.TrckType,
                DTrckType=D_track.TrckType,
                ODataOra=O_track.DataOraLoc,
                DDataOra=D_track.DataOraLoc,
                Distanza=Distanza,
                DurataSec=DurataSec,
                Vmedia='%.1f' % Vmedia,
                nPunti=nPunti)

    termViaggiList.append(V)


def appendSosta(A_track, A_ProCom, P_track, P_ProCom, Id_sosta, nSosta, Id_viaggioPrec):
    if A_track is None:
        S = Sosta(Id_sosta=Id_sosta,
                  Id_terminale=P_track.Id_terminale,
                  nSosta=nSosta,
                  Id_viaggioPrec=Id_viaggioPrec,
                  Id_puntoArrivo=None,
                  Id_puntoPartenza=P_track.Id_puntoTerm,
                  A_PRO_COM=None,
                  P_PRO_COM=P_ProCom,
                  ATrckType=None,
                  PTrckType=P_track.TrckType,
                  ADataOra=None,
                  PDataOra=P_track.DataOraLoc,
                  DistAP=None,
                  DurataSec=None)
    elif P_track is None:
        S = Sosta(Id_sosta=Id_sosta,
                  Id_terminale=A_track.Id_terminale,
                  nSosta=nSosta,
                  Id_viaggioPrec=Id_viaggioPrec,
                  Id_puntoArrivo=A_track.Id_puntoTerm,
                  Id_puntoPartenza=None,
                  A_PRO_COM=A_ProCom,
                  P_PRO_COM=None,
                  ATrckType=A_track.TrckType,
                  PTrckType=None,
                  ADataOra=A_track.DataOraLoc,
                  PDataOra=None,
                  DistAP=None,
                  DurataSec=None)
    else:
        timeDiff = P_track.DataOraUTC - A_track.DataOraUTC
        DurataSec = (timeDiff.days * 86400 + timeDiff.seconds)
        DistAP = int(round(DistanzaTraDuePunti(A_track.XUTM32, A_track.YUTM32, P_track.XUTM32, P_track.YUTM32)))
        S = Sosta(Id_sosta=Id_sosta,
                  Id_terminale=A_track.Id_terminale,
                  nSosta=nSosta,
                  Id_viaggioPrec=Id_viaggioPrec,
                  Id_puntoArrivo=A_track.Id_puntoTerm,
                  Id_puntoPartenza=P_track.Id_puntoTerm,
                  A_PRO_COM=A_ProCom,
                  P_PRO_COM=P_ProCom,
                  ATrckType=A_track.TrckType,
                  PTrckType=P_track.TrckType,
                  ADataOra=A_track.DataOraLoc,
                  PDataOra=P_track.DataOraLoc,
                  DistAP=DistAP,
                  DurataSec=DurataSec)

    termSosteList.append(S)


def checkDistEuclidea():
    anom = 'e'
    nTracksTerm = len(termTracksList)

    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkDistEuclidea\n' % termTracksList[0].Id_terminale)

    for nTrack in range(1, nTracksTerm + 1):
        if nTrack > 1:
            if termTracksList[nTrack - 1].VPanState in (1, 2):
                ## Controllo se Distanza < (DistEucl+tlooDistEucl) allora pongo Distanza = DistEucl
                if (termTracksList[nTrack - 1].DistEucl - termTracksList[nTrack - 1].Distanza) > tollDistEucl:
                    t = termTracksList[nTrack - 1]
                    if termToElab != '0':
                        fLog.write(
                            '%7d - npunto: %4d VPanState: %d \t== Distanza: %6d < (DistEucl+tollDistEucl): %6d ==\n' % (
                            t.Id_terminale, t.Id_puntoTerm, t.VPanState, t.Distanza, t.DistEucl))
                    termTracksList[nTrack - 1].Distanza = termTracksList[nTrack - 1].DistEucl
                    termTracksList[nTrack - 1].TrckType = t.TrckType[0] + 'a'
                    termTracksList[nTrack - 1].Anomalia = t.Anomalia + anom


def checkDeltaSecZero():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkDeltaSecZero\n' % termTracksList[0].Id_terminale)

    anom = 'z'

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            if termTracksList[nTrack - 1].VPanState in (1, 2):
                ## Controllo DeltaSec = 0
                if termTracksList[nTrack - 1].DeltaSec == 0:
                    tp = termTracksList[nTrack - 2]  ## traccia precedente
                    tc = termTracksList[nTrack - 1]  ## traccia corrente
                    if tc.VPanSeq == '02':
                        ## elimino entrambi
                        iTracksToRemove.append(nTrack - 2)
                        iTracksToRemove.append(nTrack - 1)
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso \t== DeltaSec02 = 0 ==\n' % (
                            tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState))
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso \t== DeltaSec02 = 0 ==\n' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState))
                    elif tc.VPanSeq in ('01', '11'):
                        ## elimino il corrente (secondo della coppia) ed incremento la distanza del successivo
                        iTracksToRemove.append(nTrack - 1)
                        if termToElab != '0':
                            fLog.write(
                                '%7d - npunto: %4d VPanState: %d rimosso, Distanza succ. incrementata \t== DeltaSec01_11 = 0 ==\n' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState))
                        termTracksList[nTrack].Distanza = termTracksList[nTrack].Distanza + termTracksList[
                            nTrack - 1].Distanza
                        termTracksList[nTrack].TrckType = termTracksList[nTrack].TrckType[0] + 'a'
                        termTracksList[nTrack].Anomalia = termTracksList[nTrack].Anomalia + anom
                    elif tc.VPanSeq == '12':
                        ## elimino il precedente (primo della coppia) ed incremento la distanza del corrente
                        iTracksToRemove.append(nTrack - 2)
                        if termToElab != '0':
                            fLog.write(
                                '%7d - npunto: %4d VPanState: %d rimosso, Distanza succ. incrementata \t== DeltaSec12 = 0 ==\n' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState))
                        termTracksList[nTrack - 1].Distanza = termTracksList[nTrack - 1].Distanza + termTracksList[
                            nTrack - 2].Distanza
                        termTracksList[nTrack - 1].TrckType = termTracksList[nTrack - 1].TrckType[0] + 'a'
                        termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                    nTrack = nTrack - len(iTracksToRemove)
                    updateTrackList(nTrack - 1)


def checkMinTrackSpace():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkMinTrackSpace\n' % termTracksList[0].Id_terminale)

    anom = 'd'

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            if termTracksList[nTrack - 1].VPanState in (1, 2):
                ## Controllo minTrackSpace ===
                if termTracksList[nTrack - 1].Distanza < minTrackSpace:
                    tp = termTracksList[nTrack - 2]  ## traccia precedente
                    tc = termTracksList[nTrack - 1]  ## traccia corrente
                    if tc.VPanSeq in ('01', '11'):
                        ## elimino il corrente (secondo della coppia) ed incremento la distanza del successivo
                        iTracksToRemove.append(nTrack - 1)
                        if termToElab != '0':
                            fLog.write(
                                '%7d - npunto: %4d VPanState: %d  Distanza01_11: %6d rimosso, Distanza succ. incrementata \t== Distanza01_11 < minTrackSpace ==\n' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState, tc.Distanza))
                        termTracksList[nTrack].Distanza = termTracksList[nTrack].Distanza + termTracksList[
                            nTrack - 1].Distanza
                        termTracksList[nTrack].TrckType = termTracksList[nTrack].TrckType[0] + 'a'
                        termTracksList[nTrack].Anomalia = termTracksList[nTrack].Anomalia + anom
                    elif tc.VPanSeq == '12':
                        ## elimino il precedente (primo della coppia) ed incremento la distanza del corrente
                        iTracksToRemove.append(nTrack - 2)
                        if termToElab != '0':
                            fLog.write(
                                '%7d - npunto: %4d VPanState: %d  Distanza_12  : %6d rimosso, Distanza corrente incrementata \t== Distanza12 < minTrackSpace ==\n' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState, tc.Distanza))
                        termTracksList[nTrack - 1].Distanza = termTracksList[nTrack - 1].Distanza + termTracksList[
                            nTrack - 2].Distanza
                        termTracksList[nTrack - 1].TrckType = termTracksList[nTrack - 1].TrckType[0] + 'a'
                        termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                    nTrack = nTrack - len(iTracksToRemove)
                    updateTrackList(nTrack - 1)


def checkMin02Space():
    if len(termTracksList) > 1:
        fLog.write('%7d checkMin02Space\n' % termTracksList[0].Id_terminale)

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            ## === min02Space ===
            if termTracksList[nTrack - 1].VPanSeq == '02':
                tp = termTracksList[nTrack - 2]  ## traccia precedente
                tc = termTracksList[nTrack - 1]  ## traccia corrente
                if termTracksList[nTrack - 1].Distanza < min02Space:
                    ## viaggio troppo corto, elimino entrambi
                    iTracksToRemove.append(nTrack - 2)
                    iTracksToRemove.append(nTrack - 1)
                    if termToElab != '0':
                        fLog.write('%7d - npunto: %4d VPanState: %d rimosso \t== min02Space ==\n' % (
                        tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState))
                        fLog.write('%7d - npunto: %4d VPanState: %d rimosso \t== min02Space ==\n' % (
                        tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState))
                nTrack = nTrack - len(iTracksToRemove)
                updateTrackList(nTrack - 1)


def checkMinStopTime():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkMinStopTime\n' % termTracksList[0].Id_terminale)

    anom = 's'

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            ## === minStopTime ===
            if termTracksList[nTrack - 1].VPanSeq == '20':
                tp = termTracksList[nTrack - 2]  ## traccia precedente
                tc = termTracksList[nTrack - 1]  ## traccia corrente
                if termTracksList[nTrack - 1].DeltaSec < minStopTime:
                    ## sosta troppo breve, elimino il secondo (accensione) e trasformo in movimento il primo (spegnimento)
                    iTracksToRemove.append(nTrack - 1)
                    if termToElab != '0':
                        fLog.write('%7d - npunto: %4d VPanState: %d trasf. in VPanState: 1 \t== minStopTime ==\n' % (
                        tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState))
                        fLog.write('%7d - npunto: %4d VPanState: %d rimosso \t== minStopTime ==\n' % (
                        tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState))
                    termTracksList[nTrack - 2].VPanState = 1
                    termTracksList[nTrack - 2].TrckType = '1a'
                    termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                nTrack = nTrack - len(iTracksToRemove)
                updateTrackList(nTrack - 1)


def checkExitArea():
    if len(termTracksList) > 1:
        fLog.write('%7d checkExitArea\n' % termTracksList[0].Id_terminale)

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            tp = termTracksList[nTrack - 2]  ## traccia precedente
            tc = termTracksList[nTrack - 1]  ## traccia corrente
            motivoOut = '\t== Uscita Area =='
            motivoIn = '\t== Entrata Area =='
            if tc.BordoSeq == '11':
                if tc.VPanSeq == '11' and tc.DeltaSec > maxDurataExit:
                    ##  cambio a VPanState=2 e TrckType='2s' il primo e cambio a VPanState=0 e TrckType='0s' ilcorrente
                    fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2s ' % (
                    tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivoOut + '\n')
                    termTracksList[nTrack - 2].TrckType = '2s'
                    fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 0 - TrckType = 0s ' % (
                    tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivoIn + '\n')
                    termTracksList[nTrack - 1].TrckType = '0s'
                    termTracksList[nTrack - 2].VPanState = 2
                    termTracksList[nTrack - 1].VPanState = 0
                    nTrack = nTrack - len(iTracksToRemove)
                    updateTrackList(nTrack - 1)
                elif tc.VPanSeq == '12' and tc.DeltaSec > maxDurataExit:
                    ## cambio a VPanState=2 e TrckType='2s' il primo ed elimino il corrente
                    fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2s ' % (
                    tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivoOut + '\n')
                    termTracksList[nTrack - 2].TrckType = '2s'
                    termTracksList[nTrack - 2].VPanState = 2
                    iTracksToRemove.append(nTrack - 1)
                    fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                    tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivoIn + '\n')
                    nTrack = nTrack - len(iTracksToRemove)
                    updateTrackList(nTrack - 1)


def checkMaxDistanza():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkMaxDistanza\n' % termTracksList[0].Id_terminale)

    anom = 'D'
    motivo = '\t== Distanza > maxDistanza =='

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            if termTracksList[nTrack - 1].VPanSeq == '20':
                continue
            if (termTracksList[nTrack - 1].Distanza > maxDistanza):
                tp = termTracksList[nTrack - 2]  ## traccia precedente
                tc = termTracksList[nTrack - 1]  ## traccia corrente
                if (tc.Vmedia < 5):
                    if tc.VPanSeq == '01':
                        ## elimino il primo e cambio a VPanState=0 e TrckType='0m' il corrente
                        iTracksToRemove.append(nTrack - 2)
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                            tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                            fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 0 - TrckType = 0m ' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                        termTracksList[nTrack - 1].TrckType = '0m'
                        termTracksList[nTrack - 1].VPanState = 0
                        termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                    elif tc.VPanSeq == '02':
                        ## elimino entrambi
                        iTracksToRemove.append(nTrack - 2)
                        iTracksToRemove.append(nTrack - 1)
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                            tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                    elif tc.VPanSeq == '11':
                        ##  cambio a VPanState=2 e TrckType='2m' il primo e cambio a VPanState=0 e TrckType='0m' ilcorrente
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2m ' % (
                            tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                        termTracksList[nTrack - 2].TrckType = '2m'
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 0 - TrckType = 0m ' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                        termTracksList[nTrack - 1].TrckType = '0m'
                        termTracksList[nTrack - 2].VPanState = 2
                        termTracksList[nTrack - 1].VPanState = 0
                        termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                        termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                    elif tc.VPanSeq == '12':
                        ## cambio a VPanState=2 e TrckType='2m' il primo ed elimino il corrente
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2m ' % (
                            tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                        termTracksList[nTrack - 2].TrckType = '2m'
                        termTracksList[nTrack - 2].VPanState = 2
                        termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                        iTracksToRemove.append(nTrack - 1)
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                    nTrack = nTrack - len(iTracksToRemove)
                    updateTrackList(nTrack - 1)
                else:
                    termTracksList[nTrack - 1].TrckType = termTracksList[nTrack - 1].TrckType[0] + 'a'
                    termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                    if termToElab != '0':
                        fLog.write('%7d - npunto: %4d VPanState: %d  ' % (
                        tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + ' Vel. >= 5 non modifico\n')


def check01_02MaxDeltaSec():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d check01_02MaxDeltaSec\n' % termTracksList[0].Id_terminale)

    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    anom = 'S'
    motivo = '\t== Start point errato =='

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            if (termTracksList[nTrack - 2].PanState) == 0:
                if (termTracksList[nTrack - 1].DeltaSec > DeltaSecLimite01_02):
                    tp = termTracksList[nTrack - 2]  ## traccia precedente
                    tc = termTracksList[nTrack - 1]  ## traccia corrente
                    termTracksList[nTrack - 2].TrckType = termTracksList[nTrack - 2].TrckType[0] + 'a'
                    termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                    if tc.PanSeq == '01' and tc.Vmedia < 10:
                        vel = max(random.randint(5, 20), tc.Velocita)
                        VmediaCorr = vel * 0.5
                        DeltaSecCorr = math.trunc(tc.Distanza / (VmediaCorr / 3.6))
                        utc = tc.DataOraUTC - datetime.timedelta(seconds=DeltaSecCorr)
                        termTracksList[nTrack - 2].DataOraUTC = utc
                        utc = utc.replace(tzinfo=from_zone)
                        local = utc.astimezone(to_zone)
                        DataOraLoc = local.strftime("%Y-%m-%d %H:%M:%S")
                        termTracksList[nTrack - 2].DataOraLoc = DataOraLoc
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  Vel: %d Vmedia: %d' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState, \
                            tc.Velocita, tc.Vmedia) + motivo + ' Correggo timestamp precedente ')
                            fLog.write('Distanza: %d VmediaCorr: %d DeltaSecCorr: %d  \n' % (
                            tc.Distanza, VmediaCorr, DeltaSecCorr))
                    elif tc.PanSeq == '02' and tc.Vmedia < 5:
                        VmediaCorr = random.triangular(2, 40, 12)
                        DeltaSecCorr = math.trunc(tc.Distanza / (VmediaCorr / 3.6))
                        utc = tc.DataOraUTC - datetime.timedelta(seconds=DeltaSecCorr)
                        termTracksList[nTrack - 2].DataOraUTC = utc
                        utc = utc.replace(tzinfo=from_zone)
                        local = utc.astimezone(to_zone)
                        DataOraLoc = local.strftime("%Y-%m-%d %H:%M:%S")
                        termTracksList[nTrack - 2].DataOraLoc = DataOraLoc
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  Vel: %d Vmedia: %d' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState, \
                            tc.Velocita, tc.Vmedia) + motivo + ' Correggo timestamp precedente ')
                            fLog.write('Distanza: %d DeltaSec: %d VmediaCorr: %d DeltaSecCorr: %d  \n' % (
                            tc.Distanza, tc.DeltaSec, VmediaCorr, DeltaSecCorr))
                    else:
                        if termToElab != '0':
                            fLog.write(
                                '\n%7d - npunto: %4d VPanState: %d  Vel: %d Vmedia: %d Distanza: %d DeltaSec: %d' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState, \
                                tc.Velocita, tc.Vmedia, tc.Distanza,
                                tc.DeltaSec) + motivo + ' Vmedia >= 10(01) o 5(02) ,non correggo timestamp precedente \n')
                    LogTermTracks(nTrack - 1, nTrack + 1)
                    updateTrackList(nTrack - 1)


def checkMaxDeltaSec():
    if len(termTracksList) > 1:
        if termToElab != '0':
            fLog.write('%7d checkMaxDeltaSec\n' % termTracksList[0].Id_terminale)

    anom = 'T'
    motivo = '\t== DeltaSec > maxDeltaSec =='

    nTrack = 0

    while nTrack < len(termTracksList):
        nTrack = nTrack + 1
        if nTrack > 1:
            if termTracksList[nTrack - 1].VPanSeq == '20':
                continue
            if (termTracksList[nTrack - 2].PanState) != 0:
                if (termTracksList[nTrack - 1].DeltaSec > maxDeltaSec):
                    tp = termTracksList[nTrack - 2]  ## traccia precedente
                    tc = termTracksList[nTrack - 1]  ## traccia corrente

                    if (tc.Vmedia < 5):
                        if tc.VPanSeq == '01':
                            ## elimino il primo e cambio a VPanState=0 e TrckType='0m' il corrente
                            iTracksToRemove.append(nTrack - 2)
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                                fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 0 - TrckType = 0m ' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                            termTracksList[nTrack - 1].TrckType = '0m'
                            termTracksList[nTrack - 1].VPanState = 0
                            termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                        elif tc.VPanSeq == '02':
                            ## elimino entrambi
                            iTracksToRemove.append(nTrack - 2)
                            iTracksToRemove.append(nTrack - 1)
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                                fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                        elif tc.VPanSeq == '11':
                            ##  cambio a VPanState=2 e TrckType='2m' il primo e cambio a VPanState=0 e TrckType='0m' ilcorrente
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2m ' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                            termTracksList[nTrack - 2].TrckType = '2m'
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 0 - TrckType = 0m ' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                            termTracksList[nTrack - 1].TrckType = '0m'
                            termTracksList[nTrack - 2].VPanState = 2
                            termTracksList[nTrack - 1].VPanState = 0
                            termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                            termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                        elif tc.VPanSeq == '12':
                            ## cambio a VPanState=2 e TrckType='2m' il primo ed elimino il corrente
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d  VPanState da 1 a 2 - TrckType = 2m ' % (
                                tp.Id_terminale, tp.Id_puntoTerm, tp.VPanState) + motivo + '\n')
                            termTracksList[nTrack - 2].TrckType = '2m'
                            termTracksList[nTrack - 2].VPanState = 2
                            termTracksList[nTrack - 2].Anomalia = termTracksList[nTrack - 2].Anomalia + anom
                            iTracksToRemove.append(nTrack - 1)
                            if termToElab != '0':
                                fLog.write('%7d - npunto: %4d VPanState: %d rimosso ' % (
                                tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + '\n')
                        nTrack = nTrack - len(iTracksToRemove)
                        updateTrackList(nTrack - 1)
                    else:
                        termTracksList[nTrack - 1].TrckType = termTracksList[nTrack - 1].TrckType[0] + 'a'
                        termTracksList[nTrack - 1].Anomalia = termTracksList[nTrack - 1].Anomalia + anom
                        if termToElab != '0':
                            fLog.write('%7d - npunto: %4d VPanState: %d  ' % (
                            tc.Id_terminale, tc.Id_puntoTerm, tc.VPanState) + motivo + ' Vel. >= 5 non modifico\n')


def removeTracks():
    global iTracksToRemove

    ## ordino gli indici in ordine decrescente, perché il n. di elementi si riduce ad ogni del
    if len(iTracksToRemove) > 0:
        iTracksToRemove.sort(reverse=True)
        ##        print 'remove Tracks - index iTracksToRemove sorted: ', iTracksToRemove
        for iTrack in iTracksToRemove:
            ##            print 'numero di tracks nella Lista: ', len(termTracksList)
            ##            print 'Elimino track index n.: ', iTrack, 'nPunto: ', termTracksList[iTrack].nPunto
            del termTracksList[iTrack]

    iTracksToRemove = []


def cleanTrackSeq():
    LogTermTracks(1, len(termTracksList))

    checkDeltaSecZero()

    checkDistEuclidea()

    checkMinStopTime()

    #    checkExitArea()

    check01_02MaxDeltaSec()

    checkMaxDistanza()

    checkMaxDeltaSec()

    #    checkMinTrackSpace()

    checkMin02Space()

    lastUpdateTrackList()
    LogTermTracks(1, len(termTracksList))
    scriviTermTracks()


def updateTrackList(fromNTrack):
    # aggiorno solo da FromNTrack in poi

    removeTracks()

    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return

    ##  Controllo corretta sequenza Virtual Panel State

    for nTrack in range(fromNTrack,
                        min(fromNTrack + 4, nTracksTerm + 1)):  ## nella funzione range il secondo estremo è escluso
        if nTrack > 1:
            VPanSeq = str(10 * termTracksList[nTrack - 2].VPanState + termTracksList[nTrack - 1].VPanState).zfill(2)
            termTracksList[nTrack - 1].VPanSeq = VPanSeq
            if VPanSeq not in ('02', '01', '11', '12', '20'):
                fLog.write(
                    '=== updateTrackList ===' + str(termTracksList[nTrack - 1].Id_terminale) + '- npunto: ' + str(
                        termTracksList[nTrack - 1].Id_puntoTerm) + ' - ' + VPanSeq + ' Inconsistent\n')
                print('sequenza VPan errata - programma interrotto')
                LogTermTracks(1, len(termTracksList))
                chiudiFiles()
                sys.exit(2)

    ## se la sequenza è corretta procedi ad aggiornare i valori delta

    for nTrack in range(fromNTrack, min(fromNTrack + 4, nTracksTerm + 1)):
        if nTrack > 1:
            timeDiff = termTracksList[nTrack - 1].DataOraUTC - termTracksList[nTrack - 2].DataOraUTC
            DistEucl = int(round(
                DistanzaTraDuePunti(termTracksList[nTrack - 2].XUTM32, termTracksList[nTrack - 2].YUTM32,
                                    termTracksList[nTrack - 1].XUTM32, termTracksList[nTrack - 1].YUTM32)))
            DeltaSec = (timeDiff.days * 86400 + timeDiff.seconds)
            termTracksList[nTrack - 1].DeltaSec = DeltaSec
            termTracksList[nTrack - 1].DistEucl = DistEucl
            if termTracksList[nTrack - 1].Distanza < termTracksList[nTrack - 1].DistEucl:
                if (termTracksList[nTrack - 1].DistEucl - termTracksList[nTrack - 1].Distanza) > 30:
                    termTracksList[nTrack - 1].Distanza = termTracksList[nTrack - 1].DistEucl
            if termTracksList[nTrack - 1].VPanState > 0:
                if DeltaSec > 0.1:
                    Vmedia = int(round(3.6 * (float(termTracksList[nTrack - 1].Distanza) / float(DeltaSec))))
                else:
                    Vmedia = -1
            else:
                Vmedia = None
            termTracksList[nTrack - 1].Vmedia = Vmedia


def lastUpdateTrackList():
    removeTracks()

    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return

    if len(termTracksList) > 0:
        ## se primo punto con Pannello 2 viene rimosso dalla lista
        if (termTracksList[0].VPanState == 2):
            fLog.write('Terminale: ' + str(termTracksList[0].Id_terminale) + ' primo punto VPan 2 rimosso\n')
            del termTracksList[0]
        ## se ultimo punto con Pannello 0 viene rimosso dalla lista
        if (termTracksList[len(termTracksList) - 1].VPanState == 0):
            fLog.write('Terminale: ' + str(
                termTracksList[len(termTracksList) - 1].Id_terminale) + ' ultimo punto VPan 0 rimosso\n')
            del termTracksList[len(termTracksList) - 1]

    nTracksTerm = len(termTracksList)

    if nTracksTerm == 0:
        return

    ##  Controllo finale corretta sequenza Virtual Panel State

    termTracksList[0].VPanSeq = None
    for nTrack in range(1, nTracksTerm + 1):  ## nella funzione range il secondo estremo è escluso
        if nTrack > 1:
            VPanSeq = str(10 * termTracksList[nTrack - 2].VPanState + termTracksList[nTrack - 1].VPanState).zfill(2)
            termTracksList[nTrack - 1].VPanSeq = VPanSeq
            if VPanSeq not in ('02', '01', '11', '12', '20'):
                fLog.write('=== Finale ===' + str(termTracksList[nTrack - 1].Id_terminale) + '- npunto: ' + str(
                    termTracksList[nTrack - 1].Id_puntoTerm) + ' - ' + VPanSeq + ' Inconsistent\n')
                print('sequenza VPan errata - programma interrotto')
                LogTermTracks(1, len(termTracksList))
                chiudiFiles()
                sys.exit(2)

    ## se la sequenza è corretta procedi ad aggiornare i valori delta
    for nTrack in range(1, nTracksTerm + 1):
        if nTrack > 1:
            timeDiff = termTracksList[nTrack - 1].DataOraUTC - termTracksList[nTrack - 2].DataOraUTC
            DistEucl = int(round(
                DistanzaTraDuePunti(termTracksList[nTrack - 2].XUTM32, termTracksList[nTrack - 2].YUTM32,
                                    termTracksList[nTrack - 1].XUTM32, termTracksList[nTrack - 1].YUTM32)))
            if termTracksList[nTrack - 1].VPanState in (1, 2):
                ## Controllo se Distanza < DistEucl allora pongo Distanza = DistEucl
                if termTracksList[nTrack - 1].Distanza < termTracksList[nTrack - 1].DistEucl:
                    if (termTracksList[nTrack - 1].DistEucl - termTracksList[nTrack - 1].Distanza) > 30:
                        t = termTracksList[nTrack - 1]
                        fLog.write('%7d - npunto: %4d VPanState: %d == Distanza: %6d < DistEucl: %6d ==\n' % (
                        t.Id_terminale, t.Id_puntoTerm, t.VPanState, t.Distanza, t.DistEucl))
                    termTracksList[nTrack - 1].Distanza = termTracksList[nTrack - 1].DistEucl
            DeltaSec = (timeDiff.days * 86400 + timeDiff.seconds)
            if termTracksList[nTrack - 1].VPanState > 0:
                if DeltaSec > 0.1:
                    Vmedia = int(round(3.6 * (float(termTracksList[nTrack - 1].Distanza) / float(DeltaSec))))
                else:
                    Vmedia = -1
            else:
                Vmedia = None
            DeltaDir = termTracksList[nTrack - 1].Direzione - termTracksList[nTrack - 2].Direzione
            if (DeltaDir < -180):
                DeltaDir = DeltaDir + 360
            elif (DeltaDir > 180):
                DeltaDir = DeltaDir - 360
        else:
            DeltaSec = None
            DistEucl = None
            Vmedia = None
            DeltaDir = None

        termTracksList[nTrack - 1].DeltaSec = DeltaSec
        termTracksList[nTrack - 1].DistEucl = DistEucl
        ##        if Vmedia > 250:
        ##            Vmedia = 250
        termTracksList[nTrack - 1].Vmedia = Vmedia
        termTracksList[nTrack - 1].DeltaDir = DeltaDir


def LogTermTracks(fromNTrack, toNTrack):
    if termToElab == '0':
        return
    fLog.write(
        "nTrack Id_term nPunto DataOraUTC Velocita Direzione Qualita PanState PanSeq VPanState VPanSeq TrckType Anomalia DistGps Distanza DistEucl DeltaSec Vmedia DeltaDir\n")
    nTrack = 0
    for track in termTracksList:
        nTrack = nTrack + 1
        if nTrack > toNTrack:
            break
        if nTrack >= fromNTrack:
            fLog.write('%6d %8s' % (nTrack, str(track.Id_terminale)))
            fLog.write('%7s' % str(track.Id_puntoTerm))
            fLog.write('%20s' % str(track.DataOraUTC))
            ##            fLog.write('%3s' %str(track.Bordo))
            ##            fLog.write('%4s' %str(track.BordoSeq))
            fLog.write('%6s' % str(track.Velocita))
            fLog.write('%4s' % str(track.Direzione))
            fLog.write('%3s' % str(track.Qualita))
            fLog.write('%3s' % str(track.PanState))
            fLog.write('%4s' % str(track.PanSeq))
            fLog.write('%3s' % str(track.VPanState))
            fLog.write('%4s' % str(track.VPanSeq))
            fLog.write('%4s' % str(track.TrckType))
            fLog.write('%6s' % str(track.Anomalia))
            fLog.write('%7s' % str(track.DistGps))
            fLog.write('%7s' % str(track.Distanza))
            fLog.write('%7s' % str(track.DistEucl))
            fLog.write('%8s' % str(track.DeltaSec))
            fLog.write('%6s' % str(track.Vmedia))
            fLog.write('%5s' % str(track.DeltaDir))
            fLog.write('\n')


def scriviTermTracks():
    for track in termTracksList:
        csvTWriter.writerow(track.csvRow())


def scriviTermViaggi():
    for Viaggio in termViaggiList:
        csvVWriter.writerow(Viaggio.csvRow())
    for PuntoViaggio in termPuntiViaggioList:
        csvPWriter.writerow(PuntoViaggio.csvRow())


def scriviTermSoste():
    for Sosta in termSosteList:
        csvSWriter.writerow(Sosta.csvRow())


def elaboraViaggi():
    nTracksTerm = len(termTracksList)
    # elaboro solo se sono rimasti almeno 10 punti
    if nTracksTerm < 10:
        return
    nViaggio = 0
    nPuntoV = 0
    nTrack = 0
    for track in termTracksList:
        nTrack = nTrack + 1
        nPuntoV = nPuntoV + 1
        if track.VPanState == 0:
            trackCodPro = checkProvincia(track.LonWGS84, track.LatWGS84)
            if trackCodPro == '58':
                trackProCom = checkComune(track.LonWGS84, track.LatWGS84)
                if trackProCom == '58091':
                    entroGra = checkGra(track.LonWGS84, track.LatWGS84)
                    if entroGra:
                        trackProCom = '58000'
            else:
                trackProCom = trackCodPro
            if nTrack == 1:
                P_track = track
                P_ProCom = trackProCom
                Id_sosta = track.Id_terminale * 10000
                nSosta = 1
                appendSosta(None, None, P_track, P_ProCom, Id_sosta, nSosta, None)
            else:
                A_track = D_track
                A_ProCom = D_ProCom
                P_track = track
                P_ProCom = trackProCom
                nSosta = nSosta + 1
                Id_sosta = Id_viaggio
                appendSosta(A_track, A_ProCom, P_track, P_ProCom, Id_sosta, nSosta, Id_viaggio)
            nPuntoV = 1
            Progressiva = 0
            nViaggio = nViaggio + 1
            O_track = track
            O_ProCom = trackProCom
            Id_viaggio = track.Id_terminale * 10000 + nViaggio
        ##            if nViaggio >= 1000:
        ##                Id_viaggio = Id_viaggio *-1   ## n cifre non sufficienti
        elif track.VPanState == 1:
            Progressiva = Progressiva + track.Distanza
        elif track.VPanState == 2:
            trackCodPro = checkProvincia(track.LonWGS84, track.LatWGS84)
            if trackCodPro == '58':
                trackProCom = checkComune(track.LonWGS84, track.LatWGS84)
                if trackProCom == '58091':
                    entroGra = checkGra(track.LonWGS84, track.LatWGS84)
                    if entroGra:
                        trackProCom = '58000'
            else:
                trackProCom = trackCodPro
            D_track = track
            D_ProCom = trackProCom
            Progressiva = Progressiva + track.Distanza
            appendViaggio(O_track, O_ProCom, D_track, D_ProCom, Id_viaggio, nViaggio, Progressiva, nPuntoV)

        appendPuntoViaggio(track, Id_viaggio, nViaggio, nPuntoV, Progressiva)

    A_track = termTracksList[nTracksTerm - 1]
    A_ProCom = D_ProCom
    nSosta = nSosta + 1
    Id_sosta = Id_viaggio
    appendSosta(A_track, A_ProCom, None, None, Id_sosta, nSosta, Id_viaggio)


def chiudiFiles():
    fLog.close()
    fcsvT.close()
    fcsvP.close()
    fcsvV.close()
    fcsvS.close()


def checkProvincia(x, y):
    # create point geometry
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.SetPoint_2D(0, x, y)
    lyr_prov.SetSpatialFilter(pt)

    # go over all the polygons in the layer see if one include the point
    nprov = 0
    for feat_in in lyr_prov:
        #        nprov +=1
        #       flog.write('**** nprov: '+str(nprov)+' '+'codProv: '+str(feat_in.GetFieldAsString(idx_prov))+'\n')
        # roughly subsets features, instead of go over everything
        ply = feat_in.GetGeometryRef()

        # test
        if ply.Contains(pt):
            return feat_in.GetFieldAsString(idx_prov)
        else:
            return None


def checkComune(lon, lat):
    # create point geometry
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.SetPoint_2D(0, lon, lat)
    lyr_com.SetSpatialFilter(pt)

    # go over all the polygons in the layer see if one include the point
    for feat_in in lyr_com:

        # roughly subsets features, instead of go over everything
        ply = feat_in.GetGeometryRef()

        # test
        if ply.Contains(pt):
            return feat_in.GetFieldAsString(idx_com)
        else:
            return None


def checkGra(lon, lat):
    # create point geometry
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.SetPoint_2D(0, lon, lat)
    lyr_gra.SetSpatialFilter(pt)

    # go over all the polygons in the layer see if one include the point
    for feat_in in lyr_gra:

        # roughly subsets features, instead of go over everything
        ply = feat_in.GetGeometryRef()

        # test
        if ply.Contains(pt):
            return 1
        else:
            return 0


def process(terminale):
    # import adodbapi
    global termToElab
    global termTracksList, termPuntiViaggioList, termViaggiList, termSosteList
    global iTracksToRemove
    global fLog, fcsvT, fcsvP, fcsvV, fcsvS
    global lyr_prov, idx_prov, lyr_com, idx_com, lyr_gra
    import db_connect

    rows = []

    termToElab = terminale

    outFileLog = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\ElaboraViaggi.log"

    fLog = open(outFileLog, "w")

    paramDefinition()

    # load the shape file Province as a layer
    # load the shape file Province as a layer
    drv = ogr.GetDriverByName('ESRI Shapefile')
    # ds_prov = drv.Open("N:\\PHEV-V2G\\Procedure\\ElaboraViaggi\\DatiInput\\prov2011_WGS84geo.shp")
    ds_prov = drv.Open("C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\Limiti01012018\\ProvCM01012018\\ProvCM01012018_WGS84.shp")
    lyr_prov = ds_prov.GetLayer(0)

    # field index for which i want the data extracted
    # ("COD_PRO" was what i was looking for)
    # idx_prov = lyr_prov.GetLayerDefn().GetFieldIndex("COD_PRO")
    idx_prov = lyr_prov.GetLayerDefn().GetFieldIndex("COD_PROV")

    # load the shape file Comuni as a layer
    drv = ogr.GetDriverByName('ESRI Shapefile')
    ds_com = drv.Open("C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\Limiti01012018\\Com01012018\\Com01012018_WGS84.shp")
    lyr_com = ds_com.GetLayer(0)

    # field index for which i want the data extracted
    # ("PRO_COM" was what i was looking for)
    # idx_com = lyr_com.GetLayerDefn().GetFieldIndex("PRO_COM")
    idx_com = lyr_com.GetLayerDefn().GetFieldIndex("PRO_COM")

    # load the shape file gra as a layer
    drv = ogr.GetDriverByName('ESRI Shapefile')
    # ds_gra = drv.Open("N:\\PHEV-V2G\\Procedure\\ElaboraViaggi\\DatiInput\\GraWGS84geo.shp")
    ds_gra = drv.Open("C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\Limiti01012018\\VIABILITA_PROVINCIALE\\VIABILITA_PROVINCIALE.shp")
    lyr_gra = ds_gra.GetLayer(0)

    outFilePuntiCsv = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\PuntiViaggio.csv"
    outFileViaggiCsv = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\Viaggi.csv"
    outFileSosteCsv = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\Soste.csv"
    outFileTracksCsv = r"C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files\\GPSTrackDeltaPuliti.csv"

    global csvTWriter
    fcsvT = open(outFileTracksCsv, "w")
    csvTWriter = csv.writer(fcsvT, dialect='excel', delimiter=';', lineterminator='\n')
    csvTWriter.writerow(GPSTrack.csvHeading)

    global csvPWriter
    fcsvP = open(outFilePuntiCsv, "w")
    csvPWriter = csv.writer(fcsvP, dialect='excel', delimiter=';', lineterminator='\n')
    csvPWriter.writerow(PuntoViaggio.csvHeading)

    global csvVWriter
    fcsvV = open(outFileViaggiCsv, "w")
    csvVWriter = csv.writer(fcsvV, dialect='excel', delimiter=';', lineterminator='\n')
    csvVWriter.writerow(Viaggio.csvHeading)

    global csvSWriter
    fcsvS = open(outFileSosteCsv, "w")
    csvSWriter = csv.writer(fcsvS, dialect='excel', delimiter=';', lineterminator='\n')
    csvSWriter.writerow(Sosta.csvHeading)

    # connection string for an SQL server
    # _computername = "127.0.0.1"
    #_databasename = "Octo2013G1"
    _tableTermName = "nPuntiTerm"
    # this will open a MS-SQL table with Windows authentication
    # connStr = r"Initial Catalog=%s; Data Source=%s; Provider=SQLOLEDB.1; Integrated Security=SSPI" \
    #           % (_databasename, _computername)
    # create the connection
    # dbConn = adodbapi.connect(connStr, 600)
    # make a cursor on the connection
    # dbCur = dbConn.cursor()

    conn = db_connect.connect_Octo2015()
    cur = conn.cursor()

    if termToElab == '0':
        cur.execute("SELECT Distinct Id_terminale FROM %s Order by Id_terminale" % _tableTermName)
    else:
        cur.execute("SELECT Distinct Id_terminale FROM %s Where Id_terminale = " % _tableTermName + termToElab)

    terminali = cur.fetchall()

    nTerm = 0

    for terminale in terminali:
        nTerm = nTerm + 1
        ##        if nTerm < 8208:
        ##            continue
        ##        if nTerm > 20:
        ##            break
        # print(nTerm, ' Term: ', terminale.Id_terminale)
        print(nTerm, ' Term: ', terminale)

        termTracksList = []
        termPuntiViaggioList = []
        termViaggiList = []
        termSosteList = []
        iTracksToRemove = []

        cur.execute(
            # "SELECT * FROM [GPSpointsDelta] Where Id_terminale = %d Order By DataOraUTC, nPunto" % terminale.Id_terminale)
            "SELECT * FROM GPSpointsDelta Where Id_terminale = %d Order By DataOraUTC, nPunto" % terminale)
        rows = cur.fetchall()
        nPuntiAnno = cur.rowcount

        if (nPuntiAnno < 10):
            # fLog.write('Terminale: ' + str(terminale.Id_terminale) + ' ' + str(nPuntiAnno) + ' < 10\n')
            fLog.write('Terminale: ' + str(terminale) + ' ' + str(nPuntiAnno) + ' < 10\n')
            continue

        for punto in rows:
            appendGPSTrack(punto)

        cleanTrackSeq()

        elaboraViaggi()

        scriviTermViaggi()

        scriviTermSoste()

    cur.close()
    conn.close()
    del cur, conn, rows

    chiudiFiles()


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
            print(__doc__)
            sys.exit(0)
    # process arguments
    if len(args) == 0:
        usage()

    start_time = time.time()
    start_time_str = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime())

    print( "Processing data....")
    process(args[0])  # process() is defined elsewhere

    print(  "-" * 70)
    print( "START TIME:", start_time_str)
    print ( "END TIME  :", time.strftime("%d/%m/%Y %H:%M:%S", time.localtime()))

    elapsed_sec = round((time.time() - start_time))
    print(   "Elapsed time:")
    print (  elapsed_time(int(elapsed_sec), [' year', ' week', ' day', ' hour', ' minute', ' second'], add_s=True))


if __name__ == "__main__":
    main()
