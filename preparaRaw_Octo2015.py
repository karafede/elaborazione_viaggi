# -*- coding: iso-8859-15 -*-

"""preparaRaw.py

// ----------------------- preparaRaw -------------------------
//-------------------------------------------------------------
// Legge il file di testo originale completo con i dati Octotelematics,
// ne estrae la sezione indicata nella riga di comando (da recInizioSez a recFineSez).
// Da questa sezione, per ogni riga associa il campo data ed il campo ora nel tipo DATETIME,
// divide per 1000000 le coordinate geografiche WGS84  e
// scrive il nuovo record nel file di output in formato csv indicato sulla riga di comando
//
// uso: preparaRaw <nome_file_octo> <nome_file_sezione> <recInizioSez> <recFineSez>
//

Opzioni:
  -h, --help              show this help
  <nome_file_octo>
  <nome_file_sezione>
  <recInizioSez>
  <recFineSez>

Esempio:
to run in the command line

  python preparaRaw.py "extract_enea_dataset2_01_1-10000.csv" "DatiRaw01_1-100000.csv" 1 100000
"""

# to run in the command line
# python preparaRaw_Octo2015.py "MOB_SQUARE_0.csv" "DatiRaw_Octo_11_2015_1-10000000.csv" 1 10000000
# python preparaRaw_Octo2015.py "MOB_SQUARE_0.csv" "DatiRaw_Octo_11_2015_10000001-20000000.csv" 10000001 20000000
# python preparaRaw_Octo2015.py "MOB_SQUARE_0.csv" "DatiRaw_Octo_11_2015_20000001-31919514.csv" 20000001 31919514



__autore__ = "Massimo Mancini & Federico Karagulian"
__versione__ = "$Revision: 2.0 $"
__data__ = "$Date: 2019/11/06 $"

import sys
import math
import time, datetime
import getopt
import csv

import os
cwd = os.getcwd()
# change working directoy
# os.chdir('C:\\ENEA_CAS_WORK\\Mancini_stuff\\OctoPerFederico')
os.chdir('C:\\ENEA_CAS_WORK\\Mancini_stuff\\Octo2015\\extracted_files')
cwd = os.getcwd()


class Record(object):
    csvHeading = ["Id_record_raw",
                  "Id_terminale",
                  "DataOra",
                  "LonWGS84",
                  "LatWGS84",
                  "Velocita",
                  "Direzione",
                  "Qualita",
                  "Pannello",
                  "Distanza"]
                 # "Strada"]

    def __init__(self,
                 Id_record_raw="",
                 Id_terminale="",
                 DataOra="",
                 LonWGS84="",
                 LatWGS84="",
                 Velocita="",
                 Direzione="",
                 Qualita="",
                 Pannello="",
                 Distanza=""):
                 # Strada=""):
        self.Id_record_raw = Id_record_raw
        self.Id_terminale = Id_terminale
        self.DataOra = DataOra
        self.LonWGS84 = LonWGS84
        self.LatWGS84 = LatWGS84
        self.Velocita = Velocita
        self.Direzione = Direzione
        self.Qualita = Qualita
        self.Pannello = Pannello
        self.Distanza = Distanza
        # self.Strada = Strada

    def csvRow(self):
        return [self.Id_record_raw,
                self.Id_terminale,
                self.DataOra,
                self.LonWGS84,
                self.LatWGS84,
                self.Velocita,
                self.Direzione,
                self.Qualita,
                self.Pannello,
                self.Distanza]
                # self.Strada]


def usage():
    print("for help use -h or --help")
    sys.exit(0)

def process(args):
    finpName = args[0]
    foutCsvName = args[1]
    recIni = int(args[2])
    recFine = int(args[3])

    try:
        finp = open(finpName, "r")
    except:
        print("Error: can\'t find file or read data")
        sys.exit(1)

    fcsv = open(foutCsvName, "w")
    csvWriter = csv.writer(fcsv, dialect='excel', delimiter=';', lineterminator='\n')
    csvWriter.writerow(Record.csvHeading)


    # read first record
    recNo = 0

    line = finp.readline()
    line = line.rstrip('\n')
    line = line + ","

    type = 0
    while line:
        recNo = recNo + 1
        if recNo > recFine:
            break
        if recNo >= recIni:
            if recNo % 10000 == 0:
                print(recNo)
            campi = line.split(',')
            Id_record_raw = 100000000+recNo
            #            Id_record_raw = 200000000+recNo
            #            Id_record_raw = 300000000+recNo
            #            Id_record_raw = 400000000+recNo
            #            Id_record_raw = 500000000+recNo
            #            Id_record_raw = 600000000+recNo
            #            Id_record_raw = 700000000+recNo
            #            Id_record_raw = 800000000+recNo
            #            Id_record_raw = 900000000+recNo
            #            Id_record_raw = 1000000000+recNo
            #            Id_record_raw = 1100000000+recNo
            #            Id_record_raw = 1200000000 + recNo
            lonWGS84 = int(campi[4]) / 1000000.0
            latWGS84 = int(campi[3]) / 1000000.0
            recPunto = Record(Id_record_raw, campi[0], campi[1] + " " + campi[2], lonWGS84, latWGS84, \
                              campi[5], campi[6], campi[7], campi[8], campi[9]) # campi[10]
            csvWriter.writerow(recPunto.csvRow())

        line = finp.readline()

        if line:
            line = line.rstrip('\n')
            line = line + ","

    finp.close()
    fcsv.close()


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
    if len(args) == 0:
        usage()

    start_time = time.time()
    print("START TIME:", time.strftime("%H:%M:%S", time.localtime()))

    print("Processing data....")
    process(args)  # process() is defined elsewhere

    print("END TIME:", time.strftime("%H:%M:%S", time.localtime()))
    elapsed_sec = (time.time() - start_time)
    elapsed_readable = time.strftime("%H:%M:%S", time.gmtime(elapsed_sec))
    print("-" * 70)
    print("Elapsed time (HH:MM:SS):", elapsed_readable)


if __name__ == "__main__":
    main()











