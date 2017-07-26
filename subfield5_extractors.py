from pymarc import *
import codecs
import csv
import sqlite3
import time

sqlite_file = 'notes_db.sqlite'

def writeResultsToCSV(file, list):
    outputFile = 'output_files\\'+file

    with codecs.open(outputFile, 'a', encoding='utf-8') as out:
        a = csv.writer(out, delimiter = ',', quoting=csv.QUOTE_ALL)
        a.writerows(list)

def oclcmarcRead(debug=0):
    marcFile = 'testFiles\\oclcMARC_FSU_v2.mrc'

    marc500List = []
    recordCounter = 0
    with open(marcFile, 'rb') as fh:
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)

        for record in reader:
            recordCounter += 1
            # Get MARC Record Attributes

            try:
                oclcNumber = int(record['035']['a'].replace('(OCoLC)', ''))
            except (AttributeError, TypeError, ValueError):
                oclcNumber = -1

            if debug == 1:
                print('\tOCLCNumber is ' + str(oclcNumber))

            print(str(recordCounter) + ', OCLC: ' + str(oclcNumber))

            marc500s = record.get_fields('500')
            if len(marc500s) == 0:
                continue
            for note in marc500s:
                noteList = []
                noteList.append(oclcNumber)
                noteList.append(note['a'])
                noteList.append(note['5'])
                if note['5'] is not None:
                    marc500List.append(noteList)


    return marc500List

def updateDataBase(marc500List,debug):

    for note in marc500List:
        oclcID = note[0]
        noteVal = note[1]
        sf5 = note[2]

        print('updating oclc = '+str(oclcID)+' and noteval = '+str(noteVal)+'....')

        conn = sqlite3.connect(sqlite_file)
        c = conn.cursor()
        # c.execute('UPDATE oclcnotes SET subfield5 = {mysf5} WHERE oclc = {my_id} and notes = {mynote}'.
        #           format(mysf5 = sf5, my_id=oclcID, mynote=noteVal))

        conn.execute(
            """UPDATE oclcnotes SET subfield5 = ? WHERE oclc = ? and notes = ? """,
            (sf5, oclcID, noteVal))

        conn.commit()
        # print('1):', all_rows)
        conn.close()
        print ('update complete!')

def runSB5Extract(debug=0):
    marc500List = oclcmarcRead(debug)
    updateDataBase(marc500List,debug)
    print('done')