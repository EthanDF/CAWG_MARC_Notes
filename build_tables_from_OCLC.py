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

def addDataTooclcNotes(list):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.executemany('insert into oclcNotes (oclc, noteFormOrder, notes) VALUES(?,?,?)',list)
    conn.commit()
    # print('1):', all_rows)
    conn.close()
    # return all_rows

def addOCLCNumstooclcSans500(oclcSans500, debug):

    print('adding records to oclcSans500')
    if debug == 1:
        return  oclcSans500
    t0 = time.perf_counter()

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('select OCN from oclcSans500')
    all_rows = c.fetchall()
    conn.close()

    oclcSans500List = []
    for o in all_rows:
        oclcSans500List.append(o[0])

    o = set(oclcSans500List)

    inputList = []
    inputListSet = set(inputList)
    for oclc in oclcSans500:
        if oclc not in o and oclc not in inputListSet:
            inputListSet.add(oclc)
            inputList.append([oclc])

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.executemany('insert into oclcSans500 (OCN) VALUES(?)', inputList)
    conn.commit()
    conn.close()

    timeLapse = time.perf_counter() - t0
    print('finished adding OCLC records to oclcSans500 in '+str(timeLapse)+' seconds')

def oclcmarcRead(debug=0):
    marcFile = 'testFiles\\oclcMARC.mrc'

    oclcList = []
    marc500List = []
    oclcSans500 = []

    recordCounter = 0
    with open(marcFile, 'rb') as fh:
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)

        for record in reader:
            recordCounter += 1
            # Get MARC Record Attributes

            try:
                oclcNumber = int(record['035']['a'].replace('(OCoLC)',''))
            except (AttributeError, TypeError, ValueError):
                oclcNumber = -1

            if debug == 1:
                print('\tOCLCNumber is '+str(oclcNumber))

            ldr06 = record.leader[6:7]
            if debug == 1:
                print('\tldr06 is ' + str(ldr06))

            form = 'x'
            if ldr06 in('a','t','s','m''p','i','j','c','d'):
                form = record['008'].value()[23:24]
            else:
                form = record['008'].value()[29:30]
            if debug == 1:
                print('\tform is ' + str(form))

            # gpoCheck = False
            # gpoVal = 'xxx'
            # try:
            #     gpoVal = record['040']['d']
            #     if str(gpoVal).upper() == 'GPO':
            #         gpoCheck = True
            # except (AttributeError, TypeError):
            #     gpoCheck = False

            # if debug == 1:
            #     print('\tMARC 040 $d is ' + str(gpoVal))
            #     print('\tgpoCheck ' + str(gpoCheck))

            oclcRecord = [oclcNumber, ldr06, form]
            oclcList.append(oclcRecord)

            if debug == 1:
                print('\tAlephBib Row looks like: '+str(oclcRecord))

            # Get MARC 500 Field Data

            marc500s = record.get_fields('500')
            if len(marc500s) == 0:
                oclcSans500.append(int(oclcNumber))
                if debug == 1:
                    print('\tadding to Sans500')
            marc500Counter = 0

            for note in marc500s:
                noteList = []
                noteList.append(oclcNumber)
                noteList.append(marc500Counter)
                noteList.append(note['a'])
                marc500List.append(noteList)
                marc500Counter += 1


            if debug == 1:
                print('\tMarc500 Fields are:')
                for marc500ListNotes in marc500s:
                    print('\t'+str(marc500ListNotes))



            # writeResultsToCSV('bibNotes.csv',marc500List)
    # writeResultsToCSV('oclcRecs.csv', oclcList)
    # writeResultsToCSV('oclcNotes.csv', marc500List)
    addDataTooclcNotes(marc500List)
    addOCLCNumstooclcSans500(oclcSans500, debug)
    return record
    # return (bibList, marc500List)

oclcmarcRead()
print("done!")