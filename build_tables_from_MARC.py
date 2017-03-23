from pymarc import *
import codecs
import csv
import sqlite3
import time
# from main import getMaxKeyVal

sqlite_file = 'notes_db.sqlite'

def writeResultsToCSV(file, list):
    outputFile = 'output_files\\'+file

    with codecs.open(outputFile, 'a', encoding='utf-8') as out:
        a = csv.writer(out, delimiter = ',', quoting=csv.QUOTE_ALL)
        a.writerows(list)

def getExtantBibs():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('select distinct bibNumber from alephBibs')
    all_rows = c.fetchall()
    conn.close()

    extantBibList = []
    for bib in all_rows:
        extantBibList.append(bib[0])
    return extantBibList

def checkForOCLCNumber(oclcNumber):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('Select * from alephBibs WHERE OCN = {my_id}'.
              format(my_id=oclcNumber))
    all_rows = c.fetchall()
    conn.close()
    return all_rows

def addDataTobibList(list):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.executemany('insert into alephBibs (bibNumber, OCN, LDRForm, Form, GPO, GPub) VALUES(?,?,?,?,?,?)',list)
    conn.commit()
    # print('1):', all_rows)
    conn.close()
    # return all_rows

def addDataTobibNotes(list):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.executemany('insert into bibNotes (bib, noteOrder, note, OwnCodeCount, OwnCode) VALUES(?,?,?,?,?)',list)

    # print('1):', all_rows)
    conn.commit()
    conn.close()
    # return all_rows

def marcRead(debug=0):
    marcFile = 'testFiles\\alephMARC.mrc'

    bibList = []
    marc500List = []
    # maxBibNotes = getMaxKeyVal('bibNotes','keybibNotes')
    # maxBibNotes =+ 1
    extantBibs = getExtantBibs()
    extantBibSet = set(extantBibs)

    recordCounter = 0
    with open(marcFile, 'rb') as fh:
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)

        for record in reader:
            # print(recordCounter)
            record.force_utf8 = True
            recordCounter += 1
            # Get MARC Record Attributes

            recID = record['001']
            if recID is None:
                # print("\tERROR: " + str(recordCounter) + ': No Record ID (MARC 001) Found")')
                recID = recordCounter
            else:
                recID = recID.value()
            if debug == 1:
                print('Record Number: '+str(recordCounter)+', MARC 001: ' + str(recID))

            oclcNumber = -1

            try:
                marc035s = record.get_fields('035')
                for o in marc035s:
                    if debug == 1:
                        print(o)
                    if o['a'] is None:
                        continue
                    if o['a'][:7] == '(OCoLC)':
                        oclcNumber = int(o['a'].replace('(OCoLC)',''))
                        continue
                    else:
                        pass
            except (AttributeError, TypeError, ValueError):
                if debug == 1:
                    print('hit the 035 error!')
                oclcNumber = -1

            if debug == 1:
                print('\tOCLCNumber is '+str(oclcNumber))

            oclcNumberFound = False
            skipper = 0
            t0 = time.perf_counter()
            if int(recID) in extantBibSet:
                if int(recID) < 20000000:
                    checkOCL = checkForOCLCNumber(oclcNumber)
                    if len(checkOCL) > 0:
                        oclcNumberFound = True
                        skipper = 1
                else:
                    skipper = 1

            if debug == 1:
                if oclcNumberFound == True:
                    print('\tBib found in alephBibs by OCLCNumber: id = '+str(recID)+
                          'and oclcNumber ='+str(oclcNumber))
                else:
                    print('\tBib found in alephBibs id = ' + str(recID))

            timeLapse = time.perf_counter() - t0
            if debug == 1:
                print('\t\tList scan took ' + str(timeLapse))

            if debug == 1:
                print('\tskipper = ' + str(skipper))
                if skipper == 1:
                    print('\tSkipping!!!')

            if skipper == 1:
                continue

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

            gpoCheck = False
            gpoVal = 'xxx'
            try:
                gpoVal = record['040']['d']
                if str(gpoVal).upper() == 'GPO':
                    gpoCheck = True
            except (AttributeError, TypeError):
                gpoCheck = False

            if debug == 1:
                print('\tMARC 040 $d is ' + str(gpoVal))
                print('\tgpoCheck ' + str(gpoCheck))

            gPub = False
            gpoVal = record['008'].data[28:29]
            if gpoVal != ' ':
                gPub = True

            if debug == 1:
                print('\tgPub is: '+str(gPub)+'\n\tMARC008:28 is '+str(gpoVal))


            if skipper == 0:
                alephBib = [recID, oclcNumber, ldr06, form, gpoCheck, gPub]
                bibList.append(alephBib)

                if debug == 1:
                    print('\tAlephBib Row looks like: '+str(alephBib))

            # Get MARC 500 Field Data

                marc500s = record.get_fields('500')
                marc500Counter = 0

                for note in marc500s:
                    noteList = []
                    noteList.append(recID)
                    noteList.append(marc500Counter)
                    noteForTable = note['a']
                    if noteForTable is None:
                        continue
                    noteList.append(noteForTable)
                    noteSubFieldCount = note.subfields.count('5')
                    noteList.append(noteSubFieldCount)
                    subFieldOwnCode = ''
                    if noteSubFieldCount == 1:
                        subFieldOwnCode = note['5']
                    noteList.append(subFieldOwnCode)
                    marc500List.append(noteList)
                    marc500Counter += 1
                    maxBibNotes = + 1


                if debug == 1:
                    print('\tMarc500 Fields are:')
                    for marc500ListNotes in marc500s:
                        print('\t'+str(marc500ListNotes))
            else:
                print('\tskipped!')

            stopper = 'Nope'
            if debug == 1:
                stopper = input('press n to stop')
                if stopper == 'n':
                    return  record
                    # print(1/0)

            # return record

            # writeResultsToCSV('bibNotes.csv',marc500List)
            # if recordCounter > 00000:
            #     break
    addDataTobibList(bibList)
    addDataTobibNotes(marc500List)

    # writeResultsToCSV('alephBibs.csv', bibList)
    # writeResultsToCSV('bibNotes.csv', marc500List)
    # return (bibList, marc500List)

marcRead()
print("\ndone!")