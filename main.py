from fuzzywuzzy import fuzz
import csv
import codecs
# from build_tables_from_MARC import *
# from build_tables_from_OCLC import *

import sqlite3
sqlite_file = 'notes_db.sqlite'

def queryDB(table_name, column_2, inputID):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('Select * from {tn} WHERE {cn} = {my_id}'.
              format(tn=table_name, cn=column_2, my_id=inputID))
    all_rows = c.fetchall()

    # print('1):', all_rows)

    conn.close()
    return all_rows

def addResultsTonotesAnalysis(list):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.executemany('insert into notesAnalysis (KeyBibNotes, keyoclcNotes, FuzzRatio, PartialRatio, TokenSortRatio, TokenSetRatio) VALUES(?,?,?,?,?,?)',list)

    # print('1):', all_rows)
    conn.commit()
    conn.close()
    # return all_rows


def getMaxKeyVal(table_name, keyfield):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('Select max({cn}) from {tn}'.
              format(tn=table_name, cn=keyfield))
    all_rows = c.fetchall()

    # print('1):', all_rows)
    conn.close()

    maxKeyVal = all_rows[0][0]
    if maxKeyVal is None:
        maxKeyVal = 0
    else:
        maxKeyVal = + 1

    return maxKeyVal

def queryAlephNotesDB(table_name, column_2, inputID):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('Select * from {tn} WHERE OwnCodeCount = 1 and {cn}  = {my_id}'.
              format(tn=table_name, cn=column_2, my_id=inputID))
    all_rows = c.fetchall()

    # print('1):', all_rows)

    conn.close()
    return all_rows


def createBibIndex(bibsToOCNS):
    bibIndex = {}
    for x in bibsToOCNS:
        bibIndex[x[0]] = x[1]
    return bibIndex

def getBibToOCN():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('SELECT * FROM alephBibs')
    bibsToOCNS = c.fetchall()
    conn.close()
    return bibsToOCNS

def writeResultsToCSV(keyNoteAnalysis, KeybibNotes, keyoclcNotes, ratioRatio,ratioPartialRatio,ratioTokenSort,ratioTokenSet):
    outputFile = 'output_files\\notesAnalysis.csv'
    # import time
    # now = time.strftime('%Y-%m-%d %H:%M:%S')
    data = [[str(keyNoteAnalysis), str(KeybibNotes), str(keyoclcNotes), str(ratioRatio),str(ratioPartialRatio),str(ratioTokenSort),str(ratioTokenSet)]]

    with codecs.open(outputFile, 'a', encoding='utf-8') as out:
        a = csv.writer(out, delimiter = ',', quoting=csv.QUOTE_ALL)
        a.writerows(data)

def execute():
    bibsToOCNS = getBibToOCN()
    bibIndex = createBibIndex(bibsToOCNS)
    bibs = bibIndex.keys()
    # maxValNotesAnalysis = getMaxKeyVal('notesAnalysis','keyNoteAnalysis')
    resultList = []

    for b in bibs:
        ocn = bibIndex[b]
        alephNotes = queryAlephNotesDB('bibNotes','bib',b)
        oclcNotes = queryDB('oclcNotes', 'oclc', ocn)

        # print(alephNotes, oclcNotes)

        aN = []
        for x in alephNotes:
            keyBibNote = x[0]
            keyBib = x[1]
            aFormOrder = x[2]
            aNote = x[3]
            aSubFieldCount = x[4]
            alephCompList = [keyBibNote, aNote]
            aN.append(alephCompList)

        # print("aN is: "+str(aN))

        oN = []
        for x in oclcNotes:
            keyOCLCNote = x[0]
            oclcNumber = x[1]
            nFormOrder = x[2]
            oNote = x[3]
            oclcCompList = [keyOCLCNote, oNote]
            oN.append(oclcCompList)

        # print("oN is: " + str(oN))

        for x in aN:
            keyAlephNote = x[0]
            alephNoteVal = x[1]

            for y in oN:
                keyOCLCNote = y[0]
                oclcNoteVal = y[1]

                ratioRatio = fuzz.ratio(alephNoteVal, oclcNoteVal)
                ratioPartialRatio = fuzz.partial_ratio(alephNoteVal, oclcNoteVal)
                ratioTokenSort = fuzz.token_sort_ratio(alephNoteVal, oclcNoteVal)
                ratioTokenSet = fuzz.token_set_ratio(alephNoteVal, oclcNoteVal)

                result = [keyAlephNote, keyOCLCNote, ratioRatio, ratioPartialRatio, ratioTokenSort, ratioTokenSet]
                # print('Comparison Result: '+str(result))
                resultList.append(result)
                # print('Length of resultList: '+str(len(resultList)))

                # stopper = input('press n to break')
                # if stopper == 'n':
                #     break
                # writeResultsToCSV(keyAlephNote,keyOCLCNote,ratioRatio,ratioPartialRatio,ratioTokenSort,ratioTokenSet)
                # maxValNotesAnalysis += 1

    addResultsTonotesAnalysis(resultList)
    return resultList

# marcRead()
# oclcmarcRead()
execute()