from fuzzywuzzy import fuzz
import csv
import codecs
import time
# from build_tables_from_MARC import *
# from build_tables_from_OCLC import *

import sqlite3
sqlite_file = 'notes_db.sqlite'

def queryDB(table_name, column_2, inputID):
    """this needs to be updated - the queries need to be specific - i'm getting too many bibs right now that I don't need
    the result is too many comparisons. The OCLC numbers should be the binding results here"""
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('Select * from {tn} WHERE {cn} = {my_id}'.
              format(tn=table_name, cn=column_2, my_id=inputID))
    all_rows = c.fetchall()

    # print('1):', all_rows)

    conn.close()
    return all_rows

def buildKeyBibNotesDB():
    """Query to get back the comparable rows of KeyBibNotes and the associated OCLC Number"""
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    print('querying for KB Dictionary')
    c.execute('select distinct b.keybibNotes, c.oclc, b.NoteOrder, b.note from alephBibs a join bibNotes b on a.bibNumber = b.bib join altOCLC c on a.OCN = c.altoclc e join oclcNotes o on c.oclc = o.oclc left join notesAnalysis n on b.keybibNotes = n.KeybibNotes where a.OCN > 0 and a.GPO = 0  and a.gPub = 0 and b.OwnCodeCount = 1 and n.KeybibNotes is null')
    all_rows = c.fetchall()
    print('query finished')

    conn.close()
    return all_rows

def kbnDictionary():
    "create a dictionary of KeyBibNotes from query results"
    kbn = {}
    returnedRows = buildKeyBibNotesDB()
    # print(type(kbn))
    for r in returnedRows:
        kbn[r[0]] = (r[1], r[2], r[3])
    return kbn


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

    c.execute('SELECT distinct a.bibNumber, c.oclc FROM alephBibs a join altOCLC c on a.ocn = c.oclc where a.OCN > 0 and a.GPO = 0 and a.gPub = 0')
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
    t0 = time.perf_counter()
    # bibsToOCNS = getBibToOCN()
    # bibIndex = createBibIndex(bibsToOCNS)
    # bibs = bibIndex.keys()
    # maxValNotesAnalysis = getMaxKeyVal('notesAnalysis','keyNoteAnalysis')
    resultList = []
    recordCounter = 0

    print('\nStarting to build compare lists...')

    bibNotesDict = kbnDictionary()

    for b in bibNotesDict.keys():

        # this query is too broad and returns uncomparable notes
        # alephNotes = queryAlephNotesDB('bibNotes','bib',b)
        alephNotes = bibNotesDict[b]
        ocn = alephNotes[0]
        oclcNotes = queryDB('oclcNotes', 'oclc', ocn)

        # print('working on bib: '+str(b))
        # print(alephNotes, oclcNotes)

        aN = []

        keyBibNote = b
        # keyBib = x[1]
        aFormOrder = alephNotes[1]
        aNote = alephNotes[2]
        # aSubFieldCount = x[4]
        alephCompList = [keyBibNote, aNote]
        aN.append(alephCompList)

        # print("aN is: "+str(aN))
        oN = []
        for x in oclcNotes:
            keyOCLCNote = x[0]
            # oclcNumber = x[1]
            # nFormOrder = x[2]
            oNote = x[3]
            oclcCompList = [keyOCLCNote, oNote]
            oN.append(oclcCompList)


        # print("oN is: " + str(oN))

        # print('\tRunning the comparisons, record...')

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

                recordCounter += 1

            if len(resultList) >= 10000:
                print('writing to database, up to record '+str(recordCounter))
                t1 = time.perf_counter()
                addResultsTonotesAnalysis(resultList)
                writingTimeLapse = time.perf_counter() - t1
                print('\tDone! (in '+str(writingTimeLapse)+' seconds)')
                resultList = []
                # stop = input('press n to stop')
                # if stop == 'n':
                #     print(1/0)
        # print('\tComparisons done! - resultList length at ' + str(len(resultList)))

    print('writing to database')
    addResultsTonotesAnalysis(resultList)
    totaltimeLapse = time.perf_counter() - t0
    print('Completely Done! (in '+str(totaltimeLapse)+' seconds)')
    # return resultList

# marcRead()
# oclcmarcRead()
execute()