import sqlite3
sqlite_file = 'notes_db.sqlite'

stopper = input('\n**************************************************\n'
                'executing this script will delete all tables from the related database\n'
                'Are you sure you want to continue?\n'
                'If yes, type and enter "yes"\n')
if stopper != 'yes':
    print("\n\n\n***********************\nthat was close!\n")
    print(1/0)


def deleteTables():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    c.execute('delete from notesAnalysis;')
    # c.execute('delete from oclcNotes;')
    # c.execute('delete from bibNotes;')
    # c.execute('delete from alephBibs;')

    # print('1):', all_rows)

    conn.commit()
    conn.close()

deleteTables()