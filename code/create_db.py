'''
This script is used to take the scriptures contained in 
the ascii files and put them in as entries of a table
in a database.

@author: Peter Hillyard
@email: peterhillyard@gmail.com
'''

import sqlite3 as sqlite3
import glob as glob
import sys as sys
import os as os

# This function gets the book name from the file name
def get_book_name(book_name):
    split_book_name = book_name.split('-')

    # Change numbers for 1st, 2nd, 3rd, etc
    if len(split_book_name) > 1:
        if split_book_name[0] == '1':
            split_book_name[0] = '1st'
        elif split_book_name[0] == '2':
            split_book_name[0] = '2nd'
        elif split_book_name[0] == '3':
            split_book_name[0] = '3rd'
        elif split_book_name[0] == '4':
            split_book_name[0] = '4th'
        out_list = [split_book_name[0], split_book_name[1].capitalize()]
        if len(split_book_name) > 2:
            out_list += [item for item in split_book_name[2:]]
        return ' '.join(out_list)
    
    # Capitalize as appropriate
    split_book_name = book_name.split('_')
    if len(split_book_name) > 1:
        split_book_name[0] = split_book_name[0].capitalize()
        return ' '.join(split_book_name)

    return split_book_name[0].capitalize()

# This function creates the scripture table in the database
def create_table(db_name, table_name):
    
    sql_create_table = """CREATE TABLE %s (
                        id INTEGER PRIMARY KEY,
                        work TEXT,
                        book TEXT,
                        chapter INTEGER,
                        verse INTEGER,
                        scripture TEXT);""" % table_name

    with sqlite3.connect(db_name) as db:
        cursor = db.cursor()
        # Drop the table if it already exists
        cursor.execute("DROP TABLE if EXISTS {0}".format(table_name))
        # Add the table schema
        cursor.execute(sql_create_table)
        # commit the changes
        db.commit()




if __name__ == '__main__':

    # Create table in database
    db_name = '../database/scriptures.db'
    table_name = 'scriptures'
    create_table(db_name, table_name)

    # SQL string to add entry into table
    sql_insert = """INSERT INTO scriptures 
                    (work, book, chapter, verse, scripture) 
                    values (?, ?, ?, ?, ?);"""

    # Hard coded directory names and their abbreviation
    dir_name_list = ['Old Testament', 'New Testament', 'Book of Mormon', 'Doctrine and Covenants', 'Pearl of Great Price']
    work_name_conversion_dict = {'Old Testament': 'ot',
                                'New Testament': 'nt',
                                'Book of Mormon': 'bom',
                                'Doctrine and Covenants': 'dnc',
                                'Pearl of Great Price': 'pofgp'}
    
    # Path to ascii files
    path_to_ascii = '../scripture_ascii/'
    
    with sqlite3.connect(db_name) as db:
        cursor = db.cursor()
        
        for dir_name in dir_name_list:
            # Get the name of the work
            work_str = work_name_conversion_dict[dir_name]

            # get a list of all of the books in this work
            full_path = path_to_ascii + '%s/*' % (dir_name)
            book_name_list = sorted(glob.glob(full_path))

            # get a formatted book name for each book file
            for book_name in book_name_list:
                split_book_name = os.path.basename(book_name).split('.')[1]
                book_name_str = get_book_name(split_book_name)

                print work_str, book_name_str

                with open(book_name, 'r') as fin:
                    for line in fin:
                        if len(line) < 2:
                            continue
                        # Find where the chapter/verse and scripture text are separated
                        space_idx = (line[4:].find(' '))

                        # Get the chapter and verse
                        ch_vs_str = line[4:(4+space_idx)]
                        ch_str = ch_vs_str.split(':')[0]
                        vs_str = ch_vs_str.split(':')[1]

                        # Get the scripture text
                        verse_text = line[(5+space_idx):].strip('\n')

                        # Add entry to the table
                        cursor.execute(sql_insert, (work_str, book_name_str, int(ch_str), int(vs_str), verse_text))
        
        # Commit the adds to the database
        db.commit()

                    













