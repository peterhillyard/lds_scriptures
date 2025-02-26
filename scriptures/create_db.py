from dataclasses import dataclass
import io
import pathlib
import sqlite3
from typing import Dict, Generator, List, Optional, Tuple
from urllib import request
import zipfile

WORKS_IN_CONTENTS = ["BIBLE", "BOOK OF MORMON", "DOCTRINE AND COVENANTS", "PEARL OF GREAT PRICE"]


@dataclass
class RawVerse:
    work: str
    book: str
    chapter: str
    verse: int
    text: str

@dataclass
class Verse:
    """
    A verse stores the text of the verse
    """
    number: int
    text: str

@dataclass
class Chapter:
    """
    A chapter stores the verses of that chapter. The verses inside the book are stored in a dictionary where the key is the verse number and the value is the verse object
    """
    number: int
    verses_by_number: Dict[int, Verse]


@dataclass
class BookAndWork:
    """
    A book and work is a tuple that stores the book name and the work name
    """
    book: str
    work: str

class DataFetcher:
    def generate_files_inside_zip(self, url: str)->Generator[Tuple[zipfile.ZipFile, zipfile.ZipInfo], None,None]:
        """
        Create a generator that yields the zip file and the zip info for each file inside the zip file

        Args:
            url: url to the zip file
        Returns:
            generator that yields the zip file and the zip info for each file inside the zip file
        """
        with request.urlopen(url) as f:
            with zipfile.ZipFile(io.BytesIO(f.read())) as the_zip_file:
                for zipinfo in the_zip_file.infolist():
                    yield the_zip_file, zipinfo
        
    def generate_lines_from_embedded_zip_file(self, the_zip_file:zipfile.ZipFile, zip_info:zipfile.ZipInfo)->Generator[str, None,None]:
        """
        Goes through each line in the zip file and yields the text of that line
        """
        with the_zip_file.open(zip_info) as thefile:
            for line in io.TextIOWrapper(thefile):
                if line == "\n":
                    continue
                yield line.replace("\n", "")

class VerseParser:
    def parse(self, text:str, book_by_abbreviation: Dict[str, BookAndWork])->RawVerse:
        # A verse from the file has the following format
        # <book abbreviation> <chapter>:<verse> <text>
        split_text = text.split(" ")
        abbreviation = split_text[0]
        try:
            chapter_verse_split = split_text[1].split(":")
            chapter = chapter_verse_split[0] # POFGP had non integer chapters
            verse = int(chapter_verse_split[1])
        except:
            print(text)
            raise ValueError
        verse_text = " ".join(split_text[2:])

        return RawVerse(
            work=book_by_abbreviation[abbreviation].work,
            book=book_by_abbreviation[abbreviation].book,
            chapter=chapter,
            verse=verse,
            text=verse_text,
        )


class RawVersesBuilder:

    def build(self, url:str) -> List[RawVerse]:
        """
        Build list of raw verse objects from the files inside the zipfile

        Args:
            url: url to the zip file
        Returns:
            the raw verses
        """
        book_and_work_by_abbreviation = {}
        raw_verses = list()
        for the_zip_file, zip_info in DataFetcher().generate_files_inside_zip(url):
            if any([v in zip_info.filename for v in ["Readme", "index", "Facsimile"]]):
                continue
            elif "Contents" in zip_info.filename:
                book_and_work_by_abbreviation = ContentsParser().build_book_and_work_by_abbreviation(the_zip_file, zip_info)
            else:
                assert len(book_and_work_by_abbreviation) > 0
                new_verses = self.get_raw_verses(the_zip_file, zip_info, book_and_work_by_abbreviation)
                raw_verses.extend(new_verses)
        
        return raw_verses
    
    def get_raw_verses(self, the_zip_file:zipfile.ZipFile, zip_info: zipfile.ZipInfo, book_by_abbreviation: Dict[str, BookAndWork])-> List[RawVerse]:
        """
        Build a list of RawVerse objects from the zip file
        """
        return [
            VerseParser().parse(line, book_by_abbreviation) 
            for line in DataFetcher().generate_lines_from_embedded_zip_file(the_zip_file, zip_info)
        ]
    
class ScripturesBuilder:
    def build(self, urls: List[str]) -> List[RawVerse]:
        """
        Loop through all urls and build up a list of raw verses

        Args:
            urls: list of urls to the zip files
        Returns:
            all raw verses from the zip files
        """
        raw_verses = list()
        for url in urls:
            new_verses = RawVersesBuilder().build(url)
            raw_verses.extend(new_verses)
        
        return raw_verses


class ContentsParser:
    def build_book_and_work_by_abbreviation(self, the_zip_file:zipfile.ZipFile, zip_info: zipfile.ZipInfo)->Dict[str, BookAndWork]:
        
        """
        Looks through the Contents file and builds a dictionary where the key is the abbreviation and the value is the book and work object

        The following is an example of the contents file.
                    TABLE OF CONTENTS I
                        In order of appearance

                                BIBLE

            Preface   . . . . . . . . . . . . . . . . .   PRE
            Genesis   . . . . . . . . . . . . . . . . .   GEN 
            Exodus    . . . . . . . . . . . . . . . . .   EXO 
            Leviticus   . . . . . . . . . . . . . . . .   LEV 
            ...
            Matthew   . . . . . . . . . . . . . . . . .   MAT 
            Mark    . . . . . . . . . . . . . . . . . .   MAR 
            Luke    . . . . . . . . . . . . . . . . . .   LUK 


                            BOOK OF MORMON

            Preface   . . . . . . . . . . . . . . . . .   PRB
            1-Nephi   . . . . . . . . . . . . . . . . .   NE1 
            2-Nephi   . . . . . . . . . . . . . . . . .   NE2 
            ...

                        DOCTRINE AND COVENANTS

            Preface   . . . . . . . . . . . . . . . . .   PRD
            Doctrine-and-Covenants  . . . . . . . . . .   D&C
            Official-Declarations   . . . . . . . . . .   DEC


                        PEARL OF GREAT PRICE

            Preface   . . . . . . . . . . . . . . . . .   PRP
            Moses   . . . . . . . . . . . . . . . . . .   MOS 
            Abraham   . . . . . . . . . . . . . . . . .   ABR 
            ...

        It would yield the following dictionary
        {
            "GEN": BookAndWork(book="Genesis", work="BIBLE"),
            "NE1": BookAndWork(book="1-Nephi", work="BOOK OF MORMON"),
            "D&C": BookAndWork(book="D&C", work="DOCTRINE AND COVENANTS"),
            ...
        }

        Args:
            the_zip_file: the zip file object
            zip_info: the zip info object
        Returns:
            book_and_work_by_abbreviation: dictionary where the key is the abbreviation and the value is the book and work object
        """
        book_and_work_by_abbreviation: Dict[str, BookAndWork] = {}
        current_work = ""
        for line in DataFetcher().generate_lines_from_embedded_zip_file(the_zip_file, zip_info):
            if "TABLE OF CONTENTS II" in line:
                break
            
            if any([v in line for v in WORKS_IN_CONTENTS]):
                current_work = line.lstrip(" ").rstrip(" ").replace("\n", "")
            
            if ". . . ." not in line:
                continue

            split_line = line.split(".")
            book = split_line[0].replace(" ", "")
            abbreviation = split_line[-1].replace(" ", "")
            book_and_work_by_abbreviation[abbreviation] = BookAndWork(book=book, work=current_work)
        return book_and_work_by_abbreviation


class DatabaseCreator:
    def create_database_table(self, path_to_db:pathlib.Path, table_name:str)->None:
        sql_create_table = f"\
            CREATE TABLE IF NOT EXISTS {table_name} (\
                id INTEGER PRIMARY KEY,\
                work TEXT NOT NULL,\
                book TEXT NOT NULL,\
                chapter TEXT NOT NULL,\
                verse INTEGER NOT NULL,\
                scripture TEXT NOT NULL\
            )"
        with sqlite3.connect(path_to_db) as connection:
            cursor = connection.cursor()
            # Drop the table if it already exists
            cursor.execute("DROP TABLE if EXISTS {0}".format(table_name))
            # Add the table schema
            cursor.execute(sql_create_table)
            # commit the changes
            connection.commit()
    
    def insert_verses_into_database(self, path_to_db:pathlib.Path, table_name:str, raw_verses: List[RawVerse]) -> None:
        sql_insert = f"INSERT INTO {table_name} (work, book, chapter, verse, scripture) values (?, ?, ?, ?, ?);"
        with sqlite3.connect(path_to_db) as db:
            cursor = db.cursor()

            for raw_verse in raw_verses:
                cursor.execute(
                    sql_insert, 
                    (
                        raw_verse.work, 
                        raw_verse.book, 
                        raw_verse.chapter, 
                        raw_verse.verse, 
                        raw_verse.text
                    )
                )

        


def main()->None:
    # TODO or use paths to downloaded zips which must be user-provided
    urls = [
        "https://ldsguy.tripod.com/Iron-rod/kjv-lds.zip",
        "https://ldsguy.tripod.com/Iron-rod/bom.zip",
        "https://ldsguy.tripod.com/Iron-rod/dnc.zip",
        "https://ldsguy.tripod.com/Iron-rod/pofgp.zip",
    ]
    db_name = pathlib.Path("scriptures.db")
    table_name = "scriptures"
    
    raw_verses = ScripturesBuilder().build(urls)

    DatabaseCreator().create_database_table(path_to_db=db_name, table_name=table_name)
    DatabaseCreator().insert_verses_into_database(path_to_db=db_name, table_name=table_name, raw_verses=raw_verses)

    

# build the lookup for abbreviation to book name
if __name__ == "__main__":
    main()