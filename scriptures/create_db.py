from dataclasses import dataclass
import io
import pathlib
import sqlite3
from typing import Dict, Generator, List, Tuple
from urllib import request
import zipfile

@dataclass
class RawVerse:
    book: str
    chapter: str
    verse: int
    text: str

@dataclass
class Verse:
    number: int
    text: str

@dataclass
class Chapter:
    number: int
    verses_by_number: Dict[int, Verse]

@dataclass
class Book:
    name: str
    chapters_by_number: Dict[int, Chapter]

@dataclass
class Work:
    name: str
    books_by_name: Dict[str, Book]

class DataFetcher:
    def generate_files_inside_zip(self, url: str)->Generator[Tuple[zipfile.ZipFile, zipfile.ZipInfo], None,None]:
        with request.urlopen(url) as f:
            with zipfile.ZipFile(io.BytesIO(f.read())) as the_zip_file:
                for zipinfo in the_zip_file.infolist():
                    yield the_zip_file, zipinfo
        
    def generate_lines_from_embedded_zip_file(self, the_zip_file:zipfile.ZipFile, zip_info:zipfile.ZipInfo)->Generator[str, None,None]:
        with the_zip_file.open(zip_info) as thefile:
            for line in io.TextIOWrapper(thefile):
                if line == "\n":
                    continue
                yield line.replace("\n", "")

class VerseParser:
    def parse(self, text:str, book_by_abbreviation: Dict[str, str])->RawVerse:
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
            book=book_by_abbreviation[abbreviation],
            chapter=chapter,
            verse=verse,
            text=verse_text,
        )

class BookBuilder:
    def build(self, the_zip_file:zipfile.ZipFile, zip_info: zipfile.ZipInfo, book_by_abbreviation: Dict[str, str])->Book:
        book = Book(name="", chapters_by_number={})
        alpha_chapter_mappings = {}

        for line in DataFetcher().generate_lines_from_embedded_zip_file(the_zip_file, zip_info):
            raw_verse = VerseParser().parse(line, book_by_abbreviation)

            if raw_verse.chapter in alpha_chapter_mappings:
                chapter_to_use = alpha_chapter_mappings[raw_verse.chapter]
            elif raw_verse.chapter.isalpha():
                alpha_chapter_mappings[raw_verse.chapter] = min(list(book.chapters_by_number.keys())) - 1
                chapter_to_use = alpha_chapter_mappings[raw_verse.chapter]
            else:
                chapter_to_use = int(raw_verse.chapter)

            verse = Verse(number=raw_verse.verse,text=raw_verse.text)
            chapter = book.chapters_by_number.setdefault(
                chapter_to_use, 
                Chapter(number=chapter_to_use, verses_by_number={})
            )
            chapter.verses_by_number[verse.number] = verse
        book.name = raw_verse.book
        
        return book

class WorkBuilder:

    def build(self, url:str) -> Work:
        book_by_abbreviation = {}
        the_work = Work(name="", books_by_name={})
        for the_zip_file, zip_info in DataFetcher().generate_files_inside_zip(url):
            if any([v in zip_info.filename for v in ["Readme", "index", "Facsimile"]]):
                continue
            elif "Contents" in zip_info.filename:
                book_by_abbreviation = ContentsParser().build_book_by_abbreviation(the_zip_file, zip_info)
            else:
                assert len(book_by_abbreviation) > 0
                book = BookBuilder().build(the_zip_file, zip_info, book_by_abbreviation)
                the_work.books_by_name[book.name] = book
        
        the_work.name = zip_info.filename.split("/")[0]
        return the_work
    
class ScripturesBuilder:
    def build(self, urls: List[str]) -> Dict[str, Work]:
        works_by_name:Dict[str, Work] = {}
        for url in urls:
            the_work = WorkBuilder().build(url)
            works_by_name[the_work.name] = the_work
        
        return works_by_name
            


class ContentsParser:
    def build_book_by_abbreviation(self, the_zip_file:zipfile.ZipFile, zip_info: zipfile.ZipInfo)->Dict[str, str]:
        book_by_abbreviation: Dict[str, str] = {}
        for line in DataFetcher().generate_lines_from_embedded_zip_file(the_zip_file, zip_info):
            if ". . . ." not in line:
                continue

            split_line = line.split(".")
            book = split_line[0].replace(" ", "")
            abbreviation = split_line[-1].replace(" ", "")
            book_by_abbreviation[abbreviation] = book
        return book_by_abbreviation

class VerseGenerator:
    def generate_verse(self, work_by_name: Dict[str, Work]) -> Generator[Tuple[str, RawVerse],None,None]:
        for work in work_by_name.values():
            for book in work.books_by_name.values():
                for chapter in book.chapters_by_number.values():
                    for verse in chapter.verses_by_number.values():
                        yield (
                            work.name, 
                            RawVerse(
                                book=book.name,
                                chapter=chapter.number,
                                verse=verse.number,
                                text=verse.text
                            )
                        )



class DatabaseCreator:
    def create_database_table(self, path_to_db:pathlib.Path, table_name:str)->None:
        sql_create_table = f"\
            CREATE TABLE IF NOT EXISTS {table_name} (\
                id INTEGER PRIMARY KEY,\
                work TEXT NOT NULL,\
                book TEXT NOT NULL,\
                chapter INTEGER NOT NULL,\
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
    
    def insert_verses_into_database(self, path_to_db:pathlib.Path, table_name:str, work_by_name:Dict[str, Work]):
        sql_insert = f"INSERT INTO {table_name} (work, book, chapter, verse, scripture) values (?, ?, ?, ?, ?);"
        with sqlite3.connect(path_to_db) as db:
            cursor = db.cursor()

            for work_name, raw_verse in VerseGenerator().generate_verse(work_by_name):
                cursor.execute(
                    sql_insert, 
                    (
                        work_name, 
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
    
    works_by_name = ScripturesBuilder().build(urls)

    DatabaseCreator().create_database_table(path_to_db=db_name, table_name=table_name)
    DatabaseCreator().insert_verses_into_database(path_to_db=db_name, table_name=table_name, work_by_name=works_by_name)

    

# build the lookup for abbreviation to book name
if __name__ == "__main__":
    main()