import sqlite3
from itertools import zip_longest
from dejavu.database import Database


class SQLiteDatabase(Database):
    type = "sqlite3"

    FINGERPRINTS_TABLENAME = "fingerprints"
    SONGS_TABLENAME = "songs"

    FIELD_FINGERPRINTED = "fingerprinted"

    CREATE_SONGS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS {SONGS_TABLENAME} (
            {Database.FIELD_SONG_ID} INTEGER PRIMARY KEY AUTOINCREMENT,
            {Database.FIELD_SONGNAME} TEXT NOT NULL,
            {FIELD_FINGERPRINTED} INTEGER DEFAULT 0,
            {Database.FIELD_FILE_SHA1} BLOB NOT NULL UNIQUE
        );
    """

    CREATE_FINGERPRINTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS {FINGERPRINTS_TABLENAME} (
            {Database.FIELD_HASH} BLOB NOT NULL,
            {Database.FIELD_SONG_ID} INTEGER NOT NULL,
            {Database.FIELD_OFFSET} INTEGER NOT NULL,
            UNIQUE ({Database.FIELD_HASH},
                    {Database.FIELD_SONG_ID},
                    {Database.FIELD_OFFSET}),
            FOREIGN KEY ({Database.FIELD_SONG_ID})
                REFERENCES {SONGS_TABLENAME}({Database.FIELD_SONG_ID})
                ON DELETE CASCADE
        );
    """

    INSERT_SONG = f"""
        INSERT INTO {SONGS_TABLENAME}
        ({Database.FIELD_SONGNAME}, {Database.FIELD_FILE_SHA1})
        VALUES (?, ?);
    """

    INSERT_FINGERPRINT = f"""
        INSERT OR IGNORE INTO {FINGERPRINTS_TABLENAME}
        ({Database.FIELD_HASH}, {Database.FIELD_SONG_ID}, {Database.FIELD_OFFSET})
        VALUES (?, ?, ?);
    """

    SELECT_MULTIPLE = f"""
        SELECT {Database.FIELD_HASH},
               {Database.FIELD_SONG_ID},
               {Database.FIELD_OFFSET}
        FROM {FINGERPRINTS_TABLENAME}
        WHERE {Database.FIELD_HASH} IN (%s);
    """

    SELECT_SONG = f"""
        SELECT {Database.FIELD_SONGNAME},
               hex({Database.FIELD_FILE_SHA1}) AS {Database.FIELD_FILE_SHA1}
        FROM {SONGS_TABLENAME}
        WHERE {Database.FIELD_SONG_ID} = ?;
    """

    SELECT_SONG_BY_NAME = f"""
        SELECT {Database.FIELD_SONG_ID},
               hex({Database.FIELD_FILE_SHA1}) AS {Database.FIELD_FILE_SHA1}
        FROM {SONGS_TABLENAME}
        WHERE {Database.FIELD_SONGNAME} = ?;
    """

    UPDATE_SONG_FINGERPRINTED = f"""
        UPDATE {SONGS_TABLENAME}
        SET {FIELD_FINGERPRINTED} = 1
        WHERE {Database.FIELD_SONG_ID} = ?;
    """

    DELETE_UNFINGERPRINTED = f"""
        DELETE FROM {SONGS_TABLENAME}
        WHERE {FIELD_FINGERPRINTED} = 0;
    """

    SELECT_UNIQUE_SONG_IDS = f"""
        SELECT COUNT(DISTINCT {Database.FIELD_SONG_ID})
        FROM {SONGS_TABLENAME}
        WHERE {FIELD_FINGERPRINTED} = 1;
    """

    SELECT_NUM_FINGERPRINTS = f"""
        SELECT COUNT(*) FROM {FINGERPRINTS_TABLENAME};
    """

    def __init__(self, filename="dejavu.db", **_):
        super().__init__()
        self.filename = filename
        self.conn = sqlite3.connect(self.filename, check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON")

    def setup(self):
        cur = self.conn.cursor()
        cur.execute(self.CREATE_SONGS_TABLE)
        cur.execute(self.CREATE_FINGERPRINTS_TABLE)
        cur.execute(self.DELETE_UNFINGERPRINTED)
        self.conn.commit()

    def empty(self):
        cur = self.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self.FINGERPRINTS_TABLENAME}")
        cur.execute(f"DROP TABLE IF EXISTS {self.SONGS_TABLENAME}")
        self.conn.commit()
        self.setup()

    def insert_song(self, songname, file_hash):
        cur = self.conn.cursor()
        cur.execute(self.INSERT_SONG, (songname, bytes.fromhex(file_hash)))
        self.conn.commit()
        return cur.lastrowid

    def insert(self, hash, sid, offset):
        self.conn.execute(
            self.INSERT_FINGERPRINT,
            (bytes.fromhex(hash), sid, offset)
        )

    def insert_hashes(self, sid, hashes):
        rows = [(bytes.fromhex(h), sid, o) for h, o in hashes]
        self.conn.executemany(self.INSERT_FINGERPRINT, rows)
        self.conn.commit()

    def return_matches(self, hashes):
        mapper = {h.upper(): o for h, o in hashes}
        values = list(mapper.keys())

        for chunk in grouper(values, 999):
            placeholders = ",".join("?" * len(chunk))
            query = self.SELECT_MULTIPLE % placeholders
            cur = self.conn.execute(
                query, [bytes.fromhex(h) for h in chunk]
            )
            for h, sid, offset in cur:
                yield sid, offset - mapper[h.hex().upper()]

    def get_song_by_id(self, sid):
        cur = self.conn.execute(self.SELECT_SONG, (sid,))
        return cur.fetchone()

    def get_song_by_name(self, name):
        cur = self.conn.execute(self.SELECT_SONG_BY_NAME, (name,))
        return cur.fetchone()

    def set_song_fingerprinted(self, sid):
        self.conn.execute(self.UPDATE_SONG_FINGERPRINTED, (sid,))
        self.conn.commit()

    def get_num_songs(self):
        return self.conn.execute(self.SELECT_UNIQUE_SONG_IDS).fetchone()[0]

    def get_num_fingerprints(self):
        return self.conn.execute(self.SELECT_NUM_FINGERPRINTS).fetchone()[0]


def grouper(iterable, n):
    args = [iter(iterable)] * n
    return ([v for v in values if v] for values in zip_longest(*args))
