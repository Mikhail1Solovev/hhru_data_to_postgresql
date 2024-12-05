import psycopg2
from psycopg2 import sql
from configparser import ConfigParser

def config(filename='data/config.ini', section='database'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db

def create_database():
    params = config()
    conn = psycopg2.connect(dbname='postgres', user=params['user'], password=params['password'], host=params['host'], port=params['port'])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [params['dbname']])
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(params['dbname'])))
    cur.close()
    conn.close()

def create_tables():
    commands = (
        """
        CREATE TABLE employers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE vacancies (
            id SERIAL PRIMARY KEY,
            employer_id INTEGER NOT NULL,
            title VARCHAR(255) NOT NULL,
            salary INTEGER,
            url VARCHAR(255) NOT NULL,
            FOREIGN KEY (employer_id)
            REFERENCES employers (id)
            ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()
    conn.close()
