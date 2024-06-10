import psycopg2
import configparser


class DBManager:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.conn = psycopg2.connect(
            dbname=config['database']['dbname'],
            user=config['database']['user'],
            password=config['database']['password'],
            host=config['database']['host'],
            port=config['database']['port']
        )
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        self.cur.execute(
            "SELECT e.name, COUNT(v.id) FROM employers e LEFT JOIN vacancies v ON e.id = v.employer_id GROUP BY e.name")
        return self.cur.fetchall()

    def get_all_vacancies(self):
        self.cur.execute(
            "SELECT e.name, v.title, v.salary, v.url FROM vacancies v JOIN employers e ON v.employer_id = e.id")
        return self.cur.fetchall()

    def get_avg_salary(self):
        self.cur.execute("SELECT AVG(salary) FROM vacancies WHERE salary IS NOT NULL")
        return self.cur.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        avg_salary = self.get_avg_salary()
        self.cur.execute("SELECT title, salary, url FROM vacancies WHERE salary > %s", (avg_salary,))
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        self.cur.execute("SELECT title, salary, url FROM vacancies WHERE title ILIKE %s", (f'%{keyword}%',))
        return self.cur.fetchall()

    def __del__(self):
        if hasattr(self, 'cur'):
            self.cur.close()
        if hasattr(self, 'conn'):
            self.conn.close()
