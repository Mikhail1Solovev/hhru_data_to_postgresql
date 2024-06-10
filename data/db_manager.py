import psycopg2
from configparser import ConfigParser

class DBManager:
    def __init__(self, config_file='config.ini', config_section='database'):
        """
        Инициализирует объект DBManager и устанавливает соединение с базой данных.

        :param config_file: Путь к файлу конфигурации.
        :param config_section: Раздел конфигурации с параметрами подключения.
        """
        # Чтение параметров подключения из config.ini
        parser = ConfigParser()
        parser.read(config_file)
        db_config = {
            'dbname': parser.get(config_section, 'dbname'),
            'user': parser.get(config_section, 'user'),
            'password': parser.get(config_section, 'password'),
            'host': parser.get(config_section, 'host'),
            'port': parser.get(config_section, 'port')
        }
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании.

        :return: Список кортежей с именем компании и количеством вакансий.
        """
        self.cur.execute("SELECT e.name, COUNT(v.id) FROM employers e LEFT JOIN vacancies v ON e.id = v.employer_id GROUP BY e.name")
        return self.cur.fetchall()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию.

        :return: Список кортежей с информацией о вакансиях.
        """
        self.cur.execute("SELECT e.name, v.title, v.salary, v.url FROM vacancies v JOIN employers e ON v.employer_id = e.id")
        return self.cur.fetchall()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.

        :return: Средняя зарплата.
        """
        self.cur.execute("SELECT AVG(salary) FROM vacancies WHERE salary IS NOT NULL")
        return self.cur.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

        :return: Список кортежей с информацией о вакансиях.
        """
        avg_salary = self.get_avg_salary()
        self.cur.execute("SELECT title, salary, url FROM vacancies WHERE salary > %s", (avg_salary,))
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.

        :param keyword: Ключевое слово для поиска вакансий.
        :return: Список кортежей с информацией о вакансиях.
        """
        self.cur.execute("SELECT title, salary, url FROM vacancies WHERE title ILIKE %s", (f'%{keyword}%',))
        return self.cur.fetchall()

    def __del__(self):
        """
        Закрывает курсор и соединение с базой данных при удалении объекта DBManager.
        """
        self.cur.close()
        self.conn.close()
