from tabulate import tabulate
import requests
import inquirer
from data.db_manager import DBManager


def get_employers_data(employer_ids):
    """
    Получает данные о работодателях с сайта hh.ru.

    param employer_ids: Список идентификаторов работодателей.

    """
    url = 'https://api.hh.ru/employers'
    employers_data = []
    for employer_id in employer_ids:
        response = requests.get(f'{url}/{employer_id}')
        if response.status_code == 200:
            employers_data.append(response.json())
    return employers_data


def get_vacancies_data(employer_id, limit=3):
    """
    Получает данные о вакансиях работодателя с сайта hh.ru.

    :param employer_id: Идентификатор работодателя.
    :param limit: Ограничение на количество вакансий.

    """
    url = f'https://api.hh.ru/vacancies?employer_id={employer_id}&per_page={limit}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['items']
    return []


def insert_employers_data(db_manager, employers):
    """
    Вставляет данные о работодателях в базу данных.

    :param db_manager: Объект DBManager для работы с базой данных.
    :param employers: Список данных о работодателях.
    """
    for employer in employers:
        db_manager.cur.execute("""
            INSERT INTO employers (id, name, url) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (id) DO NOTHING
            RETURNING id
        """, (employer['id'], employer['name'], employer['alternate_url']))
        db_manager.conn.commit()


def insert_vacancies_data(db_manager, vacancies, employer_id):
    """
    Вставляет данные о вакансиях в базу данных.

    :param db_manager: Объект DBManager для работы с базой данных.
    :param vacancies: Список данных о вакансиях.
    :param employer_id: Идентификатор работодателя.
    """
    unique_vacancies = {vacancy['id']: vacancy for vacancy in vacancies}.values()
    for vacancy in unique_vacancies:
        salary_from = None
        if vacancy.get('salary'):
            salary_from = vacancy['salary'].get('from')
        db_manager.cur.execute(
            "INSERT INTO vacancies (employer_id, title, salary, url) VALUES (%s, %s, %s, %s)",
            (employer_id, vacancy['name'], salary_from, vacancy['alternate_url'])
        )
        db_manager.conn.commit()


def group_vacancies_by_company(vacancies):
    """
    Группирует вакансии по компаниям.

    :param vacancies: Список вакансий.

    """
    companies = {}
    for vacancy in vacancies:
        company_name = vacancy[0]
        if company_name not in companies:
            companies[company_name] = []
        companies[company_name].append(vacancy)
    return companies


def display_menu():
    """
    Отображает меню и возвращает выбор пользователя.


    """
    questions = [
        inquirer.List(
            'choice',
            message="Выберите действие",
            choices=[
                'Показать список всех компаний и количество вакансий у каждой компании',
                'Показать список всех вакансий',
                'Показать среднюю зарплату по вакансиям',
                'Показать список всех вакансий с зарплатой выше средней',
                'Показать вакансии с ключевым словом',
                'Выход'
            ]
        )
    ]
    return inquirer.prompt(questions)['choice']


def main():
    """
    Основная функция, выполняющая логику приложения.
    """
    # Список интересных компаний
    employer_ids = [
        '1740',  # Яндекс
        '3529',  # Сбербанк
        '1455',  # МТС
        '9077',  # Газпром
        '8120',  # Ростелеком
        '8050',  # ВТБ
        '80',  # Альфа-Банк
        '15478',  # VK (бывший Mail.Ru Group)
        '78638',  # Тинькофф
        '8070'  # РЖД
    ]

    employers = get_employers_data(employer_ids)
    vacancies = {emp_id: get_vacancies_data(emp_id, limit=3) for emp_id in employer_ids}

    db_manager = DBManager()

    # Вставка данных о работодателях
    insert_employers_data(db_manager, employers)

    # Вставка данных о вакансиях
    for emp_id in employer_ids:
        insert_vacancies_data(db_manager, vacancies[emp_id], emp_id)

    while True:
        choice = display_menu()

        if choice == 'Показать список всех компаний и количество вакансий у каждой компании':
            companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count()
            print(tabulate(companies_and_vacancies_count, headers=["Компания", "Количество вакансий"], tablefmt="grid"))

        elif choice == 'Показать список всех вакансий':
            all_vacancies = db_manager.get_all_vacancies()
            limited_vacancies = []
            for company, vacancies in group_vacancies_by_company(all_vacancies).items():
                limited_vacancies.extend(vacancies[:3])
            print(tabulate(limited_vacancies, headers=["Компания", "Название вакансии", "Зарплата", "Ссылка"],
                           tablefmt="grid"))

        elif choice == 'Показать среднюю зарплату по вакансиям':
            avg_salary = db_manager.get_avg_salary()
            print(f"Средняя зарплата по вакансиям: {avg_salary}")

        elif choice == 'Показать список всех вакансий с зарплатой выше средней':
            vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
            print(tabulate(vacancies_with_higher_salary, headers=["Название вакансии", "Зарплата", "Ссылка"],
                           tablefmt="grid"))

        elif choice == 'Показать вакансии с ключевым словом':
            keyword = input("Введите ключевое слово: ")
            python_vacancies = db_manager.get_vacancies_with_keyword(keyword)
            print(tabulate(python_vacancies, headers=["Название вакансии", "Зарплата", "Ссылка"], tablefmt="grid"))

        elif choice == 'Выход':
            print("Выход из программы")
            break


if __name__ == "__main__":
    main()
