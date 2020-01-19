import requests
import string
import itertools
import sqlite3
import concurrent.futures
import pdb

MAX_WORKERS = 32

def allo_request(hint):
    url = f'https://allo.ua/ru/catalogsearch/ajax/suggest/?currentTheme=main&currentLocale=ru_RU&q={hint}'
    #print(f'sending request to url with hint: {hint}')
    r = requests.get(url)
    result = r.json()['query'] if 'query' in r.json() else []
    #print(f'received result for {hint}: {result}')
    return result

def get_hints():
    alph = list(string.ascii_lowercase)
    alph_2 = ["".join([x, y]) for x in alph for y in alph]
    alph_3 = ["".join([x, y, z]) for x in alph for y in alph for z in alph]
    return alph + alph_2 + alph_3

with sqlite3.connect('suggest.mysql3') as conn:
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS suggests( '
                'hint text NOT NULL,'
                'suggest text)')
    c.execute("SELECT hint FROM suggests")
    fetch = c.fetchall()

    db_hints = set([x[0] for x in fetch])
    print(f'db_hints: {len(db_hints)}')
    all_hints = set(get_hints())
    print(f'all_hints: {len(all_hints)}')
    hints = all_hints - db_hints
    print(f'hints: {len(hints)}')
    i = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(allo_request, hint): hint for hint in hints}
        for future in concurrent.futures.as_completed(future_to_url):
            hint = future_to_url[future]
            i += 1
            try:
                suggests = future.result()
                if suggests:
                    for suggest in suggests:
                        c.execute(f'INSERT INTO suggests VALUES("{hint}","{suggest}")')
                else:
                    c.execute(f'INSERT INTO suggests VALUES("{hint}","")')
                conn.commit()
                print(f'{i}:{len(hints)} committed suggests for hint: {hint}')
            except Exception as exc:
                print(f'{hint} generated an exception: {exc}')
