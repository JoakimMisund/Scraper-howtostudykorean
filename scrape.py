import requests

from bs4 import BeautifulSoup
import pandas as pd
import re
import pprint
pp = pprint.PrettyPrinter(indent=4)

import json
import uuid
import pickle
import time
import random
from collections import OrderedDict
from pathlib import Path


global_tables = {'fermentables': None,
                 'hops': None,
                 'water': None,
                 'yeasts': None,
                 'description': None,
                 'stats': None}

def load_global_tables():
    for table_type in global_tables.keys():
        filepath = Path(f"./data/{table_type}")
        if not filepath.is_file():
            continue
        df = pd.read_csv(filepath)
        global_tables[table_type] = df

def update_global_tables(data, expand_id):
    print(f"Adding for {expand_id}")
    for table_type, df in data.items():
        if global_tables[table_type] is None:
            global_tables[table_type] = df

        table = global_tables[table_type]
        table = table.drop(table[table['expand_id'] == expand_id].index)
        global_tables[table_type] = table.append(df, ignore_index=True)
def print_tables(tables):
    for table_type, df in tables.items():
        print(table_type)
        print(df)
def print_global_tables():
    print_tables(global_tables)

def store_global_tables():
    for table_type, df in global_tables.items():
        filepath = Path(f"./data/{table_type}")
        df = df.to_csv(filepath, index=False)


cached_filename = "./cache/cache"
cached_directory = "./cache"

def cached_request(root_url, headers, data, method=requests.post):

    hassh = "".join([json.dumps(d, sort_keys=True) for d in [root_url, data]])
    

    for line in open(cached_filename):
        if hassh in line:
            h, filename = line.split("|")
            filename = filename.strip()
            #print("Using cached request!")
            return pickle.load(open(filename, 'br'))

    response = method(root_url, headers=headers, data=data)

    filename = cached_directory + "/" + str(uuid.uuid4())
    while (Path(filename).is_file()):
        filename = cached_directory + "/" + str(uuid.uuid4())
    pickle.dump(response, open(filename,'bw'))
    fp = open(cached_filename, 'a')
    fp.write(f"{hassh}|{filename}\n")

    sleep_time = 5 + random.random()
    print(f"Sleeping for {sleep_time}")
    time.sleep(sleep_time)
    
    return response
    

root_url = "https://www.brewersfriend.com/search/index.php"
headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0'}


# in structure
def dig(content):
    if type(content) in [int, str]:
        return str(content)
    if content.find("span") is not None:
        return dig(content.find("span"))
    elif content.find("a") is not None:
        return dig(content.find("a"))
    elif content.find("strong") is not None:
        return dig(content.find("strong"))
    if len(content.contents) > 0:
        return content.contents[0]
    return ""

def remove_excess(line):

    try:
        if line.name == 'i':
            line = line.contents[0]
    except Exception as e:
        pass

    
    line = re.sub("[\t\s]+", " ", line)
    line = re.sub("[:]{1}", "", line)
    return line.strip()

def get_recipe_details(relative_url, expand_id):
    url = f"https://www.brewersfriend.com{relative_url}"
    response = cached_request(url, headers, {}, method=requests.get)

    doc = BeautifulSoup(response.text, features="html.parser")

    if "Permission Error" in str(doc):
        return

    tables = {}

    for match in doc.find_all("div", {'class': "brewpart", "id": ["water", "hops", "fermentables"]}):
        brewpart_id = match["id"]

        match = match.find("table")

#        if (brewpart_id == "hops"):
#                print("------------",match,"------------")
        
        columns = []
        for col in match.find("tr").find_all("th"):
            column_name = remove_excess(dig(col))
            columns.append(column_name)

        rows = []
        for table in match.find_all("tr")[1:-1]:
            row = []
            for row_entry in table.find_all("td"):
                value = remove_excess(dig(row_entry))
                row.append(value)
            rows.append(row)

        df = pd.DataFrame(rows, columns=columns)
        tables[brewpart_id] = df

    for match in doc.find_all("div", {'class': "brewpart", "id": ["yeasts"]}):
        brewpart_id = match["id"]

        columns = []
        rows = []
        for yeast_table in match.find_all("table", recursive=False):
            head = yeast_table.find("thead")
            fill_columns = False
            if len(columns) == 0:
                fill_columns = True
                columns.append("Name")
            
            row = [remove_excess(dig(head.find("span")))]
            for table in yeast_table.find("table").find_all("tr"):
                if table.find("div", "brewpartlabel") is None:
                    continue

                if table.find("tr") is not None:
                    continue

                for row_entry in table.find_all("td"):
                    if row_entry.find("div", "brewpartlabel") is not None:
                        what = remove_excess(dig(row_entry.find("div", "brewpartlabel")))
                        if fill_columns:
                            columns.append(what)

                        if what not in columns:
                            sys.exit(1)
                    else:
                        value = remove_excess(dig(row_entry))
                        row.append(value)
            rows.append(row)
        df = pd.DataFrame(rows, columns=columns)    
        tables[brewpart_id] = df

    columns = []
    row = []
    match = doc.find("div", {'class':'description'})
    for item in match.find_all("span", {'class':'viewStats'}):
        key = remove_excess(dig(item.find("span", {'class':'firstLabel'})))
        try:
            value = remove_excess(dig(item.contents[3]))
        except:
            value = remove_excess(dig(item.contents[2]))

        possible_span = item.find("span", {'class': None, 'itemprop': None})
        possible_strong = item.find("strong")
        if possible_span:
            value = value + " " + remove_excess(dig(possible_span))
        elif possible_strong and remove_excess(dig(possible_strong)) != value:
            value = value + " " + remove_excess(dig(possible_strong))

        columns.append(key);
        row.append(value);
    tables["description"] = pd.DataFrame([row], columns=columns)

    columns = []
    row = []
    match = doc.find("div", {'class':'viewrecipe'}).find("div")
    for item in match.find_all("div", recursive=False):
        stat_id = item["id"]
        key = remove_excess(dig(item.find("label")))
        value = remove_excess(dig(item.find("div")))

        columns.append(key);
        row.append(value);
    tables["stats"] = pd.DataFrame([row], columns=columns)
    print(url)
    #print(doc.prettify())
    #print(tables)

    for key, table in tables.items():
        table["expand_id"] = expand_id
    #for key, table in tables.items():
    #    print(key)
    #    print(table)
    update_global_tables(tables, expand_id)



def get_unit_url(unit):
    return f"https://www.howtostudykorean.com/unit{unit}/"

def find_unit_table(doc):
    return doc.find("div", {'class': 'table-wrapper'})

def find_menu(doc):
    return doc.find("ul", {'id':'menu-top-navigation'})

def find_links(doc, limit=None, recursive=True, title=None, contents=None):
    return doc.find_all("a", href=True, limit=limit,
                        recursive=recursive, title=title, contents=contents)

def filter_dig_string(items, string):
    return [item for item in items if dig(item).startswith(string)]
def filter_lessons(links):
    return filter_dig_string(links, "Lessons")
def filter_units(links):
    return filter_dig_string(links, "UNIT")

def filter_extension_links(items, extension):
    return [item for item in items if item["href"].endswith(extension)]
def filter_pdf_links(links):
    return filter_extension_links(links, "pdf")
def filter_mp3_links(links):
    return filter_extension_links(links, "mp3")

def find_tbody(doc):
    return doc.find("tbody")
def find_body(doc):
    return doc.find("body")
def find_tds(doc):
    return doc.find_all("td")

def parse_mp3_word(mp3):
    word = dig(mp3)
    translation = "NA"
    
    translation_block = mp3.parent.find("span")
    if (translation_block is not None):
        translation = dig(translation_block)
    else:

        alt = parse_non_mp3_word(mp3.parent.text)
        if alt is not None:
            alt["mp3"] = str(mp3["href"])
            return alt
    
    return {"mp3": str(mp3["href"]),
            "word": str(word),
            "translation": str(translation)}
def find_mp3_vocabulary(doc):
    links = find_links(doc)
    mp3s = filter_mp3_links(links)
    return list(map(parse_mp3_word, mp3s))

def parse_non_mp3_word(line):
    if ("=" in line and line.count("=") == 1):
        word, translation = line.split("=")
    else:
        return None

    return {"mp3": None,
            "word": str(word),
            "translation": str(translation)}
    
def parse_non_mp3_words(vocab):
    return list(filter(lambda a: a is not None, map(parse_non_mp3_word, vocab.text.split("\n"))))

def find_non_mp3_vocabulary(doc):

    vocab_block = doc.find("span", text=lambda a:  a is not None and a.strip() == "Vocabulary")

    if (vocab_block is None):
        vocab_block = doc.find("a", text=lambda a:  a is not None and a.strip() == "Vocabulary")

    if (vocab_block is None):
        first_vocab = doc.find("p")
    else:
        first_vocab = vocab_block.parent.findNext("p")

    words = []
    cur_entry = first_vocab
    while(cur_entry and "Introduction" not in dig(cur_entry)):
        if (filter_mp3_links(find_links(cur_entry)) == []):
            words = words + parse_non_mp3_words(cur_entry)

        cur_entry = cur_entry.findNext("p")

    return words

def handle_lesson_page(url):
    response = cached_request(url, headers, {}, method=requests.get)
    doc = BeautifulSoup(response.text, features="html.parser")

    #links = find_links(doc)
    #pdf = filter_pdf_links(links)

    mp3_vocab = find_mp3_vocabulary(doc)
    non_mp3_vocab = find_non_mp3_vocabulary(doc)
    

    return {"vocabulary": {"mp3": mp3_vocab, "other": non_mp3_vocab},
            "url": url}

#handle_lesson_page("https://www.howtostudykorean.com/unit-5/lessons-101-108/lesson-101/")
#handle_lesson_page("https://www.howtostudykorean.com/?page_id=711")
#handle_lesson_page("https://www.howtostudykorean.com/upper-intermediate-korean-grammar/unit-4-lessons-76-83/lesson-79/")
def handle_unit_page(url):
    response = cached_request(url, headers, {}, method=requests.get)
    doc = BeautifulSoup(response.text, features="html.parser")

    body = find_tbody(doc)
    if body is None:
        body = find_body(doc)

    rows = find_tds(body)
    #links = [find_links(row, limit=1, contents=lambda a: print(a)) for row in rows]
    links = [row.find_all("a", href=True, limit=1,
                          text=lambda a: a is not None and a.startswith("Lesson") ) for row in rows]
    all_data = {}
    for link in links:
        if (len(link) == 0):
            continue


        link = link[0]


        if link.text is not None and not link.text.startswith("Lesson"):
            continue
        if link.text is not None and "ini" in link.text:
            continue
        try:
            if not link["title"].startswith("Unit") and not link["title"].startswith("Lesson"):
                continue;
        except:
            pass

        #print(f"{link['href']}:")
        #print(link)
        lesson_data = handle_lesson_page(link["href"])
        all_data[link.text] = lesson_data
        
    return all_data

#handle_unit_page("https://www.howtostudykorean.com/unit1/unit-1-lessons-1-8/")
#handle_unit_page("https://www.howtostudykorean.com/unit1/unit-1-lessons-9-16/")
#handle_unit_page("https://www.howtostudykorean.com/unit1/unit-1-lessons-17-25-2/")
#handle_unit_page("https://www.howtostudykorean.com/unit-2-lower-intermediate-korean-grammar/unit-2-lessons-34-41")

def handle_unit(unit):
    url = get_unit_url(unit)
    response = cached_request(url, headers, {}, method=requests.get)
    doc = BeautifulSoup(response.text, features="html.parser")

    #table = find_unit_table(doc)
    menu = find_menu(doc)
    links = find_links(menu)
    
    lessons = filter_lessons(links)
    units = filter_lessons(links)

    
    all_data = {}
    for lesson in lessons:
        name = dig(lesson)
        url = lesson["href"]
        data = handle_unit_page(url)

        print(name, url)
        all_data[name] = data

    with open("data.json", "wb") as f:
        f.write(json.dumps(all_data, indent=4).encode('utf-8'))
        
handle_unit(0)
