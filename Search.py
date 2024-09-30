import time
# For path 
import os
# For finding links and tokenization
import re
# Filepath hash is file id
import xxhash
# File formats support
from docx import Document
# Text processing
import simplemma
import contractions
# Data format
import shelve
# Concurrency
import concurrent.futures
import threading
# Config
import configparser


APP_DIR = ".NoteSearch"
WORD_SHELF_NAME = 'WordIndices'
SOURCE_SHELF_NAME = 'Sources'

word_shelf_lock = threading.Lock()
source_shelf_lock = threading.Lock()

merged_dictionary = {}

langs = "en"
stop_words = []

def add_file_to_global_dict(file_dict, file_id):
     with word_shelf_lock:
        for key, value in file_dict.items():
            if key not in merged_dictionary:
                merged_dictionary[key] = {}
            merged_dictionary[key][file_id] = value


def read_file(file_path):
    if os.path.exists(file_path):
        # Extracting file extension 
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.txt':
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            return content
        elif file_ext == '.docx':
            doc = Document(file_path)
            content = ""
            for paragraph in doc.paragraphs:
                content += paragraph.text
                content += "\n"
            return content
    else:
        print(f"The file at {file_path} does not exist.")


def extract_links(text):
    link_pattern = r'https?://\S+|ftp://\S+|www\.\S+|(?:\b\w+\.\w+\b)'
    links = re.findall(link_pattern, text)
    text = re.sub(link_pattern, '', text)
    return links, text
    

def tokenize(text):
    # Pattern for word
    pattern = r'\b[a-zA-Z]+\b'
    # Finding all words and collecting them into array
    tokenized_array = re.findall(pattern, text)
    return tokenized_array


def index_link(link, sources_db_path):
    # Firsly listing patterns of known websites
    youtube_pattern = (
        r'(?:https?:\/\/)?(?:www\.)?'
        r'(?:youtube\.com\/(?:watch\?v=|channel\/|playlist\?list=)|'
        r'youtu\.be\/)'
        r'[a-zA-Z0-9_-]{11}'  
    )
    youtube_regex = re.compile(youtube_pattern)

    # Secondly iterating over listed patterns and extracting from known sources
    if youtube_regex.match(link):
        print(link)


def index_file(file_path, source_shelf):
    if os.path.exists(file_path): 
        file_content = read_file(file_path)
        if not file_content == None:
            file_id = xxhash.xxh32_hexdigest(file_path.encode())
            with source_shelf_lock:
                    source_shelf[file_id] = file_path
                
            links, text = extract_links(file_content)
            
            # Firstly iterating over links cause they are also sources
            for link in links:
                index_link(link, source_shelf)
                
            file_content_array = tokenize(contractions.fix(text))
            
            file_dict = {}
            for word_pos in range(0, len(file_content_array)):
                if(file_content_array[word_pos] != '' and not (file_content_array[word_pos] in stop_words)):
                    word = simplemma.lemmatize(file_content_array[word_pos].lower(), lang = langs, greedy=True)
                    value = file_dict.get(word, [])
                    value.append(word_pos)
                    file_dict[word] = value
            
            add_file_to_global_dict(file_dict, file_id)

    else:
        print(f"File does not exist: %s", {file_path})
        
        
def index_dir(dir_path, word_db_name = WORD_SHELF_NAME, sources_db_name = SOURCE_SHELF_NAME):
    start = time.time()
    word_db_path = os.path.join(dir_path, APP_DIR, word_db_name)
    sources_db_path = os.path.join(dir_path, APP_DIR, sources_db_name)
    
    with shelve.open(sources_db_path) as source_shelf:

            for dir_path, dir_names, file_names in os.walk(dir_path):
                dir_names.remove(APP_DIR)
        
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # Create a dictionary to map file paths to their future
                    futures = {executor.submit(index_file, os.path.join(dir_path, file_name), source_shelf): file_name for file_name in file_names}
                    
    # DB creation
    with shelve.open(word_db_path) as word_shelf:
        for key, value in merged_dictionary.items():
            word_shelf[key] = value

    end = time.time()
    print("Dir indexing time: ")
    print(end - start)
                    

def search_in_dir(phrase, dir_path, word_db_name = WORD_SHELF_NAME, sources_db_name = SOURCE_SHELF_NAME):
    if(phrase is not None and phrase != ""):
    
        # Checking if dir is valid
        if len(dir_path) > 3 and os.path.isdir(dir_path) :
            app_folder = os.path.join(dir_path, APP_DIR)
            if not os.path.isdir(app_folder):
                 os.mkdir(app_folder)
            word_db_path = os.path.join(app_folder, word_db_name)
            sources_db_name = os.path.join(app_folder, sources_db_name)
            if not os.path.exists(word_db_path+".dat"):
                 index_dir(dir_path)

            # Preparing phrase to search
            phrase_array = tokenize(contractions.fix(phrase))
            filtred_phrase = []
            for word in phrase_array:
                 if word != '' and not word in stop_words:
                    filtred_phrase.append(simplemma.lemmatize(word.lower(), lang = langs, greedy=True))

            word_indexes = {}
            word_db_path = os.path.join(dir_path, APP_DIR, word_db_name)
            with shelve.open(word_db_path) as word_shelf:
                 for word in filtred_phrase:
                     word_indexes[word] = word_shelf.get(word, {})

            common_files = set(next(iter(word_indexes.values())))
            for file_ids_list in word_indexes.values():
                common_files &= set(file_ids_list)
        
            common_files = list(common_files)
            i = 0
            while i < len(common_files):
                file = common_files[i]
                suitable = True
                last_positions = word_indexes[filtred_phrase[0]][file]
                for j in range(1, len(filtred_phrase)):
                    positions_in_current_file = word_indexes[filtred_phrase[j]][file]
                    next_positions = []
                    for pos_1 in last_positions:
                        for pos_2 in positions_in_current_file:
                            if abs(pos_2 - pos_1) < 5:
                                next_positions.append(pos_2)
                    if len(next_positions) == 0:
                        suitable = False
                        break
                    last_positions = next_positions
                if not suitable:
                    common_files.remove(file)
                else:
                    i += 1

            common_files_paths = []
            sources_db_path = os.path.join(dir_path, APP_DIR, sources_db_name)
            with shelve.open(sources_db_path) as source_shelf:
                for file in common_files:
                     common_files_paths.append(source_shelf.get(file, {}))
            
            for file in common_files_paths:
                print(file)

            return common_files_paths
        else:
            return ["Invalid directory"]
    else:
        return ["Search query can't be null"]


def search_(phrase):
    config = configparser.RawConfigParser()
    config.read('config.ini')
    config_dict = dict(config.items('General'))

    search_dir = config_dict["search_folder"]
    print(f"Directory: {search_dir}")

    langs_list = [item.strip() for item in config_dict["stopwords_langs"].split(',')]
    langs = tuple(langs_list)
    for lang in langs:
        stop_words_lang = read_file(f"stop_words\\stopwords_{lang}.txt")
        stop_words.extend(tokenize(stop_words_lang))

    return(search_in_dir(phrase, search_dir))



#shelve_file = shelve.open(os.path.join(search_dir, APP_DIR, WORD_SHELF_NAME))
#for key in shelve_file:
#        print(f"{key}: {shelve_file[key]}")
