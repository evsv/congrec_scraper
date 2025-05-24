import sqlite3
import os
import requests
import re
import time
from json import load
import pandas as pd
from bs4 import BeautifulSoup

# GETTING DATAFRAME OF RECORD URLS
records_conn = sqlite3.Connection("records_info.db")
record_df = pd.read_sql_query("""SELECT * FROM records_ix""", records_conn)
record_df["article_title_lower"] = record_df["article_title"].str.lower()
with open("config.json", "r") as f:
    config = load(f)

# FILTERING FOR REQUIRED ARTICLES
proc_terms = config["house_sen_proc_terms"]

# Filtering for house articles which are not procedural
house_section_mask = (record_df.section == "House Section") &\
                      ~record_df.article_title_lower.str.contains(proc_terms)
senate_section_mask = (record_df.section == "Senate Section") &\
                       ~record_df.article_title_lower.str.contains(proc_terms)
req_articles = record_df.loc[house_section_mask | senate_section_mask, :].reset_index()

# CREATING FOLDERS TO STORE ARTICLES
# Getting folder structure
root_folder = "scraped_articles"
volume_folders = req_articles["volume"].unique()
issue_folders = req_articles[["volume", "issue"]].drop_duplicates()
section_folders = req_articles[["volume", "issue", "section"]].drop_duplicates()

# Creating folders for each volume, issue and section
for vol in volume_folders:
    os.makedirs(os.path.join(root_folder, str(vol)), exist_ok=True)
for ix, iss in issue_folders.iterrows():
    os.makedirs(os.path.join(root_folder, str(iss["volume"]), str(iss["issue"])), 
                exist_ok=True)
for ix, sec in section_folders.iterrows():    
    os.makedirs(os.path.join(root_folder, str(sec["volume"]), str(sec["issue"]), 
                             str(sec["section"])), 
                exist_ok=True)

# PULLING ARTICLES INTO FOLDERS
def proc_article(row, session_obj: requests.session, rootpath: str) -> str:
    """Helper function to iterate through the dataframe containing the required articles,
    pull the article text from the url, save it to an output file and return the path
    of the output file. The pulled text is cleaned to remove any header or boilerplate
    text. 

    Args:
        row : Dataframe row that the function is applied on
        session_obj (requests.session): A request.session object where an initial request 
                                        has already been made on a sample URL. This is done
                                        to store a session cookie to while fetching each article
        rootpath (str): The root folder where the articles are to be stored

    Returns:
        str: Path where the article is stored
    """
    # Defining the output path and name
    op_fclean = r"; congressional record vol. [0-9]+, no. [0-9]+"
    article_title = re.sub(op_fclean, "", row["article_title_lower"])
    op_fname = article_title.replace("/", " ") + ".txt"

    op_fpath = os.path.join(rootpath, str(row["volume"]), 
                            str(row["issue"]), row["section"], op_fname)

    # Getting the response
    try:
        raw_resp = session_obj.get(row["article_url"]).text
        cleantext = BeautifulSoup(raw_resp, "lxml").text

        header_txt = "From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]"
        pages_fmt = r"\[Pages \w+-\w*\]"
        page_fmt = r"\[\[.*?\]\]"
        cleantext = cleantext.replace(header_txt, "")
        cleantext = re.sub(pages_fmt, "", cleantext)
        cleantext = re.sub(page_fmt, "", cleantext)
    except Exception as e:
        print(f"Error {e} processing article {article_title}")
        cleantext = "Error when pulling"
        op_fpath = "Error when pulling"

    print(f"Finished {op_fname}")
    with open(op_fpath, "w") as f:
        f.write(cleantext)

    time.sleep(3)
    return op_fpath

# Initialising session
rec_session = requests.session()
init = rec_session.get(req_articles["article_url"][0])

# Iterating and pulling each article
req_articles["article_fpath"] = req_articles.apply(proc_article, axis=1, 
                                                   session_obj = rec_session,
                                                   rootpath = root_folder)
req_articles.to_csv("required_articles_index.csv")
