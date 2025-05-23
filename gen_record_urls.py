"""Script containing the code for the first part of the congressional pipeline scraper. 
This script takes as its inputs the first and last volumes of the congressional records,
and compiles a table of the URLs of all the articles within these issues, to be pulled
at a subsequent point.

The bulk of the script is concerned with parsing through the congressional record API 
response. The data is structured as follows:

1. A volume response, split up into pages. Each item in the volume response contains 
   a url to make an issue request. When the issue request is made, we get an issue 
   response
2. The issue response is split up into pages, and each item within the response 
   contains a url to make a section request. When the section request is made, we
   get the section response
3. Each item in the section response contains the URL for an article within that 
   section, which we extract and store

The final table is stored within a sqlite3 database table. 
"""

import requests
import sqlite3
from json import load
from typing import List

# DEFINING FUNCTIONS AND PARAMETERS
def proc_issue(issue_req: str, api_key: str) -> List:
    """Helper function to get the JSON response for an issue, iterate through the
    articles of that response, and compile the urls of the formatted text articles
    within the response

    Args:
        issue_req (str): Request string URL for the issue of interest
        api_key (str): API key to make requests

    Returns:
        List: A list containing the section name of the article, the article name and
        a link to the article text
 
    """
    issue_response = requests.get(issue_req).json()    
    issue_dta = []
    pg_no = 1
    # Going through each "page" of the issue json response
    while "next" in issue_response["pagination"]:
        issue_sections = issue_response["articles"]
        # Going through each section in the issue
        for section in issue_sections:
            # Going through each article in the section
            for article in section["sectionArticles"]:
                article_txt_url = next(item for item in article["text"]
                                    if item["type"] == "Formatted Text")
                article_txt_url = article_txt_url["url"]

                issue_dta.append([section["name"], article["title"], article_txt_url])

        issue_req = issue_response["pagination"]["next"] + f"&api_key={api_key}"
        issue_response = requests.get(issue_req).json()
        pg_no+=1

    # Processing the last page of the issue json response
    last_issue_section = issue_response["articles"]
    for section in last_issue_section:
        # Going through each article in the section
        for article in section["sectionArticles"]:
            article_txt_url = next(item for item in article["text"]
                                if item["type"] == "Formatted Text")
            article_txt_url = article_txt_url["url"]

            issue_dta.append([section["name"], article["title"], article_txt_url])

    return(issue_dta)

if __name__ == "__main__":

    starting_volno = 164
    ending_volno = 167
    tbl_create_sql = """CREATE TABLE IF NOT EXISTS records_ix
                        (volume int, issue int, issue_date text, section text, article_title text, article_url text)"""
    with open("config.json", "r") as f:
        config = load(f)
    api_key = config["api_key"]

    # CREATING THE DB FOR HOLDING CONGRESSIONAL RECORD INFO
    congrec_ix_conn = sqlite3.Connection("records_info.db")
    congrec_cursor = congrec_ix_conn.cursor()
    congrec_cursor.execute(tbl_create_sql)

    # ITERATING OVER EACH VOLUME
    vol_articles = []
    for vol_no in range(starting_volno, ending_volno+1):

        # Getting the response with the urls of issues in the volume
        vol_req = f"https://api.congress.gov/v3/daily-congressional-record/{vol_no}/?api_key={api_key}"
        vol_response = requests.get(vol_req).json()

        page = 1
        # Iterating through each "page" of response, getting the issues in that page, storing article urls
        # from that issue, and moving on to the next page
        while "next" in vol_response["pagination"].keys():
            # Iterating through each issue, processing articles within the issue and saving 
            # the list of articles in a list
            issue_no = 1
            for issue in vol_response["dailyCongressionalRecord"]:
                issue_info = [vol_no, int(issue["issueNumber"]), issue["issueDate"]]
                issue_req = issue["url"].replace("?format=json", f"/articles?api_key={api_key}")
                issue_articles = proc_issue(issue_req, api_key)
                issue_articles = [issue_info + article_info for article_info in issue_articles]
                vol_articles = vol_articles + issue_articles
                print(f"Finished processing issue {issue_no} from the {page} page of volume {vol_no}")
                issue_no += 1

            # Getting the "next page" of responses
            next_request = vol_response["pagination"]["next"].replace("&format=json", "")
            next_request = next_request + f"&api_key={api_key}"
            print(next_request)
            vol_response = requests.get(next_request).json()
            page += 1

        # "Terminating" page processing, to process articles from the last response page
        for issue in vol_response["dailyCongressionalRecord"]:
            issue_info = [vol_no, int(issue["issueNumber"]), issue["issueDate"]]
            issue_req = issue["url"].replace("?format=json", f"/articles?api_key={api_key}")
            issue_articles = proc_issue(issue_req, api_key)
            issue_articles = [issue_info + article_info for article_info in issue_articles]
            vol_articles = vol_articles + issue_articles
            print(f"Processing issue {issue_no} from the last page of volume {vol_no}")
            # print(issue_req)

    # Loading URLs into database
    url_load_query = """
        INSERT INTO records_ix (volume, issue, issue_date, section, article_title, article_url)
        VALUES (?, ?, ?, ?, ?, ?);
    """
    congrec_cursor.executemany(url_load_query, vol_articles)
    congrec_ix_conn.commit()
    congrec_ix_conn.close()
