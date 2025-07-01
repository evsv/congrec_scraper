import requests
from json import load
import pandas as pd

if __name__ == "__main__":

    # SETTING UP
    start_congno = 115 #Congressional Record #164 begins in the second half of this congress
    end_congno = 117 #Congressional Record #167 begins in the first half of this congress

    with open("config.json", "r") as f:
        config = load(f)
    api_key = config["api_key"]

    result_dfs = []

    # ITERATING THROUGH EACH CONGRESS
    for congno in range(start_congno, end_congno+1):
        memb_req = f"https://api.congress.gov/v3/member/congress/{congno}/?api_key={api_key}"
        memb_resp = requests.get(memb_req).json()
        # Iterating through the pages of the response
        while "next" in memb_resp["pagination"].keys():
            # Getting the members from the current page
            membs = memb_resp["members"]
            membs_df = pd.json_normalize(membs)
            membs_df["chamber"] = membs_df["terms.item"].apply(lambda x: x[0]["chamber"])
            membs_df = membs_df[["name", "partyName", "state", "chamber"]]
            membs_df["congress"] = congno
            result_dfs.append(membs_df)

            # Getting next page of response
            next_req = memb_resp["pagination"]["next"].replace("&format=json", "")
            next_req = next_req + f"&api_key={api_key}"
            memb_resp = requests.get(next_req).json()
            
        
        # Processing final page
        membs = memb_resp["members"]
        membs_df = pd.json_normalize(membs)
        membs_df["chamber"] = membs_df["terms.item"].apply(lambda x: x[0]["chamber"])
        membs_df = membs_df[["name", "partyName", "state", "chamber"]]
        membs_df["congress"] = congno
        result_dfs.append(membs_df)

    memb_df = pd.concat(result_dfs)
    memb_df.groupby(["congress", "chamber"]).size()
    memb_df["last_name"] = memb_df["name"].str.split(",", expand=True)[0]
    memb_df.to_csv("congressional_members.csv", index=False)