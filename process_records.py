from os import path, makedirs
import re
from json import dump
from typing import Dict
import pandas as pd
import spacy
from pandas.core.series import Series as pdrow


# SETTING UP
op_rootdir = "parsed_records"
pulled_records_ix = pd.read_csv("required_articles_index.csv")
makedirs(op_rootdir, exist_ok=True)

# DEFINING FUNCTIONS
# TODO: HANDLE THE CASES WHERE THERE"S AN EXCEPTION IN THE OUTER FUNCTION
def proc_parsed_article(parsed_article: Dict) -> Dict:
    
    """Helper function to perform the NLP pre-processing on the congressional record article
    split into speaker-speeches key-value pairs. The function first removes texts that aren't
    related to speeches by legislators (including those by the speaker), applies the en_core_web_sm
    Spacy NLP pipeline on the text of the speech, and then returns the dictionary with the speeches
    as Spacy pipeline corpora. 

    Args:
        parsed_article (Dict): Speaker-speech key-value pair dictionary

    Returns:
        Dict: Speaker-speech key-value pair dictionary where the speech has been processed into a 
        Spacy corpus object
    """
    # Retaining only speeches by legislators from the parsed text
    # TODO: LOOK INTO IF THE BELOW EXCLUDING KEYS MAKE SENSE
    orig_url = parsed_article["original_url"]
    excluding_keys = ["the speaker", "undefined_text", "original_url"]
    parsed_article = {key: value for key, value in parsed_article.items() 
                      if key.lower().strip() not in excluding_keys}

    # Processing speeches to contain only nouns and adjectives
    nlp = spacy.load('en_core_web_sm')
    procced_text = {}
    for k,v in parsed_article.items():
        
        txt_model = nlp(v)
        txt_model = [[token.lemma_.lower() for token in doc 
                    if token.pos_ == "NOUN" or token.pos_ == "ADJ"]
                    for doc in txt_model.sents]
        txt_model = [sent for sent in txt_model if len(sent) > 3]

        procced_text[k] = txt_model
    procced_text["original_url"] = orig_url

    return procced_text

def parse_articles(article_row: pdrow) -> str: 
    """Helper function to read in a downloaded congressional record article given a filepath,
    split the article up into speeches made by different speakers, arrange the speeches into
    a dictionary based on speaker-speech key value pairs and output this dictionary as a JSON. 
    The function also attempts to validate this process by comparing the number of speeches and
    speakers obtained by splitting the record, and returning an error if these numbers don't line 
    up. The document is split on the occurrence of the strings "The SPEAKER", "Mr. NAME", "Ms. NAME"
    or "Miss NAME", which precedes the text of a speaker's speech. 

    Args:
        article_row (pdrow): The row of the dataframe containing the input paths for each article

    Returns:
        str: The output path of the processed article
    """
    regex_split_pattern = r"(The SPEAKER|Mr\. [A-Z \-]{2,}|Ms\. [A-Z \-]{2,}|Miss [A-Z \-]{2,})"
    with open(article_row["article_fpath"], "r") as f:
        article_text = f.read()

    article_cleantext = re.sub(r"\s+", " ", article_text)
    article_split = re.split(regex_split_pattern, article_cleantext)

    # Getting indices of speakers and corresponding speeches in the text
    speaker_ixs = [i for i, s in enumerate(article_split) if re.search(regex_split_pattern, s)]
    speech_ixs = [ix+1 for ix in speaker_ixs]
    undefined_ixs = [i for i,s in enumerate(article_split) if 
                     i not in speaker_ixs + speech_ixs]

    # Validating all the indices
    ix_len_check = len(speaker_ixs + speech_ixs + undefined_ixs) == len(article_split)
    speaker_ixs_check = set(speaker_ixs).issubset(set(range(len(article_split))))
    speech_ixs_check = set(speech_ixs).issubset(set(range(len(article_split))))
    undefined_ixs_check = set(undefined_ixs).issubset(set(range(len(article_split))))
    speaker_speech_correspond_check = len(speaker_ixs) == len(speech_ixs)

    if not ix_len_check:
        return "Error in splitting attribution. Total number of indices greater than splits"
    if not speech_ixs_check:
        return "Error in speech attribution. Speech index not in index range"
    if not speaker_ixs_check:
        return "Error in speaker attribution. Speaker index not in index range"
    if not undefined_ixs_check:
        return "Error in undefined text attribution. Indices not in index range"
    if not speaker_speech_correspond_check:
        return "Error in speaker-speech correspondence. Speaker index does not match speech index"

    # Building dictionary of speakers with speeches
    article_dict = {article_split[spk_ix]: article_split[spch_ix] 
                    for spk_ix, spch_ix in zip(speaker_ixs, speech_ixs)}
    undefined_text = [article_split[i] for i in undefined_ixs]
    article_dict["undefined_text"] = undefined_text
    article_dict["original_url"] = article_row["article_url"]
    
    # Applying the NLP pipeline to the dictionary of speakers and speeches
    procced_article = proc_parsed_article(article_dict)

    # Writing dictionary to JSON
    ip_f = path.split(article_row["article_fpath"])
    op_fname = ip_f[1].replace(".txt", ".json")
    op_fpath = path.join(op_rootdir, op_fname)
    with open(op_fpath, "w") as f:
        dump(procced_article, f)
    
    print(f"Finished {op_fpath}")
    return op_fpath

pulled_records_ix["parsed_fpaths"] = pulled_records_ix.apply(parse_articles, axis = 1)
pulled_records_ix.to_csv("parsed_records_index.csv", index=False)
