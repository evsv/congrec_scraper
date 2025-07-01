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
speaker_mapping = pd.read_csv("congressional_members.csv")
makedirs(op_rootdir, exist_ok=True)

# DEFINING FUNCTIONS
# TODO: HANDLE THE CASES WHERE THERE"S AN EXCEPTION IN THE OUTER FUNCTION
def proc_speech(speech_text: str) -> str:
    
    """Helper function to perform the NLP pre-processing on a speech found in the congressional 
    record article. This function is a wrapper to apply the en_core_web_sm Spacy NLP pipeline on the text of the 
    speech, and return the tokenised speech maintaining only nouns and adjectives. 

    Args:
        speech_text (str): Text of the speech found by splitting the congressional record article

    Returns:
        str: A list of tokens from the original speech, retaining only adverbs and nouns
    """
    # Processing speeches to contain only nouns and adjectives
    nlp = spacy.load('en_core_web_sm')
    try:
        txt_model = nlp(speech_text)
        txt_model = [[token.lemma_.lower() for token in doc 
                    if token.pos_ == "NOUN" or token.pos_ == "ADJ"]
                    for doc in txt_model.sents]
        txt_model = [sent for sent in txt_model if len(sent) > 3]
    except Exception as e:
        return f"Error with NLP parsing: {e}"

    return txt_model

def parse_articles(article_row: pdrow, speaker_lname_mapping: Dict) -> str: 
    """Helper function to read in a downloaded congressional record article given a filepath,
    split the article up into speeches made by different speakers, arrange the speeches into
    a dictionary based on speaker-speech key value pairs and output this dictionary as a JSON. 
    The function also attempts to validate this process by comparing the number of speeches and
    speakers obtained by splitting the record, and returning an error if these numbers don't line 
    up. The document is split on the occurrence of the strings "The SPEAKER", "Mr. NAME", "Ms. NAME"
    or "Miss NAME", which precedes the text of a speaker's speech. 

    Args:
        article_row (pdrow): The row of the dataframe containing the input paths for each article
        speaker_lname_mapping (Dict): A dictionary where the keys correspond to the last name of
                                      of the legislators, and values correspond to their party

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
    def get_party(speaker: str, mapping: Dict) -> str:
        procced_speaker = speaker.lower()\
                        .replace("mr.", "").replace("ms.", "")\
                        .strip()
        party = mapping.get(procced_speaker, "Not Found")
        return party

    article_speeches = [{"speaker": article_split[spk_ix],
                         "party": get_party(article_split[spk_ix], speaker_lname_mapping),
                         "speech": proc_speech(article_split[spch_ix])}
                        for spk_ix, spch_ix in zip(speaker_ixs, speech_ixs)]

    # Removing speeches by the speaker
    article_speeches = [speech for speech in article_speeches 
                        if speech["speaker"].lower().strip() != "the speaker"]

    # Writing dictionary to JSON
    ip_f = path.split(article_row["article_fpath"])
    op_fname = ip_f[1].replace(".txt", ".json")
    op_fpath = path.join(op_rootdir, op_fname)
    with open(op_fpath, "w") as f:
        dump(article_speeches, f)
    
    print(f"Finished {op_fpath}")
    return op_fpath

speaker_lname_mapping = speaker_mapping[["last_name", "chamber", "partyName"]]\
                        .drop_duplicates()
speaker_lname_mapping["last_name_counts"] = speaker_lname_mapping\
                                            .groupby(["last_name", "chamber"])\
                                            .transform("count")
speaker_lname_mapping.loc[speaker_lname_mapping["last_name_counts"] > 1, "partyName"] = "Ambiguous"
lname_iter = zip(speaker_lname_mapping["last_name"], speaker_lname_mapping["partyName"])
speaker_lname_mapping = {speaker.lower(): party for speaker, party in lname_iter}

pulled_records_ix["parsed_fpaths"] = pulled_records_ix.apply(parse_articles, axis = 1, 
                                                             speaker_lname_mapping=speaker_lname_mapping)
pulled_records_ix.to_csv("parsed_records_index.csv", index=False)

