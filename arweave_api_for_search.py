import requests
import json
from jose.utils import base64url_encode, base64url_decode, base64
from arweave.utils import owner_to_address
from bs4 import BeautifulSoup
import time
from cleantext import clean
import re
import argparse
import spacy
from textacy import preprocessing

preproc = preprocessing.make_pipeline(
    preprocessing.remove.html_tags,
    preprocessing.remove.accents,
    preprocessing.normalize.bullet_points,
    preprocessing.normalize.unicode,
    preprocessing.normalize.whitespace,
    preprocessing.normalize.quotation_marks
    )

def get_transaction_owner(tx):
    response = requests.get("https://arweave.net/tx/" + tx + "/owner")
    return owner_to_address(response.text)

def get_transaction_data(tx):
    response = requests.get("https://arweave.net/tx/" + tx + "/data")
    return base64url_decode(response.content).decode()

def get_transaction_tags(tx):
    response = requests.get("https://arweave.net/tx/" + tx + "/tags")
    tags = {}
    for tag in response:
        name = base64url_decode(tag["name"].encode()).decode("utf-8").replace(":","_").replace("-","_").lower()
        value = base64url_decode(tag["value"].encode()).decode("utf-8")
        tags[name] = value
    return tags

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

#################################

def get_page_text(tx):
    response = requests.get("https://arweave.net/"+tx)
    return response.text

def check_if_html(page_text):
    try:
        if "<body>" in page_text or "<title>" in page_text:
            return True
        else:
            return False
    except: return False

def get_html_title(bs):
    try: r = bs.title.text
    except: r = ""
    return r

def get_html_description(bs):
    try: r = bs.description
    except: r = ""
    return r

def get_html_img_alt_text(bs):
    try: r = [img['alt'] for img in bs.find_all('img', alt=True)]
    except: r = ""
    return r

def get_html_keywords(bs):
    try:
        metatags = bs.find_all("meta", attrs={"name":["keywords", "news_keywords", "tags", "application-name"]})
        metalist = list(dict.fromkeys([tag["content"] for tag in metatags]))
    except: metalist = ""
    return metalist

def get_html_text(bs):
    soup_text = bs.findAll(["p","h1","h2","h3","h4","h5","title"])
    if isinstance(soup_text, list) and len(soup_text)>0:
        soup_text = [i.text for i in soup_text]
        soup_text = " ".join(soup_text)
        soup_text = " ".join(soup_text.split())
        # soup_text = soup_text.encode('utf-8', 'replace').decode()
        return soup_text
    else:
        return ""

###############################

def get_entities(page_text, limit=25):
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(str(page_text)[0:min(len(page_text), 1500)])
    ents = dict()
    ent_label = list()
    ent_text = list()
    for ent in doc.ents:
        if ent.text is not None:
            # entities.append([ent.text.strip(), ent.label_.strip()])
            ent_label.append(clean(ent.label_, no_emoji=True, no_punct=True))
            ent_text.append(clean(ent.text))
    max_len = min(len(ent_label), limit)
    for i in range(max_len):
        try:
            ents[ent_label[i]] = ents[ent_label[i]] + [ent_text[i]]
        except:
            ents[ent_label[i]] = [ent_text[i]]
    result = {}
    for cat in list(ents.keys()):
        lst = [i.encode('utf-8', 'replace').decode() for i in ents[cat]]
        lst = list(dict.fromkeys(lst))
        result[cat] = lst
    return result


from spacy.language import Language
from spacy_language_detection import LanguageDetector

def _language(nlp, name):
    return LanguageDetector(seed=42)  # We use the seed 42

def get_language(text_string):
    # print(" -- starting language")
    nlp = spacy.load("en_core_web_sm")
    Language.factory("language_detector", func=LanguageDetector(seed=42))
    nlp.add_pipe('language_detector', last=True)
    doc = nlp(text_string)
    language = doc._.language
    return language["language"]

###############################

def magicId (content):
    if content is not None:
        from magic_lib import Magic
        m = Magic()
        try:
            filetype = m.from_buffer(content)
        except:
            filetype = "libmagic error"
        return filetype
    else: pass

def whatTheFile (tx):
    if tx is not None:
        data = requests.get("https://arweave.net/" + tx)
        return magicId(data.content)
    else: pass

##############################

def get_graphql(input_height):
    url = 'https://arweave.net/graphql'
    query = """query {
        transactions(block: {min: """+str(input_height)+""", max: """+str(input_height)+"""}) {
            edges {
                node {
                    id,
                    data {
                        size
                        type
                    },
                    tags {
                        name,
                        value
                    },
                    block {
                        id
                        timestamp
                        height
                        previous
                    }
                }
            }
        }
    }"""
    response = requests.post(url, json={'query': query})
    print("Arweave GraphQL Response -- status_code: {}".format(str(response.status_code)))
    json_list_of_dicts = json.loads(response.text)["data"]["transactions"]["edges"]
    return json_list_of_dicts



def runArweaveAPI(input_height):
    print("------------------------------------------------------")
    print("BLOCK HEIGHT: {}".format(str(input_height)))
    json_data = get_graphql(455088)#input_height)
    arweave_api_data = []
    for i in json_data:
        TX = i["node"]
        tx_id = TX["id"]
        timestamp = TX["block"]["timestamp"]
        # print("---------------------")
        # print("BLOCK HEIGHT: {}".format(str(input_height)))
        print("TRANSACTION: {}".format(str(tx_id)))
        # print("---------------------")

        filetype = whatTheFile(tx_id)

        # tx_data = get_transaction("http://arweave.net")

        data = {"time": timestamp,
                "tx_id": str(tx_id),
                "block_height": str(TX["block"]["height"]),
                "file_type": str(filetype),
                "data_type": str(TX["data"]["type"]),
                "data_size": str(TX["data"]["size"])
                }

        for tag in TX["tags"]:
            name = re.sub(r"[\:|\.|\-|\+|\@|\#|\$|\%\|^|\&|\*|\(|\)|\*|\/|\\]", "_", clean(tag["name"]))
             # str(tag["name"]).replace(":","_").replace(".","_").replace("-","_")
            if "rsa" not in name and "key" not in name and "digest" not in name:
                data[name] = str(tag["value"])
        
        data["owner"] = get_transaction_owner(tx_id)
        data["page_url"] = "https://arweave.net/"+tx_id
        print(data["page_url"])
########################################################################################

        is_image = False
        is_image_list = ["jpeg", "jpg", "bmp", "png", "svg", "image", "gif", "video"]
        for i in is_image_list:
            if i in str(data["file_type"]) or i in str(data["data_type"]):
                is_image = True
                if i != "video" and i != "image":
                    data["image_url"] = "https://arweave.net/"+tx_id+"."+i
                break
        print(" -- is_image = " + str(is_image))

        is_text = False
        is_text_list = ["text", "xml", "json", "ascii", "html"]
        for i in is_text_list:
            if i in str(data["file_type"]) or i in str(data["data_type"]):
                is_text = True
                break
        print(" -- is_text = " + str(is_text))

        
        if is_image == True:
            data["image_url"] = "https://arweave.net/"+tx_id

        page_text = get_page_text(tx_id).encode('utf-8-sig').decode("utf-8")
        MAX_FINAL_LENGTH = 5000

        if is_image == False and is_text == True:
            print("is_image=False and is_text=True")
            try:
                page_soup = BeautifulSoup(page_text, "html5lib")
                page_soup_text = get_html_text(page_soup)
                preproc_bs4 = preprocessing.make_pipeline(
                    preprocessing.remove.accents,
                    preprocessing.normalize.bullet_points,
                    preprocessing.normalize.unicode,
                    preprocessing.normalize.whitespace,
                    preprocessing.normalize.quotation_marks,
                    preprocessing.remove.brackets
                    )
                source_text = preproc_bs4(page_soup_text)
                # data["source_text"] = "EXCEPT-- " + source_text[0:min(MAX_FINAL_LENGTH, len(source_text))]
                if page_soup.body or page_soup.title or page_soup.meta:
                    data["source_text"] = "PAGE_SOUP_TEXT -- " + source_text[0:min(MAX_FINAL_LENGTH, len(source_text))].replace("\"","")
                    try: data["keywords"] = get_html_keywords(page_soup)
                    except: pass
                    try: data["title"] = get_html_title(page_soup)
                    except: pass
                    if data["title"]:
                        data["page_url"] = "https://arweave.net/"+tx_id+".html"
            except:
                data["source_text"] = "EXCEPT-- " + preproc(page_text)[0:min(2500, len(page_text))]
                try:
                    json_media_url_parser = re.findall(r'\"uri\":\"(https://arweave.net/[A-z0-9_|\=|\-]+\??[A-z0-9_|\=]+)\",\s?"type":\s?\"([A-z0-9|\-|\_]+)\/', source_text)
                    if len(json_media_url_parser) > 0:
                        for i in json_media_url_parser:
                            items = list(i)
                            if items[1] == "image":
                                data["image_url"] = items[0]
                            elif items[1] == "video":
                                data["video_url"] = items[0]
                except: pass

           
            try:
                ents = get_entities(data["source_text"])
                for key in ents.keys():
                    data["entity_"+str(key)] = ents[key]
            except:
                pass

            try:
                lang = get_language(data["source_text"])
                data["language"] = lang
            except:
                pass
        

        post_to_es(data)
        # with open('ES_height_history.txt', 'r') as f:
        # last_block = f.readlines()[-1]
        # try:
        
        # except:
        #     time.sleep(1)
        #     with open('ES_height_history.txt', 'a') as f:
        #         f.write("\n"+str(height)+","+str(tx_id))
            # arweave_api_data.append(data)

    # return arweave_api_data


# print(runArweaveAPI(330220))

def post_to_es(json_item):
    url = "http://localhost:3002/api/as/v1/engines/arweave-transactions/documents"
    payload = json.dumps(json_item, skipkeys=True, ensure_ascii=False, indent=4).encode('utf-8-sig').decode("utf-8").replace(u"\ufeff", "")
    # print(json_item.keys())
    print(payload)
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer private-4rb14sxvw4rvxmdiqkh6w79f'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    print("ES response: " + str(response.text))

    # if dict(response.text)["id"] != "null":
    #     with open('C:/Users/ascot/Documents/GitHub/magic-url-filetype-identifier/magic-url-filetype-identifier - Copy for concurrent run/ES_height_history.txt', 'a') as f:
    #         f.write(str("\n"+str(height)+","+str(tx_id)))


parser = argparse.ArgumentParser(description="Just an example")
parser.add_argument('-s', "--starting_height",)
parser.add_argument('-e', "--ending_height",)
args = parser.parse_args()

# try:
#     starting_block = int(args.starting_height)
#     ending_block = int(args.ending_height)
# except:
#     starting_block = int(input("Starting Block:  "))
#     ending_block = int(input("Ending Block:  "))


starting_block = 480025
ending_block = 480025#490000

block = starting_block
while block <= ending_block:
    start_time = time.time()
    # try:
    #     runArweaveAPI(block)
    # except:
    #     pass
    runArweaveAPI(block)
    # for i in new_data:
    # i = new_data
    # post_to_es(i)
    # print(i)
    # print("\n")
    block += 1
    print("--- %s seconds ---" % (time.time() - start_time))



