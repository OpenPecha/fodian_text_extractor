"""
Extract documents from MongoDB 'index' collection that have both
"Discourses" and "Root text" in their categories.
"""

from pymongo import MongoClient
from datetime import date
import json
import os
import re

# MongoDB connection string
CONNECTION_STRING = "mongodb+srv://samdup:EvSo4O19mOmBSlXI@cluster0.c0n0ihj.mongodb.net/"

def connect_to_mongodb():
    """Establish connection to MongoDB."""
    client = MongoClient(CONNECTION_STRING)
    return client

def extract_discourse_root_texts(client, database_name="chinese"):
    """
    Extract all documents from 'index' collection where categories
    contain both "Discourses" and "Root text".
    """
    db = client[database_name]
    collection = db["index"]
    
    # Query for documents containing both categories
    query = {
        "categories": {
            "$all": [
                "Madhyamaka",
                "Prasangika",
                "Bodhicaryavatara",
                "Root text"
            ]
        }
    }
    
    documents = list(collection.find(query))
    return documents

def get_index_discourses_root_texts():
    client = connect_to_mongodb()
    
    try:
        # Test connection
        client.admin.command('ping')
        documents = extract_discourse_root_texts(client)
        list_text = []
        
        for doc in documents:
            categories = doc.get('categories', [])
            list_text.append(categories[-1])
            print(categories[-1])
        return list_text
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("\nConnection closed.")

def get_texts_by_index_title(index_title):
    client = connect_to_mongodb()
    db = client["chinese"]
    collection = db["texts"]
    query = {
        "title": index_title
    }
    documents = list(collection.find(query))

    json_payload = {
        "root_texts": [],
        "translations": []
    }
    for doc in documents:
        title = {}
        segment_annotation, base_text, first_segment = parse_text_chapters(doc['chapter'])
        language = "zh" if doc['actualLanguage'] == "he" else doc['actualLanguage']
        version_title = doc['versionTitle']
        version_title = re.sub(r'\s*\[[^\]]*\]', '', version_title).strip()
        title[language] = version_title

        # only for root and translation
        type = "root" if language == "bo" else "translation"

        payload = {
            "metadata": {
                "text_type": type, 
                "instance_type": "critical",
                "source": doc['versionSource'],
                "colophon": first_segment,
                "incipit_title": title,
                "language": language,
                "category_id": "rw8oWUd1WtwqeD2x0ZMSm",
                "license": "CC0", 
                "copyright": "Public Domain",
                "contributions": [],
                "date": date.today().isoformat(),
                "bdrc": "",
            },
            "segment_annotation": segment_annotation,
            "biblography_annotation": [],
            "content": base_text
        }
        if type == "root":
            json_payload["root_texts"].append(payload)
        else:
            json_payload["translations"].append(payload)

    for index, translation in enumerate(json_payload["translations"]):
        alignment_annotation, target_annotation = get_allignment_annotation(json_payload["root_texts"][0]["segment_annotation"], translation["segment_annotation"])
        json_payload["translations"][index]["target_annotation"] = target_annotation
        json_payload["translations"][index]["alignment_annotation"] = alignment_annotation

        filtered_segment_annotation = [item for item in translation["segment_annotation"] if item["span"]["start"] != item["span"]["end"]]
        json_payload["translations"][index]["segment_annotation"] = filtered_segment_annotation

    # Write json_payload to file
    os.makedirs("json/Madhyamaka", exist_ok=True)
    output_filename = f"json/Madhyamaka/{index_title.replace('/', '_')}.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    print(f"Written to {output_filename}")

def get_allignment_annotation(root_annotation, translation_annotation):
    alignment_annotation = []
    target_annotation = []
    for root_span, translation_span in zip(root_annotation, translation_annotation):
        if translation_span['span']['end'] == translation_span['span']['start']:
            continue
        alignment_annotation.append({
            "start": translation_span['span']['start'],
            "end": translation_span['span']['end']
        })
        target_annotation.append({
            "start": root_span['span']['start'],
            "end": root_span['span']['end']
        })
    return alignment_annotation, target_annotation


def get_segment_annotation(chapter_flat):
    annotation = []
    pos = 0
    for segment in chapter_flat:
        start = pos
        end = pos + len(segment)
        # if end == start: 
        #     start = 0
        #     end = 0
        annotation.append({
            "span": {
                "start": start,
                "end": end
            }
        })
        pos = end
    return annotation

def parse_text_chapters(chapters):
    chapter_flat = [x for sub in chapters for x in sub]
    cleaned_data = [re.sub(r'\s*<br\s*/?>\s*', '', item).strip() for item in chapter_flat]
    segment_annotation = get_segment_annotation(cleaned_data)
    base_text = "".join(cleaned_data)

    return segment_annotation, base_text, chapter_flat[0]

if __name__ == "__main__":
    list_text = get_index_discourses_root_texts()
    for text in list_text:
        get_texts_by_index_title(text)
