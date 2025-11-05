import requests
from io import BytesIO
import json
import os
import urllib3
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import etree
from .xml_load_old import xml_load
from .load_xml import load_from_xml


global domcklik
domcklik = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def strip_namespace(elem):
    for el in elem.iter():
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]

def tag_values(node, x, **tag):
    tags_chain = tag[x]['tags']
    name = tag[x]["attrs"].get("name")
    value = tag[x]["attrs"].get("values")
    number = int(tag[x].get("number", 0))
    if x == "flat_id" and name:
        return node.attrib.get(name, "")
    current = node
    for i, t in enumerate(tags_chain):
        if i == len(tags_chain) - 1:
            found_all = current.findall(t)
            if not found_all:
                return ""
            if name and value:
                filtered = [f for f in found_all if f.attrib.get(name) == value]
                if not filtered:
                    return ""
                current = filtered[number] if number < len(filtered) else filtered[-1]
                return current.text.strip() if current.text else ""
            if name:
                current = found_all[number] if number < len(found_all) else found_all[-1]
                return current.attrib.get(name, "")
            current = found_all[number] if number < len(found_all) else found_all[-1]
            return current.text.strip() if current.text else ""
        found_all = current.findall(t)
        if not found_all:
            return ""
        current = found_all[number] if number < len(found_all) else found_all[-1]
    if name:
        return current.attrib.get(name, "")
    return current.text.strip() if current.text else ""

def tag_values_domcklik(node, x, **tag):
    tags_chain = tag[x]['tags']
    number = int(tag[x].get("number", 0))
    current = node
    for t in tags_chain:
        if t.startswith(".."):
            steps = t.count("..")
            for _ in range(steps):
                current = current.getparent()
                if current is None:
                    return ""
        else:
            found_all = current.findall(t)
            if not found_all:
                return ""
            current = found_all[number] if number < len(found_all) else found_all[-1]
    return current.text.strip() if current.text else ""

def process_offer(offer, id, tag_for_threads):
    flat = {}
    flat["fid_id"] = str(id)
    flat["date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if domcklik is False:
        for key in tag_for_threads:
            flat[key] = tag_values(offer, key, **tag_for_threads)
        flat["type_room"] = (flat.get("type_room", "") or "") + (flat.get("rooms", "") or "")
    else:
        for key in tag_for_threads:
            flat[key] = tag_values_domcklik(offer, key, **tag_for_threads)
        flat["type_room"] = (flat.get("type_room", "") or "") + (flat.get("rooms", "") or "")
    return flat


def xml_load(url, id, **tag):
    global domcklik
    try:
        temp_file = f"/home/ubuntu/sait/main/base_flat/temp_{id}.xml"
        with requests.get(url, stream=True, verify=False) as response:
            with open(temp_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)

        tag_for_threads = tag.copy()
        tag_for_threads.pop('parents', None)
        parent_tag = tag['parents'][-1]

        batch_size = 50
        batch = []

        with open(f"/home/ubuntu/sait/main/base_flat/{id}.jsonl", "w", encoding="utf-8") as f_out:
            context = etree.iterparse(temp_file, events=("end",))
            for event, elem in context:
                if '}' in elem.tag:
                    elem.tag = elem.tag.split('}', 1)[1]

                if elem.tag == parent_tag:
                    batch.append(elem)

                if len(batch) >= batch_size:
                    with ThreadPoolExecutor(max_workers=3) as executor:
                        futures = [executor.submit(process_offer, e, id, tag_for_threads) for e in batch]
                        for fut in as_completed(futures):
                            flat = fut.result()
                            f_out.write(json.dumps(flat, ensure_ascii=False) + "\n")
                    batch.clear()
                    elem.clear()
            if batch:
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [executor.submit(process_offer, e, id, tag_for_threads) for e in batch]
                    for fut in as_completed(futures):
                        flat = fut.result()
                        f_out.write(json.dumps(flat, ensure_ascii=False) + "\n")
                batch.clear()

        if not os.path.exists(f"/home/ubuntu/sait/main/base_flat/{id}.jsonl") or os.path.getsize(f"/home/ubuntu/sait/main/base_flat/{id}.jsonl") == 0:
            return xml_load_old(url, id, **tag)
        # load_from_xml(filename=f"{id}.jsonl")
        return id, True

    except Exception as e:
        print("Ошибка:", e)
        return id, False

    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                print("Error")