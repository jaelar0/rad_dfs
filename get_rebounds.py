import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def exec_query(index):
    conn = psycopg2.connect(
        database="RAD_DFS", user="postgres", password="Tigers11", host="localhost", port="5432"
    )
    id_df = pd.read_sql("""SELECT * FROM public."NBA_API_RAW_DATA" t1 WHERE t1."DATA" not in ('<h1>Developer Over Rate</h1>', '<h1>Developer Over Qps</h1>')""", conn)
    
    conn.close()

    xml_data = id_df["DATA"].values.tolist()
    xml_cont = xml_data[index].strip()

    with open("./xml_play_data.xml", 'w', encoding='utf-8') as new_file:
        new_file.write(xml_cont)

def remove_namespace(tree):
    for elem in tree.iter(): 
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]

def get_player_att(child_tag):
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    player_id_list = []
    player_names = []

    for play_element in root.iter('event'):
        for child in play_element.findall(".//statistics"):
            child_txt = child.findall(child_tag)
            for child_element in child_txt:
                if child_element is not None and 'nullified' not in child_element.attrib:
                    for plr_txt in child_element.findall('player'):
                        player_name = plr_txt.get('full_name')
                        player_id = plr_txt.get('id')
                        player_names.append(player_name)
                        player_id_list.append(player_id)
    
    return player_id_list, player_names

def get_event_id(child_tag):
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    event_ids = []

    for play_element in root.iter('event'):
        for child in play_element.findall(".//statistics"):
            child_txt = child.findall(child_tag)
            for child_element in child_txt:
                if child_element is not None and 'nullified' not in child_element.attrib:
                    for plr_txt in child_element.findall('player'):
                        id_name = play_element.get('id')
                        event_ids.append(id_name)
    
    return event_ids

def get_tag_att(child_tag):
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    full_data = []

    for play_element in root.iter('event'):
        for child in play_element.findall(".//statistics"):
            child_txt = child.findall(child_tag)
            for child_element in child_txt:
                if child_element is not None and 'nullified' not in child_element.attrib:
                    for plr_txt in child_element.findall('player'):
                        attributes_child = child_element.attrib
                        if attributes_child:
                            full_data.append(attributes_child)
    
    tag_data_df = pd.DataFrame(full_data)

    return tag_data_df

def connect_psql(dataframe, table_name, exists):
    conn_string = 'postgresql://postgres:Tigers11@localhost:5432/RAD_DFS'
    engine = create_engine(conn_string)

    dataframe.to_sql(table_name, con=engine, if_exists=exists, index=False)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.close()

def main():

    for i in range(1, 798):
    # for i in range(0, 798):
        exec_query(i)
        tree = ET.parse('./xml_play_data.xml')
        remove_namespace(tree)
        tree.write('./xml_data_output.xml')
    
        # Rebound Statistics Tbl
        reb_player_id_list, reb_player_names = get_player_att('rebound')
        reb_event_ids = get_event_id('rebound')
        reb_player_df = get_tag_att('rebound')

        reb_player_df["player_id"] = reb_player_id_list
        reb_player_df["player_name"] = reb_player_names
        reb_player_df["event_id"] = reb_event_ids

        connect_psql(reb_player_df, "REBOUNDS_CURR", 'append')

        print(i)
        print("-----")

if __name__ == "__main__":
    main()
