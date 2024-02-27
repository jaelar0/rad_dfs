import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import warnings
warnings.filterwarnings('ignore')

xml_file = "./xml_data_output.xml"
tree = ET.parse(xml_file)
root = tree.getroot()

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

def get_oc_events(child_tag):
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    event_ids = []

    for play_element in root.iter('event'):
        for child in play_element.findall(".//on_court"):
            child_txt = child.findall(child_tag)
            for child_element in child_txt:
                if child_element is not None and 'nullified' not in child_element.attrib:
                    for plr_txt in child_element.findall('player'):
                        id_name = play_element.get('id')
                        event_ids.append(id_name)
    return event_ids

def get_oc_players(child_tag):
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    player_names = []
    player_id_list = []
    jersey_list = []

    for play_element in root.iter('event'):
        for child in play_element.findall(".//on_court"):
            child_txt = child.findall(child_tag)
            for child_element in child_txt:
                if child_element is not None and 'nullified' not in child_element.attrib:
                    for plr_txt in child_element.findall('player'):
                        player_name = plr_txt.get('full_name')
                        player_id = plr_txt.get('id')
                        player_jersey = plr_txt.get('jersey_number')
                        player_names.append(player_name)
                        player_id_list.append(player_id)
                        jersey_list.append(player_jersey)

    return player_names, player_id_list, jersey_list

def get_quarter_id():
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    quarter_ids = []
    quarter_nums = []

    for play_element in root.iter('quarter'):
        for child in play_element.findall(".//event"):
            if child is not None and 'nullified' not in child.attrib:
                attributes_child = child.attrib
                if attributes_child:
                    id_name = play_element.get('id')
                    quarter_num = play_element.get('number')
                    quarter_ids.append(id_name)
                    quarter_nums.append(quarter_num)

    return quarter_ids, quarter_nums

def get_quarter_att():
    xml_file = "./xml_data_output.xml"
    tree = ET.parse(xml_file)
    root = tree.getroot()

    quarter_atts = []

    for play_element in root.iter('quarter'):
        for child in play_element.findall(".//event"):
            if child is not None and 'nullified' not in child.attrib:
                attributes_child = child.attrib
                if attributes_child:
                    quarter_atts.append(attributes_child)

    
    qt_data_df = pd.DataFrame(quarter_atts)

    return qt_data_df

def connect_psql(dataframe, table_name, exists):
    conn_string = 'postgresql://postgres:Tigers11@localhost:5432/RAD_DFS'
    engine = create_engine(conn_string)

    dataframe.to_sql(table_name, con=engine, if_exists=exists, index=False)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    conn.close()

def main():

    for i in range(837, 877):
    # for i in range(0, 798):
        exec_query(i)
        tree = ET.parse('./xml_play_data.xml')
        remove_namespace(tree)
        tree.write('./xml_data_output.xml')
        xml_file = "./xml_data_output.xml"
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Field Goal Statistics Tbl
        fg_player_id_list, fg_player_names = get_player_att('fieldgoal')
        fg_event_ids = get_event_id('fieldgoal')
        fg_player_df = get_tag_att('fieldgoal')

        fg_player_df["player_id"] = fg_player_id_list
        fg_player_df["player_name"] = fg_player_names
        fg_player_df["event_id"] = fg_event_ids

        # Assists Statistics Tbl
        ast_player_id_list, ast_player_names = get_player_att('assist')
        ast_event_ids = get_event_id('assist')
        ast_player_df = get_tag_att('assist')

        ast_player_df["player_id"] = ast_player_id_list
        ast_player_df["player_name"] = ast_player_names
        ast_player_df["event_id"] = ast_event_ids
        ast_player_df["assist_ind"] = 1
        
        # Rebound Statistics Tbl
        reb_player_id_list, reb_player_names = get_player_att('rebound')
        reb_event_ids = get_event_id('rebound')
        reb_player_df = get_tag_att('rebound')

        reb_player_df["player_id"] = reb_player_id_list
        reb_player_df["player_name"] = reb_player_names
        reb_player_df["event_id"] = reb_event_ids

        # Free Throws Statistics Tbl
        ft_player_id_list, ft_player_names = get_player_att('freethrow')
        ft_event_ids = get_event_id('freethrow')
        ft_player_df = get_tag_att('freethrow')

        ft_player_df["player_id"] = ft_player_id_list
        ft_player_df["player_name"] = ft_player_names
        ft_player_df["event_id"] = ft_event_ids

        # Foul(er) Statistics Tbl
        pf_player_id_list, pf_player_names = get_player_att('personalfoul')
        pf_event_ids = get_event_id('personalfoul')
        pf_player_df = get_tag_att('personalfoul')

        pf_player_df["player_id"] = pf_player_id_list
        pf_player_df["player_name"] = pf_player_names
        pf_player_df["event_id"] = pf_event_ids
        pf_player_df["personal_foul_ind"] = 1

        # Foul(ee) Statistics Tbl
        fd_player_id_list, fd_player_names = get_player_att('fouldrawn')
        fd_event_ids = get_event_id('fouldrawn')
        fd_player_df = get_tag_att('fouldrawn')

        fd_player_df["player_id"] = fd_player_id_list
        fd_player_df["player_name"] = fd_player_names
        fd_player_df["event_id"] = fd_event_ids
        fd_player_df["foul_drawn_ind"] = 1

        # Blocker Statistics Table
        blkr_player_id_list, blkr_player_names = get_player_att('block')
        blkr_event_ids = get_event_id('block')
        blkr_player_df = get_tag_att('block')

        blkr_player_df["player_id"] = blkr_player_id_list
        blkr_player_df["player_name"] = blkr_player_names
        blkr_player_df["event_id"] = blkr_event_ids
        blkr_player_df["blocker_ind"] = 1

        # Blockee Statistics Table
        blke_player_id_list, blke_player_names = get_player_att('attemptblocked')
        blke_event_ids = get_event_id('attemptblocked')
        blke_player_df = get_tag_att('attemptblocked')

        blke_player_df["player_id"] = blke_player_id_list
        blke_player_df["player_name"] = blke_player_names
        blke_player_df["event_id"] = blke_event_ids
        blke_player_df["blockee_ind"] = 1

        # Steals Statistics Table
        steal_player_id_list, steal_player_names = get_player_att('steal')
        steal_event_ids = get_event_id('steal')
        steal_player_df = get_tag_att('steal')

        steal_player_df["player_id"] = steal_player_id_list
        steal_player_df["player_name"] = steal_player_names
        steal_player_df["event_id"] = steal_event_ids
        steal_player_df["steal_ind"] = 1

        # Turnovers Statistics Table
        turnover_player_id_list, turnover_player_names = get_player_att('turnover')
        turnover_event_ids = get_event_id('turnover')
        turnover_player_df = get_tag_att('turnover')

        turnover_player_df["player_id"] = turnover_player_id_list
        turnover_player_df["player_name"] = turnover_player_names
        turnover_player_df["event_id"] = turnover_event_ids
        turnover_player_df["turnover_ind"] = 1 

        # Home/Away Players On Court
        home_player_names, home_player_ids, home_jerseys = get_oc_players('home')
        away_player_names, away_player_ids, away_jerseys = get_oc_players('away')

        home_event_ids = get_oc_events('home')
        away_event_ids = get_oc_events('away')

        home_oc_df = pd.DataFrame(
            {
            'event_id': home_event_ids,
            'player_id': home_player_ids,
            'player_name': home_player_names,
            'player_jersey': home_jerseys
            })

        away_oc_df = pd.DataFrame(
            {
            'event_id': away_event_ids,
            'player_id': away_player_ids,
            'player_name': away_player_names,
            'player_jersey': away_jerseys
            })

        # Game Details (High Level)
        game = root.attrib
        game_details_df = pd.DataFrame([game])
        # game_details_df["inseason_tournament"] = np.nan

        # Quarter Data
        final_qids, final_qum = get_quarter_id()
        quarter_df = get_quarter_att()

        quarter_df["quarter_id"] = final_qids
        quarter_df["quarter_number"] = final_qum

        # Quarter - Game XREF
        game_ids_quarter_xref = []
        quarter_xref = []

        for play_element in root.iter('game'):
            for child in play_element.findall(".//quarter"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        quarter_xref.append(attributes_child)

        quarter_xref_df = pd.DataFrame(quarter_xref)

        for play_element in root.iter('game'):
            for child in play_element.findall(".//quarter"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        id_name = play_element.get('id')
                        game_ids_quarter_xref.append(id_name)

        quarter_xref_df['game_id'] = game_ids_quarter_xref

        # Final team game details
        game = root.attrib
        game_id = game.get('id')

        away_data = []
        away_record = []
        home_game_data = []
        home_record = []

        for play_element in root.iter('home'):
            for child in play_element.findall(".//record"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        home_record.append(attributes_child)

        home_record_df = pd.DataFrame(home_record)

        for play_element in root.iter('game'):
            for child in play_element.findall(".//home"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        home_game_data.append(attributes_child)

        game_h_data = home_game_data[0]
        home_game_data_df = pd.DataFrame([game_h_data])
        home_game_data_df["home_ind"] = 1
        home_game_data_df["wins"] = home_record_df["wins"]
        home_game_data_df["losses"] = home_record_df["losses"]

        home_game_data_df.head()

        for play_element in root.iter('away'):
            for child in play_element.findall(".//record"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        away_record.append(attributes_child)

        away_record_df = pd.DataFrame(away_record)

        for play_element in root.iter('game'):
            for child in play_element.findall(".//away"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        away_data.append(attributes_child)

        game_aw_data = away_data[0]
        away_game_data_df = pd.DataFrame([game_aw_data])
        away_game_data_df["home_ind"] = 0 
        away_game_data_df["wins"] = away_record_df["wins"]
        away_game_data_df["losses"] = away_record_df["losses"]

        full_result_data = pd.concat([home_game_data_df, away_game_data_df], ignore_index=True)
        full_result_data['game_id'] = game_id

        # Event Locations
        event_locs = []
        loc_data = []

        for play_element in root.iter('event'):
            for child in play_element.findall(".//location"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        loc_data.append(attributes_child)

        loc_data_df = pd.DataFrame(loc_data)

        for play_element in root.iter('event'):
            for child in play_element.findall(".//location"):
                if child is not None and 'nullified' not in child.attrib:
                    attributes_child = child.attrib
                    if attributes_child:
                        id_name = play_element.get('id')
                        event_locs.append(id_name)

        loc_data_df["event_id"] = event_locs

        # Dataframes to pg DB
        connect_psql(fg_player_df, "FIELD_GOALS_CURR", 'append')
        connect_psql(ast_player_df, "ASSISTS_CURR", 'append')
        connect_psql(reb_player_df, "REBOUNDS_CURR", 'append')
        connect_psql(pf_player_df, "FOULER_CURR", 'append')
        connect_psql(fd_player_df, "FOULEE_CURR", 'append')
        connect_psql(blkr_player_df, "BLOCKER_CURR", 'append')
        connect_psql(blke_player_df, "BLOCKEE_CURR", 'append')
        connect_psql(steal_player_df, "STEALS_CURR", 'append')
        connect_psql(turnover_player_df, "TURNOVERS_CURR", 'append')
        connect_psql(home_oc_df, "HOME_ON_COURT_CURR", 'append')
        connect_psql(away_oc_df, "AWAY_ON_COURT_CURR", 'append')
        connect_psql(game_details_df, "GAME_DETAILS_CURR", 'append')
        connect_psql(quarter_df, "QUARTER_DETAILS_CURR", 'append')
        connect_psql(quarter_xref_df, "QUARTER_GAME_XREF_CURR", 'append')
        connect_psql(full_result_data, "TEAM_GAME_DETAILS_CURR", 'append')
        connect_psql(loc_data_df, "EVENT_LOCATIONS_CURR", 'append')

        print(i)
        print("-----")
        print(game_id)

if __name__ == "__main__":
    main()
