import sqlite3
import pandas as pd
import xml.etree.ElementTree as ET
import http.client
import time
import psycopg2
from sqlalchemy import create_engine

# Run on correct path 

def get_all_ids():
    con = sqlite3.connect("""./RAD.sqlite3""")
    game_schedules = pd.read_sql("""SELECT * FROM 
                                NBA_GAME_DETAILS_CUR 
                                WHERE DATE(GAME_DATE) = '2024-02-14'
                                """, con=con)
    con.close()

    game_ids = game_schedules["GAME_ID"].to_list()

    return game_ids

def get_game_data(id):
    conn = http.client.HTTPSConnection("api.sportradar.us")

    conn.request("GET", f"http://api.sportradar.us/nba/trial/v8/en/games/{id}/pbp.xml?api_key=53ak3zzs9hjyq6ewb93khn8h")
    res = conn.getresponse()
    data = res.read()
    full_data = data.decode("utf-8")

    fd_df_dict = {
        "DATA": [full_data],
        "YEAR": [2023]
    }

    fd_df = pd.DataFrame(fd_df_dict)

    return fd_df

def connect_psql(dataframe, table_name, exists):
    conn_string = 'postgresql://postgres:Tigers11@localhost:5432/RAD_DFS'
    engine = create_engine(conn_string)

    dataframe.to_sql(table_name, con=engine, if_exists=exists, index=False)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

def main():
    game_list = get_all_ids()

    for i in range(0, len(game_list)):
        df = get_game_data(game_list[i])
        # test_xml_data = df["DATA"].values.tolist()
        # test_xml_cont = test_xml_data[0].strip()

        # with open("./xml_rec_data.xml", 'w', encoding='utf-8') as new_file:
        #     new_file.write(test_xml_cont)
        connect_psql(df, 'NBA_API_RAW_DATA', 'append')
        print(i)
        print(game_list[i])
        time.sleep(3)

if __name__ == "__main__":
    main()
