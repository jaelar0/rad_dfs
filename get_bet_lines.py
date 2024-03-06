import psycopg2
import pandas as pd
import json
import requests
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta
today = date.today()
yesterday = datetime.now() - timedelta(1)

yesterday = datetime.strftime(yesterday, '%Y-%m-%d')

def connect_psql(dataframe, table_name, exists):
    conn_string = 'postgresql://postgres:Tigers11@localhost:5432/RAD_DFS'
    engine = create_engine(conn_string)

    dataframe.to_sql(table_name, con=engine, if_exists=exists, index=False)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

def get_underdog():
    url = "https://api.underdogfantasy.com/beta/v3/over_under_lines"

    payload={}
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Client-Type': 'web',
    'Client-Version': '202301131646',
    'User-Latitude': '34.80577893876544',
    'User-Longitude': '-82.61383625145704',
    'Client-Device-Id': '2c5131ed-7057-4162-9dca-bb4feaf614e3',
    'Referring-Link': '',
    'Client-Request-Id': '6980ff24-87e8-44b4-838e-784ee78f11e0',
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI1YThlYWU4Yy1lYjc5LTQxMGQtYWVkMi1jZjdjNDFiNWRkMDIiLCJzdWIiOiI4YjM2NjBlOS1jYWJmLTRhMTQtYWI0Mi02MTNhOGRhM2YyMTciLCJzY3AiOiJ1c2VyIiwiYXVkIjpudWxsLCJpYXQiOjE2NzM2Njc1NjksImV4cCI6MTY3NjI5NzMxNX0.2izNBZLkUa6_hgzQlxMd2xRR1ofuviRtJQclU-6G4Mk',
    'Origin': 'https://underdogfantasy.com',
    'Connection': 'keep-alive',
    'Referer': 'https://underdogfantasy.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'If-None-Match': 'W/"ed578f97b69887054ceb04ac79bd2a05"',
    'TE': 'trailers'    
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    json_file = open('./nfl_betting.json', 'w')
    json_file.write(response.text)
    json_file.close()

def get_bet_df():
    r = open('./nfl_betting.json')
    data = json.load(r)
    over_under = data['over_under_lines']

    bet = []
    bet_type = []
    bet_amt = []
    bet_id = []

    i = 0

    while i < len(over_under):
        bet_id.append(over_under[i]['over_under']['appearance_stat']['appearance_id'])
        bet.append(over_under[i]['over_under']['title'])
        bet_type.append(over_under[i]['over_under']['appearance_stat']['display_stat'])
        bet_amt.append(over_under[i]['stat_value'])
        
        i = i + 1


    bet_dict = {
        "bet_id": bet_id,
        "bet_title": bet,
        "bet_type": bet_type,
        "bet_line": bet_amt
    }

    bets_df = pd.DataFrame(bet_dict)
    bets_df["DATE"] = today

    return bets_df

def get_player_bet():
    r = open('./nfl_betting.json')
    data = json.load(r)
    players_data = data['players']

    player_id = []
    first_name = []
    last_name = []
    image_url = []
    position_id = []
    sport_id = []
    team_id = []

    i = 0

    while i < len(players_data):
        player_id.append(players_data[i]['id'])
        first_name.append(players_data[i]['first_name'])
        last_name.append(players_data[i]['last_name'])
        image_url.append(players_data[i]['image_url'])
        position_id.append(players_data[i]['position_id'])
        sport_id.append(players_data[i]['sport_id'])
        team_id.append(players_data[i]['team_id'])
        i = i + 1

    player_dict = {
        "player_id": player_id,
        "first_name": first_name,
        "last_name": last_name,
        "image_url": image_url,
        "position_id": position_id,
        "sport_id": sport_id,
        "team_id": team_id
    }

    players_df = pd.DataFrame(player_dict)
    players_df["run_date"] = today

    return players_df


def get_bets_id():
    r = open('./nfl_betting.json')
    data = json.load(r)
    bet_ids = data['appearances']

    bet_id = []
    player_id = []

    i = 0

    while i < len(bet_ids):
        bet_id.append(bet_ids[i]['id'])
        player_id.append(bet_ids[i]['player_id'])
        i = i + 1

    bet_ids_dict = {
        "bet_id": bet_id,
        "player_id": player_id,
    }

    bet_ids_df = pd.DataFrame(bet_ids_dict)
    bet_ids_df["run_date"] = today

    return bet_ids_df

def get_game_bet():
    r = open('./nfl_betting.json')
    data = json.load(r)
    game_detail = data['games']

    game_title = []
    away_team_id = []
    home_team_id = []
    match_time = []
    sport_id = []
    game_date = []

    i = 0

    while i < len(game_detail):
        game_title.append(game_detail[i]['title'])
        away_team_id.append(game_detail[i]['away_team_id'])
        home_team_id.append(game_detail[i]['home_team_id'])
        match_time.append(game_detail[i]['match_progress'])
        sport_id.append(game_detail[i]['sport_id'])
        game_date.append(game_detail[i]['scheduled_at'])
        
        i = i + 1

    games_dict = {
        "game_title": game_title,
        "away_team_id": away_team_id,
        "home_team_id": home_team_id,
        "match_time": match_time,
        "sport_id": sport_id,
        "game_date": game_date
    }

    games_df = pd.DataFrame(games_dict)
    games_df["run_date"] = today
    games_df["home_team_name"] = games_df["game_title"].str.split("@").str[1]
    games_df["away_team_name"] = games_df["game_title"].str.split("@").str[0]

    return games_df

def main():
    get_underdog()

    bets_df = get_bet_df()
    bets_player_df = get_player_bet()
    bet_xref_df = get_bets_id()
    bet_game_df = get_game_bet()

    # Dataframes to SQLite
    connect_psql(bets_df, "BET_OVERVIEW_RAW", 'append')
    connect_psql(bets_player_df, "BET_PLAYER_INFO_RAW", 'append')
    connect_psql(bet_xref_df, "BET_RAW_XREF", 'append')
    connect_psql(bet_game_df, "BET_GAME_INFO_RAW", 'append')      
    print("Data Injected!")

if __name__ == "__main__":
    main()

