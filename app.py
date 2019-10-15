# -*- coding: utf-8 -*-
# Copyright (c) 2019 Linh Pham
# wwdtm_winstreaks is relased under the terms of the Apache License 2.0
"""Calculate and display panelist win streaks from scores in the WWDTM
Stats database"""

#import argparse
from collections import OrderedDict
import json
import math
import os
from typing import List, Dict
import mysql.connector
from mysql.connector.errors import DatabaseError, ProgrammingError
#from wwdtm.panelist import core, info, details, utility

def retrieve_panelists(database_connection: mysql.connector.connect
                      ) -> List[Dict]:
    """Retrieve a list of panelists with their panelist ID and name"""
    cursor = database_connection.cursor()
    query = ("SELECT p.panelistid, p.panelist FROM ww_panelists p "
             "WHERE p.panelistid <> 17 "
             "ORDER BY p.panelist ASC;")
    cursor.execute(query)
    result = cursor.fetchall()

    if not result:
        return None

    panelists = []
    for row in result:
        panelist = OrderedDict()
        panelist["id"] = row[0]
        panelist["name"] = row[1]
        panelists.append(panelist)

    return panelists

def retrieve_panelist_ranks(panelist_id: int,
                            database_connection: mysql.connector.connect
                           ) -> List[Dict]:
    """Retrieve a list of show dates and the panelist rank for the
    requested panelist ID"""
    cursor = database_connection.cursor()
    query = ("SELECT s.showdate, pm.showpnlrank FROM ww_showpnlmap PM "
             "JOIN ww_shows s ON s.showid = pm.showid "
             "WHERE pm.panelistid = %s AND "
             "s.bestof = 0 AND s.repeatshowid IS NULL "
             "ORDER BY s.showdate ASC;")
    cursor.execute(query, (panelist_id,))
    result = cursor.fetchall()

    if not result:
        return None

    ranks = []
    for row in result:
        info = OrderedDict()
        info["show_date"] = row[0].isoformat()
        info["rank"] = row[1]
        ranks.append(info)

    return ranks

def calculate_panelist_win_streaks(database_connection: mysql.connector.connect):
    """Retrieve panelist stats and calculate their win streaks"""
    panelists = retrieve_panelists(database_connection)
    for panelist in panelists:
        print("{}:".format(panelist["name"]))
        longest_win_streak = 0
        longest_win_streak_with_draws = 0
        total_wins = 0
        total_wins_with_draws = 0

        shows = retrieve_panelist_ranks(panelist["id"], database_connection)

        # Calculate win streaks 
        current_streak = 0
        for show in shows:
            if show["rank"] == "1":
                total_wins += 1
                current_streak += 1

                if current_streak > longest_win_streak:
                    longest_win_streak = current_streak
            else:
                current_streak = 0

        # Calculate win streaks with draws
        current_streak_with_draws = 0
        for show in shows:
            if show["rank"] == "1" or show["rank"] == "1t":
                    total_wins_with_draws += 1
                    current_streak_with_draws += 1
                    if current_streak_with_draws > longest_win_streak_with_draws:
                        longest_win_streak_with_draws = current_streak_with_draws
            else:
                current_streak_with_draws = 0

        print("  First Place:                    {}".format(total_wins))
        print("  First Place + Draws:            {}".format(total_wins_with_draws))

        print("  Longest win streak              {}".format(longest_win_streak))
        print("  Longest win streak with draws:  {}".format(longest_win_streak_with_draws))
        print("\n\n")
    return None

def load_config(app_environment) -> Dict:
    """Load configuration file from config.json"""
    with open('config.json', 'r') as config_file:
        config_dict = json.load(config_file)

    if app_environment.startswith("develop"):
        if "development" in config_dict:
            config = config_dict["development"]
        else:
            raise Exception("Missing 'development' section in config file")
    elif app_environment.startswith("prod"):
        if "production" in config_dict:
            config = config_dict['production']
        else:
            raise Exception("Missing 'production' section in config file")
    else:
        if "local" in config_dict:
            config = config_dict["local"]
        else:
            raise Exception("Missing 'local' section in config file")

    return config


def main():
    """Pull in scoring data and generate image based on the data"""
    app_environment = os.getenv("APP_ENV", "local").strip().lower()
    config = load_config(app_environment)
    database_connection = mysql.connector.connect(**config["database"])
    calculate_panelist_win_streaks(database_connection)
    return None



# Only run if executed as a script and not imported
if __name__ == "__main__":
    main()
