# -*- coding: utf-8 -*-


import os
import json
import gspread
from google.oauth2.service_account import Credentials

def get_client():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

gc = get_client()

# 🔴 PASTE YOUR GOOGLE SHEET NAME HERE (must already exist)
SHEET_NAME = "IPL_2026_Dashboard"

sh = gc.open(SHEET_NAME)

TABS = ["Batting", "Bowling", "Extras", "Fantasy_Points"]
existing = [w.title for w in sh.worksheets()]
for tab in TABS:
    if tab not in existing:
        sh.add_worksheet(title=tab, rows="5000", cols="30")
        print(f"✅ Created tab: {tab}")

print("✅ Authenticated and sheet ready!")

import requests, zipfile, io, json, os
import pandas as pd
from pathlib import Path
from datetime import date

ZIP_URL    = "https://cricsheet.org/downloads/ipl_male_json.zip"
DATA_DIR   = Path("ipl_json")
START_DATE = "2026-03-28"

print("⬇️  Fetching latest IPL data from Cricsheet...")
r = requests.get(ZIP_URL, timeout=60)
r.raise_for_status()
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    z.extractall(DATA_DIR)
print(f"✅ {len(list(DATA_DIR.glob('*.json')))} match files available")


def parse_match(filepath):
    with open(filepath) as f:
        match = json.load(f)

    info       = match["info"]
    match_date = info["dates"][0]
    teams      = info["teams"]
    venue      = info.get("venue", "Unknown")
    winner     = info.get("outcome", {}).get("winner", "N/A")
    match_id   = f"{match_date}_{teams[0]}_vs_{teams[1]}"

    batting_rows, bowling_rows, extras_rows = [], [], []

    for inn in match["innings"]:
        team       = inn["team"]
        overs_data = inn["overs"]
        bat_stats, bowl_stats = {}, {}
        maiden_candidates = {}
        extras = {"wides":0, "noballs":0, "byes":0, "legbyes":0}

        for over_obj in overs_data:
            over_num = over_obj["over"]
            for d in over_obj["deliveries"]:
                batter  = d["batter"]
                bowler  = d["bowler"]
                runs    = d["runs"]
                bat_run = runs["batter"]
                ex_type = d.get("extras", {})
                is_wide = "wides"   in ex_type
                is_nb   = "noballs" in ex_type

                if batter not in bat_stats:
                    bat_stats[batter] = {"Runs":0,"Balls":0,"Fours":0,"Sixes":0,"Dots":0,"Dismissed":False}
                if not (is_wide or is_nb):
                    bat_stats[batter]["Balls"] += 1
                bat_stats[batter]["Runs"] += bat_run
                if bat_run == 4: bat_stats[batter]["Fours"] += 1
                if bat_run == 6: bat_stats[batter]["Sixes"] += 1
                if bat_run == 0 and not is_wide: bat_stats[batter]["Dots"] += 1

                if "wickets" in d:
                    for w in d["wickets"]:
                        dismissed = w.get("player_out", batter)
                        if dismissed in bat_stats:
                            bat_stats[dismissed]["Dismissed"] = True

                if bowler not in bowl_stats:
                    bowl_stats[bowler] = {"Balls":0,"Runs":0,"Wickets":0,"Wides":0,"NoBalls":0,"DotBalls":0,"LBW_Bowled":0,"Maidens":0}
                if not is_wide:
                    bowl_stats[bowler]["Balls"] += 1
                bowl_stats[bowler]["Runs"] += runs["total"]
                if is_wide: bowl_stats[bowler]["Wides"]   += 1
                if is_nb:   bowl_stats[bowler]["NoBalls"] += 1
                if runs["total"] == 0: bowl_stats[bowler]["DotBalls"] += 1
                if "wickets" in d:
                    for w in d["wickets"]:
                        kind = w.get("kind", "")
                        if kind not in ("run out", "obstructing the field"):
                            bowl_stats[bowler]["Wickets"] += 1
                        if kind in ("lbw", "bowled"):
                            bowl_stats[bowler]["LBW_Bowled"] += 1

                key = (bowler, over_num)
                if key not in maiden_candidates:
                    maiden_candidates[key] = 0
                maiden_candidates[key] += runs["total"]

                for k in extras: extras[k] += ex_type.get(k, 0)

        for (bowler, _), total_runs in maiden_candidates.items():
            if total_runs == 0 and bowler in bowl_stats:
                bowl_stats[bowler]["Maidens"] += 1

        field_stats = {}
        for over_obj in overs_data:
            for d in over_obj["deliveries"]:
                if "wickets" not in d: continue
                for w in d["wickets"]:
                    kind = w.get("kind", "")
                    for fielder in w.get("fielders", []):
                        fname = fielder.get("name", "")
                        if not fname: continue
                        if fname not in field_stats:
                            field_stats[fname] = {"Catches":0,"Stumpings":0,"RunOut_Direct":0,"RunOut_Indirect":0}
                        if   kind == "caught":  field_stats[fname]["Catches"]       += 1
                        elif kind == "stumped": field_stats[fname]["Stumpings"]     += 1
                        elif kind == "run out":
                            n_fielders = len(w.get("fielders", []))
                            if n_fielders == 1:
                                field_stats[fname]["RunOut_Direct"]   += 1
                            else:
                                field_stats[fname]["RunOut_Indirect"] += 1

        base = {"MatchID": match_id, "Date": match_date, "Venue": venue, "Winner": winner, "BattingTeam": team}

        for batter, s in bat_stats.items():
            sr = round(s["Runs"] / s["Balls"] * 100, 1) if s["Balls"] > 0 else 0
            batting_rows.append({**base, "Player": batter,
                "Runs": s["Runs"], "Balls": s["Balls"], "Fours": s["Fours"],
                "Sixes": s["Sixes"], "Dots": s["Dots"], "SR": sr, "Dismissed": s["Dismissed"]})

        for bowler, s in bowl_stats.items():
            overs_str = f"{s['Balls']//6}.{s['Balls']%6}"
            econ = round(s["Runs"] / (s["Balls"]/6), 2) if s["Balls"] > 0 else 0
            bowling_rows.append({**base, "Player": bowler,
                "Overs": overs_str, "Balls": s["Balls"], "Runs": s["Runs"],
                "Wickets": s["Wickets"], "Economy": econ,
                "DotBalls": s["DotBalls"], "Maidens": s["Maidens"],
                "Wides": s["Wides"], "NoBalls": s["NoBalls"], "LBW_Bowled": s["LBW_Bowled"],
                "Catches": field_stats.get(bowler,{}).get("Catches",0),
                "Stumpings": field_stats.get(bowler,{}).get("Stumpings",0),
                "RunOut_Direct": field_stats.get(bowler,{}).get("RunOut_Direct",0),
                "RunOut_Indirect": field_stats.get(bowler,{}).get("RunOut_Indirect",0)})

        extras_rows.append({**base,
            "Wides": extras["wides"], "NoBalls": extras["noballs"],
            "Byes": extras["byes"], "LegByes": extras["legbyes"],
            "Total": sum(extras.values())})

        for fname, fs in field_stats.items():
            if fname not in bowl_stats:
                bowling_rows.append({**base, "Player": fname,
                    "Overs":"0.0","Balls":0,"Runs":0,"Wickets":0,"Economy":0,
                    "DotBalls":0,"Maidens":0,"Wides":0,"NoBalls":0,"LBW_Bowled":0,
                    "Catches":fs["Catches"],"Stumpings":fs["Stumpings"],
                    "RunOut_Direct":fs["RunOut_Direct"],"RunOut_Indirect":fs["RunOut_Indirect"]})

    return (pd.DataFrame(batting_rows), pd.DataFrame(bowling_rows), pd.DataFrame(extras_rows))


all_bat, all_bowl, all_ext = [], [], []
for fpath in sorted(DATA_DIR.glob("*.json")):
    with open(fpath) as f:
        d = json.load(f)["info"]
    if d["dates"][0] >= START_DATE:
        bat, bowl, ext = parse_match(fpath)
        all_bat.append(bat); all_bowl.append(bowl); all_ext.append(ext)

batting_df = pd.concat(all_bat,  ignore_index=True) if all_bat  else pd.DataFrame()
bowling_df = pd.concat(all_bowl, ignore_index=True) if all_bowl else pd.DataFrame()
extras_df  = pd.concat(all_ext,  ignore_index=True) if all_ext  else pd.DataFrame()

print(f"✅ Parsed {len(all_bat)} matches | {len(batting_df)} batting rows | {len(bowling_df)} bowling rows")


# ── Fantasy Points ────────────────────────────────────────────────────────────

def batting_points(row):
    pts = 0
    r, b = row["Runs"], row["Balls"]
    pts += r * 1
    pts += row["Fours"] * 4
    pts += row["Sixes"] * 6
    if r >= 25:  pts += 4
    if r >= 50:  pts += 8
    if r >= 75:  pts += 12
    if r >= 100: pts += 16
    if r == 0 and row["Dismissed"]: pts -= 2
    if b >= 10:
        sr = r / b * 100
        if   sr > 170:  pts += 6
        elif sr > 150:  pts += 4
        elif sr >= 130: pts += 2
        elif 60 <= sr <= 70: pts -= 2
        elif 50 <= sr < 60:  pts -= 4
        elif sr < 50:        pts -= 6
    return pts


def bowling_points(row):
    pts = 0
    w, balls = row["Wickets"], row["Balls"]
    pts += row["DotBalls"] * 1
    pts += w * 30
    pts += row["LBW_Bowled"] * 8
    pts += row["Maidens"] * 12
    if w >= 3: pts += 4
    if w >= 4: pts += 8
    if w >= 5: pts += 12
    if balls >= 12:
        econ = row["Runs"] / (balls / 6)
        if   econ < 5:   pts += 6
        elif econ < 6:   pts += 4
        elif econ <= 7:  pts += 2
        elif econ <= 11: pts -= 2
        elif econ <= 12: pts -= 4
        else:            pts -= 6
    return pts


def fielding_points(row):
    pts  = row["Catches"]         * 8
    pts += row["Stumpings"]       * 12
    pts += row["RunOut_Direct"]   * 12
    pts += row["RunOut_Indirect"] * 6
    if row["Catches"] >= 3: pts += 4
    return pts


bat_pts = batting_df[["MatchID","Date","Venue","Winner","BattingTeam","Player","Runs","Balls","Fours","Sixes","Dismissed"]].copy()
bat_pts["Bat_Pts"] = bat_pts.apply(batting_points, axis=1)

bowl_pts = bowling_df[["MatchID","Date","BattingTeam","Player","Balls","Runs","Wickets","DotBalls","Maidens","LBW_Bowled","Catches","Stumpings","RunOut_Direct","RunOut_Indirect"]].copy()
bowl_pts["Bowl_Pts"]  = bowl_pts.apply(bowling_points,  axis=1)
bowl_pts["Field_Pts"] = bowl_pts.apply(fielding_points, axis=1)

fantasy = pd.merge(
    bat_pts[["MatchID","Date","Venue","Winner","BattingTeam","Player","Bat_Pts"]],
    bowl_pts[["MatchID","BattingTeam","Player","Bowl_Pts","Field_Pts"]],
    on=["MatchID","BattingTeam","Player"], how="outer"
).fillna(0)

# Group by MatchID + Player ONLY (not BattingTeam) so a player's entries
# across both innings collapse into one row before the +4 is applied
fantasy = fantasy.groupby(
    ["MatchID","Date","Venue","Winner","Player"], as_index=False
).agg({"Bat_Pts":"sum", "Bowl_Pts":"sum", "Field_Pts":"sum"})

# +4 participation bonus applied exactly once per player per match
fantasy["Total_Pts"] = fantasy["Bat_Pts"] + fantasy["Bowl_Pts"] + fantasy["Field_Pts"] + 4

fantasy = fantasy.sort_values(["Date","MatchID","Total_Pts"], ascending=[True,True,False])

print(f"✅ Fantasy points calculated for {fantasy['Player'].nunique()} players across {fantasy['MatchID'].nunique()} matches")
print(fantasy[["Date","Player","Bat_Pts","Bowl_Pts","Field_Pts","Total_Pts"]].head(10).to_string(index=False))

# import requests, zipfile, io, json, os
# import pandas as pd
# from pathlib import Path
# from datetime import date

# ZIP_URL    = "https://cricsheet.org/downloads/ipl_male_json.zip"
# DATA_DIR   = Path("ipl_json")
# START_DATE = "2026-03-28"

# print("⬇️  Fetching latest IPL data from Cricsheet...")
# r = requests.get(ZIP_URL, timeout=60)
# r.raise_for_status()
# with zipfile.ZipFile(io.BytesIO(r.content)) as z:
#     z.extractall(DATA_DIR)
# print(f"✅ {len(list(DATA_DIR.glob('*.json')))} match files available")


# def parse_match(filepath):
#     with open(filepath) as f:
#         match = json.load(f)

#     info       = match["info"]
#     match_date = info["dates"][0]
#     teams      = info["teams"]
#     venue      = info.get("venue", "Unknown")
#     winner     = info.get("outcome", {}).get("winner", "N/A")
#     match_id   = f"{match_date}_{teams[0]}_vs_{teams[1]}"

#     batting_rows, bowling_rows, extras_rows = [], [], []

#     for inn in match["innings"]:
#         team       = inn["team"]
#         overs_data = inn["overs"]
#         bat_stats, bowl_stats = {}, {}
#         maiden_candidates = {}
#         extras = {"wides":0, "noballs":0, "byes":0, "legbyes":0}

#         for over_obj in overs_data:
#             over_num = over_obj["over"]
#             for d in over_obj["deliveries"]:
#                 batter  = d["batter"]
#                 bowler  = d["bowler"]
#                 runs    = d["runs"]
#                 bat_run = runs["batter"]
#                 ex_type = d.get("extras", {})
#                 is_wide = "wides"   in ex_type
#                 is_nb   = "noballs" in ex_type

#                 if batter not in bat_stats:
#                     bat_stats[batter] = {"Runs":0,"Balls":0,"Fours":0,"Sixes":0,"Dots":0,"Dismissed":False}
#                 if not (is_wide or is_nb):
#                     bat_stats[batter]["Balls"] += 1
#                 bat_stats[batter]["Runs"] += bat_run
#                 if bat_run == 4: bat_stats[batter]["Fours"] += 1
#                 if bat_run == 6: bat_stats[batter]["Sixes"] += 1
#                 if bat_run == 0 and not is_wide: bat_stats[batter]["Dots"] += 1

#                 if "wickets" in d:
#                     for w in d["wickets"]:
#                         dismissed = w.get("player_out", batter)
#                         if dismissed in bat_stats:
#                             bat_stats[dismissed]["Dismissed"] = True

#                 if bowler not in bowl_stats:
#                     bowl_stats[bowler] = {"Balls":0,"Runs":0,"Wickets":0,"Wides":0,"NoBalls":0,"DotBalls":0,"LBW_Bowled":0,"Maidens":0}
#                 if not is_wide:
#                     bowl_stats[bowler]["Balls"] += 1
#                 bowl_stats[bowler]["Runs"] += runs["total"]
#                 if is_wide: bowl_stats[bowler]["Wides"]   += 1
#                 if is_nb:   bowl_stats[bowler]["NoBalls"] += 1
#                 if runs["total"] == 0: bowl_stats[bowler]["DotBalls"] += 1
#                 if "wickets" in d:
#                     for w in d["wickets"]:
#                         kind = w.get("kind", "")
#                         if kind not in ("run out", "obstructing the field"):
#                             bowl_stats[bowler]["Wickets"] += 1
#                         if kind in ("lbw", "bowled"):
#                             bowl_stats[bowler]["LBW_Bowled"] += 1

#                 key = (bowler, over_num)
#                 if key not in maiden_candidates:
#                     maiden_candidates[key] = 0
#                 maiden_candidates[key] += runs["total"]

#                 for k in extras: extras[k] += ex_type.get(k, 0)

#         for (bowler, _), total_runs in maiden_candidates.items():
#             if total_runs == 0 and bowler in bowl_stats:
#                 bowl_stats[bowler]["Maidens"] += 1

#         field_stats = {}
#         for over_obj in overs_data:
#             for d in over_obj["deliveries"]:
#                 if "wickets" not in d: continue
#                 for w in d["wickets"]:
#                     kind = w.get("kind", "")
#                     for fielder in w.get("fielders", []):
#                         fname = fielder.get("name", "")
#                         if not fname: continue
#                         if fname not in field_stats:
#                             field_stats[fname] = {"Catches":0,"Stumpings":0,"RunOut_Direct":0,"RunOut_Indirect":0}
#                         if   kind == "caught":  field_stats[fname]["Catches"]       += 1
#                         elif kind == "stumped": field_stats[fname]["Stumpings"]     += 1
#                         elif kind == "run out":
#                             n_fielders = len(w.get("fielders", []))
#                             if n_fielders == 1:
#                                 field_stats[fname]["RunOut_Direct"]   += 1
#                             else:
#                                 field_stats[fname]["RunOut_Indirect"] += 1

#         base = {"MatchID": match_id, "Date": match_date, "Venue": venue, "Winner": winner, "BattingTeam": team}

#         for batter, s in bat_stats.items():
#             sr = round(s["Runs"] / s["Balls"] * 100, 1) if s["Balls"] > 0 else 0
#             batting_rows.append({**base, "Player": batter,
#                 "Runs": s["Runs"], "Balls": s["Balls"], "Fours": s["Fours"],
#                 "Sixes": s["Sixes"], "Dots": s["Dots"], "SR": sr, "Dismissed": s["Dismissed"]})

#         for bowler, s in bowl_stats.items():
#             overs_str = f"{s['Balls']//6}.{s['Balls']%6}"
#             econ = round(s["Runs"] / (s["Balls"]/6), 2) if s["Balls"] > 0 else 0
#             bowling_rows.append({**base, "Player": bowler,
#                 "Overs": overs_str, "Balls": s["Balls"], "Runs": s["Runs"],
#                 "Wickets": s["Wickets"], "Economy": econ,
#                 "DotBalls": s["DotBalls"], "Maidens": s["Maidens"],
#                 "Wides": s["Wides"], "NoBalls": s["NoBalls"], "LBW_Bowled": s["LBW_Bowled"],
#                 "Catches": field_stats.get(bowler,{}).get("Catches",0),
#                 "Stumpings": field_stats.get(bowler,{}).get("Stumpings",0),
#                 "RunOut_Direct": field_stats.get(bowler,{}).get("RunOut_Direct",0),
#                 "RunOut_Indirect": field_stats.get(bowler,{}).get("RunOut_Indirect",0)})

#         extras_rows.append({**base,
#             "Wides": extras["wides"], "NoBalls": extras["noballs"],
#             "Byes": extras["byes"], "LegByes": extras["legbyes"],
#             "Total": sum(extras.values())})

#         for fname, fs in field_stats.items():
#             if fname not in bowl_stats:
#                 bowling_rows.append({**base, "Player": fname,
#                     "Overs":"0.0","Balls":0,"Runs":0,"Wickets":0,"Economy":0,
#                     "DotBalls":0,"Maidens":0,"Wides":0,"NoBalls":0,"LBW_Bowled":0,
#                     "Catches":fs["Catches"],"Stumpings":fs["Stumpings"],
#                     "RunOut_Direct":fs["RunOut_Direct"],"RunOut_Indirect":fs["RunOut_Indirect"]})

#     return (pd.DataFrame(batting_rows), pd.DataFrame(bowling_rows), pd.DataFrame(extras_rows))


# all_bat, all_bowl, all_ext = [], [], []
# for fpath in sorted(DATA_DIR.glob("*.json")):
#     with open(fpath) as f:
#         d = json.load(f)["info"]
#     if d["dates"][0] >= START_DATE:
#         bat, bowl, ext = parse_match(fpath)
#         all_bat.append(bat); all_bowl.append(bowl); all_ext.append(ext)

# batting_df = pd.concat(all_bat,  ignore_index=True) if all_bat  else pd.DataFrame()
# bowling_df = pd.concat(all_bowl, ignore_index=True) if all_bowl else pd.DataFrame()
# extras_df  = pd.concat(all_ext,  ignore_index=True) if all_ext  else pd.DataFrame()

# print(f"✅ Parsed {len(all_bat)} matches | {len(batting_df)} batting rows | {len(bowling_df)} bowling rows")

# def batting_points(row):
#     pts = 0
#     r, b = row["Runs"], row["Balls"]
#     pts += r * 1
#     pts += row["Fours"] * 4
#     pts += row["Sixes"] * 6
#     if r >= 25:  pts += 4
#     if r >= 50:  pts += 8
#     if r >= 75:  pts += 12
#     if r >= 100: pts += 16
#     if r == 0 and row["Dismissed"]: pts -= 2
#     if b >= 10:
#         sr = r / b * 100
#         if   sr > 170:  pts += 6
#         elif sr > 150:  pts += 4
#         elif sr >= 130: pts += 2
#         elif 60 <= sr <= 70: pts -= 2
#         elif 50 <= sr < 60:  pts -= 4
#         elif sr < 50:        pts -= 6
#     return pts


# def bowling_points(row):
#     pts = 0
#     w, balls = row["Wickets"], row["Balls"]
#     pts += row["DotBalls"] * 1
#     pts += w * 30
#     pts += row["LBW_Bowled"] * 8
#     pts += row["Maidens"] * 12
#     if w >= 3: pts += 4
#     if w >= 4: pts += 8
#     if w >= 5: pts += 12
#     if balls >= 12:
#         econ = row["Runs"] / (balls / 6)
#         if   econ < 5:   pts += 6
#         elif econ < 6:   pts += 4
#         elif econ <= 7:  pts += 2
#         elif econ <= 11: pts -= 2
#         elif econ <= 12: pts -= 4
#         else:            pts -= 6
#     return pts


# def fielding_points(row):
#     pts  = row["Catches"]        * 8
#     pts += row["Stumpings"]      * 12
#     pts += row["RunOut_Direct"]  * 12
#     pts += row["RunOut_Indirect"]* 6
#     if row["Catches"] >= 3: pts += 4
#     return pts


# bat_pts = batting_df[["MatchID","Date","Venue","Winner","BattingTeam","Player","Runs","Balls","Fours","Sixes","Dismissed"]].copy()
# bat_pts["Bat_Pts"] = bat_pts.apply(batting_points, axis=1)

# bowl_pts = bowling_df[["MatchID","Date","BattingTeam","Player","Balls","Runs","Wickets","DotBalls","Maidens","LBW_Bowled","Catches","Stumpings","RunOut_Direct","RunOut_Indirect"]].copy()
# bowl_pts["Bowl_Pts"]  = bowl_pts.apply(bowling_points,  axis=1)
# bowl_pts["Field_Pts"] = bowl_pts.apply(fielding_points, axis=1)

# fantasy = pd.merge(
#     bat_pts[["MatchID","Date","Venue","Winner","BattingTeam","Player","Bat_Pts"]],
#     bowl_pts[["MatchID","BattingTeam","Player","Bowl_Pts","Field_Pts"]],
#     on=["MatchID","BattingTeam","Player"], how="outer"
# ).fillna(0)

# fantasy["Total_Pts"] = fantasy["Bat_Pts"] + fantasy["Bowl_Pts"] + fantasy["Field_Pts"] + 4  # +4 participation bonus

# fantasy = fantasy.sort_values(["Date","MatchID","Total_Pts"], ascending=[True,True,False])

# print(f"✅ Fantasy points calculated for {fantasy['Player'].nunique()} players across {fantasy['MatchID'].nunique()} matches")
# print(fantasy[["Date","Player","Bat_Pts","Bowl_Pts","Field_Pts","Total_Pts"]].head(10).to_string(index=False))

from gspread_dataframe import get_as_dataframe, set_with_dataframe

def upsert_sheet(worksheet, new_df, dedup_cols):
    try:
        existing = get_as_dataframe(worksheet, evaluate_formulas=False).dropna(how="all")
        existing = existing.dropna(subset=dedup_cols)
    except:
        existing = pd.DataFrame()

    if not existing.empty:
        existing_keys = set(existing[dedup_cols].astype(str).agg("|".join, axis=1))
        new_keys      = new_df[dedup_cols].astype(str).agg("|".join, axis=1)
        new_rows      = new_df[~new_keys.isin(existing_keys)]
    else:
        new_rows = new_df

    if new_rows.empty:
        print(f"  ↩️  No new rows for '{worksheet.title}'")
        return 0

    combined = pd.concat([existing, new_rows], ignore_index=True) if not existing.empty else new_rows
    set_with_dataframe(worksheet, combined)
    print(f"  ✅ '{worksheet.title}': added {len(new_rows)} new rows (total: {len(combined)})")
    return len(new_rows)


print("📤 Pushing to Google Sheets...")
upsert_sheet(sh.worksheet("Batting"),        batting_df, ["MatchID","Player","BattingTeam"])
upsert_sheet(sh.worksheet("Bowling"),        bowling_df, ["MatchID","Player","BattingTeam"])
upsert_sheet(sh.worksheet("Extras"),         extras_df,  ["MatchID","BattingTeam"])
upsert_sheet(sh.worksheet("Fantasy_Points"),
    fantasy[["MatchID","Date","Venue","Winner","Player","Bat_Pts","Bowl_Pts","Field_Pts","Total_Pts"]],
    ["MatchID","Player"])

print("🎉 All sheets updated!")

from datetime import datetime

print(f"🔄 Daily refresh started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

print("⬇️  Downloading latest Cricsheet data...")
r = requests.get(ZIP_URL, timeout=60)
r.raise_for_status()
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    z.extractall(DATA_DIR)

all_bat, all_bowl, all_ext = [], [], []
for fpath in sorted(DATA_DIR.glob("*.json")):
    with open(fpath) as f:
        d = json.load(f)["info"]
    if d["dates"][0] >= START_DATE:
        bat, bowl, ext = parse_match(fpath)
        all_bat.append(bat); all_bowl.append(bowl); all_ext.append(ext)

batting_df = pd.concat(all_bat,  ignore_index=True) if all_bat  else pd.DataFrame()
bowling_df = pd.concat(all_bowl, ignore_index=True) if all_bowl else pd.DataFrame()
extras_df  = pd.concat(all_ext,  ignore_index=True) if all_ext  else pd.DataFrame()

bat_pts = batting_df[["MatchID","Date","Venue","Winner","BattingTeam","Player","Runs","Balls","Fours","Sixes","Dismissed"]].copy()
bat_pts["Bat_Pts"] = bat_pts.apply(batting_points, axis=1)

bowl_pts = bowling_df[["MatchID","Date","BattingTeam","Player","Balls","Runs","Wickets","DotBalls","Maidens","LBW_Bowled","Catches","Stumpings","RunOut_Direct","RunOut_Indirect"]].copy()
bowl_pts["Bowl_Pts"]  = bowl_pts.apply(bowling_points,  axis=1)
bowl_pts["Field_Pts"] = bowl_pts.apply(fielding_points, axis=1)

fantasy = pd.merge(
    bat_pts[["MatchID","Date","Venue","Winner","BattingTeam","Player","Bat_Pts"]],
    bowl_pts[["MatchID","BattingTeam","Player","Bowl_Pts","Field_Pts"]],
    on=["MatchID","BattingTeam","Player"], how="outer"
).fillna(0)
fantasy["Total_Pts"] = fantasy["Bat_Pts"] + fantasy["Bowl_Pts"] + fantasy["Field_Pts"]

print("📤 Pushing to Google Sheets...")
upsert_sheet(sh.worksheet("Batting"),        batting_df, ["MatchID","Player","BattingTeam"])
upsert_sheet(sh.worksheet("Bowling"),        bowling_df, ["MatchID","Player","BattingTeam"])
upsert_sheet(sh.worksheet("Extras"),         extras_df,  ["MatchID","BattingTeam"])
upsert_sheet(sh.worksheet("Fantasy_Points"),
    fantasy[["MatchID","Date","Venue","Winner","Player","Bat_Pts","Bowl_Pts","Field_Pts","Total_Pts"]],
    ["MatchID","Player"])

print(f"🎉 Done at {datetime.now().strftime('%H:%M:%S')}")

