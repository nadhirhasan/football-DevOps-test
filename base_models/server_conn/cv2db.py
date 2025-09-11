import os
import sys
import json

from tqdm import tqdm

from DBConnector import DBConnector


def clean_event_classification_str(str1):
    return None if str1 is None else str1.replace("-", " ").replace("_", " ")

def write_events_to_db(db_conn, events, match_id):
    # format events and participants
    formatted_events = []
    formatted_participants = []
    for i, event in enumerate(events):
        participants = []
        # events: (event_id, match_id, type, subtype, result, start_time, end_time)
        try:
            formatted_events.append((
                match_id,
                clean_event_classification_str(event["event_type"]),
                clean_event_classification_str(event.get("event_subtype", None)),
                clean_event_classification_str(event.get("event_result", None)),
                round(event["start_frame"]),
                round(event["end_frame"]),
            ))
        except Exception as e:
            print(e)
            print(event)
            exit()

        # participants: (event_id, match_id, tracker_id, type)
        event_participants = event["participants"]
        for participant in event_participants:
            if participant in ["center_player", "center_team", "substitute_team"]:
                continue
            participants.append((event_participants[participant], participant))
        formatted_participants.append(participants[:])
    
    # write the events to DB
    print("Writing Events to DB...")
    final_participants = []
    for i in tqdm(range(len(formatted_events))):
        event_id = db_conn.execute("INSERT INTO events (match_id, type, subtype, result, start_time, end_time) VALUES (%s, %s, %s, %s, %s, %s)", formatted_events[i])
        final_participants.extend([(event_id, part[0], part[1]) for part in formatted_participants[i]])

    # write the event participants to DB
    db_conn.execute_many("INSERT INTO event_participants (event_id, tracker_id, type) VALUES (%s, %s, %s)", final_participants)


def determine_display_id(player):
    # find all matched previous players
    all_matches = {}
    for i in range(len(player["frames"])):
        matches = player["frames"][i]["top_matches"]
        for matched_id, confidence in matches:
            if matched_id not in all_matches:
                all_matches[matched_id] = []
            all_matches[matched_id].append(confidence)
    
    # aggregrate confidences
    best_match, best_match_len = player["id"], None
    for match in all_matches:
        match_len = len(all_matches[match])
        agg_conf = sum(all_matches[match])/match_len
        if agg_conf > 0.85 and (best_match_len is None or match_len > best_match_len):
            best_match = match
            best_match_len = match_len
    
    # check if confidence is high enough to overwrite
    return best_match


def write_player_overlays_to_db(db_conn, players, match_id):
    # format overlays and bboxes
    c1, c2 = 0, 0
    formatted_player_overlays = []
    formatted_bboxes = []
    for player in players:
        tracker_id = player["id"]
        display_id = determine_display_id(player)
        c1 += 1
        if display_id != tracker_id:
            c2 += 1
        binary_team = player["frames"][0]["team_id"]
        # player_overlays: (tracker_id, match_id, team_id, display_id, jersey_number, team)
        formatted_player_overlays.append((tracker_id, match_id, None, display_id, None, binary_team))
        for i in range(len(player["frames"])):
            frame = player["frames"][i]
            # player_overlay_bboxes: (tracker_id, match_id, x, y, w, h, pitch_x, pitch_y, timestamp)
            formatted_bboxes.append((
                tracker_id,
                match_id,
                frame["x1"],
                frame["y1"],
                frame["width"],
                frame["height"],
                frame["pitch_x"],
                frame["pitch_y"],
                frame["frame_second"]
            ))
    
    # write the player_overlays and bboxes to DB
    print("Writing Player Overlays...")
    print(f"Number of player overlays: {len(formatted_player_overlays)}. Number of player bboxes: {len(formatted_bboxes)}")
    db_conn.execute_many(
        "INSERT INTO player_overlays (tracker_id, match_id, team_id, display_id, jersey_number, team) VALUES (%s, %s, %s, %s, %s, %s)", 
        formatted_player_overlays
    )
    write_size = 5000
    for i in tqdm(range(0, len(formatted_bboxes)+write_size, write_size)):
        db_conn.execute_many(
            "INSERT INTO player_overlay_bboxes (tracker_id, match_id, x, y, w, h, pitch_x, pitch_y, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            formatted_bboxes[i:min(i+write_size, len(formatted_bboxes))]
        )


def write_ball_overlays_to_db(db_conn, ball_frames, match_id):
    # format ball overlay bboxes
    formatted_ball_bboxes = []
    for ball_frame in ball_frames:
        formatted_ball_bboxes.append((
            match_id,
            ball_frame["pitch_x"],
            ball_frame["pitch_y"],
            ball_frame["frame_second"]
        ))
    
    # write to db
    print("Writing Ball Overlays...")
    db_conn.execute_many("INSERT INTO ball_overlay_bboxes (match_id, pitch_x, pitch_y, timestamp) VALUES (%s, %s, %s, %s)", formatted_ball_bboxes)


def delete_match_data(db_conn, match_id):
    # get event ids
    event_ids = db_conn.execute_and_read("SELECT event_id FROM events WHERE match_id = %s", (match_id,))
    event_ids = [event["event_id"] for event in event_ids]

    # delete events and participants
    print("Deleting events...")
    if len(event_ids) > 0:
        placeholder = ", ".join(["%s"]*len(event_ids))
        event_ids = tuple(event_ids)
        db_conn.execute(f"DELETE FROM event_participants WHERE event_id IN ({placeholder})", event_ids)
        db_conn.execute(f"DELETE FROM events WHERE event_id IN ({placeholder})", event_ids)

    # delete player and ball bboxes
    print("Deleting player overlay bboxes...")
    db_conn.execute("DELETE FROM player_overlay_bboxes WHERE match_id = %s", (match_id,))
    print("Deleting ball overlay bboxes...")
    db_conn.execute("DELETE FROM ball_overlay_bboxes WHERE match_id = %s", (match_id,))
    print("Deleting player overlays...")
    db_conn.execute("DELETE FROM player_overlays WHERE match_id = %s", (match_id,))


def db2json(db_conn, match_id):
    # fetch data
    print("Fetching events...")
    events = db_conn.execute_and_read("SELECT * FROM events WHERE match_id = %s", (match_id,))
    if len(events) == 0:
        return
    event_ids = [event["event_id"] for event in events]
    placeholder = ", ".join(["%s"]*len(event_ids))
    event_participants = db_conn.execute_and_read(f"SELECT * FROM event_participants WHERE event_id IN ({placeholder})", tuple(event_ids))
    print("Fetching player overlays...")
    player_overlays = db_conn.execute_and_read("SELECT * FROM player_overlays WHERE match_id = %s", (match_id,))
    player_overlay_bboxes = db_conn.execute_and_read("SELECT * FROM player_overlay_bboxes WHERE match_id = %s", (match_id,))
    print("Fetching ball overlays")
    ball_overlay_bboxes = db_conn.execute_and_read("SELECT * FROM ball_overlay_bboxes WHERE match_id = %s", (match_id,))

    # save data
    with open(f"db_data_{match_id}.json", "w") as f:
        data = {
            "events": events,
            "event_participants": event_participants,
            "player_overlays": player_overlays,
            "player_overlay_bboxes": player_overlay_bboxes,
            "ball_overlay_bboxes": ball_overlay_bboxes
        }
        json.dump(data, f, indent=4)

def dbjson2db(db_conn, match_id):
    # load and prepare tuples of data
    print("Preparing data...")
    import os
    if not os.path.exists(f"db_data_{match_id}.json"):
        return
    with open(f"db_data_{match_id}.json") as f:
        data = json.load(f)
    events = data["events"]
    event_tuples = [(event["event_id"], event["match_id"], event["type"], event["subtype"], event["result"], event["start_time"], event["end_time"],) for event in events]
    event_participants = data["event_participants"]
    event_participant_tuples = [(participant["event_id"], participant["tracker_id"], participant["type"],) for participant in event_participants]
    player_overlays = data["player_overlays"]
    player_overlay_tuples = [(overlay["tracker_id"], overlay["match_id"], overlay["team_id"], overlay["display_id"], overlay["jersey_number"], overlay["team"],) for overlay in player_overlays]
    player_bboxes = data["player_overlay_bboxes"]
    player_bbox_tuples = [(bbox["tracker_id"], bbox["match_id"], bbox["x"], bbox["y"], bbox["w"], bbox["h"], bbox["pitch_x"], bbox["pitch_y"], bbox["timestamp"]) for bbox in player_bboxes]
    ball_bboxes = data["ball_overlay_bboxes"]
    ball_bbox_tuples = [(bbox["match_id"], bbox["pitch_x"], bbox["pitch_y"], bbox["timestamp"]) for bbox in ball_bboxes]

    # insert data into db
    print("Inserting events...")
    db_conn.execute_many("INSERT INTO events (event_id, match_id, type, subtype, result, start_time, end_time) VALUES (%s, %s, %s, %s, %s, %s, %s)", event_tuples)
    db_conn.execute_many("INSERT INTO event_participants (event_id, tracker_id, type) VALUES (%s, %s, %s)", event_participant_tuples)
    print("Inserting player overlays...")
    db_conn.execute_many("INSERT INTO player_overlays (tracker_id, match_id, team_id, display_id, jersey_number, team) VALUES (%s, %s, %s, %s, %s, %s)", player_overlay_tuples)
    write_size = 10000
    for i in tqdm(range(0, len(player_bbox_tuples)+write_size, write_size)):
        db_conn.execute_many(
            "INSERT INTO player_overlay_bboxes (tracker_id, match_id, x, y, w, h, pitch_x, pitch_y, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            player_bbox_tuples[i:min(i+write_size, len(player_bbox_tuples))]
        )
    print("Inserting ball overlays...")
    db_conn.execute_many("INSERT INTO ball_overlay_bboxes (match_id, pitch_x, pitch_y, timestamp) VALUES (%s, %s, %s, %s)", ball_bbox_tuples)
    

def check_matches(db_conn):
    matches = db_conn.execute_and_read("SELECT match_id, m3u8_link FROM matches WHERE processing_status = %s", ("s3_uploaded",))
    print(matches)

def update_match_status(db_conn, match_id, status):
    db_conn.execute("UPDATE matches SET processing_status = %s WHERE match_id = %s", (status, match_id,))

    
if __name__ == "__main__":
    # db connection
    host = os.environ["DB_HOST"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    database = "analytics_and_library"
    db_conn = DBConnector(host, user, password, database)

    if len(sys.argv) == 2:
        # arguments
        match_id = int(sys.argv[1])
        print("Deleting match data")
        delete_match_data(db_conn, match_id)
    elif len(sys.argv) == 4:
        # arguments
        match_id = int(sys.argv[1])
        event_detections_path = sys.argv[2]
        overlay_data_path = sys.argv[3]

        # write events
        with open(event_detections_path) as f:
            event_data = json.load(f)
        write_events_to_db(db_conn, event_data["events"], match_id)

        # write player overlays
        with open(overlay_data_path) as f:
            overlay_data = json.load(f)
        write_player_overlays_to_db(db_conn, overlay_data["players"], match_id)

        # write ball overlays
        write_ball_overlays_to_db(db_conn, overlay_data["ball_info"], match_id)
    else:
        for i in range(1, 10):
            print(i)
            #db2json(db_conn, i)
            dbjson2db(db_conn, i)
        
    
    