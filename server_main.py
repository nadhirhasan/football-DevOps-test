import subprocess
import argparse
from multiprocessing import Process
from pathlib import Path
import time
import os
from base_models.server_conn.DBConnector import DBConnector
import boto3
import logging

logging.basicConfig(level=logging.INFO,filename='app.log', filemode='a')

def run_script(cmd):
    logging.info(f"Running script: {cmd}")
    subprocess.run(cmd, shell=True)

def update_match_status(db_conn, match_id, status):
    db_conn.execute("UPDATE matches SET processing_status = %s WHERE match_id = %s", (status, match_id,))

def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--source_video_path', type=str, required=True, help='Path to the video file')
    parser.add_argument("--match_id", type=int, required=True, help="Match ID")
    parser.add_argument("--s3_link", type=str, required=True, help="S3 link for the video")
    parser.add_argument("--folds", default="all", type=str)
    parser.add_argument('--device', type=str, default='cuda', help='Device to use (e.g., cuda or cuda:0)')
    parser.add_argument("--overwrite", default=False, type=bool)
    args = parser.parse_args()

    host = "analytics-and-library.cxqeuoaemxbe.ca-central-1.rds.amazonaws.com"
    user = "admin"
    password = "qsCLwrB742VRjyN58ubpKU"
    database = "analytics_and_library"

    logging.info("starting db connection")
    db_conn = DBConnector(host, user, password, database)

    # access_key = os.environ["AWS_ACCESS_KEY"]
    # secret_key = os.environ["AWS_SECRET_KEY"]
    # boto3_session = boto3.session.Session(
    #     aws_access_key_id=access_key,
    #     aws_secret_access_key=secret_key,
    # )
    # s3 = boto3_session.client("s3")

    #################   Arguments   #################
    device = args.device
    overwrite = args.overwrite
    folds = args.folds
    match_id = args.match_id
    m3u8_link = args.s3_link  
    start_time = time.time()

    logging.info("Arguments parsed")

    while True:        
        match_folder_name = f"match_{match_id}"
        output_folder = f"outputs/{match_folder_name}"
        source_video_path = f"games/{match_folder_name}/output.mp4"
        # download_cmd = f"ffmpeg -i {m3u8_link} -ss 00:15:00  -t 00:10:00 -c copy {source_video_path}"

        ################# Download Video #################
        logging.info("Match Downloading...")
        print(m3u8_link)
        time.sleep(5)
        # download_cmd = f"ffmpeg -i {m3u8_link} -vf scale=1280:720,fps=30 -c:v libx264 -crf 23 -preset veryfast -c:a copy {source_video_path}"
        # if not os.path.exists(source_video_path):
        #     os.mkdir(f"games/{match_folder_name}")
        #     run_script(download_cmd)
        #     print("video downloded!")
        # else:
        #     print("video Already Downloaded!")

        
        source_video_path_ = Path(source_video_path)
        game_name = source_video_path_.parent.name  # 'game_shot'
        logging.info(f"Overwrite: {overwrite}")
        # Commands
        ball_cmd = ball_cmd = f"python base_models/ball_detection.py --source_video_path {source_video_path} --device {device} --ball_model_path objectDetection_Weights/first_ckpt/best.pt {'--overwrite' if overwrite else ''} --ball_detection_mode yolo"
        pitch_cmd = f"python base_models/pitch_detection_v2.py --source_video_path {source_video_path} --pitch_model_kp_path keypointDetection_Weights/custom_keypoint --pitch_model_line_path keypointDetection_Weights/custom_line {'--overwrite' if overwrite else ''} --display true"
        # player_cmd = f"python base_models/player_detection.py --source_video_path {source_video_path} --device {device} --player_model_path objectDetection_Weights/second_ckpt/best_v2_1280.pt {'--overwrite' if overwrite else ''} --display true"
        player_cmd = f"python base_models/player_detection.py --source_video_path {source_video_path} --device {device} --player_model_path objectDetection_Weights/1024_yolov11_6000f_batch4/weights/best.pt {'--overwrite' if overwrite else ''} --display true"

        ball_action_cmd = f'python base_models/ball_action_spotting/ball_action_spot.py --game_name {game_name} --folds "{folds}" {"--overwrite" if overwrite else ""}'
        event_ensemble_cmd = f'python base_models/ball_action_spotting/event_ensemble.py --games {game_name} --folds "{folds}"'

        team_cmd = f"python base_models/team_classifier.py --source_video_path {source_video_path} --device {device} {'--overwrite' if overwrite else ''}"
        reid_cmd = f"python base_models/player_reid.py --source_video_path {source_video_path} {'--overwrite' if overwrite else ''}"
        eventD_cmd = f"python base_models/event_detection.py --source_video_path {source_video_path} {'--overwrite' if overwrite else ''}"
        log2db_cmd = f"python base_models/server_conn/cv2db.py {match_id} {output_folder}/event_detections_pad.json {output_folder}/player_detections_yolo.json"



        ########################   Video Processing ########################################
        logging.info("Video Processing....")
        time.sleep(1200)
        logging.info("Video Processing Completed.")

        # # Step 1: Run ball, pitch, player, ball_action in parallel
        # p1 = Process(target=run_script, args=(ball_cmd,))
        # p2 = Process(target=run_script, args=(pitch_cmd,))
        # p3 = Process(target=run_script, args=(player_cmd,))
        # p4 = Process(target=run_script, args=(ball_action_cmd,))

        # p1.start()
        # p2.start()
        # p3.start()
        # p4.start()

        # # Wait for player detection to complete
        # p3.join()

        # # Step 2: Run team classifier
        # subprocess.run(team_cmd, shell=True)

        

        # # Wait for ball_action to finish before running event ensemble
        # p4.join()
        # subprocess.run(event_ensemble_cmd, shell=True)

        # # Wait for event ensemble AND player detection to finish before running event detection
        # p2.join()
        # subprocess.run(reid_cmd, shell=True)
        # subprocess.run(eventD_cmd, shell=True)
        # subprocess.run(log2db_cmd, shell=True)

        # db_conn = DBConnector(host, user, password, database)

        
        update_match_status(db_conn,match_id,"cv_processed")
        logging.info("Match Status Updated...")



        #####################   Json Output Upload to s3 ########################################
        logging.info("Json File Uploading to s3....")
        time.sleep(5)
        # s3.upload_file(f"{output_folder}/player_detections_yolo.json", "man-match-recordings", f"{match_id}_player_detections.json")
        # s3.upload_file(f"{output_folder}/event_detections_pad.json", "man-match-recordings", f"{match_id}_event_detections.json")

        logging.info(f"Time to process match {match_id} is {time.time() - start_time}!")
        
        break
    end_time = time.time()
    logging.info(f"Time Taken to process match {match_id}: {end_time - start_time} Seconds.")

if __name__ == '__main__':
    main()
    
    



