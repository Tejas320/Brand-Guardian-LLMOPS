import os
import time
import logging
import requests
import yt_dlp
from azure.identity import DefaultAzureCredential

logger = logging.getLogger("video-indexer")
logging.basicConfig(level=logging.INFO)


class VideoIndexerService:

    def __init__(self):
        self.account_id = os.getenv("AZURE_VI_ACCOUNT_ID")
        self.location = os.getenv("AZURE_VI_LOCATION")
        self.subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        self.resource_group = os.getenv("AZURE_RESOURCE_GROUP")
        self.vi_name = os.getenv("AZURE_VI_NAME")

        if not all([
            self.account_id,
            self.location,
            self.subscription_id,
            self.resource_group,
            self.vi_name
        ]):
            raise Exception("Missing Azure Video Indexer environment variables")

        self.credential = DefaultAzureCredential()

        # cache tokens
        self.arm_token = None
        self.vi_token = None


    # ------------------------
    # Azure ARM Access Token
    # ------------------------
    def get_access_token(self):

        if self.arm_token:
            return self.arm_token

        try:
            token_obj = self.credential.get_token(
                "https://management.azure.com/.default"
            )

            self.arm_token = token_obj.token

            logger.info("Azure ARM token acquired")

            return self.arm_token

        except Exception as e:
            logger.error(f"Failed to get ARM token: {e}")
            raise


    # ------------------------
    # Video Indexer Account Token
    # ------------------------
    def get_account_token(self):

        if self.vi_token:
            return self.vi_token

        arm_token = self.get_access_token()

        url = (
            f"https://management.azure.com/subscriptions/{self.subscription_id}"
            f"/resourceGroups/{self.resource_group}"
            f"/providers/Microsoft.VideoIndexer/accounts/{self.vi_name}"
            f"/generateAccessToken?api-version=2024-01-01"
        )

        headers = {
            "Authorization": f"Bearer {arm_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "permissionType": "Contributor",
            "scope": "Account"
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            raise Exception(
                f"Failed to get VI account token: {response.text}"
            )

        self.vi_token = response.json()["accessToken"]

        logger.info("Video Indexer account token acquired")

        return self.vi_token


    # ------------------------
    # Download YouTube Video
    # ------------------------
    def download_youtube_video(
        self,
        youtube_url,
        output_path="temp_video.mp4"
    ):

        logger.info(f"Downloading YouTube video: {youtube_url}")

        ydl_opts = {
            "format": "best",
            "outtmpl": output_path,
            "quiet": False,
            "no_warnings": False,
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "web"]
                }
            },
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                )
            }
        }

        try:

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([youtube_url])

            logger.info("Download complete")

            return output_path

        except Exception as e:

            raise Exception(
                f"YouTube download failed: {str(e)}"
            )


    # ------------------------
    # Upload Video
    # ------------------------
    def upload_video(self, video_path, video_name):

        vi_token = self.get_account_token()

        url = (
            f"https://api.videoindexer.ai/"
            f"{self.location}/Accounts/{self.account_id}/Videos"
        )

        params = {
            "accessToken": vi_token,
            "name": video_name,
            "privacy": "Private",
            "indexingPreset": "Default"
        }

        logger.info(f"Uploading video: {video_path}")

        with open(video_path, "rb") as f:

            files = {
                "file": f
            }

            response = requests.post(
                url,
                params=params,
                files=files
            )

        if response.status_code != 200:

            raise Exception(
                f"Upload failed: {response.text}"
            )

        data = response.json()

        video_id = data.get("id")

        if not video_id:
            raise Exception("Upload succeeded but no video ID returned")

        logger.info(f"Upload successful. Video ID: {video_id}")

        return video_id


    # ------------------------
    # Wait for Processing
    # ------------------------
    def wait_for_processing(self, video_id):

        vi_token = self.get_account_token()

        logger.info(f"Waiting for processing: {video_id}")

        url = (
            f"https://api.videoindexer.ai/"
            f"{self.location}/Accounts/{self.account_id}"
            f"/Videos/{video_id}/Index"
        )

        params = {
            "accessToken": vi_token
        }

        while True:

            response = requests.get(
                url,
                params=params
            )

            if response.status_code != 200:

                raise Exception(
                    f"Failed to get status: {response.text}"
                )

            data = response.json()

            state = data.get("state")

            logger.info(f"Current state: {state}")

            if state == "Processed":

                logger.info("Processing completed")

                return data

            if state == "Failed":
                raise Exception("Video indexing failed")

            if state == "Quarantined":
                raise Exception("Video quarantined")

            time.sleep(30)


    # ------------------------
    # Extract Transcript + OCR
    # ------------------------
    def extract_data(self, vi_json):

        transcript = []

        ocr = []

        videos = vi_json.get("videos", [])

        for video in videos:

            insights = video.get("insights", {})

            for t in insights.get("transcript", []):
                transcript.append(t.get("text", ""))

            for o in insights.get("ocr", []):
                ocr.append(o.get("text", ""))

        result = {

            "transcript": " ".join(transcript),

            "ocr_text": ocr,

            "metadata": {

                "duration":
                    vi_json.get(
                        "summarizedInsights", {}
                    ).get("duration"),

                "video_id":
                    vi_json.get("id"),

                "platform": "youtube"
            }
        }

        return result


    # ------------------------
    # Full Pipeline
    # ------------------------
    def process_youtube_video(self, youtube_url):

        video_path = self.download_youtube_video(youtube_url)

        video_name = os.path.basename(video_path)

        video_id = self.upload_video(
            video_path,
            video_name
        )

        vi_json = self.wait_for_processing(video_id)

        result = self.extract_data(vi_json)

        logger.info("Extraction completed")

        return result


# ------------------------
# Example Usage
# ------------------------
if __name__ == "__main__":

    vi = VideoIndexerService()

    youtube_url = "https://youtu.be/VIDEO_ID"

    result = vi.process_youtube_video(youtube_url)

    print("\nTranscript:\n")
    print(result["transcript"][:500])

    print("\nOCR Text:\n")
    print(result["ocr_text"])

    print("\nMetadata:\n")
    print(result["metadata"])
