import os
import re
import json
import shutil
import sqlite3
import argparse
import subprocess
import concurrent.futures
from pathlib import Path
import logging
from time import sleep

import requests
import base64
from musicbrainzngs import set_useragent, search_recordings
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from lyricsgenius import Genius
import yt_dlp
from shutil import which

# Import Mutagen classes for Ogg Opus and FLAC Pictures
from mutagen.oggopus import OggOpus
from mutagen.flac import Picture
from base64 import b64encode

# --- Constants ---
DOWNLOAD_DIR = Path("downloads/raw")
INFO_DIR = Path("downloads/info")
COVER_DIR = Path("downloads/covers")
SORTED_DIR = Path("Sorted")
DB_PATH = Path("downloads.db")
USER_AGENT = "YTMusicDownloader/2.0 (https://yourwebsite.example.com)"
LOG_FILE = "application.txt" # Define the log file name

# Set the user agent for MusicBrainz
set_useragent("YTMusicDownloader", "2.0", "https://yourwebsite.example.com")

# Use your Genius token below
GENIUS_TOKEN = "4LJb8knqz8-bSU5Xcn_x6pCrtDDZed2xnZqXtix6rUDNueyXiRrggf_QS40Iym93"
genius = Genius(GENIUS_TOKEN) if GENIUS_TOKEN else None

# --- Logging Setup ---
# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO) # Set the overall logging level

# Clear any existing handlers to prevent duplicate output if this script is run multiple times
# without a full process restart (e.g., in some interactive environments).
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    handler.close()

# Create a console handler and add it to the logger
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
root_logger.addHandler(console_handler)

# --- Helper Functions ---
def check_dependencies():
    logging.info("Checking for required dependencies (yt-dlp, aria2c).")
    if not which("yt-dlp"):
        logging.critical("yt-dlp not found. Please install it to proceed.")
        raise FileNotFoundError("yt-dlp not found. Please install it.")
    if not which("aria2c"):
        logging.warning("aria2c not found. Downloads might be slower. Consider installing it for better performance.")
    logging.info("Dependency check complete.")

def setup_dirs():
    logging.info("Setting up necessary download directories.")
    for d in [DOWNLOAD_DIR, INFO_DIR, COVER_DIR, SORTED_DIR]:
        d.mkdir(parents=True, exist_ok=True)
        logging.info(f"Ensured directory exists: {d}")
    logging.info("Directory setup complete.")

def setup_database():
    logging.info("Setting up the SQLite database.")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS downloads (
                                video_id TEXT PRIMARY KEY,
                                artist TEXT,
                                title TEXT,
                                album TEXT,
                                filepath TEXT
                            )''')
            conn.commit()
            logging.info("Database table 'downloads' ensured to exist.")
    except sqlite3.Error as e:
        logging.error(f"Database error during setup: {e}")
        raise
    logging.info("Database setup complete.")

def is_downloaded(video_id):
    logging.debug(f"Checking if video ID {video_id} is already downloaded.")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM downloads WHERE video_id = ?", (video_id,))
            result = c.fetchone() is not None
            logging.debug(f"Video ID {video_id} download status: {result}")
            return result
    except sqlite3.Error as e:
        logging.error(f"Database error while checking download status for {video_id}: {e}")
        return False

def mark_as_downloaded(video_id, artist, title, album, filepath):
    logging.info(f"Marking video ID {video_id} as downloaded: '{artist} - {title}' from album '{album}'.")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO downloads (video_id, artist, title, album, filepath) VALUES (?, ?, ?, ?, ?)",
                      (video_id, artist, title, album, str(filepath)))
            conn.commit()
            logging.info(f"Successfully marked {video_id} as downloaded.")
    except sqlite3.Error as e:
        logging.error(f"Database error while marking {video_id} as downloaded: {e}")

def clean_title(title):
    original_title = title
    title = re.sub(r"\s*\(.*?(official.*?|from.*?|audio|video).*?\)", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\[.*?\]", "", title)
    title = re.sub(r"[-–—]\s*official.*$", "", title, flags=re.IGNORECASE)
    logging.debug(f"Cleaned title from '{original_title}' to '{title.strip()}'.")
    return title.strip()

def extract_artist_and_title(title):
    cleaned_title = clean_title(title)
    match = re.match(r"^(?P<artist>.+?)\s*[-–—:]\s*(?P<title>.+)$", cleaned_title)
    if match:
        artist, extracted_title = match.group("artist"), match.group("title")
        logging.debug(f"Extracted artist '{artist}' and title '{extracted_title}' from '{title}'.")
        return artist, extracted_title
    logging.debug(f"Could not extract artist from title '{title}'. Returning None for artist.")
    return None, cleaned_title

def download_audio(entry, rate_limit):
    video_id = entry['id']
    title = entry['title']
    output_template = DOWNLOAD_DIR / f"{video_id}.%(ext)s"
    logging.info(f"Attempting to download audio for '{title}' ({video_id}).")
    cmd = [
        "yt-dlp",
        "--external-downloader", "aria2c",
        "--external-downloader-args", "aria2c:-x 16 -k 1M",
        "--extract-audio",
        "--audio-format", "opus",
        "--audio-quality", "0",
        "--embed-metadata",
        "--output", str(output_template),
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    if rate_limit:
        cmd += ["--throttled-rate", rate_limit]
        logging.debug(f"Applying rate limit: {rate_limit}")

    try:
        logging.debug(f"Executing download command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True)
        expected_file = next(DOWNLOAD_DIR.glob(f"{video_id}.*"), None)
        if not expected_file:
            logging.error(f"Download failed for video ID {video_id}. No file found after yt-dlp execution.")
            raise FileNotFoundError(f"Download failed for video ID {video_id}. Check yt-dlp output.")
        logging.info(f"Successfully downloaded audio for '{title}' to {expected_file}.")
        return expected_file
    except subprocess.CalledProcessError as e:
        logging.error(f"Error downloading {video_id} ('{title}'): {e.stderr.decode()}")
        return None
    except FileNotFoundError as e:
        logging.error(f"Error during audio download setup for {video_id} ('{title}'): {e}")
        return None

def fetch_metadata(video_id, title, artist):
    logging.info(f"Fetching metadata for '{title}' by '{artist}' from MusicBrainz and YouTube.")
    try:
        result = search_recordings(recording=title, artist=artist, limit=5)
        for recording in result.get("recording-list", []):
            for release in recording.get("release-list", []):
                if 'secondary-type-list' in release and 'Compilation' in release['secondary-type-list']:
                    continue
                if release.get('release-group', {}).get('primary-type') == 'Album':
                    album = release.get('title', title)
                    logging.info(f"Found MusicBrainz metadata: Album='{album}', Track='{recording.get('position', '1')}'.")
                    return {
                        'album': album,
                        'tracknumber': recording.get('position', '1'),
                        'release_id': release.get('id')
                    }
        logging.info(f"No suitable album metadata found on MusicBrainz for '{title}' by '{artist}'.")
    except Exception as e:
        logging.warning(f"Error fetching metadata from MusicBrainz for '{title} - {artist}': {e}. Trying YouTube as fallback.")
        sleep(1) # Be nice to the API

    try:
        with yt_dlp.YoutubeDL({"quiet": True, "extract_flat": True, "useragent": USER_AGENT}) as ydl:
            info_dict = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            album = info_dict.get("album", title)
            logging.info(f"Found YouTube metadata: Album='{album}'.")
            return {
                'album': album,
                'tracknumber': '1',
                'release_id': None
            }
    except Exception as e:
        logging.warning(f"Error fetching metadata from YouTube for '{title} - {artist}': {e}. Using title as album.")
        return {
            'album': title,
            'tracknumber': '1',
            'release_id': None
        }
    return { # Fallback if both fail
        'album': title,
        'tracknumber': '1',
        'release_id': None
    }

def fetch_youtube_thumbnail(video_id):
    output_template = str(COVER_DIR / f"{video_id}")
    logging.info(f"Fetching thumbnail for video ID {video_id}.")
    try:
        subprocess.run([
            "yt-dlp",
            f"https://www.youtube.com/watch?v={video_id}",
            "--skip-download",
            "--write-thumbnail",
            "--convert-thumbnails", "jpg",
            "-o", output_template
        ], check=True, capture_output=True)
        thumb_files = list(COVER_DIR.glob(f"{video_id}.*"))
        if thumb_files:
            logging.info(f"Successfully fetched thumbnail to {thumb_files[0]}.")
            return thumb_files[0]
        logging.warning(f"No thumbnail file found after download attempt for {video_id}.")
        return None
    except subprocess.CalledProcessError as e:
        logging.error(f"Error downloading thumbnail for {video_id}: {e.stderr.decode()}")
        return None
    except FileNotFoundError as e:
        logging.error(f"Error during thumbnail download setup for {video_id}: {e}")
        return None

def fetch_lyrics(title, artist):
    logging.info(f"Attempting to fetch lyrics for '{title}' by '{artist}'.")
    if genius:
        try:
            song = genius.search_song(title, artist)
            if song and song.lyrics:
                logging.info(f"Successfully fetched lyrics for '{title}' by '{artist}'.")
                return song.lyrics
            logging.warning(f"Lyrics not found on Genius for '{title}' by '{artist}'.")
        except Exception:
            logging.warning(f"Error fetching lyrics for '{title} - {artist}' from Genius. Retrying after a short delay.")
            sleep(1) # Be nice to the API
    else:
        logging.warning("Genius token not provided. Skipping lyrics fetching.")
    return f"Lyrics for {title} by {artist} not found."

def embed_metadata(file_path, meta):
    """
    Embed the cover art into the opus file using Mutagen.
    If embedding fails, the cover image will still be copied to the album folder.
    """
    logging.info(f"Attempting to embed metadata into {file_path.name}.")
    if not file_path or file_path.suffix != ".opus":
        logging.warning(f"Skipping metadata embedding for invalid file type or path: {file_path}")
        return

    cover_path = meta.get("cover_path")
    if not cover_path or not os.path.exists(cover_path):
        logging.warning(f"Cover file not found for embedding: {cover_path}")
        return

    try:
        covart = Picture()
        with open(cover_path, "rb") as img:
            covart.data = img.read()
        covart.type = 3  # Cover (front)
        
        audio = OggOpus(str(file_path))
        audio["metadata_block_picture"] = b64encode(covart.write()).decode("ascii")
        audio.save()
        logging.info(f"Successfully embedded cover art into {file_path.name}.")
        del audio # Explicitly delete the object to release file handle
    except Exception as e:
        logging.error(f"Error embedding metadata using Mutagen for {file_path.name}: {e}", exc_info=True)
        # If embedding fails, the file might be corrupted or in an unknown state.
        # It's safer to remove it and let the user re-download if needed.
        if file_path.exists():
            try:
                os.remove(file_path)
                logging.warning(f"Removed potentially corrupted file {file_path.name} after embedding error.")
            except OSError as remove_e:
                logging.error(f"Failed to remove corrupted file {file_path.name}: {remove_e}")

def generate_nfo(meta, out_dir):
    nfo_path = out_dir / f"{meta['title']}.nfo"
    logging.info(f"Generating NFO file for '{meta['title']}' at {nfo_path}.")
    try:
        with open(nfo_path, "w", encoding="utf-8") as f: # Specify encoding for broader compatibility
            f.write(f"Artist: {meta['artist']}\n")
            f.write(f"Title: {meta['title']}\n")
            f.write(f"Album: {meta['album']}\n")
            f.write(f"Track Number: {meta['tracknumber']}\n")
            f.write(f"Lyrics:\n{meta['lyrics']}\n")
        logging.info(f"Generated NFO successfully: {nfo_path}")
    except IOError as e:
        logging.error(f"Error writing NFO file {nfo_path}: {e}")

def organize(file_path, meta):
    logging.info(f"Organizing file {file_path.name}.")
    if not file_path:
        logging.warning("Skipping organization for invalid file path.")
        return None

    # Check if the source file exists before attempting any operations
    if not file_path.exists():
        logging.error(f"Source file '{file_path.name}' does not exist for organization. It might have been deleted or moved by a previous step.")
        return None

    album_lower = meta.get("album", "Singles").lower()
    if album_lower == "singles":
        out_dir = SORTED_DIR / "Singles" / meta["artist"]
        out_name = f"{meta['artist']} - {meta['title']}.opus"
    else:
        out_dir = SORTED_DIR / "Albums" / meta["artist"] / meta["album"]
        out_name = f"{meta['tracknumber'].zfill(2)} - {meta['title']}.opus"

    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / out_name
    
    logging.info(f"Attempting to move '{file_path.name}' to '{final_path}'.")
    try:
        # Use copy and remove as a more robust alternative to shutil.move
        # This handles cases where direct move might fail due to cross-device links or other issues
        shutil.copy2(str(file_path), str(final_path))
        os.remove(str(file_path))
        logging.info(f"Successfully copied and removed original file. Final path: {final_path}.")
        
        # Always copy the cover image into the folder as "cover.jpg"
        if meta.get("cover_path") and Path(meta["cover_path"]).exists():
            target_cover = out_dir / "cover.jpg"
            logging.info(f"Copying cover image from {meta['cover_path']} to {target_cover}.")
            shutil.copy2(meta["cover_path"], target_cover)
            logging.info(f"Copied cover image to {target_cover}")
        else:
            logging.warning("No cover image path provided or file does not exist for copying.")
        
        generate_nfo(meta, out_dir)
        logging.info(f"Organization complete for {final_path.name}.")
        return final_path
    except IOError as e:
        logging.error(f"Error during organization (copy/remove) for {file_path.name} to {final_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during organization for {file_path.name}: {e}", exc_info=True)
        return None

def process_track(entry):
    video_id = entry.get("id")
    raw_title = entry.get("title")
    logging.info(f"Starting processing for track: '{raw_title}' (ID: {video_id}).")

    if is_downloaded(video_id):
        logging.info(f"Skipping already downloaded track: {raw_title} ({video_id}).")
        return

    artist, title = extract_artist_and_title(raw_title)
    if not artist:
        artist = entry.get("uploader", "Unknown")
        logging.info(f"Could not extract artist from title '{raw_title}'. Using uploader: '{artist}'.")

    path = download_audio(entry, rate_limit=args.rate_limit)
    if not path:
        logging.error(f"Failed to download audio for: '{raw_title}' ({video_id}). Skipping further processing for this track.")
        return

    mb_meta = fetch_metadata(video_id, title, artist)
    album = mb_meta.get("album", "Singles") if mb_meta else "Singles"
    tracknumber = mb_meta.get("tracknumber", "1") if mb_meta else "1"
    
    cover_path = fetch_youtube_thumbnail(video_id)
    if not cover_path:
        logging.warning(f"Could not retrieve thumbnail for {video_id}. Metadata embedding might be incomplete.")

    lyrics = fetch_lyrics(title, artist)

    # Attempt to embed metadata (cover art) into the opus file using Mutagen.
    embed_metadata(path, {
        "title": title,
        "artist": artist,
        "album": album,
        "tracknumber": tracknumber,
        "cover_path": cover_path,
        "lyrics": lyrics
    })

    final_path = organize(path, {
        "title": title,
        "artist": artist,
        "album": album,
        "tracknumber": tracknumber,
        "cover_path": cover_path,
        "lyrics": lyrics
    })

    if final_path:
        mark_as_downloaded(video_id, artist, title, album, final_path)
        logging.info(f"Finished processing and organizing track: '{title}' by '{artist}'.")
    else:
        logging.error(f"Failed to organize track: '{title}' by '{artist}'.")

def main():
    # Before starting the application logic, clear the previous log file
    # and re-add the file handler to ensure a fresh log file.
    if os.path.exists(LOG_FILE):
        try:
            os.remove(LOG_FILE)
            # Log this to console, as the file handler might be closed or not yet re-initialized
            print(f"INFO: Cleared previous log file: {LOG_FILE}")
        except OSError as e:
            print(f"ERROR: Error clearing previous log file {LOG_FILE}: {e}")

    # Remove existing file handlers from the root logger to prevent appending to old file
    # This loop is crucial if the script is run multiple times within the same process
    # (e.g., in an IDE or interactive shell)
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            root_logger.removeHandler(handler)

    # Add a new FileHandler with 'w' mode to create a fresh log file for this run
    file_handler = logging.FileHandler(LOG_FILE, mode='w')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(file_handler)

    logging.info("Application started.")
    check_dependencies()
    setup_dirs()
    setup_database()
    
    logging.info(f"Fetching playlist information from: {args.playlist}")
    try:
        playlist_json = subprocess.run([
            "yt-dlp", "--flat-playlist", "--dump-single-json", "--user-agent", USER_AGENT, args.playlist
        ], capture_output=True, check=True, text=True).stdout
        playlist_data = json.loads(playlist_json)
        entries = playlist_data.get("entries", [])
        if args.max_downloads:
            entries = entries[:args.max_downloads]
            logging.info(f"Limiting downloads to the first {args.max_downloads} tracks.")
        logging.info(f"Found {len(entries)} tracks in the playlist.")
    except subprocess.CalledProcessError as e:
        logging.critical(f"Error fetching playlist information: {e.stderr.decode()}. Exiting.")
        return
    except json.JSONDecodeError as e:
        logging.critical(f"Error decoding playlist JSON: {e}. Exiting.")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TextColumn("[green]{task.completed}/{task.total}"),
        TimeElapsedColumn()
    ) as progress:
        task = progress.add_task("Downloading & Processing Tracks", total=len(entries))
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(process_track, e) for e in entries]
            for future in concurrent.futures.as_completed(futures):
                progress.update(task, advance=1)
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An unexpected error occurred in a worker thread: {e}", exc_info=True) # exc_info=True to log traceback

    logging.info("Application finished.")
    # Ensure all buffered log messages are written to the file before exiting
    logging.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and tag music from a YouTube playlist using yt-dlp + aria2c.")
    parser.add_argument(
        "--playlist",
        default="https://www.youtube.com/playlist?list=PLqnAFiDF6ndWz0M-UyzEP2nDVG-QaK2-E",
        help="YouTube playlist URL (default: sample playlist)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent workers for multithreaded downloads (default: 4)"
    )
    parser.add_argument(
        "--rate-limit",
        type=str,
        default="1M",
        help="Download rate limit to avoid YouTube throttling (e.g., 1M, 500K). Default: 1M"
    )
    parser.add_argument(
        "--max-downloads",
        type=int,
        default=400,
        help="Maximum number of downloads to process"
    )
    args = parser.parse_args()
    main()
