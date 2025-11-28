import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from datetime import datetime, timedelta

# Define the start date of the 2025-2026 NBA regular season (October 21, 2025)
# This date is used to calculate the range of days to scrape.
SEASON_START_DATE_STR = "20251021"

def scrape_past_season_data(end_date_str=None, start_date_str=SEASON_START_DATE_STR):
    """
    Scrapes NBA scoreboard data from ESPN pages, iterating backward from the
    end date (defaults to yesterday) to the start of the NBA season.

    Args:
        end_date_str (str, optional): The date to start scraping from (YYYYMMDD).
                                      Defaults to yesterday's date.
        start_date_str (str): The earliest date to scrape (YYYYMMDD).
                              Defaults to the start of the 2025-2026 season.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted scoreboard data.
    """
    base_url = "https://www.espn.com.au/nba/scoreboard/_/date/{page}"
    raw_game_data = []

    # Get today's date and calculate yesterday's date (safe default end point)
    today = datetime.now()
    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
    else:
        # Default to yesterday for a safe, completed day's data
        end_date = today - timedelta(days=1)

    start_date = datetime.strptime(start_date_str, "%Y%m%d")

    # Calculate the number of days to iterate (including the start date)
    delta = end_date - start_date
    num_days = delta.days + 1

    # Ensure we don't try to scrape before the season started
    if num_days <= 0:
        print(f"Start date {start_date_str} is after or the same as the end date {end_date.strftime('%Y%m%d')}. No data to scrape.")
        return pd.DataFrame()

    print(f"Calculated range: {num_days} days to scrape, starting from {end_date.strftime('%Y%m%d')} down to {start_date_str}.")

    # Set up User-Agent header to mimic a web browser and avoid 403 Forbidden errors.
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # The loop iterates backward from the end_date to the start_date
    for i in range(num_days):
        # Calculate the date by subtracting the index 'i' from the end date
        current_date = end_date - timedelta(days=i)
        # Format the date into the required YYYYMMDD string for the URL
        page_date_str = current_date.strftime("%Y%m%d")
        url = base_url.format(page=page_date_str)

        # 1. Fetch the content
        print(f"Attempting to fetch data for URL: {url}")

        # Exponential backoff mechanism (added a simple version for robustness)
        max_retries = 3
        for attempt in range(max_retries):
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 200:
                break

            print(f"Error fetching page {page_date_str}. Status Code: {response.status_code}. Retrying in {2**attempt} seconds...")
            if response.status_code == 403:
                print("Error 403: Forbidden. The server likely blocked the request. Halting immediate retries.")
                break # Don't keep hammering if explicitly blocked
            time.sleep(2**attempt)

        # If all retries failed and status is not 200, skip this date
        if response.status_code != 200:
            print(f"Failed to fetch data for {page_date_str} after {max_retries} attempts. Skipping.")
            continue

        # 2. Parse the content
        soup = BeautifulSoup(response.text, "html.parser")

        # 3. Extract the date from the header (e.g., "Wednesday, October 22, 2025")
        date_header = soup.find('h3', class_='Card__Header__Title--no-theme')
        # Use the scraped date if available, otherwise use the date string we generated
        game_date = date_header.get_text(strip=True) if date_header else page_date_str

        # 4. Find all individual game sections
        game_sections = soup.find_all("section", class_=lambda x: x and "Scoreboard" in x.split() and x.split()[0] != "Card")

        if not game_sections:
            print(f"No game sections found on page {page_date_str}. Skipping.")
            continue

        print(f"Found {len(game_sections)} games on page {game_date}. Extracting data.")

        # 5. Process each game
        for game in game_sections:

            # NOTE: Status is removed as requested by the user.

            # 5b. Extract quarter/period headers (e.g., '1', '2', '3', '4', 'OT')
            header_div = game.find("div", class_="ScoreboardScoreCell__Headings")
            current_quarter_headers = []
            if header_div:
                current_quarter_headers = [
                    h.get_text(strip=True)
                    for h in header_div.find_all("div", class_="ScoreboardScoreCell__Heading")
                    if h.get_text(strip=True) != 'T'
                ]

            # 5c. Extract competitor (team) details
            team_items = game.find_all("li", class_=lambda x: x and "ScoreboardScoreCell__Item" in x)

            # Teams are always listed in order: [0] Away, [1] Home
            team_locations = ["Away", "Home"]

            for i, team_item in enumerate(team_items):
                team_name_tag = team_item.find("div", class_="ScoreCell__TeamName--shortDisplayName")
                team_name = team_name_tag.get_text(strip=True) if team_name_tag else "N/A"

                # Get all period scores (Q1, Q2, Q3, Q4, OT1, OT2, ...)
                linescores_div = team_item.find("div", class_="ScoreboardScoreCell_Linescores")
                period_scores = []
                if linescores_div:
                    period_scores = [
                        score.get_text(strip=True)
                        for score in linescores_div.find_all("div", class_="ScoreboardScoreCell__Value")
                    ]

                # Get total score
                total_score_tag = team_item.find("div", class_="ScoreCell__Score")
                total_score = total_score_tag.get_text(strip=True) if total_score_tag else "N/A"

                raw_game_data.append({
                    "Date": game_date,
                    "Team": team_name,
                    "Location": team_locations[i],
                    # "Status": game_status, # <-- REMOVED Status
                    "Total": total_score,
                    "LineScores": period_scores,
                    "QuarterHeaders": current_quarter_headers
                })

        # Politeness delay
        delay = random.uniform(1, 5)
        print(f"Completed scraping page {game_date}. Waiting {delay:.2f} seconds.")
        time.sleep(delay)

    # --- Final DataFrame Construction (outside the loop) ---
    all_quarter_headers = set()
    for row_dict in raw_game_data:
        all_quarter_headers.update(row_dict["QuarterHeaders"])

    def sort_key(header):
        if header.startswith('OT'):
            try:
                num = int(header[2:]) if len(header) > 2 else 1
            except ValueError:
                num = 1
            return 4 + num
        else:
            try:
                return int(header)
            except ValueError:
                return 0

    sorted_quarter_headers = sorted([h for h in all_quarter_headers if h], key=sort_key)

    # Updated columns: "Status" is removed
    final_columns = ["Date", "Team", "Location"] + sorted_quarter_headers + ["Total"]
    final_data = []

    for row_dict in raw_game_data:
        current_quarter_map = dict(zip(row_dict["QuarterHeaders"], row_dict["LineScores"]))
        # Updated row creation: "Status" is removed
        final_row = [row_dict["Date"], row_dict["Team"], row_dict["Location"]]

        for header in sorted_quarter_headers:
            final_row.append(current_quarter_map.get(header, ''))

        final_row.append(row_dict["Total"])
        final_data.append(final_row)

    # Build and return the DataFrame
    df = pd.DataFrame(final_data, columns=final_columns)
    return df

# --- Example Usage and File Output ---

print("Starting NBA Scoreboard data scrape...")
df = scrape_past_season_data()

# 1. Output the DataFrame to the console
print("\n--- Resulting DataFrame (showing first 10 rows) ---")
print(df.head(10))
print(f"\nDataFrame shape: {df.shape}")

# 2. Save the DataFrame to a CSV file
csv_filename = "nba_scoreboard_data.csv"
try:
    # index=False prevents writing the pandas row index to the CSV file
    df.to_csv(csv_filename, index=False)
    print(f"\nSuccess! Data saved to {csv_filename}. The Status column has been removed.")
except Exception as e:
    print(f"\nError saving data to CSV: {e}")
