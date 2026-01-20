import time
import csv
import pandas as pd
import requests
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import os
from datetime import datetime

#Get data from The Numbers
def Scraper():
    print("Starting The Numbers scraper...")

    # Generate URLs programmatically up to 2501
    urls = ["https://www.the-numbers.com/box-office-records/worldwide/all-movies/genres/horror"]
    for i in range(101, 2502, 100):
        urls.append(f"https://www.the-numbers.com/box-office-records/worldwide/all-movies/genres/horror/{i}")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    #Build the .csv file now so I can add the data as I go, that way if my IP Address gets blocked, I can keep whatever I've done up to this point
    csv_file = r"C:\Users\zacha\School\Random\CreepyCrawler\horror_box_office.csv"

    # Write header once
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Rank", "Released", "Movie", "Worldwide Box Office", "Domestic Box Office", "International Box Office"])

    for url in urls:
        print(f"Scraping: {url}")

        driver.get(url)
        time.sleep(5)

        table = driver.find_element(By.XPATH, "//table")
        rows = table.find_elements(By.XPATH, ".//tbody/tr")

        batch_rows = []  # store rows for this URL
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == 6:
                rank = cols[0].text.strip()
                released = cols[1].text.strip()
                movie = cols[2].text.strip()
                worldwide = cols[3].text.strip()
                domestic = cols[4].text.strip()
                international = cols[5].text.strip()

                batch_rows.append([rank, released, movie, worldwide, domestic, international])

        # Append rows to CSV after each URL
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(batch_rows)

        print(f"Finished scraping {url}, saved {len(batch_rows)} rows.")

    driver.quit()
    print("Scraping complete.")

    # Load full dataframe
    df_box = pd.read_csv(csv_file)
    return df_box

#Get movie data from TMDb
def Movies():
    print("Starting TMDb movie fetch...")

    API_KEY = "964bd2f36bac0026469c29be9dd122f3"
    ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI5NjRiZDJmMzZiYWMwMDI2NDY5YzI5YmU5ZGQxMjJmMyIsIm5iZiI6MTc2MDQ1NzgzMS41ODgsInN1YiI6IjY4ZWU3NDY3ODMyODdjZjdjODBkODE3YSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.G3KtTYURQTgSB8DETUuL0UWclkApnlq3aXMyjmcLAUA"

    url = "https://api.themoviedb.org/3/discover/movie"

    # Auth headers (preferred over API key in URL)
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "accept": "application/json"
    }

    all_results = []
    max_pages = 100

    for page in range(1, max_pages + 1):
        params = {
            "with_genres": "27",  # Genre ID for Horror
            "language": "en-US",
            "sort_by": "popularity.desc",
            "page": page
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)
            
            print(f"Fetched page {page} with {len(results)} movies.")

            if page >= data.get("total_pages", 0):
                break
        else:
            print("Error:", response.status_code, response.text)
            break
        
        print(f"Fetched TMDb page {page}.")

        time.sleep(0.25)


    with open(r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_horror_movies.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4)

    movies_df = pd.DataFrame(all_results)

    print("Total movies collected:", len(movies_df))

    return movies_df

#Get credits data from TMDb
def Credits(ids):
    print("Starting TMDb credits fetch...")

    API_KEY = "964bd2f36bac0026469c29be9dd122f3"
    ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI5NjRiZDJmMzZiYWMwMDI2NDY5YzI5YmU5ZGQxMjJmMyIsIm5iZiI6MTc2MDQ1NzgzMS41ODgsInN1YiI6IjY4ZWU3NDY3ODMyODdjZjdjODBkODE3YSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.G3KtTYURQTgSB8DETUuL0UWclkApnlq3aXMyjmcLAUA"
    
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "accept": "application/json"
    }

    all_results = []

    for i, id in enumerate(ids, start=1):
        url = f"https://api.themoviedb.org/3/movie/{id}/credits"

        params = {
            "language": "en-US",
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            cast = data.get("cast", [])
            crew = data.get("crew", [])
            all_results.append({"id": id, "cast": cast, "crew": crew})
            
        else:
            print("Error:", response.status_code, response.text)

        if i % 25 == 0:
            print(f"Fetched credits for {i}/{len(ids)} movies...")

        time.sleep(0.25)

    with open(r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_credits.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4)

    df_credits = pd.DataFrame(all_results)

    return df_credits

#Get keywords data from TMDb
def Keywords(ids):
    print("Starting TMDb keywords fetch...")

    API_KEY = "964bd2f36bac0026469c29be9dd122f3"
    ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI5NjRiZDJmMzZiYWMwMDI2NDY5YzI5YmU5ZGQxMjJmMyIsIm5iZiI6MTc2MDQ1NzgzMS41ODgsInN1YiI6IjY4ZWU3NDY3ODMyODdjZjdjODBkODE3YSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.G3KtTYURQTgSB8DETUuL0UWclkApnlq3aXMyjmcLAUA"
    
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "accept": "application/json"
    }

    all_results = []

    for i, id in enumerate(ids, start=1):
        url = f"https://api.themoviedb.org/3/movie/{id}/keywords"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            keywords = data.get("keywords", [])
            all_results.append({"id": id, "keywords": keywords})
            
        else:
            print("Error:", response.status_code, response.text)

        if i % 25 == 0:
            print(f"Fetched keywords for {i}/{len(ids)} movies...")

        time.sleep(0.25)

    with open(r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_keywords.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4)

    df_keywords = pd.DataFrame(all_results)

    return df_keywords

#Merge all data into one dataset
def Merge():
    print("Starting data merge process...")

    #Check if files exist and load files as dfs, if not run the functions to create them
    box_file = r"C:\Users\zacha\School\Random\CreepyCrawler\horror_box_office.csv"
    movies_file = r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_horror_movies.json"
    credits_file = r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_credits.json"
    keywords_file = r"C:\Users\zacha\School\Random\CreepyCrawler\tmdb_keywords.json"
    if os.path.exists(box_file):
        df_box = pd.read_csv(box_file)
        print(f"Loaded {len(df_box)} rows from horror_box_office.csv") 
    else:
        df_box = Scraper()
    if os.path.exists(movies_file):
        df_movies = pd.read_json(movies_file)
        print(f"Loaded {len(df_movies)} TMDb movies") 
    else:
        df_movies = Movies()

    ids = df_movies['id'].tolist()
    if os.path.exists(credits_file):
        df_credits = pd.read_json(credits_file) 
        print(f"Loaded {len(df_credits)} credit records")
    else:
        df_credits = Credits(ids)
    if os.path.exists(keywords_file):
        df_keywords = pd.read_json(keywords_file) 
        print(f"Loaded {len(df_keywords)} keyword records")
    else:
        df_keywords = Keywords(ids)

    #Merge all TMDb files together
    merged_df = pd.merge(df_movies, df_credits, on="id", how="inner")
    merged_df = pd.merge(merged_df, df_keywords, on="id", how="inner")

    #Merge API and Scraper dataframes
    df_box.rename(columns={"Movie": "original_title"}, inplace=True)

    # Ensure year is consistent in both
    merged_df["release_year"] = pd.to_datetime(merged_df["release_date"], errors="coerce").dt.year
    df_box["Released"] = pd.to_numeric(df_box["Released"], errors="coerce")

    merged_df = pd.merge(merged_df, df_box, left_on=["original_title", "release_year"], right_on=["original_title", "Released"], how="left")

    #Sort merged dataframe
    merged_df["Rank"] = pd.to_numeric(merged_df["Rank"], errors="coerce")
    merged_df.sort_values(by="Rank", ascending=True, inplace=True)

    # Save overall dataset to .json file
    json_file = r"C:\Users\zacha\School\Random\CreepyCrawler\horror_dataset.json"
    merged_df.to_json(json_file, orient="records", indent=4, force_ascii=False)
    print(f"Saved merged dataset to {json_file} ({len(merged_df)} rows total)")
    
    return merged_df

print("Checking if final dataset exists...")
if os.path.exists(r"C:\Users\zacha\School\Random\CreepyCrawler\horror_dataset.json"):
    print("Found existing dataset, loading now...")
    df = pd.read_json(r"C:\Users\zacha\School\Random\CreepyCrawler\horror_dataset.json")
else:
    print("Dataset not found, starting full pipeline...")
    df = Merge()
print("File ready to go!")
print()

#subset_df = merged.head(100)

#One movie from every decade (1890 optional)
decades = [1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020]

#3 languages
languages = []

#Starring each actor
starring = ['Paul Bartel', 'Barbara Crampton', 'Wings Hauser', 'Olivia Hussey', 'Peter Jason', 'Justin Long', 'P.J. Soles', 'Kristen Stewart', 'Tony Todd', 'Mary Wuornov']

#Directed by each director
directed = ['Lucio Fulci', 'Lloyd Kaufman', 'David Lynch', 'Jose Mojica Marins', 'Edward D. Wood Jr.']

#Watch 2 halloween specials/episodes
episodes = []

#3 films cited in "Science Fiction / Double Feature"
cited_count = 3
cited = ['The Day the Earth Stood Still', 'The Invisible Man', 'King Kong', 'It Came From Outer Space', 'Doctor X', 'Forbidden Planet', 'Tarantula!', 
         'The Day of the Triffids', 'Night of the Demon', 'When Worlds Collide']

#3 horror films featuring Rocky thespians
thespians_count = 3
thespians = []

rocky_movies = df[df['original_title'] == "The Rocky Horror Picture Show"]
for cast in rocky_movies['cast'].iloc[0]:
    if cast.get('name') not in thespians:
        thespians.append(cast.get('name'))

#All in one night
#I couldn't find any way to determine what movies are over one night, so I just manually entered some movies I found from lists online
one_night = ['Night of the Living Dead', 'The Mist', 'Jeepers Creepers', '1408', 'Halloween', 'The Evil Dead', 'The Strangers', '[REC]', '30 Days of Night', "You're Next", 'The Autopsy of Jane Doe', 'From Dusk Till Dawn', 'The Texas Chainsaw Massacre', 'Halloween Kills', 'Hatchet 3', 'Hush', 'Evil Dead', 'Cabin In The Woods', 'The Strangers']

#Cannibalism
cannibalism = ['cannibal', 'cannibalism', 'cannibals', 'flesh-eating', 'human meat', 'survivalist eating']

#Extraterrestrials
extraterrestrials = ['alien life-form', 'alien', 'space', 'alien contact', 'alien monster', 'alien encounter', 'space capsule', 'alien hostility', 'spaceship', 'UFO', 'extraterrestrial', 'invasion', 'abduction', 'ET']

#Mad Science Laboratory
mad_science = ['scientist', 'mad scientist', 'experiment', 'laboratory', 'mad doctor', 'genetic experiment', 'secret lab', 'frankenstein', 'chemicals', 'mutation', 'unethical science']

#The Old Dark House
old_dark_house = ['haunted house', 'mansion', 'creaky mansion', 'stormy night', 'old estate', 'gloomy manor', 'inheritance', 'isolated home']

#Transylvania
transylvania = ['transylvania', 'vampire', 'castle', 'dracula', 'gothic castle', 'eastern europe', 'bloodsucker']

#Spare (Body) Parts
body_parts = ['body horror', 'mutation', 'mutant', 'transformation', 'bodily dismemberment', 'gore', 'prosthetic', 'organ harvesting', 'reanimation']

#Took a Wrong Turn
wrong_turn = ['lost', 'ghost town', 'deserted road', 'wrong path', 'stranded', 'isolation', 'mistake', 'wilderness']

#American Gothic Horror
american_gothic = ['gothic horror', 'urban gothic', 'small town', 'decay', 'countryside', 'southern gothic', 'dark secrets', 'family curse']

#Black Horror (Black Director, predominately Black Cast)
black_horror = ['race-conscious', 'african american man', 'racial issues', 'black cast', 'african american culture', 'social commentary', 'black horror director']

#Brazil
brazil = ['amazon rainforest', 'amazon river', 'brazil', 'rainforest horror', 'jungle', 'remote village', 'latin america']

#Final Girl
final_girl = ['final girl', 'sole survivor', 'last woman standing', 'female protagonist', 'survivor', 'heroine', 'teen horror']

#Hixploitation
hixploitation = ['hillbilly', 'backwoods', 'rural horror', 'inbred', 'redneck', 'country terror', 'swamp', 'isolated family']

#Horror Musical (Cannot be RHPS)
musical = ['musical', 'singing', 'dance numbers', 'horror musical', 'song', 'theatrical', 'stage horror']

#Church
church = ['church', 'clergy', 'religion', 'priest', 'demonic ritual', 'sacrilegious', 'cathedral', 'exorcism']

#Eco Horror
eco = ['environmental horror', 'nature revenge', 'pollution', 'toxic', 'wildlife attack', 'forest horror', 'ecological disaster']

#Nonlinear Horror
nonlinear = ['nonlinear narrative', 'time loop', 'non-chronological', 'flashback', 'fragmented timeline', 'disjointed story']

#Phantom
phantom = ['ghost', 'apparition', 'spirit', 'haunting', 'phantom', 'specter', 'poltergeist', 'supernatural presence']

#Ritual Sacrifice
ritual = ['ritual', 'sacrifice', 'cult', 'ceremony', 'occult', 'dark ritual', 'blood ritual', 'ancient rites']

#A Sinister Conspiracy
conspiracy = ['conspiracy', 'cover-up', 'secret organization', 'government plot', 'hidden agenda', 'shadowy figures', 'intrigue']

#Splatter Flick
splatter = ['splatter', 'gore', 'blood', 'over-the-top violence', 'slasher', 'graphic horror', 'body horror', 'gruesome']

#Title Contains a Color
color = ['red', 'black', 'white', 'green', 'blue', 'yellow', 'purple', 'pink', 'orange', 'violet', 'crimson', 'scarlet']

#Stephen King Adaptation
king_count = 1

#Year: 1965
year_1965_count = 1

overall_criteria_count = 1
def iterate_movies():
    max_criteria_count = 0
    best_movie = None

    for row in df.itertuples(index=False):
        criteria_count = 0

        if row.release_year // 10 * 10 in decades:
            criteria_count += 1

        if row.original_language not in languages and len(languages) < 3:
            criteria_count += 1

        counted_thespian = False

        for actor in row.cast:
            if actor.get('name') in starring:
                criteria_count += 1
            if actor.get('name') in thespians and thespians_count > 0 and row.original_title != "The Rocky Horror Picture Show" and not counted_thespian:
                criteria_count += 1
                counted_thespian = True

        for crew in row.crew:
            if crew.get('job') == 'Director' and crew.get('name') in directed:
                criteria_count += 1
            if crew.get('name') == 'Stephen King' and king_count > 0:
                criteria_count += 1
        
        if row.original_title in cited and cited_count > 0 and row.original_title != "The Rocky Horror Picture Show":
            criteria_count += 1

        # Initialize flags before the loop
        counted_one_night = False
        counted_cannibalism = False
        counted_extraterrestrials = False
        counted_mad_science = False
        counted_old_dark_house = False
        counted_transylvania = False
        counted_body_parts = False
        counted_wrong_turn = False
        counted_american_gothic = False
        counted_black_horror = False
        counted_brazil = False
        counted_final_girl = False
        counted_hixploitation = False
        counted_musical = False
        counted_church = False
        counted_eco = False
        counted_nonlinear = False
        counted_phantom = False
        counted_ritual = False
        counted_conspiracy = False
        counted_splatter = False
        
        for keyword in row.keywords:
            kw_name = keyword.get('name')
            
            if row.original_title != "The Rocky Horror Picture Show":
                if kw_name in one_night and not counted_one_night:
                    criteria_count += 1
                    counted_one_night = True
                if kw_name in cannibalism and not counted_cannibalism:
                    criteria_count += 1
                    counted_cannibalism = True
                if kw_name in extraterrestrials and not counted_extraterrestrials:
                    criteria_count += 1
                    counted_extraterrestrials = True
                if kw_name in mad_science and not counted_mad_science:
                    criteria_count += 1
                    counted_mad_science = True
                if kw_name in old_dark_house and not counted_old_dark_house:
                    criteria_count += 1
                    counted_old_dark_house = True
                if kw_name in transylvania and not counted_transylvania and row.original_title != "Interview with the Vampire":
                    criteria_count += 1
                    counted_transylvania = True
                if kw_name in body_parts and not counted_body_parts:
                    criteria_count += 1
                    counted_body_parts = True
                if kw_name in wrong_turn and not counted_wrong_turn:
                    criteria_count += 1
                    counted_wrong_turn = True
            if kw_name in american_gothic and not counted_american_gothic and row.original_title != "El orfanato":
                criteria_count += 1
                counted_american_gothic = True
            if kw_name in black_horror and not counted_black_horror:
                criteria_count += 1
                counted_black_horror = True
            if kw_name in brazil and not counted_brazil:
                criteria_count += 1
                counted_brazil = True
            if kw_name in final_girl and not counted_final_girl:
                criteria_count += 1
                counted_final_girl = True
            if kw_name in hixploitation and not counted_hixploitation:
                criteria_count += 1
                counted_hixploitation = True
            if kw_name in musical and not counted_musical:
                criteria_count += 1
                counted_musical = True
            if kw_name in church and not counted_church:
                criteria_count += 1
                counted_church = True
            if kw_name in eco and not counted_eco:
                criteria_count += 1
                counted_eco = True
            if kw_name in nonlinear and not counted_nonlinear:
                criteria_count += 1
                counted_nonlinear = True
            if kw_name in phantom and not counted_phantom:
                criteria_count += 1
                counted_phantom = True
            if kw_name in ritual and not counted_ritual:
                criteria_count += 1
                counted_ritual = True
            if kw_name in conspiracy and not counted_conspiracy:
                criteria_count += 1
                counted_conspiracy = True
            if kw_name in splatter and not counted_splatter:
                criteria_count += 1
                counted_splatter = True

        for col in color:
            if col in row.original_title.lower():
                criteria_count += 1
                break  # Only count once even if multiple colors match

        if row.release_year == 1965 and year_1965_count > 0:
            criteria_count += 1

        if criteria_count > max_criteria_count:
            max_criteria_count = criteria_count
            best_movie = row.id

    return best_movie

run_count = 0
while run_count < 50:
    best_movie_id = iterate_movies()

    if best_movie_id is None:
        print("No more movies meet any criteria.")
        break

    best_movie = df[df['id'] == best_movie_id]
    print("Best movie:", best_movie['original_title'].iloc[0])

    best_movie_decade = best_movie['release_year'].iloc[0] // 10 * 10 
    if best_movie_decade in decades:
        print("Decade:", best_movie['release_year'].iloc[0])
        decades.remove(best_movie_decade)

    if best_movie['original_language'].iloc[0] not in languages and len(languages) < 3:
        print("Language:", best_movie['original_language'].iloc[0])
        languages.append(best_movie['original_language'].iloc[0])

    for actor in best_movie['cast'].iloc[0]:
        if actor.get('name') in starring:
            print("Starring:", actor.get('name'))
            starring.remove(actor.get('name'))
        if actor.get('name') in thespians and thespians_count > 0:
            print("Rocky Thespian:", actor.get('name'))
            thespians.remove(actor.get('name'))
            thespians_count -= 1

    for crew in best_movie['crew'].iloc[0]:
        if crew.get('job') == 'Director' and crew.get('name') in directed:
            print("Directed by:", crew.get('name'))
            directed.remove(crew.get('name'))
        if crew.get('name') == 'Stephen King' and king_count > 0:
            print("Stephen King Adaptation")
            king_count -= 1

    if best_movie['original_title'].iloc[0] in cited and cited_count > 0:
        print("Cited in 'Science Fiction / Double Feature'")
        cited.remove(best_movie['original_title'].iloc[0])
        cited_count -= 1

    for keyword in best_movie['keywords'].iloc[0]:
        kw_name = keyword.get('name')
        
        if kw_name in one_night:
            print("One Night Only")
            one_night.clear()
        if kw_name in cannibalism:
            print("Cannibalism")
            cannibalism.clear()
        if kw_name in extraterrestrials:
            print("Extraterrestrials")
            extraterrestrials.clear()
        if kw_name in mad_science:
            print("Mad Science Laboratory")
            mad_science.clear()
        if kw_name in old_dark_house:
            print("Old Dark House")
            old_dark_house.clear()
        if kw_name in transylvania:
            print("Transylvania / Vampire")
            transylvania.clear()
        if kw_name in body_parts:
            print("Spare (Body) Parts / Mutation")
            body_parts.clear()
        if kw_name in wrong_turn:
            print("Took a Wrong Turn / Lost")
            wrong_turn.clear()
        if kw_name in american_gothic:
            print("American Gothic Horror")
            american_gothic.clear()
        if kw_name in black_horror:
            print("Black Horror")
            black_horror.clear()
        if kw_name in brazil:
            print("Brazil / Amazon")
            brazil.clear()
        if kw_name in final_girl:
            print("Final Girl")
            final_girl.clear()
        if kw_name in hixploitation:
            print("Hixploitation")
            hixploitation.clear()
        if kw_name in musical:
            print("Horror Musical")
            musical.clear()
        if kw_name in church:
            print("Church / Religious Horror")
            church.clear()
        if kw_name in eco:
            print("Eco Horror")
            eco.clear()
        if kw_name in nonlinear:
            print("Nonlinear Horror")
            nonlinear.clear()
        if kw_name in phantom:
            print("Phantom / Ghost")
            phantom.clear()
        if kw_name in ritual:
            print("Ritual / Sacrifice")
            ritual.clear()
        if kw_name in conspiracy:
            print("Sinister Conspiracy")
            conspiracy.clear()
        if kw_name in splatter:
            print("Splatter / Gore")
            splatter.clear()

    for col in color:
        if col.lower() in best_movie['original_title'].iloc[0].lower():
            print("Title Contains a Color")
            color.clear()
            break

    if best_movie['release_year'].iloc[0] == 1965:
        print("Year: 1965")
        year_1965_count -= 1

    df = df[df['id'] != best_movie_id]

    run_count += 1

    print("---" * 10)