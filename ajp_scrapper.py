import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AJPScraper:
    def __init__(self):
        self.head = {
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'en-US,en;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Cache-Control': 'max-age=0', 
            'Connection': 'keep-alive',
        }
        self.cat_list = []
        self.match_list = []
        self.event_list = []
        self.event_number = []
        self.div_number_list = []

    def scrape(self, start_event, end_event):
        for i in range(start_event, end_event + 1):
            try:
                url = f'https://ajptour.com/en/event/{i}/schedule/matchlist'
                response = requests.get(url, headers=self.head, allow_redirects=False).text
                soup = bs(response, 'html.parser')

                x = soup.find('ul', {'class': 'pagination'})
                if x:
                    logging.info(f'Number of pages found: {len(x)}')

                try:
                    page_size = len(x)
                    logging.info(f'URL: {url}')

                    for ps in range(1, page_size):
                        url2 = f'{url}?page={ps}'
                        logging.info(f'Processing page: {ps}, URL: {url2}')
                        response = requests.get(url2, headers=self.head, allow_redirects=False).text
                        soup = bs(response, 'html.parser')

                        category = soup.find_all('div', attrs={'class': 'category-row'})
                        matches = soup.find_all('div', class_="match-row well well-inverted well-extra-condensed end")
                        div_number = soup.find_all('div', attrs={'class': 'number'})
                        event = soup.find('h1')

                        logging.info(f'Event name: {event.text}')

                        for cat in range(len(div_number)):
                            self.cat_list.append(category[cat].text.strip(''))
                            self.match_list.append(matches[cat].text.strip(''))
                            self.div_number_list.append(div_number[cat].text.strip(''))
                            self.event_list.append(event.text)
                            self.event_number.append(i)

                except Exception as e:
                    logging.warning(f"Event number {i} does not exist: {e}")
                    continue

            except requests.exceptions.RequestException as e:
                logging.error(f"Error accessing {url}: {e}")
                continue


    def save_data(self):
        # Save the parquet file in the current directory
        current_directory = os.getcwd()
        save_path = os.path.join(current_directory, 'ajpdata.parquet')        
        df = pd.DataFrame({
            'category': self.cat_list,
            'matches': self.match_list,
            'event': self.event_list,
            'event_number': self.event_number,
            'div_number': self.div_number_list
        })
        df.to_parquet(save_path)
        logging.info(f'Rows loaded: {df.shape[0]}')

if __name__ == "__main__":
    # Input the starting and ending event numbers
    start_event = int(input("Enter the starting event number: "))
    end_event = int(input("Enter the ending event number: "))

    scraper = AJPScraper()
    scraper.scrape(start_event, end_event)
    scraper.save_data()
