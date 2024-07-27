import asyncio
import time

import aiohttp
import chardet
from requests.exceptions import ConnectionError, Timeout, RequestException, SSLError
from google.oauth2 import service_account
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
import re

#pip install google-cloud
#pip install google-auth
#pip install google-auth-oauthlib
#pip install meta-ai-api
#pip install aiohttp

exclude_domains = [
    'sentry.wixpress.com', 'sentry.io', 'sentry-next.wixpress.com',
    'sentry-new.myshopline.com', 'mydomain.com', 'www.newsdiffs.org',
    'inbox.com', 'template.blog', 'template.blog.videos',
    'template.blog.sound', 'template.page.upsell.js', '2x.progressive.png.jpg',
    '3x.progressive.png.jpg', 'template.product', 'template.search.js',
    '4.0.31.min.css', '4.0.31.min.js', '1.2.0.min.css', '8.4.4.min.js',
    '8.4.4.min.css', '3x.webp', 'template.product.js', 'jebsen.com', '11.0.5.js',
    'support.com', 'mysite.com', 'example.com', 'company.com', '2x.png',
    'mail.com', 'email.com', '3x.jpg', '3x.png', '2x.jpg', '2x.png', '2.jpg',
    '2.png', 'email.tst', 'yourcompany.com', 'support.com', '11.0.5.js', '11.css',
    'layout.theme.js', '2x.heic', '2x.gif', 'domain.com', '2x.webp',
    'sentry.zipify.com', 'error-tracking.zipify.com', 'x10.com', 'layout.theme.css',
    '2x.progressive.jpg', 'emailprovider.com', '2x.jpeg', '3x.png', '2.3.0.js',
    '4.49.1.js', '2x.svg', 'template.page.faqs.chunk.66f84.js',
    'layout.theme.chunk.bd911.js', 'template.search.chunk.e95f5.js',
    'template.gift', 'template.product.chunk.f7037.js', 'template.collection',
    '2x.webp', '2x.progressive.jpg', 'yourdomain.com', '3x.gif', 'xyz.com',
    'domainname.com', 'somemail.com', 'bb-template.cart', 'bb-template.product.min.js',
    'bb-polyfills.min.js', 'bb-app-layer.min.js', 'bb-template.customer', 'info.com',
    'xxx.xxx', '2x.static.png', 'newsletter.com', '3x.progressive.jpg', '3x.png',
    '3.13.0.min.js', '2x.svg', 'layout.password', 'xyz.com', 'mysite.com'
]

# Define the function to filter rows in Google Sheets
def filter_rows(spreadsheet_id, sheet_name, filter_criteria, retries=3, delay=5, timeout=60):
    # Define the scope
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Authenticate and create the service
    creds = service_account.Credentials.from_service_account_file("key1.json", scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Attempt the request with retries
    for attempt in range(retries):
        try:
            # Call the Sheets API with a specified timeout
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute(num_retries=3)
            values = result.get('values', [])

            if not values:
                raise ValueError("No data found in the spreadsheet.")

            # Assuming the first row contains headers
            headers = values[0]
            rows = [dict(zip(headers, row)) for row in values[1:]]  # Convert rows to dictionaries

            # Filter rows based on the criteria
            filtered_rows = [row for row in rows if row.get(filter_criteria['a']) == filter_criteria['b']]

            return filtered_rows
        except HttpError as error:
            if error.resp.status in [502, 503, 504]:  # Retry for server errors
                time.sleep(delay)
            else:
                continue
        except TimeoutError as e:
            time.sleep(delay)
        except Exception as e:
            continue

# Define the async function to get a file using HTTP
async def get_file(session, url, row):
    text = None
    html = None
    link = url

    try:
        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.read()  # Read content as bytes
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return text, html, link, row

    # Detect encoding
    encoding = chardet.detect(content)['encoding']
    if encoding is None:
        encoding = 'utf-8'  # Fallback to 'utf-8' if encoding detection fails

    # Decode content
    content = content.decode(encoding)

    # Process response content (e.g., parse with BeautifulSoup)
    soup = BeautifulSoup(content, 'html.parser')
    html = content
    tex = soup.get_text()
    text = tex[:50000]

    return text, html, link, row

# Define the async function to post data using HTTP
async def post_data(session, url, data):
    async with session.post(url, json=data) as response:
        return await response.json()

# Define the function to update rows in Google Sheets
def update_row(spreadsheet_id, sheet_name, filter_criteria, update_data, retries=3, delay=5):
    # Define the scope
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Authenticate and create the service
    creds = service_account.Credentials.from_service_account_file("key1.json", scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Attempt the request with retries
    for attempt in range(retries):
        try:
            # Call the Sheets API to get the data
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
            values = result.get('values', [])

            if not values:
                raise ValueError("No data found in the spreadsheet.")

            # Assuming the first row contains headers
            headers = values[0]
            rows = [dict(zip(headers, row)) for row in values[1:]]

            # Find the row to update
            row_index = None
            for i, row in enumerate(rows):
                if row.get(filter_criteria['a']) == filter_criteria['b']:
                    row_index = i + 1  # Adjust for 0-based index and header row
                    break

            if row_index is not None:
                # Ensure the row has the same number of columns as the headers
                while len(values[row_index]) < len(headers):
                    values[row_index].append("")

                # Update the row
                for key, value in update_data.items():
                    if key in headers:
                        col_index = headers.index(key)
                        values[row_index][col_index] = value

                # Update the sheet with the new values
                update_range = f"{sheet_name}!A1"
                body = {
                    "values": values
                }
                sheet.values().update(spreadsheetId=spreadsheet_id, range=update_range, valueInputOption="RAW", body=body).execute()
                return
            else:
                continue
        except HttpError as error:
            if error.resp.status in [502, 503, 504]:  # Retry for server errors
                time.sleep(delay)
                print('Done 3')
            else:
                print('Done 31')
                continue
        except Exception as e:
            print('Done 4')
            continue

def find_first_email(text_content, exclude_domains):
    text = text_content
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    
    for email in emails:
        if not any(domain in email for domain in exclude_domains):
            return email
    
    return None  # Return None if no valid email is found

def extract_usa_phone_numbers(text):
    # Regular expression pattern for USA phone numbers
    pattern = r'\b(?:\(?(\d{3})\)?[\s.-]?)?(\d{3})[\s.-]?(\d{4})\b'
    
    # Find the first match in the text
    match = re.search(pattern, text)
    
    if match:
        # Format the match into standard phone number format
        area_code = match.group(1)
        central_office_code = match.group(2)
        line_number = match.group(3)
        
        if area_code:
            phone_number = '({}) {}-{}'.format(area_code, central_office_code, line_number)
        else:
            phone_number = '{}-{}'.format(central_office_code, line_number)
        
        return phone_number
    
    return None


async def main():
    # Get filtered rows from Google Sheets
    rows = filter_rows(
        '19zBfIQTeVdd01YoB3tEfpkBOqKkDpY3kamZHrlOVN6c',
        'woocommerce_2',
        {'a': 'Status', 'b': 'FALSE'}
    )

    # Create an async session
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks
        tasks = []
        for row in rows:
            # Replace 'url_column' with the actual column name in your Google Sheets
            if row['Status'] == "FALSE":
                url = row['Link']
                tasks.append(get_file(session, url, row))

        # Run the tasks concurrently
        results = await asyncio.gather(*tasks)

        # Process the results
        for i, (text, html, link, row) in enumerate(results):
            email = None
            phone_number = None
            email_status = None
            phone_number_status = None

            if text == None and html == None:
                update_data = {
                    'Status': 'TRUE',
                }
                print('Done 1')
                update_row('19zBfIQTeVdd01YoB3tEfpkBOqKkDpY3kamZHrlOVN6c', 'woocommerce_2', {'a': 'Link', 'b': link}, update_data)
                continue

            if row['Email Status'] == "FALSE":
                #Extract emails
                email = find_first_email(html, exclude_domains)

                #Check if the email exists
                if email == None:
                    email_status = 'FALSE'
                else:
                    email_status = 'TRUE'     

            if row['Phone Number Status'] == "FALSE":
                phone_number = extract_usa_phone_numbers(html)

                #Check if the phone_number exists
                if phone_number == None:
                    phone_number_status = 'FALSE'
                else:
                    phone_number_status = 'TRUE'
            
            # Update the row in Google Sheets with the extracted data
            update_data = {
                'Web Data': text,
                'Email': email,
                'Phone Number': phone_number,
                'Status': 'TRUE',
                'Email Status': email_status,
                'Phone Number Status': phone_number_status
            }

            # Update the row
            print('Done 2')
            update_row('19zBfIQTeVdd01YoB3tEfpkBOqKkDpY3kamZHrlOVN6c', 'woocommerce_2', {'a': 'Link', 'b': link}, update_data)

if __name__ == '__main__':
    asyncio.run(main())
