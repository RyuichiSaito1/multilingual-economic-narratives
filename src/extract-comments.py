import json
import re
import html
from datetime import datetime
from collections import defaultdict

# Convert UNIX timestamp to datetime
def unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp)

# Preprocess the body text for neural network training
def preprocess_text(text):
    # Decode HTML entities
    text = html.unescape(text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Filter records based on conditions and convert JSON to TSV
def filter_records(file_path, search_keywords, tsv_file_path, monthly_counts_file_path):
    
    # ☆ Mod
    # Updated date range to include end of day on December 31, 2022
    start_date = datetime(2016, 1, 1, 0, 0, 0)
    end_date = datetime(2022, 12, 31, 23, 59, 59)
    
    tsv_lines = []
    headers = ["created_date", "subreddit_id", "id", "author", "parent_id", "body", "score"] 
    processed_bodies = set()  # To track processed body text
    
    # Dictionary to count records by month (key: 'YYYY-MM', value: count)
    month_counts = defaultdict(int)
    
    # Open the file and process each line as a separate JSON object
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                record = json.loads(line.strip())
                
                # Ensure 'created_utc' is treated as an integer
                created_utc = int(record.get("created_utc", 0))
                created_date = unix_to_datetime(created_utc)
                
                # Extract additional fields
                subreddit_id = record.get('subreddit_id', '')
                comment_id = record.get('id', '')
                author = record.get('author', '')
                parent_id = record.get('parent_id', '')
                score = record.get('score', 0)  # Extracting the score field
                
                # Preprocess the body text
                body_text = preprocess_text(record.get('body', ''))
                
                # Skip if the body text has already been processed (i.e., is a duplicate)
                if body_text in processed_bodies:
                    continue
                # Add the body text to the set to track it
                processed_bodies.add(body_text)
                
                # Check if any of the search_keywords are in the processed 'body' field
                if any(keyword in body_text for keyword in search_keywords):
                    if start_date <= created_date <= end_date:
                        # Prepare TSV line with all required fields, including 'score'
                        tsv_line = "\t".join([
                            created_date.isoformat(),
                            subreddit_id,
                            comment_id,
                            author,
                            parent_id,
                            body_text,
                            str(score) 
                        ])
                        tsv_lines.append(tsv_line)
                        
                        # Extract the year and month to track the count
                        year_month = created_date.strftime('%Y-%m')  # 'YYYY-MM' format
                        month_counts[year_month] += 1
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
            except ValueError as e:
                print(f"Value error: {e}")

    # Write the TSV lines to the output file
    with open(tsv_file_path, 'w', encoding='utf-8') as tsv_file:
        # Write header line
        tsv_file.write("\t".join(headers) + "\n")
        # Write data lines
        for line in tsv_lines:
            tsv_file.write(line + "\n")
    
    # Write monthly counts to a separate tab-delimited file for Excel
    with open(monthly_counts_file_path, 'w', encoding='utf-8') as count_file:
        # Write header
        count_file.write("Month\tCount\n")
        # Write data lines
        for month, count in sorted(month_counts.items()):
            count_file.write(f"{month}\t{count}\n")
    
    # Output the monthly counts to the standard output as well
    print("\nMonthly record counts:")
    for month, count in sorted(month_counts.items()):
        print(f"{month}  {count}")

    return len(tsv_lines)  # Return the number of filtered records

# ☆ Mod
file_path = '/Users/ryuichi/Downloads/greece_comments.json'
# search_keywords = ['price', 'cost', 'inflation', 'deflation', 'expensive', 'cheap', 'purchase', 'sale', 'increasing', 'decreasing', 'rising', 'falling', 'affordable', 'unaffordable']

# Greek
# 'price', 'cost', 'fee', 'charge', 'rate', 'fare', 'outlay', 'cheap', 'affordable', 'inexpensive', 'reasonable', 'economical', 'expensive', 'exorbitant', 'unreasonable', 'uneconomical', 'extravagant'
# search_keywords = ['τιμή', 'κόστος', 'τέλος', 'χρέωση', 'τιμή', 'κόστος', 'κόστος', 'έξοδα', 'φθηνός', 'προσιτός', 'φθηνός', 'λογικός', 'οικονομικός', 'ακριβός', 'υπερβολικός', 'παράλογος', 'αντιοικονομικός', 'υπερβολικός']
search_keywords = ['τιμή', 'κόστος', 'τέλος', 'χρέωση', 'ποσοστό', 'κόμιστρο', 'επιπλέον χρέωση', 'έξοδα', 'λογαριασμός']
# 'price', 'cost', 'fee', 'charge', 'rate', 'fare', 'outlay', 'surcharge', 'expenses', 'bill',  'cheap', 'affordable', 'inexpensive', 'reasonable', 'economical', 'expensive', 'exorbitant', 'unreasonable', 'uneconomical', 'extravagant'
search_keywords = ['τιμή', 'κόστος', 'τέλος', 'χρέωση', 'τιμή', 'κόστος', 'δαπάνες', 'επιπλέον χρέωση', 'έξοδα', 'λογαριασμός', 'φθηνός', 'προσιτός', 'φθηνός', 'λογικός', 'οικονομικός', 'ακριβός', 'υπέρογκος', 'παράλογος', 'αντιοικονομικός', 'υπερβολικός']
# 'price', 'cost', 'inflation', 'deflation', 'expensive', 'cheap', 'buy', 'bought', 'sell', 'sold'
# search_keywords = ['τιμή', 'κόστος', 'πληθωρισμός', 'αποπληθωρισμός', 'ακριβός', 'φθηνός', 'αγορά', 'αγόρασα', 'πώλησα', 'πουλήσα']
# 'expensive', 'rising', 'increase', 'hike', 'cost', 'prices', 'wage', 'shortage', 'scarcity', 'cheap', 'falling', 'discount', 'sale', 'deal', 'recession', 'unemployment', 'layoffs', 'slowing'
search_keywords = ['ακριβό', 'αύξηση', 'αύξηση', 'κόστος', 'τιμές', 'μισθός', 'έλλειψη', 'σπανιότητα', 'φθηνό', 'μείωση', 'έκπτωση', 'πώληση', 'συμφωνία', 'ύφεση', 'ανεργία', 'απολύσεις', 'επιβράδυνση']
# 'price', 'cost', 'inflation', 'deflation', 'expensive', 'cheap', 'buy', 'bought', 'purchase', 'sell', 'sold', 'sale'
search_keywords = ['τιμή', 'κόστος', 'πληθωρισμός', 'αποπληθωρισμός', 'ακριβός', 'φθηνός', 'αγορά', 'αγόρασα', 'αγορά', 'πώληση', 'πουλήθηκε', 'πώληση']

# Hungarian
# search_keywords = ['ár', 'költség', 'infláció', 'defláció', 'drága', 'olcsó', 'vásárlás', 'akció', 'növekvő', 'csökkenő', 'emelkedő', 'csökkenő', 'megfizethető', 'megfizethetetlen', 'túlárazott', 'alulárazott', 'értékes', 'értéktelen', 'alku', 'kedvezmény', 'felár', 'megtakarítás', 'kiadás', 'költségvetés', 'bérek', 'fizetés', 'nyereség', 'veszteség']

# ☆ Mod
tsv_file_path = '/Users/ryuichi/My Drive/03 University/04_Ph.D._Tsukuba/Research/03 Multilingual Study/data/tsv/greece_comments_2.tsv'  # Output TSV file path
# ☆ Mod
monthly_counts_file_path = '/Users/ryuichi/My Drive/03 University/04_Ph.D._Tsukuba/Research/03 Multilingual Study/data/monthly_count/greece_comments_count_2.txt'  # Monthly counts file path

filtered_count = filter_records(file_path, search_keywords, tsv_file_path, monthly_counts_file_path)

# Print the number of filtered records
print(f"Number of filtered records: {filtered_count}")