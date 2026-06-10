import requests
from config import logger, headers, url


def connect_to_api():
    stocks = ['TSLA', 'MSFT', 'GOOGL']
    
    json_response = []
    
    for stock in range(0, len(stocks)):

        querystring = {"function":"TIME_SERIES_INTRADAY",
                    "symbol": f"{stocks[stock]}",
                    "outputsize":"compact",
                    "datatype":"json",
                    "interval":"5min"}
        try:
            response = requests.get(url, headers=headers, params=querystring) # Make the API request with the specified URL, headers, and query parameters
            
            response.raise_for_status()  # Check if the request was successful
            
            data = response.json()  # Parse the JSON response
            logger.info(f"Stocks {stocks[stock]} loaded successfully")     # Log a success message indicating that the data was fetched successfully
            
            json_response.append(data)  # Append the data to the list
            
        except requests.exceptions.RequestException as e:       # Catch any exceptions that occur during the API request and log an error message with the stock symbol and the exception details
            logger.error(f"An error occurred while fetching data for: {e}")    # Log an error message indicating that an error occurred while fetching data for the specific stock, along with the exception details
            break # Stop the loop if an error occurs to prevent further API calls
        
        
    return json_response  # Return the list of JSON responses from the API





def extract_json(response):                     # Define a function to extract relevant data from the JSON response
    records =[]                                 # Initialize an empty list to store the extracted records
    
    for data in response:
        symbol = data['Meta Data']['2. Symbol']  # Extract the stock symbol from the metadata
        
        for data_str, metrics in data['Time Series (5min)'].items():  # Iterate through the time series data
            record = {                          # Create a record dictionary to store the extracted data for each time point
                'symbol': symbol,               # Include the stock symbol in the record
                'datetime': data_str,           # Include the datetime string in the record
                'open': metrics["1. open"],         # Extract the open price from the metrics and include it in the record
                'high': metrics["2. high"],         # Extract the high price from the metrics and include it in the record
                'low': metrics["3. low"],           # Extract the low price from the metrics and include it in the record
                'close': metrics["4. close"]        # Extract the close price from the metrics and include it in the record
                ##'volume': metrics["5. volume"]            # Extract the volume from the metrics and include it in the record (commented out for now)
            }
            records.append(record)  # Append the extracted record to the list of records
    return records  # Return the list of extracted records