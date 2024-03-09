import json
import pandas as pd
from json import JSONDecoder
from functools import partial
import re
import os


def process_trade_history(section):
    trade_history = json.loads(section.split(':', 1)[1])
    print("Trade History:")
    for trade in trade_history:
        print(trade)  # Or perform any other processing


def process_sandbox_logs(sandbox_logs_section):
    logs = [line.strip() for line in sandbox_logs_section.split('\n\n') if line.strip()]

    # Process each JSON object
    print("\nSandbox Logs:")
    for log in logs:
        log_data = json.loads(log)
        sandbox_log = log_data.get('sandboxLog')
        lambda_log = log_data.get('lambdaLog')
        timestamp = log_data.get('timestamp')
        print(f"Timestamp: {timestamp}, Sandbox Log: {sandbox_log}, Lambda Log: {lambda_log}")


def main():
    # Split the content into sections
    with open('/Users/lbw/Downloads/imctest.log', 'r') as f:
        file_content = f.read()
    sections = file_content.split('\n\n')
    process_sandbox_logs(file_content)
    # Process each section
    for section in sections:
        section_header, section_content = section.split(':', 1)
        if section.startswith('Trade History:'):
            process_trade_history(section)
        elif section.startswith('Activities log:'):
            process_activities_log(section)
        elif section.startswith('Sandbox logs:'):
            process_sandbox_logs(section)


def json_parse(fileobj, decoder=JSONDecoder(), buffersize=2048):
    buffer = ''
    for chunk in iter(partial(fileobj.read, buffersize), ''):
        buffer += chunk
        while buffer:
            try:
                result, index = decoder.raw_decode(buffer)
                yield result
                buffer = buffer[index:].lstrip()
            except ValueError:
                # Not enough data to decode, read more
                break


def sandbox(file):
    return_df = pd.DataFrame()
    with open(file, 'r') as file_handle:
        # print(next(file_handle))  # ignore the first line I have stripped out the first line when writing the three separate files
        for data in json_parse(file_handle):
            return_df = pd.concat([return_df, pd.DataFrame([data])], ignore_index=True)

    return return_df


def trade_histories(file):
    return_df = pd.DataFrame()
    with open(file, 'r') as infh:
        # print(next(infh))  # ignore the first line
        for data in json_parse(infh):
            return_df = pd.concat([return_df, pd.DataFrame(data)], ignore_index=True)
    # process object
    return return_df


def process_activities_log(file):
    print("Activities Log:")
    df = pd.read_csv(file, header=1, sep=';')
    return df


def extract_attributes_2(trader_data):
    # Some trader data log entries do not have values
    if trader_data == "":
        return {}, {}

    # observations_pattern = re.compile(r'Observations: \((.*?)\)')
    acceptable_price_pattern = re.compile(r'Acceptable price : (\d+)')
    buy_order_depth_pattern = re.compile(r'Buy Order depth : (\d+)')
    sell_order_depth_pattern = re.compile(r'Sell order depth : (\d+)')
    sell_pattern = re.compile(r'SELL (\d+x \d+)')

    # Extract attributes using regular expressions

    # observations = observations_pattern.search(trader_data).group(1)
    acceptable_price_matches = acceptable_price_pattern.findall(trader_data)
    buy_order_depth_matches = buy_order_depth_pattern.findall(trader_data)
    sell_order_depth_matches = sell_order_depth_pattern.findall(trader_data)
    sell_matches = sell_pattern.findall(trader_data)
    sells = ', '.join(sell_matches)

    plain_value_data = {
        "Acceptable price": int(acceptable_price_matches[0]),
        "Buy Order depth": int(buy_order_depth_matches[0]),
        "Sell order Depth": int(sell_order_depth_matches[0]),
        "Sells": sell_matches[0]
    }

    conversion_data = {
        "Acceptable price": int(acceptable_price_matches[1]),
        "Buy Order depth": int(buy_order_depth_matches[1]),
        "Sell order Depth": int(sell_order_depth_matches[1]),
        "Sells": sell_matches[1]
    }

    attributes = {
        "Observations": {
            "plainValueObservations": plain_value_data,
            "conversionObservations": conversion_data
        }
    }

    return plain_value_data, conversion_data


def extract_trader_data(sandbox_data):
    sandbox_data['plainValueObservations'], sandbox_data["conversionObservations"] = zip(
        *sandbox_data['lambdaLog'].apply(extract_attributes_2))

    return sandbox_data


def process_imc_log(file):
    with open(file, 'r') as f:
        file_content = f.read()
    sections = file_content.split('\n\n')
    filebase = os.path.splitext(os.path.basename(file))[0]
    file_path = os.path.dirname(file)

    # Write each section to a separate file
    for section in sections:
        lines = section.strip().split('\n')
        section_name = lines[0].strip(':').replace(" ", "_")
        section_data = '\n'.join(lines[1:])
        with open(file_path + "/" + filebase + "_" + section_name.strip() + '.log', 'w') as f:
            f.write(section_data)

    sandbox_df = sandbox(file_path + "/" + filebase + "_Sandbox_logs.log")
    trader_data_df = extract_trader_data(sandbox_df)
    activities_df = process_activities_log(file_path + "/" + filebase + "_Activities_log.log")
    trade_history_df = trade_histories(file_path + "/" + filebase + "_Trade_History.log")

    return sandbox_df, trader_data_df, activities_df, trade_history_df


if __name__ == '__main__':
    pd.set_option('display.width', 999, 'display.max_columns', 99, 'display.max_colwidth', 99)

    path = '/Users/lbw/Downloads/IMC/'

    # Test file to make it easier to debug
    log_file = path + 'imctest.log'
    # original file
    # log_file = path + 'f581c373-bbcd-4550-b2f8-1e5af6cdae81.log'

    for df in process_imc_log(log_file):
        print(df)

    pass
