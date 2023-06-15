import argparse
import pandas as pd
import json
import os

# import sys
# sys.stdout = open('output.txt','wt')

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

# The output json should contain information for every customer and has the following fields:
# customer_id, loyalty_score, product_id, product_category, purchase_count


def convert_output(customers_location,products_location,transactions_location,output_location):

    path = transactions_location
    file_list = os.listdir(path)

    for file_name in file_list:
        print(file_name)
        final_json = []
        final_df = pd.DataFrame()

        df = pd.read_csv(customers_location)
        df_1 = pd.read_csv(products_location)

        # temp_file_data = '../input_data/starter/transactions/d=2019-03-01/transactions.json'
        # temp_data = read_json_file(temp_file_data)
        temp_data = read_json_file(path+file_name+'/transactions.json')
        for i in range(len(temp_data)):
            filtered_data = df[df['customer_id'] == temp_data[i]['customer_id']]
            loyalty_score = filtered_data.iloc[0, 1]
            product_id = []
            product_category = []
            for j in range(len(temp_data[i]['basket'])):
                product_id.append(temp_data[i]['basket'][j]['product_id'])
                filtered_data_1 = df_1[df_1['product_id'] == temp_data[i]['basket'][j]['product_id']]
                product_category.append(filtered_data_1.iloc[0,1])
            temp_json = {}
            temp_json['customer_id'] = temp_data[i]['customer_id']
            temp_json['loyalty_score'] = loyalty_score
            temp_json['purchase_count'] = len(temp_data[i]['basket'])
            temp_json['product_id'] = product_id
            temp_json['product_category'] = product_category
            final_json.append(temp_json)
            # final_df = final_df.append(pd.DataFrame(temp_json))
            final_df = pd.concat([final_df, pd.DataFrame(temp_json)], ignore_index=True)


        for item in final_json:
            item['loyalty_score'] = int(item['loyalty_score'])

        json_data = json.dumps(final_json)
        print(json_data)
        if not os.path.exists(output_location+'/'+file_name):
            os.makedirs(output_location+'/'+file_name)

        output_file = os.path.join(output_location+'/'+file_name, "output.json")
        os.makedirs(output_location+'/'+file_name, exist_ok=True)

        with open(output_file, "w") as file:
            # json.dump(json_data, file)
            file.write(json_data)

        final_df.to_csv(output_location+'/'+file_name+"/output_data.csv", index=False)
        df = pd.DataFrame(final_json)
        df.to_csv(output_location+'/'+file_name+"/output_data_v2.csv", index=False)
        
def main():
    params = get_params()
    convert_output('.'+params['customers_location'], '.'+params['products_location'], '.'+params['transactions_location'], '.'+params['output_location'])


def read_csv_file(path):
    df = pd.read_csv(path)
    return df

def read_json_file(path):
    with open(path, 'r') as file:
        lines = file.readlines()

    fixed_lines = []
    for line in lines:
        line = line.strip()
        if line:
            fixed_lines.append(line)

    json_data = '[' + ','.join(fixed_lines) + ']'
    data = json.loads(json_data)
    return data

if __name__ == "__main__":
    main()

