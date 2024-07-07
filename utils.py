
import pandas as pd

def create_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def remove_spaces_and_punctuation(input_string):
    # Replace spaces and whitespaces with an empty string
    cleaned_string = input_string.replace(" ", "").replace("\t", "").replace("\n", "").replace("\r", "")
    # Remove periods and commas
    cleaned_string = cleaned_string.replace(".", "").replace(",", "")
    return cleaned_string


# not needed you can use the model_dump method for array/dict of objects insted of to do model_dump for each object
def normalize_data_df(data_res):

    converted_data = [row.model_dump(mode="unchanged") for row in data_res]
    print(converted_data)
    return pd.DataFrame(converted_data)

def normalize_data_arr_of_dict(data_res):

    converted_data = [row.model_dump(mode="unchanged") for row in data_res]
    return converted_data


def create_filter_query(filter_dict:dict, main_query:str, bind_object:dict):

    for key in filter_dict:
        if filter_dict[key] is not None and len(filter_dict[key]) > 0:
            main_query = f"""{main_query} AND {key} IN %({key})s"""
            bind_object[key] = tuple(filter_dict[key])
    return main_query, bind_object


def create_search_query(search:str, serach_columns_list:list, main_query:str, bind_object:dict):

    search_query = (" OR ").join([f"{column} LIKE %(search)s" for column in serach_columns_list])
    search_query= f""" AND ({search_query})""" 
    print(search_query)

    main_query = f"""{main_query}  {search_query}"""
    bind_object["search"] = f"%{search}%"

    return main_query,bind_object


