import pandas as pd
import os


def get_alternate_names(df, index):
    # create data frame for condition names in each row
    alternate_name_mask = df.columns.str.contains('condition_name_')
    alternate_name_df = df.iloc[:, alternate_name_mask]
    new_df = alternate_name_df.iloc[index].dropna()

    # array of all alternate names for particular condition
    alternate_names = []

    for value in new_df:
        alternate_names.append(value)

    return alternate_names


def get_interventions(df, index):
    headers = ['int_description_', 'timeframe_int', 'age_use_int',
               'contra_int', 'qualscale_reclass_drug', 'rev1_eff_reclass_drug']

    # create dataframe of all interventions for a particular row
    int_description_mask = df.columns.str.contains('int_description_')
    int_description_mask = df.iloc[:, int_description_mask]
    new_df = int_description_mask.iloc[index].dropna()

    # array of all interventions for a particular condition
    interventions_array = []

    for i in range(1, new_df.size+1):
        doc = {}
        for header in headers:
            if f'{header}{i}' in df.columns:
                if pd.isnull(df[f'{header}{i}'][index]) == False:
                    doc[f'{header}{i}'] = df[f'{header}{i}'][index]
        interventions_array.append(doc)

    return interventions_array


def get_references(df, index):
    headers = ['pmid_title_', 'pmid_', 'pmid_date_', 'pmid_journal_']

    # create data frame of all references for a particular row
    references_mask = df.columns.str.contains('pmid_title_')
    references_df = df.iloc[:, references_mask]
    new_df = references_df.iloc[index].dropna()

    # array of all references for a particular condition
    references_array = []

    for i in range(1, new_df.size+1):
        doc = {}
        for header in headers:
            if f'{header}{i}' in df.columns:
                if pd.isnull(df[f'{header}{i}'][index]) == False:
                    doc[f'{header}{i}'] = df[f'{header}{i}'][index]
        references_array.append(doc)

    return references_array


def load_data(data_folder):
    df = pd.read_excel(os.path.join(
        data_folder, 'GTRx_Joined_Data2-1-2022.xlsx'), sheet_name='GTRx_Joined_Data2-1-2022')

    for index, row in df.iterrows():
        id = row['record_id']
        condition_name = row['condition_name']
        clinical_summary = row['rcigm_clinical_summary']
        freq_per_birth = row['freq_per_birth']
        pattern_of_inheritance = row['pattern_of_inheritance']
        alternate_names = get_alternate_names(df, index)
        interventions = get_interventions(df, index)
        references = get_references(df, index)

        doc = {}
        doc['_id'] = id
        doc['condition'] = {
            'condition_name': condition_name,
            'alternate_names': alternate_names,
            'freq_per_birth': freq_per_birth,
            'pattern_of_inheritance': pattern_of_inheritance,
        }
        doc['clinical_summary'] = clinical_summary
        doc['interventions'] = interventions
        doc['references'] = references
        yield doc
