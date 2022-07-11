import pandas as pd
import os
import re
import json
import warnings


def get_alternate_names(df, index):
    # data frame for row's alternate names
    alternate_name_mask = df.columns.str.contains('condition_name_')
    alternate_name_df = df.iloc[:, alternate_name_mask]
    new_df = alternate_name_df.iloc[index].dropna()

    # array of parsed alternate names
    alternate_names = []

    for value in new_df:
        alternate_names.append(value)

    return alternate_names


def get_references(df, index):
    headers = {'title': 'pmid_title_',
               'pmid': 'pmid_',
               'date': 'pmid_date_',
               'journal': 'pmid_journal_'}

    # dataframe of all references for a condition
    references_mask = df.columns.str.contains('pmid_title_')
    references_df = df.iloc[:, references_mask]
    new_df = references_df.iloc[index].dropna()

    # array of parsed references
    references_array = []

    for i in range(1, new_df.size+1):
        doc = {}
        for key, value in headers.items():
            if f'{value}{i}' in df.columns and pd.isnull(df[f'{value}{i}'][index]) == False:
                cell_content = df[f'{value}{i}'][index]
                if isinstance(cell_content, float):
                    cell_content = int(cell_content)
                doc[key] = cell_content
        doc != {} and references_array.append(doc)

    return references_array


def get_clinical_summary(df, index):
    headers = ['rcigm_clinical_summary', 'rcigm_clinical_summary2']

    # array of parsed clinical summaries
    clinical_summary_array = []

    for header in headers:
        if header in df.columns:
            if pd.isnull(df[header][index]) == False:
                clinical_summary_array.append(df[header][index])

    return clinical_summary_array


def get_condition(df, index):
    headers = ['condition_name', 'freq_per_birth', 'pattern_of_inheritance']

    omim = df['record_id'][index].split('OMIM:')[-1]
    clinical_summary = get_clinical_summary(df, index)
    alternate_names = get_alternate_names(df, index)

    # object with condition information
    condition_information = {}

    for header in headers:
        if header in df.columns:
            if pd.isnull(df[header][index]) == False:
                condition_information[header] = df[header][index]

    if clinical_summary != []:
        condition_information['clinical_summary'] = clinical_summary
    if alternate_names != []:
        condition_information['alternate_names'] = alternate_names
    condition_information['omim'] = omim

    return condition_information


def load_data(data_folder):
    df = pd.read_excel(os.path.join(
        data_folder, 'GTRx_Joined_Data2-1-2022.xlsx'), engine='openpyxl')

    int_headers = {
        'priority_class': 'priority_class_drug',
        'timeframe': 'timeframe_int',
        'age_use': 'age_use_int',
        'contra': 'contra_int',
        'qualscale_reclass': 'qualscale_reclass_drug',
        'rev1_eff_reclass': 'rev1_eff_reclass_drug'
    }

    for index, row in df.iterrows():
        headers = ['use_group_', 'add_int_description_']
        interventions_mask = df.columns.str.contains(
            '|'.join(headers))
        interventions_df = df.iloc[:, interventions_mask]
        new_df = interventions_df.iloc[index].dropna()

        level2_groups = []

        _id = row['record_id'].split('-')[-1]
        subject = get_condition(df, index)
        predicate = 'treated_by'
        references = get_references(df, index)

        for key, value in new_df.items():
            if 'add_int_' in key:
                add_int_number = key.split('add_int_description_')[-1]

                add_int_description = row[f'add_int_description_{add_int_number}']
                if pd.isnull(row[f'add_int_detail_{add_int_number}']) == False:
                    add_int_detail = row[f'add_int_detail_{add_int_number}']

                doc = {}
                doc['_id'] = _id
                doc['subject'] = subject
                doc['predicate'] = predicate
                doc['object'] = {
                    'add_int_description': add_int_description, 'add_int_detail': add_int_detail}
                doc['references'] = references
                yield doc

            if 'Retain' in value:
                group_number = key.split('use_group_')[-1]
                level2_groups.append(group_number)

                for level2_group in level2_groups:

                    int_cell = df[f'level2_group{level2_group}'][index]
                    int_descriptions = re.sub(
                        '\[|\]', '', int_cell).split(',')

                    doc = {'_id': _id}
                    object = {'intervention': [], 'level2_group': level2_group}

                    for intervention in int_descriptions:
                        int_number = intervention.split('int_description_')[-1]
                        inxight = ""

                        description = row[f'int_description_{int_number}']
                        int_link = row[f'int_link_{int_number}']

                        if 'drugs.ncats.io/drug/' in str(int_link):
                            inxight = df[f'int_link_{int_number}'][index].split(
                                'https://drugs.ncats.io/drug/')[-1]

                        intervention_information = {}
                        intervention_information['name'] = description
                        if inxight != "":
                            intervention_information['inxight'] = inxight
                            doc['_id'] += '-'+inxight
                        else:
                            warnings.warn(
                                f'inxight not found: {_id}, {description}')
                        if f'int_class{int_number}' in df.columns and pd.isnull(row[f'int_class{int_number}']) == False:
                            intervention_information['int_class'] = row[f'int_class_{int_number}']

                        object['intervention'].append(intervention_information)

                    for key, value in int_headers.items():
                        if f'{value}{level2_group}' in df.columns and pd.isnull(row[f'{value}{level2_group}']) == False:
                            cell_content = row[f'{value}{level2_group}']
                            if isinstance(cell_content, float):
                                cell_content = int(cell_content)
                            object[key] = cell_content

                    doc['subject'] = subject
                    doc['predicate'] = predicate
                    doc['object'] = object
                    doc['references'] = references

                    yield doc


stuff = load_data('')

json_data = {"records": []}
for item in stuff:
    json_data["records"].append(item)

with open('gtrx_data.json', 'w') as f:
    json.dump(json_data, f)
