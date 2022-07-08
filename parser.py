import pandas as pd
import os
import re


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


def get_interventions(df, index):
    headers = ['int_description_', 'timeframe_int', 'age_use_int',
               'contra_int', 'qualscale_reclass_drug', 'rev1_eff_reclass_drug']

    # dataframe of interventions for a condition
    int_description_mask = df.columns.str.contains('int_description_')
    int_description_mask = df.iloc[:, int_description_mask]
    new_df = int_description_mask.iloc[index].dropna()

    # array of parsed intervention information
    interventions_array = []

    for i in range(1, new_df.size+1):
        doc = {}
        for header in headers:
            if f'{header}{i}' in df.columns:
                if pd.isnull(df[f'{header}{i}'][index]) == False:
                    doc[f'{header}{i}'] = df[f'{header}{i}'][index]
        doc != {} and interventions_array.append(doc)

    return interventions_array


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

    for index, row in df.iterrows():
        interventions_mask = df.columns.str.contains('use_group_')
        interventions_df = df.iloc[:, interventions_mask]
        new_df = interventions_df.iloc[index].dropna()

        level2_groups = []

        for key, value in new_df.items():
            if 'Retain' in value:
                group_number = key.split('use_group_')[-1]
                level2_groups.append(group_number)

        for level2_group in level2_groups:

            int_cell = df[f'level2_group{level2_group}'][index]
            int_descriptions = re.sub(
                '\[|\]', '', int_cell).split(',')

            doc = {"_id": row['record_id'].split('-')[-1]}
            object = {"intervention": []}

            for int in int_descriptions:
                int_number = int.split('int_description_')[-1]
                inxight = ""

                description = row[f'int_description_{int_number}']
                int_link = row[f'int_link_{int_number}'][index]

                if 'drugs.ncats.io/drug/' in int_link:
                    inxight = df[f'int_link_{int_number}'][index].split(
                        'https://drugs.ncats.io/drug/')[-1]

                int_class = df[f'int_class_{int_number}'][index]

                intervention = {}
                intervention['name'] = description
                if inxight != "":
                    intervention['inxight'] = inxight
                    doc['_id'] += '-'+inxight
                intervention['int_class'] = int_class

                object['intervention'].append(intervention)

            object['level2_group'] = level2_group
            object['priority_class'] = row[f'priority_class_drug{level2_group}']
            object['timeframe'] = row[f'timeframe_int{level2_group}']
            object['age_use'] = row[f'age_use_int{level2_group}']
            object['contra'] = row[f'contra_int{level2_group}']
            object['qualscale_reclass'] = row[f'qualscale_reclass_drug{level2_group}']
            object['rev1_eff_reclass'] = row[f'rev1_eff_reclass_drug{level2_group}']

            doc["subject"] = get_condition(df, index)
            doc['predicate'] = 'treated_by'
            doc["object"] = object
            doc["references"] = get_references(df, index)

            yield doc
