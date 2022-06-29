import pandas as pd
import os


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
    headers = ['pmid_title_', 'pmid_', 'pmid_date_', 'pmid_journal_']

    # dataframe of all references for a condition
    references_mask = df.columns.str.contains('pmid_title_')
    references_df = df.iloc[:, references_mask]
    new_df = references_df.iloc[index].dropna()

    # array of parsed references
    references_array = []

    for i in range(1, new_df.size+1):
        doc = {}
        for header in headers:
            if f'{header}{i}' in df.columns:
                if pd.isnull(df[f'{header}{i}'][index]) == False:
                    doc[f'{header}{i}'] = df[f'{header}{i}'][index]
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

    return condition_information


def get_gene_information(df, index):
    headers = ['db_hgnc_gene_id', 'db_hgnc_gene_symbol']

    # object with gene information
    gene_information = {}

    for header in headers:
        if header in df.columns:
            if pd.isnull(df[header][index]) == False:
                gene_information[header] = df[header][index]

    return gene_information


def load_data(data_folder):
    df = pd.read_excel(os.path.join(
        data_folder, 'GTRx_Joined_Data2-1-2022.xlsx'), engine='openpyxl')

    for index, row in df.iterrows():
        id = row['record_id']
        condition = get_condition(df, index)
        gene_information = get_gene_information(df, index)
        interventions = get_interventions(df, index)
        references = get_references(df, index)

        doc = {}
        doc['_id'] = id
        doc['condition'] = condition
        if gene_information != {}:
            doc['gene information'] = gene_information
        if interventions != []:
            doc['interventions'] = interventions
        if references != []:
            doc['references'] = references
        yield doc
