import pandas as pd

df = pd.read_csv("data.csv", low_memory=False)


def get_alternate_names(index):
    alternate_name_mask = df.columns.str.contains('condition_name_')
    alternate_name_df = df.iloc[:, alternate_name_mask]
    new_df = alternate_name_df.iloc[index].dropna()

    alternate_names = []

    for value in new_df:
        alternate_names.append(value)

    return alternate_names


def get_interventions(index):
    int_description_mask = df.columns.str.contains('int_description_')
    int_description_mask = df.iloc[:, int_description_mask]
    new_df = int_description_mask.iloc[index].dropna()

    final_array = []
    for i in range(1, new_df.size+1):
        doc = {}
        doc['name'] = df[f'int_description_{i}'][index]
        if f'timeframe_int{i}' in df.columns:
            if pd.isnull(df[f'timeframe_int{i}'][index]) == False:
                doc['timeframe'] = df[f'timeframe_int{i}'][index]
        if f'age_use_int{i}' in df.columns:
            if pd.isnull(df[f'age_use_int{i}'][index]) == False:
                doc['age_group'] = df[f'age_use_int{i}'][index]
        if f'contra_int{i}' in df.columns:
            if pd.isnull(df[f'contra_int{i}'][index]) == False:
                doc['constrained_groups'] = df[f'contra_int{i}'][index]
        if f'qualscale_reclass_drug{i}' in df.columns:
            if pd.isnull(df[f'qualscale_reclass_drug{i}'][index]) == False:
                doc['qualscale_reclass'] = df[f'qualscale_reclass_drug{i}'][index]
        if f'rev1_eff_reclass_drug{i}' in df.columns:        
            if pd.isnull(df[f'rev1_eff_reclass_drug{i}'][index]) == False:
                doc['rev1_eff_reclass_drug'] = df[f'rev1_eff_reclass_drug{i}'][index]
        final_array.append(doc)

    return final_array


def get_references(index):
    references_mask = df.columns.str.contains('pmid_title_')

    references_df = df.iloc[:, references_mask]

    new_df = references_df.iloc[index].dropna()

    final_array = []
    for i in range(1, new_df.size+1):
        doc = {}
        if pd.isnull(df[f'pmid_title_{i}'][index]) == False:
            doc['title'] = df[f'pmid_title_{i}'][index]
        if pd.isnull(df[f'pmid_{i}'][index]) == False:
            doc['pmid'] = df[f'pmid_{i}'][index]
        if pd.isnull(df[f'pmid_date_{i}'][index]) == False:
            doc['year'] = df[f'pmid_date_{i}'][index]
        if pd.isnull(df[f'pmid_journal_{i}'][index]) == False:
            doc['journal'] = df[f'pmid_journal_{i}'][index]
        final_array.append(doc)

    return final_array


for index, row in df.iterrows():
    id = row['record_id']
    condition_name = row['condition_name']
    clinical_summary = row['rcigm_clinical_summary']
    freq_per_birth = row['freq_per_birth']
    pattern_of_inheritance = row['pattern_of_inheritance']
    alternate_names = get_alternate_names(index)
    interventions = get_interventions(index)
    references = get_references(index)

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
    print(doc)
