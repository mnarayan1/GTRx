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
    search_headers = ['timeframe_int', 'age_use_int',
                      'contra_int', 'qualscale_reclass_drug', 'rev1_eff_reclass_drug']
    interventions_mask = df.columns.str.contains('|'.join(search_headers))
    int_description_mask = df.columns.str.contains('int_description_')

    interventions_df = df.iloc[:, interventions_mask]
    int_description_mask = df.iloc[:, int_description_mask]

    new_df = int_description_mask.iloc[index].dropna()

    final_array = []
    for i in range(1, new_df.size+1):
        doc = {}
        doc['name'] = df[f'int_description_{i}'][index]
        if pd.isnull(df[f'timeframe_int{i}'][index]) == False:
            doc['timeframe'] = df[f'timeframe_int{i}'][index]
        if pd.isnull(df[f'age_use_int{i}'][index]) == False:
            doc['age_group'] = df[f'age_use_int{i}'][index]
        if pd.isnull(df[f'contra_int{i}'][index]) == False:
            doc['constrained_groups'] = df[f'contra_int{i}'][index]
        if pd.isnull(df[f'qualscale_reclass_drug{i}'][index]) == False:
            doc['qualscale_reclass'] = df[f'qualscale_reclass_drug{i}'][index]
        if pd.isnull(df[f'rev1_eff_reclass_drug{i}'][index]) == False:
            doc['rev1_eff_reclass_drug'] = df[f'rev1_eff_reclass_drug{i}'][index]
        final_array.append(doc)

    return final_array


for index, row in df.iterrows():
    id = row['record_id']
    condition_name = row['condition_name']
    clinical_summary = row['rcigm_clinical_summary']
    alternate_names = get_alternate_names(index)
    interventions = get_interventions(index)

    doc = {}
    doc['_id'] = id
    doc['condition_name'] = condition_name
    doc['alternate_names'] = alternate_names
    doc['clinical_summary'] = clinical_summary
    doc['interventions'] = interventions
    print(doc)
