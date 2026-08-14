import os
from pandas import read_csv
import re
import json
import shutil
import csv
TARGET_ROOT_DIR = "/Users/akhan/training/sample-data/target"
SOURCE_ROOT_DIR = "/Users/akhan/training/sample-data/source"

def get_all_run_ids():
    df = read_csv('/Users/akhan/training/sample-data/master-file.csv')
    return df['RUNID'].unique()


def reformat_string(input_string):
    segments = input_string.split('>')
    
    cleaned_segments = []
    for segment in segments:
        s = segment.strip()
        
        s = s.replace('.', '')
        
        s = re.sub(r'\s+', '_', s)
        
        if s:
            cleaned_segments.append(s)
            
    return "/".join(cleaned_segments)

def generate_paths(row):
    _domain = row['DOMAIN'].strip().lower()
    _subdomain = row['SUBDOMAIN'].strip().lower()
    _project_name = reformat_string(row["TEST_PROJECT_NAME"].strip().lower())
    _run_id = str(row["RUNID"])
    _design_steps = str(row["TEST_ENTITY"]).strip().lower() if row["TEST_ENTITY"] and row["TEST_ENTITY"] != "NULL" else None
    _test_type = reformat_string(row["ENTITY"].strip().lower())

    if _design_steps is None:
        return {"target_attachment_path": [os.path.join(_domain, _subdomain, _project_name, _run_id, _test_type)], "target_metadata_path": os.path.join(_domain, _subdomain, _project_name, _run_id), "source_attachment_path": os.path.join(_domain, _subdomain, _test_type, _run_id)}
    else:
        return {"target_attachment_path": [os.path.join(_domain, _subdomain, _project_name, _run_id, _design_steps), os.path.join(_domain, _subdomain, _project_name, _run_id, _test_type)], "target_metadata_path": os.path.join(_domain, _subdomain, _project_name, _run_id), "source_attachment_path": os.path.join(_domain, _subdomain, _test_type, _run_id), "source_design_steps_attachment_path": os.path.join(_domain, _subdomain, _design_steps, _run_id)}

def get_all_rows_for_run_id(run_id):
    df = read_csv('/Users/akhan/training/sample-data/master-file.csv')
    return df[df['RUNID'] == run_id]

def create_target_directory(path):
    os.makedirs(path, exist_ok=True)

def create_target_paths(target_attachment_path, target_metadata_path):
    for path in target_attachment_path:
        create_target_directory(os.path.join(TARGET_ROOT_DIR, path))
    create_target_directory(os.path.join(TARGET_ROOT_DIR, target_metadata_path))

def create_metadata_file(path,metadata):
    with open(os.path.join(path, "metadata.json"), "w") as f:
        json.dump(metadata, f)
    return os.path.join(path, "metadata.json")

def get_metadata_for_run_id(run_id):
    df = get_all_rows_for_run_id(run_id)
    
    if df.empty:
        return {}

    first_row = df.iloc[0]
    test_project_name = str(first_row.get("TEST_PROJECT_NAME", "")).strip().lower()
    desstep_mask = (
        df["TEST_ENTITY"].notna() & 
        (df["TEST_ENTITY"].astype(str).str.strip().str.upper() == "DESSTEPS")
    )
    
    desstep_rows = df[desstep_mask]

    if not desstep_rows.empty:
        design_steps = desstep_rows["STEPNUMBER"].tolist()
    else:
        design_steps = None

    metadata = {
        "run_id": str(run_id),
        "test_project_name": test_project_name,
        "test_entity": None if first_row.get("TEST_ENTITY") == "NULL" else first_row.get("TEST_ENTITY"),
        "test_type": first_row.get("ENTITY"),
        "domain": first_row.get("DOMAIN"),
        "subdomain": first_row.get("SUBDOMAIN"),
        "design_steps": design_steps,  # Added step list
        "project_name": first_row.get("PROJECT_NAME"),
        "project_description": first_row.get("PROJECT_DESCRIPTION"),
        "project_status": first_row.get("PROJECT_STATUS"),
        "project_start_date": first_row.get("PROJECT_START_DATE"),
        "project_end_date": first_row.get("PROJECT_END_DATE"),
        "project_owner": first_row.get("PROJECT_OWNER"),
        "project_owner_email": first_row.get("PROJECT_OWNER_EMAIL"),
        "project_owner_phone": first_row.get("PROJECT_OWNER_PHONE"),
    }

    return metadata

def copy_files_to_target(source_attachment_path, target_attachment_path):

    if not os.path.exists(source_attachment_path):
        return
    
    source_file_count = len(os.listdir(source_attachment_path))
    source_file_path_size = sum(os.path.getsize(os.path.join(source_attachment_path, file)) for file in os.listdir(source_attachment_path))

    for file in os.listdir(source_attachment_path):
        shutil.copy(os.path.join(source_attachment_path, file), os.path.join(target_attachment_path, file))

    target_file_count = len(os.listdir(target_attachment_path))
    target_file_path_size = sum(os.path.getsize(os.path.join(target_attachment_path, file)) for file in os.listdir(target_attachment_path))

    return {
        "source_file_count": source_file_count,
        "source_file_path_size": source_file_path_size,
        "target_file_count": target_file_count,
        "target_file_path_size": target_file_path_size
    }

def write_mismatch_report(run_id, source_file_count, source_file_path_size, target_file_count, target_file_path_size):
    with open(os.path.join(TARGET_ROOT_DIR, "mismatch_report.csv"), "a") as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow([run_id, source_file_count, source_file_path_size, target_file_count, target_file_path_size])


with open(os.path.join(TARGET_ROOT_DIR, "mismatch_report.csv"), "w") as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(["run_id", "source_file_count", "source_file_path_size", "target_file_count", "target_file_path_size"])
for run_id in get_all_run_ids():

    df = get_all_rows_for_run_id(run_id)
    print("Processing run ID: ", run_id)
    paths = generate_paths(df.iloc[0])
    create_target_paths(paths["target_attachment_path"], paths["target_metadata_path"])
    metadata = get_metadata_for_run_id(run_id)
    create_metadata_file(os.path.join(TARGET_ROOT_DIR, paths["target_metadata_path"]), metadata)
    if "source_design_steps_attachment_path" in paths:
        copy_result = copy_files_to_target(paths["source_design_steps_attachment_path"], paths["target_attachment_path"][0])
        if copy_result:
            write_mismatch_report(run_id, copy_result["source_file_count"], copy_result["source_file_path_size"], copy_result["target_file_count"], copy_result["target_file_path_size"])
        else:
            write_mismatch_report(run_id, 0, 0, 0, 0)
        copy_result = copy_files_to_target(paths["source_attachment_path"], paths["target_attachment_path"][1])
        if copy_result:
            write_mismatch_report(run_id, copy_result["source_file_count"], copy_result["source_file_path_size"], copy_result["target_file_count"], copy_result["target_file_path_size"])
        else:
            write_mismatch_report(run_id,0, 0, 0, 0)
    else:
        copy_result = copy_files_to_target(paths["source_attachment_path"], paths["target_attachment_path"][0])
        if copy_result:
            write_mismatch_report(run_id, copy_result["source_file_count"], copy_result["source_file_path_size"], copy_result["target_file_count"], copy_result["target_file_path_size"])
        else:
            write_mismatch_report(run_id,0, 0, 0, 0)