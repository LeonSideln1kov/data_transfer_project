from db_class_creator import CSVToModelConverter
import os
import yaml


IP = "0.0.0.0"
PORT = 5432
# TODO transfer to .env
USERNAME = "admin"
PASSWORD = "admin"
DB_NAME = "covid19"
CONNECTION_URL = f"postgresql://{USERNAME}:{PASSWORD}@{IP}:{PORT}/{DB_NAME}"
THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def main():
    # get config
    with open("config.yaml", "r") as stream:
        config  = yaml.safe_load(stream)
       
    # create table classes
    converter = CSVToModelConverter("models/generated_models.py")
    
    for table in config["DataFiles"]:
        converter.convert_csv(
            csv_path=f"{THIS_DIR}/{table["path"]}",
            class_name=table["class_name"],
            table_name=table["table_name"],
            key_column=table["key_column"],
        )


if __name__ == "__main__":
    main()
