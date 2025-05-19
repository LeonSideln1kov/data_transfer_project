from pathlib import Path
import pandas as pd
from typing import Dict, Optional


class ModelBuilder:
    """Builder for generating SQLAlchemy classes"""

    def __init__(self):
        self._reset()
        self.__type_mapping = {
            "int64": "Integer()",
            "float64": "Float()",
            "datetime": "DateTime()",
            "str": "String(255)"
        }
    
    def _reset(self):
        self.class_name = ""
        self.table_name = ""
        self.columns: Dict[str, str] = {}

    def set_names(self, class_name: str, table_name: str):
        self.class_name = class_name
        self.table_name = table_name
        return self

    def add_column(self, name: str, dtype: type):
        self.columns[name] = self.__type_mapping.get(dtype.__name__, "String(255)")
        return self

    def build(self, key_column: str) -> str:
        code = f"class {self.class_name}(Base):\n"
        code += f"    __tablename__ = '{self.table_name}'\n"
        code += f"    __table_args__ = {{'extend_existing': True}}\n\n"
        
        for name, dtype in self.columns.items():
            code += f"    {name.lower()
                               .strip()
                               .replace("(", "")
                               .replace(")", "")
                               .replace("'", " ")
                               .replace("-", "_")
                               .replace(" ", "_")} = Column({dtype}{", primary_key=True" if name == key_column else ""})\n"
        
        self._reset()
        return code
    
class ModelFileManager:
    """Facade for developing file with classes"""

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self._ensure_base_import()

    def _ensure_base_import(self):
        if not self.file_path.exists():
            with open(self.file_path, "w") as f:
                f.write("from sqlalchemy.orm import DeclarativeBase\n")
                f.write("from sqlalchemy import Column, Integer, String, Float, DateTime\n\n\n")
                f.write("class Base(DeclarativeBase):\n")
                f.write("    pass\n\n")

    def add_class(self, class_code: str):
        with open(self.file_path, "a") as f:
            f.write("\n" + class_code)

class CSVToModelConverter:
    """Facade-controller for entire process"""

    def __init__(self, output_file: str):
        self.builder = ModelBuilder()
        self.file_manager = ModelFileManager(output_file)

    def convert_csv(self, csv_path: str, class_name: str, table_name: Optional[str], key_column: str):
        df = pd.read_csv(csv_path, nrows=100)
        table_name = table_name or f"tbl_{class_name.lower()}"
        builder = self.builder.set_names(class_name, table_name)
        for col, dtype in df.dtypes.items():
            builder.add_column(col, type(df[col].iloc[0]))
        self.file_manager.add_class(builder.build(key_column=key_column))
