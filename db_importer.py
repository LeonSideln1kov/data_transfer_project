import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
import logging
from pathlib import Path


class DatabaseImporter:
    """Facede for importing CSV's in PostgreSQL"""

    db_url: str
    engine: Engine | None = None
    session: Session | None = None

    def __init__(self, db_url):
        self.db_url = db_url

    def connect(self):
        """Set connection with DB"""
        try:
            self.engine = create_engine(self.db_url)
            self._session = sessionmaker(bind=self.engine)
            logging.info("Successfully connected to the database")
        except SQLAlchemyError as e:
            logging.info(f"Connection error: {e}")
            raise

    def import_csv(self, 
                   csv_path: Path | str,
                   table_name: str, 
                   if_exists="replace",
                   sep=","
                ):
        try: 
            df = pd.read_csv(csv_path, sep=sep)

            with self.engine.begin() as connection: 
                df.to_sql(
                    name=table_name,
                    con=connection,
                    if_exists=if_exists,
                    index=False
                ) 
        except Exception as e:
            logging.info(f"Import failed: {e}")
            raise
    
    @property
    def session(self):
        return self._session
        