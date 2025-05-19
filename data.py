import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Table, Column, String


Base = automap_base()


class OurWorldInData(Base):
    __tablename__ = "owid_covid"
    iso_code = Column(String, primary_key=True)
