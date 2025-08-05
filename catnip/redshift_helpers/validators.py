from typing import List
from catnip.redshift_helpers.lookups import REDSHIFT_RESERVED_WORDS
import re

class ColumnValidator:

    def __init__(self, column_names: List[str]):
        self.column_names = column_names

    def validate_column_names(self) -> bool:

        ## get reserved words
        reserved_words = [r.strip().lower() for r in REDSHIFT_RESERVED_WORDS]

        ## check reserved words
        for col in self.column_names:
            try:
                assert col not in reserved_words
            except AssertionError:
                raise ValueError(f"DataFrame column name {col} is a reserved word in Redshift! 😩")

        ## check for spaces
        pattern = re.compile(r'\s')
        for col in self.column_names:
            try:
                assert not pattern.search(col)
            except AssertionError:
                raise ValueError(f"DataFrame column name {col} has a space! 😩 Remove spaces from column names and retry!")

        return True