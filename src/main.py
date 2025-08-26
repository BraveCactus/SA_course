import os
import sys
sys.path.insert(1, os.path.join(sys.path[0],'..'))

from queries.core import create_tables, insert_data
from queries.core import SyncCore, AsyncCore
from queries.orm import SyncOrm

if __name__ == "__main__":
    # SyncCore.create_tables()
    # SyncCore.insert_workers()
    # SyncCore.select_workers()
    # SyncCore.update_workers()

    SyncOrm.create_tables()
    SyncOrm.insert_workers()    
    # SyncOrm.update_workers()
    SyncOrm.insert_resumes()
    SyncOrm.insert_additional_resumes()
    # SyncOrm.join_cte_subquery_window_func()
    # SyncOrm.select_workers_with_selectin_relationship()
    SyncOrm.select_workers_with_condition_relationship()