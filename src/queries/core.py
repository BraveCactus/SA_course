import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import URL, create_engine, insert, text, select, update

from models import metadata_obj, workers_table, WorkersOrm
from database import sync_engine, async_engine, sync_session_factory, async_session_factory, Base

# Если написать engine.begin() то после выхода из контексного менеджера будет совершен COMMIT

def get_version_sync():
    with sync_engine.connect() as conn:
        res = conn.execute(text('SELECT VERSION()'))
        print(f"{res.all()}")

async def get_version_async():
    async with async_engine.connect() as conn:
        res = await conn.execute(text('SELECT VERSION()'))
        print(f"{res.all()}")

class SyncCore:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True
    
    @staticmethod
    def insert_workers():
        with sync_engine.connect() as conn:
            stmt = insert(workers_table).values(
                [
                    {'username': 'Jack'},
                    {'username': 'Michael'},
                ]
            )
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(workers_table) # SELECT * FROM workers
            result = conn.execute(query)
            workers = result.all()
            print(workers)

    @staticmethod
    def update_workers(worker_id: int = 2, new_username: str = 'Misha'):
        with sync_engine.connect() as conn:
            # stmt = text('UPDATE workers SET username=:username WHERE id=:id')
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table)
                .values(username=new_username)
                # .where(workers_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )
            conn.execute(stmt)
            conn.commit()

class AsyncCore:
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)


def create_tables():
    sync_engine.echo = False
    Base.metadata.drop_all(sync_engine)
    Base.metadata.create_all(sync_engine)
    sync_engine.echo = True

async def insert_data():    
    async with async_session_factory() as session:
        worker_bobr = WorkersOrm(username='Bobr')
        worker_medved = WorkersOrm(username='Medved')
        session.add_all([worker_bobr, worker_medved])
        await session.commit()

# def insert_data():
#     with sync_engine.connect() as conn:
#         # stmt = """INSERT INTO workers (username) 
#         #           VALUES ('Гриша'), ('Олег');"""
#         # conn.execute(text(stmt))
#         stmt = insert(workers_table).values(
#             [
#                 {'username': 'Гриша'},
#                 {'username': 'Олег'},
#             ]
#         )
#         conn.execute(stmt)
#         conn.commit()


