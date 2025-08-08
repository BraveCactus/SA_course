from sqlalchemy import Integer, and_, cast, func, text, insert, inspect, select
from sqlalchemy.orm import aliased, joinedload, selectinload, contains_eager

from database import Base, sync_session_factory, sync_engine, async_session_factory, async_engine
from models import WorkersOrm, ResumeOrm, Workload


class SyncOrm:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_workers():    
        with sync_session_factory() as session:
            worker_bobr = WorkersOrm(username='Bobr')
            worker_medved = WorkersOrm(username='Medved')
            session.add_all([worker_bobr, worker_medved])
            session.flush() # Отправляет данные в бд, но не коммитит
            session.commit()

    @staticmethod
    def select_workers():
        with sync_session_factory() as session:
            # worker_id = 1
            # worker_jack = session.get(WorkersOrm, worker_id)
            query = select(WorkersOrm)
            result = session.execute(query)
            workers = result.scalars().all()
            print(workers)


    @staticmethod
    def update_workers(worker_id: int = 2, new_username: str = 'Misha'):
        with sync_session_factory() as session:
            worker_michael = session.get(WorkersOrm, worker_id)
            worker_michael.username = new_username
            # session.expire_all() # сбрасывает все изменения
            # session.refresh(worker_michael) # возращает параметры к начальным значениям (которые указаны в таблице)
            session.commit()

    @staticmethod
    def insert_resumes():    
        with sync_session_factory() as session:
            resume_bobr_1 = ResumeOrm(title='Python Junior Developer', 
                                     compensation=50000, 
                                     workload=Workload.fulltime, 
                                     worker_id=1)
            resume_bobr_2 = ResumeOrm(title='Python Разработчик', 
                                     compensation=150000, 
                                     workload=Workload.fulltime, 
                                     worker_id=1)
            resume_medved_1 = ResumeOrm(title='Python Data Engineer', 
                                     compensation=250000, 
                                     workload=Workload.parttime, 
                                     worker_id=2)
            resume_medved_2 = ResumeOrm(title='Data Scientist', 
                                     compensation=350000, 
                                     workload=Workload.fulltime, 
                                     worker_id=2)
            session.add_all([resume_bobr_1, resume_bobr_2, resume_medved_1, resume_medved_2])
            # session.flush() # Отправляет данные в бд, но не коммитит
            session.commit()

    @staticmethod
    def select_resumes():
        with sync_session_factory() as session:
            # worker_id = 1
            # worker_jack = session.get(WorkersOrm, worker_id)
            query = select(ResumeOrm)
            result = session.execute(query)
            resumes = result.scalars().all()
            print(resumes)


    @staticmethod
    def update_resumes():
        ...

    @staticmethod
    def select_resumes_avg_compensations(like_language: str = "Python"):
        '''
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        '''
        with sync_session_factory() as session:
            query = (
                select(ResumeOrm.workload,
                       cast(func.avg(ResumeOrm.compensation), Integer).label('avg_compensation'))
                .select_from(ResumeOrm)
                .filter(and_(
                    ResumeOrm.title.contains(like_language),
                    ResumeOrm.compensation > 40000,
                ))
                .group_by(ResumeOrm.workload)
                .having(cast(func.avg(ResumeOrm.compensation), Integer) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True})) # Красивый вывод
            res = session.execute(query)
            result = res.all()
            print(result)

    @staticmethod
    def insert_additional_resumes():
        with sync_session_factory() as session:
            workers = [
                {"username": "Artem"},  # id 3
                {"username": "Roman"},  # id 4
                {"username": "Petr"},   # id 5
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 90000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkersOrm).values(workers)
            insert_resumes = insert(ResumeOrm).values(resumes)
            session.execute(insert_workers)
            session.execute(insert_resumes)
            session.commit()

    @staticmethod
    def join_cte_subquery_window_func(like_language: str = 'Python'):
        """
        Создает столбец со средней зарплатой для каждого вида workload.
        Создает столбец с расхождениями зарплат сотрудников от средней зарплаты.
        Сортирует сотрудников по увеличению по столбцу с расхождениями зарплат от средней зарплаты.

        WITH helper2 AS(
                SELECT *, compensation - avg_workload_compensation AS compensation_diff
                FROM
                (SELECT 
                    w.id,
                    w.username,
                    r.compensation,
                    r.workload,
                    avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation	
                FROM resumes r
                JOIN workers w ON r.worker_id = w.id) helper1                
                ORDER BY -(compensation - avg_workload_compensation)
            )
            SELECT * FROM helper2
            ORDER BY compensation_diff
        """
        with sync_session_factory() as session:
            r = aliased(ResumeOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(r, 
                       w,
                       func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label('avg_workload_compensation'),                  
                    )
                    # .select_from(r)
                    .join(r, r.worker_id == w.id).subquery('helper1')
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label('compensation_diff'),
                )
                .cte('helper2')
            )

            query =(
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )
            res = session.execute(query)
            result = res.all()
            print(result)

            # print(query.compile(compile_kwargs={'literal_binds': True}))

    @staticmethod
    def select_workers_with_lazy_relationship():
        with sync_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes))
            )
            res = session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resumes
            print(worker_1_resumes)

            worker_2_resumes = result[1].resumes
            print(worker_2_resumes)

    @staticmethod
    def select_workers_with_joined_relationship():
        """Подходит для one to one и many to one."""
        with sync_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes))
            )
            res = session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resumes
            print(worker_1_resumes)

            worker_2_resumes = result[1].resumes
            print(worker_2_resumes)

    @staticmethod
    def select_workers_with_selectin_relationship():
        """Подходит для one to many и many to many"""
        with sync_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
            )
            res = session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resumes
            print(worker_1_resumes)

            worker_2_resumes = result[1].resumes
            print(worker_2_resumes)

    @staticmethod
    def select_workers_with_condition_relationship():
        with sync_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes_parttime))
            )

            res = session.execute(query)
            result = res.unique().scalars().all()

            print(result)

    @staticmethod
    def select_workers_with_condition_relationship_contains_eager():
        with sync_session_factory() as session:
            query = (
                select(WorkersOrm)
                .join(WorkersOrm.resumes)
                .options(contains_eager(WorkersOrm.resumes))
                .filter(ResumeOrm.workload == 'parttime')
            )

            res = session.execute(query)
            result = res.unique().scalars().all()

            print(result)

    
    

            
       



