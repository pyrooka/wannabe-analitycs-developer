#!/usr/bin/env python3

import os
from datetime import datetime
from argparse import ArgumentParser

from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey


BaseSource = declarative_base()
BaseTarget = declarative_base()

class Liligo(BaseSource):
    __tablename__ = "liligo"

    index = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    metric = Column(String)

class Metrics(BaseTarget):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True)
    name = Column(String)

class Events(BaseTarget):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    day_last = Column(DateTime)
    count = Column(Integer)
    metric_id = Column(Integer, ForeignKey("metrics.id"))

    metric = relationship("Metrics")

class Data:
    def __init__(self, last: DateTime):
        self._count = 1
        self._last = last

    def increase(self, when: DateTime):
        """Increase the inner counter by 1 and set the new datetime as the last one if it's newer.

        Args:
            last (DateTime): last datetime value
        """

        self._count += 1

        if self._last < when:
            self._last = when

    def get(self) -> tuple:
        """Returns the count and last values.

        Returns:
            tuple: (count, last)
        """

        return (self._count, self._last)

def get_or_create_metric(session: Session, metric: str) -> int:
    """Returns the ID of the given metric. If not exists create it.

    Args:
        session (Session): current DB session
        metric (str): name of the metric

    Returns:
        int: ID of the metric
    """

    mid = session.query(Metrics.id).filter(Metrics.name==metric).first()
    if mid is not None:
        return mid[0]

    session.add(Metrics(name=metric))

    return session.query(Metrics.id).filter(Metrics.name==metric).first()[0]

def main():
    # Parse the CLI arguments.
    parser = ArgumentParser(description="Wanna be an analytics developer.")
    parser.add_argument("-d", "--debug", action="store_true", help="show SQL logs")
    args = parser.parse_args()

    # Get the URLs for the DB connections.
    source_db_url = os.getenv("SOURCE_DB_URL", default="mysql+pymysql://liligo:liligo@localhost:3306/liligo")
    target_db_url = os.getenv("TARGET_DB_URL", default="postgresql://liligo:liligo@localhost:5432/liligo")

    # Add pymysql driver to the MySQL DB URL if necessary.
    source_db_url = source_db_url if "mysql+pymysql://" in source_db_url else source_db_url.replace("mysql://", "mysql+pymysql://")

    # Create the DB engines.
    engine_source = create_engine(source_db_url, echo=args.debug)
    engine_target = create_engine(target_db_url, echo=args.debug)

    # Create the tables in the target DB.
    try:
        BaseTarget.metadata.create_all(engine_target, checkfirst=True)
    except Exception as e:
        print("Cannot create the tables in the target DB.", e)
        return

    # Create sessions.
    SessionSource = sessionmaker(bind=engine_source)
    SessionTarget = sessionmaker(bind=engine_target)
    session_source = SessionSource()
    session_target = SessionTarget()

    # Get last events datetime for each metrics in the target DB.
    res = session_target.query(Metrics.name, func.max(Events.day_last)).filter(Events.metric_id==Metrics.id).group_by(Metrics.name).all()
    current = {}
    for row in res:
        current[row[0]] = row[1]

    # Process the records from the source database.
    # For more CPU intensive tasks we should use multiprocessing with queue.
    processed = {}
    for i in session_source.query(Liligo).yield_per(10000):
        date = i.timestamp.date()

        last = current.get(i.metric)
        if last is not None and last >= i.timestamp:
            continue

        if (i.metric, date) not in processed:
            processed[(i.metric, date)] = Data(i.timestamp)
            continue

        processed[(i.metric, date)].increase(i.timestamp)

    # Create the new records in the target DB.
    new_records = []
    for key, value in processed.items():
        mid = get_or_create_metric(session_target, key[0])

        count, last = value.get()

        new_records.append(
            Events(count=count, day_last=last, metric_id=mid)
        )

    # Save the new modifications to the DB.
    session_target.add_all(new_records)
    session_target.commit()

    # Close the sessions.
    session_source.close()
    session_target.close()

    print("DONE")


if __name__ == "__main__":
    main()
