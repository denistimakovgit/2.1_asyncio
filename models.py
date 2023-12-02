import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Text, Float

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "1234")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "asyncio_test")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_DSN = f'postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SW_People(Base):
    __tablename__ = "sw_people"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    birth_year: Mapped[str] = mapped_column(String(50))
    eye_color: Mapped[str] = mapped_column(String(50))
    films: Mapped[str] = mapped_column(String)
    gender: Mapped[str] = mapped_column(String(50))
    hair_color: Mapped[str] = mapped_column(String(50))
    height: Mapped[float] = mapped_column(String(50))
    homeworld: Mapped[str] = mapped_column(String)
    mass: Mapped[float] = mapped_column(String(50))
    skin_color: Mapped[str] = mapped_column(String(50))
    species: Mapped[str] = mapped_column(String)
    starships: Mapped[str] = mapped_column(String)
    vehicles: Mapped[str] = mapped_column(String)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)