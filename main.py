import asyncio
import datetime
import aiohttp
from models import init_db, Session, SW_People
from more_itertools import chunked
from typing import List

CHUNK_SIZE = 10

async def get_person(person_id, session):

    response = await session.get(f'https://swapi.dev/api/people/{person_id}')
    person_data = await response.json()

    return person_data

async def get_deep_url(url, key, session):
    async with session.get(f'{url}') as response:
        result = await response.json()
        return result[key]


async def get_deep_urls(urls, key, session):
    tasks = (asyncio.create_task(get_deep_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def get_deep_data(urls, key, session):
    result_list = []
    async for item in get_deep_urls(urls, key, session):
        result_list.append(item)
        print(result_list)
    return ', '.join(result_list)

async def insert_to_db(person_dict: List[dict]):

    async with Session() as session:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as deep_session:
            for person in person_dict:
                if 'name' in person:
                    homeworld_str = await get_deep_data([person['homeworld']], 'name', deep_session)
                    films_str = await get_deep_data(person['films'], 'title', deep_session)
                    species_str = await get_deep_data(person['species'], 'name', deep_session)
                    starships_str = await get_deep_data(person['starships'], 'name', deep_session)
                    vehicles_str = await get_deep_data(person['vehicles'], 'name', deep_session)
                    people = [SW_People(name=person['name'],
                                        birth_year=person['birth_year'],
                                        eye_color=person['eye_color'],
                                        films=films_str,
                                        gender=person['gender'],
                                        hair_color=person['hair_color'],
                                        height=person['height'],
                                        homeworld=homeworld_str,
                                        mass=person['mass'],
                                        skin_color=person['skin_color'],
                                        species=species_str,
                                        starships=starships_str,
                                        vehicles=vehicles_str)]
                    session.add_all(people)
                    await session.commit()

async def main():
    await init_db()
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    for people_id_chunk in chunked(range(1,100), CHUNK_SIZE):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_db(result))

    await session.close()

    set_of_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*set_of_tasks)

if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now()-start)
