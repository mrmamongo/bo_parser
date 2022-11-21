import asyncio
import itertools

import aiohttp
import asyncpg
from bs4 import BeautifulSoup


async def parse_month(session: aiohttp.ClientSession, url: str) -> list:
    async with session.get(url) as response:
        page = BeautifulSoup(await response.text(), 'lxml')
    trs = page.find_all('tr')
    files = [row.find_all('td')[1].a['href'] for row in trs[3:-1]]
    print(files if files != [] else None)
    return [(file, f'{url}/{file}') for file in files if file.endswith(('pdf', 'xlsx', 'xls'))]


async def download_file(session, filename, file, pool):
    async with pool.acquire() as connection:
        async with connection.transaction():
            async with session.get(file) as response:
                if response.status == 200:
                    await connection.execute(
                        'INSERT INTO reports (file_type, comment, broken, file_content) VALUES ($1, $2, $3, $4)',
                        file.split('.')[-1], filename, False, await response.read()
                    )
                else:
                    await connection.execute(
                        'INSERT INTO reports (file_type, comment, broken, file_content) VALUES ($1, $2, $3, $4)',
                        None, None, True, None
                    )
            await connection.execute("DELETE FROM reports WHERE file_type IS NULL")


async def parse_year(year: int, session: aiohttp.ClientSession, url: str, pool: asyncpg.Pool):
    months = list(itertools.chain(*[month for month in
                                    await asyncio.gather(
                                        *[parse_month(session, f'{url}/{year}/{month:02d}') for month in range(1, 13)]) if
                                    month != []]))

    await asyncio.gather(*[download_file(session, filename, file, pool) for filename, file in months])


async def parse_reports(session: aiohttp.ClientSession, pool: asyncpg.Pool):
    url = 'https://www.bf-galchonok.ru/wp-content/uploads'
    for i in range(2020, 2023):
        await asyncio.gather(*[parse_year(i, session, url, pool)])
