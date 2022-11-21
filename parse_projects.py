import asyncio

import aiohttp
import asyncpg.connection
from bs4 import BeautifulSoup


async def parse_project(session: aiohttp.ClientSession, url: str, pool: asyncpg.Pool):
    async with session.get(url, timeout=20) as response:
        page = BeautifulSoup(await response.text(), 'lxml')
        name = page.find('div', {'class': 'vadims-project'}).header.h1.text
        m = page.find('main').find('div', {'class': 'block'})
        target = 'NULL'
        trends = "NULL"
        if m is not None:
            if 'project1' not in url:
                target = m.p.text
            else:
                text_start = m.text.find('Цель программы:')
                text_end = m.text.find('Программа состоит из направлений:')
                target = (m.text[text_start + len('Цель программы:'):text_end].strip())
            trends = [i.text for i in m.find_all('li') if not i.has_attr('class')]
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                'INSERT INTO projects (name, url, target, trends) VALUES ($1, $2, $3, $4)',
                name, url, target, trends
            )


async def parse_projects(session: aiohttp.ClientSession, pool: asyncpg.Pool):
    url = 'https://www.bf-galchonok.ru/projects/'
    async with session.get(url) as response:
        page = BeautifulSoup(await response.text(), 'lxml')
    projects = [url.a['href'] for url in page.find_all('article', {'class': ['project', 'type-project']})]
    await asyncio.gather(*[parse_project(session, project, pool) for project in projects])
