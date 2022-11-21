import re
from collections import namedtuple

import aiohttp
import asyncpg
from bs4 import BeautifulSoup

Expert = namedtuple('Expert', ['name', 'position', 'cv'])

pattern = re.compile(r'[\W_.,\s]+')


async def parse_expert(soup: BeautifulSoup) -> Expert:
    name = soup.find('p', {'class': 'expert__full-name'}).text
    position = pattern.sub(' ', soup.find('p', {'class': 'expert__position'}).text).strip()
    cv = pattern.sub(' ', soup.find('div', {'class': 'expert__description'}).text).strip()
    return Expert(name, position, cv)


async def parse_experts(session: aiohttp.ClientSession, pool: asyncpg.Pool):
    url = 'https://www.bf-galchonok.ru/experts/'
    async with session.get(url) as response:
        page = BeautifulSoup(await response.text(), 'lxml')

    experts = page.find_all('div', {'class': 'expert__content'})
    async with pool.acquire() as connection:
        async with connection.transaction():
            for expert in experts:
                expert = await parse_expert(expert)
                await connection.execute(
                    'INSERT INTO experts (name, position, cv) VALUES ($1, $2, $3)',
                    expert.name, expert.position, expert.cv
                )
