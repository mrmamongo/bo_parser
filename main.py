import argparse
import asyncio
import json

import aiohttp
import asyncpg

from parse_experts import parse_experts
from parse_projects import parse_projects
from parse_reports import parse_reports


async def create_pool(config: dict[str, str]) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )


async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-c', '--config', type=str, default='config.json')
    args = argparser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)
    pool = await create_pool(config)
    async with aiohttp.ClientSession() as session:
        await parse_projects(session, pool)
        await parse_experts(session, pool)
        await parse_reports(session, pool)


if __name__ == '__main__':
    asyncio.run(main())
