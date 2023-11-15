#!/usr/bin/env python3

import asyncio
from aiohttp import ClientSession
import typing as t
import json
from pathlib import Path
from main import fetch_leaders, BROWSER_USER_AGENT, DELAY, METRICS, LOGGER, Metric, MetricLeader, submit_updates

#########################################################
# START Configuration
#########################################################

PAGE_SKIP = 1_000
""" The amount of pages to skip when searching for new bounds """

MAX_PAGE = 3_000
""" The absolute last possible page of any metric """

LAST_PAGES_FILE = "last_pages.json"
""" The file where all previous last pages are stored """

#########################################################
# END Configuration
#########################################################


async def binary_search(session: ClientSession, metric: Metric, low: int = 1, high: int = MAX_PAGE) -> t.Tuple[int, str]:
    """ Finds the last page and player for a hiscore metric using binary search """
    _low = low
    _high = high
    last_page_with_data = low
    last_player: MetricLeader

    while _low <= _high:
        mid = (_low + _high) // 2
        board = await fetch_leaders(session, metric, mid)

        if board[0].rank != 1:
            last_page_with_data = mid
            last_player = board[-1]
            _low = mid + 1
        else:
            _high = mid - 1

        await asyncio.sleep(DELAY)

    return last_page_with_data, last_player


async def new_bounds(session: ClientSession, metric: Metric, low: int) -> t.Tuple[int, int]:
    """ Finds new bounds to search within by using yesterday's bounds """

    new_high = low + PAGE_SKIP
    new_low = low
    while True:
        board = await fetch_leaders(session, metric, new_high)

        if board[0].rank == 1:
            return (new_low, new_high)
        else:
            new_high += PAGE_SKIP
            new_low += PAGE_SKIP


async def find_last_players(session: ClientSession) -> t.List[MetricLeader]:
    if (Path(LAST_PAGES_FILE).is_file()):
        with open(LAST_PAGES_FILE, "r") as f:
            last_pages = json.load(f)
    else:
        last_pages = {}

    # The players ranked last in each metric
    last_players: t.List[MetricLeader] = []

    for metric in METRICS:
        LOGGER.info(f"Finding last player for {metric.name}.")

        if metric.name in last_pages:
            last_page = last_pages[metric.name]
            if last_page == MAX_PAGE:
                last_page_with_data, last_player = await binary_search(session, metric, last_page)
                last_players.append(last_player)
                continue

            low, high = await new_bounds(session, metric, last_page)

            last_page_with_data, last_player = await binary_search(session, metric, low, high)
        else:
            last_page_with_data, last_player = await binary_search(session, metric)

        last_players.append(last_player)
        last_pages[metric.name] = last_page_with_data
        LOGGER.info(f"Found last page of {metric.name} at page {last_page_with_data} and last player: {last_player.username}.")

    # Write last pages to file for use at next scrape
    data = json.dumps(last_pages, indent=4)
    with open(LAST_PAGES_FILE, "w") as f:
        LOGGER.info(f"Writing last pages to file...")
        f.write(data)

    return last_players


async def main() -> None:
    LOGGER.info("*" * 64)
    LOGGER.info("WOM Leagues Last Page Scraper starting...")

    session = ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
    last_players = await find_last_players(session)
    await session.close()
    
    LOGGER.info("Scrape complete")

    await submit_updates(last_players)
    LOGGER.info("*" * 64)
    


if __name__ == "__main__":
    asyncio.run(main())
