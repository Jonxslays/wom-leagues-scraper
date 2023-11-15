import asyncio
from aiohttp import ClientSession
import typing as t
import json
from pathlib import Path
from main import fetch_leaders, BROWSER_USER_AGENT, DELAY, Metric, METRICS

#########################################################
# START Configuration
#########################################################

PAGE_SKIP = 1_000
""" The amount of pages to skip when searching for new bounds """

MAX_PAGE = 80_000
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
    last_player = ""

    while _low <= _high:
        mid = (_low + _high) // 2
        print(f"Scraping page {mid}")
        board = await fetch_leaders(session, metric, mid)

        if board[0].rank != 1:
            last_page_with_data = mid
            last_player = board[-1].username
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


async def find_last_page(session: ClientSession) -> int:
    if (Path(LAST_PAGES_FILE).is_file()):
        with open("last_pages.json", "r") as f:
            last_pages = json.load(f)
    else:
        last_pages = {}

    # The players ranked last in each metric
    last_players = []
    for metric in METRICS:
        print(f"Finding the last page for: {metric}")

        if metric.name in last_pages:
            last_page = last_pages[metric.name]
            if last_page == MAX_PAGE:
                last_page_with_data, last_player = await binary_search(session, metric, last_page)
                last_players.append(last_player)
                continue

            print(f"previous_last_page: {last_page}")
            low, high = await new_bounds(session, metric, last_page)
            print(f"Found new bounds: low: {low}, high: {high}")

            last_page_with_data, last_player = await binary_search(session, metric, low, high)
        else:
            last_page_with_data, last_player = await binary_search(session, metric)

        print(last_page_with_data, last_player)
        # TODO: Save all the last_players, create a group with them and update them all
        last_page = last_page_with_data

    data = json.dumps(last_pages, indent=4)
    with open(LAST_PAGES_FILE, "w") as f:
        f.write(data)


async def main() -> None:

    session = ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
    await find_last_page(session)
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
