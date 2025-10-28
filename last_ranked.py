#!/usr/bin/env python3

import asyncio
from aiohttp import ClientSession
import typing as t
import json
from pathlib import Path
from main import fetch_hiscore_players, BROWSER_USER_AGENT, DELAY, LOGGER, PLAYERS_PER_PAGE, Metric, HiscorePlayer, submit_updates

#########################################################
# START Configuration
#########################################################

RANK_SKIP = 25_000
""" The amount of ranks to skip when searching for new bounds """

MAX_RANK = 2_000_000
""" The absolute max rank of any metric """

LAST_RANKS_FILE = "last_ranks.json"
""" The file where all previous last ranks are stored """

#########################################################
# END Configuration
#########################################################


NOT_ALL_METRICS: t.Final[t.List[Metric]] = [
  Metric("Bryophyta", 26, 1) # TODO: Change this to sailing when it is released
]


async def binary_search(session: ClientSession, metric: Metric, low: int = 0, high: int = MAX_RANK) -> t.Tuple[int, HiscorePlayer]:
    """ Finds the last rank and player for a hiscore metric using binary search """
    _low = low
    _high = high
    last_player: HiscorePlayer

    while _low <= _high:
        mid = (_low + _high) // 2
        board = await fetch_hiscore_players(session, metric, mid)

        # If the fetch didn't result in a 404
        if board:
            last_player = board[-1]
            _low = mid + 1

            # If the result contains less players than the amount of players per page
            # we requested, it means we are on the last page.
            if len(board) < PLAYERS_PER_PAGE:
                break
        else:
            _high = mid - 1

        await asyncio.sleep(DELAY)

    return last_player


async def new_bounds(session: ClientSession, metric: Metric, low: int) -> t.Tuple[int, int]:
    """ Finds new bounds to search within by using yesterday's bounds """

    new_high = low + RANK_SKIP
    new_low = low
    while True:
        board = await fetch_hiscore_players(session, metric, new_high)

        if not board:
            return (new_low, new_high)
        else:
            new_high += RANK_SKIP
            new_low += RANK_SKIP


async def find_last_players(session: ClientSession) -> t.List[HiscorePlayer]:
    if (Path(LAST_RANKS_FILE).is_file()):
        with open(LAST_RANKS_FILE, "r") as f:
            last_ranks = json.load(f)
    else:
        last_ranks = {}

    # The players ranked last in each metric
    last_players: t.List[HiscorePlayer] = []

    for metric in NOT_ALL_METRICS:
        LOGGER.info(f"Finding last player for {metric.name}.")

        if metric.name in last_ranks:
            last_rank = last_ranks[metric.name]
            if last_rank == MAX_RANK:
                last_player = await binary_search(session, metric, last_rank)
                last_players.append(last_player)
                continue

            low, high = await new_bounds(session, metric, last_rank)

            last_player = await binary_search(session, metric, low, high)
        else:
            last_player = await binary_search(session, metric)

        last_players.append(last_player)
        last_ranks[metric.name] = last_player.rank
        LOGGER.info(
            f"Found last ranked player, {last_player.name}, for {metric.name} on rank {last_player.rank}.")

    # Write last ranks to file for use at next scrape
    data = json.dumps(last_ranks, indent=4)
    with open(LAST_RANKS_FILE, "w") as f:
        LOGGER.info(f"Writing last ranks to file...")
        f.write(data)

    return last_players


async def main() -> None:
    LOGGER.info("*" * 64)
    LOGGER.info("WOM Leagues Last Rank Finder starting...")

    session = ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
    last_players = await find_last_players(session)
    await session.close()

    LOGGER.info("Last rank finder complete")

    await submit_updates(last_players)
    LOGGER.info("*" * 64)


if __name__ == "__main__":
    asyncio.run(main())
