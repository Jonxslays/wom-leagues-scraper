#!/usr/bin/env python3

import asyncio
import logging
import secrets
import sys
import typing as t
from logging.handlers import RotatingFileHandler

import wom
from aiohttp import ClientSession

#########################################################
# START Configuration
#########################################################

LOG_LEVEL: t.Final[int] = logging.DEBUG
"""The logging level to use, either DEBUG or INFO."""

METRIC_LIMIT: t.Final[t.Optional[int]] = 3
"""The maximum number of metrics to fetch (set to `None` to fetch all).

This can ease testing without waiting for all metrics to be fetched.
"""

PLAYERS_PER_PAGE: t.Final[int] = 50
"""The amount of players to fetch per request.

Maximum is 50.
"""

ENABLE_SEASONAL: t.Final[bool] = False
"""If `True`, fetch from the leagues hiscores, otherwise use the regular hiscores.

Useful to disable while testing before leagues is released.
"""

BASE_URL: t.Final[str] = "https://secure.runescape.com"
"""The runescape hiscores base url."""

DELAY: t.Final[int] = 5
"""The number of seconds to delay between requests to the hiscores."""

# fmt: off
BROWSER_USER_AGENT: t.Final[str] = "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
"""The user agent to send with requests to the hiscores."""
# fmt: on

WOM_API_KEY: t.Final[t.Optional[str]] = None
"""The optional API Key for WOM."""

LEADER_GROUP_NAME: t.Final[str] = f"League Leaders {secrets.token_hex(4)}"
"""The name for the temporary group on WOM.

Use secrets.token_hex to ensure the group name is not taken already.
"""

WOM_USER_AGENT: t.Final[str] = "WOM Leagues Scraper"
"""The user agent to send with requests to the WOM API."""

#########################################################
# END Configuration
#########################################################

#########################################################
# START Logging
#########################################################


def setup_logging() -> logging.Logger:
    """Sets up and returns the logger to use in the script."""
    logger = logging.getLogger(__file__)
    logger.setLevel(LOG_LEVEL)

    sh = logging.StreamHandler(sys.stdout)
    rfh = RotatingFileHandler(
        "./wom-league-scraper.log",
        maxBytes=1048576,  # 1MB
        encoding="utf-8",
        backupCount=20,
    )

    ff = logging.Formatter(
        f"[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    rfh.setFormatter(ff)
    sh.setFormatter(ff)
    logger.addHandler(rfh)
    logger.addHandler(sh)
    return logger


LOGGER: t.Final[logging.Logger] = setup_logging()

#########################################################
# END Logging
#########################################################

#########################################################
# START Models
#########################################################


class Metric:
    def __init__(self, name: str, table: int, category: t.Optional[int] = None) -> None:
        self._name = name
        self._table = table
        self._category = category

    def __str__(self) -> str:
        return self._name

    @property
    def name(self) -> str:
        """The name of the metric."""
        return self._name

    @property
    def table(self) -> int:
        """The table number for the metric."""
        return self._table

    @property
    def category(self) -> int:
        """The category of the metric, it it has one.

        All skills have no category, and everything else is category 1.
        """
        return self._category
    
class HiscorePlayer:
    def __init__(self, name: str, score: int, rank: int) -> None:
        self._name = name
        self._score = score
        self._rank = rank

    def __str__(self) -> str:
        return self._name

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def score(self) -> int:
        return self._score
    
    @property
    def rank(self) -> int:
        return self._rank

class Group:
    def __init__(self, client: wom.Client, details: wom.GroupDetail) -> None:
        self._details = details
        self._client = client

    @property
    def members(self) -> t.List[wom.GroupMembership]:
        """The members of this group."""
        return self._details.group.memberships

    @property
    def name(self) -> str:
        """The group name."""
        return self._details.group.name

    @property
    def count(self) -> int:
        """The amount of leaders in the group."""
        return len(self._details.group.memberships)

    @property
    def id(self) -> int:
        """The ID of the group on WOM."""
        return self._details.group.id

    @property
    def verification_code(self) -> str:
        """The verification cope of the group on WOM."""
        return t.cast(str, self._details.verification_code)

    def __str__(self) -> str:
        return f"WOM Group {self.name} (id: {self.id}) with {self.count} members"

    @classmethod
    async def create(cls, client: wom.Client, members: t.List[HiscorePlayer]) -> "Group":
        """Creates the group on WOM."""
        LOGGER.info("Creating group")
        result = await client.groups.create_group(
            LEADER_GROUP_NAME, *(wom.GroupMemberFragment(m.name) for m in members)
        )

        if result.is_err:
            LOGGER.error(result.unwrap_err())
            raise wom.WomError("Exiting due to previous error")

        group = cls(client, result.unwrap())
        LOGGER.info(f"Created new {group}")
        LOGGER.debug(f"Verification code: {group.verification_code}")
        return group

    async def update(self) -> None:
        """Updates the group on WOM."""
        LOGGER.info("Updating group members")
        result = await self._client.groups.update_outdated_members(
            self.id, self.verification_code
        )

        if result.is_err:
            err = result.unwrap_err()

            if "no outdated members" in err.message:
                # If all participants are up to date WOM will return HTTP 400
                # Any members that were previously untracked will have an update
                # automatically run when the group is created with them as a member
                # This is relevant because we just created this group, so we may
                # have already sent an update request for all members at that time
                LOGGER.info(err.message)
                return None
            else:
                # Something else has gone wrong
                # We don't raise an error here so the group still gets deleted
                LOGGER.error(err)
                return None

        LOGGER.info(result.unwrap().message)

    async def delete(self) -> None:
        """Deletes the group from WOM."""
        LOGGER.info("Deleting group")
        result = await self._client.groups.delete_group(self.id, self.verification_code)

        if result.is_err:
            LOGGER.error(result.unwrap_err())
            raise wom.WomError(
                f"Group deletion failed, investigate group id: {self.id}"
            )

        LOGGER.info(f"Group deleted successfully")


#########################################################
# END Models
#########################################################

#########################################################
# START Metrics
#########################################################

METRICS: t.Final[t.List[Metric]] = [
    Metric("Overall", 0, 0),
    Metric("Attack", 1, 0),
    Metric("Defence", 2, 0),
    Metric("Strength", 3, 0),
    Metric("Hitpoints", 4, 0),
    Metric("Ranged", 5, 0),
    Metric("Prayer", 6, 0),
    Metric("Magic", 7, 0),
    Metric("Cooking", 8, 0),
    Metric("Woodcutting", 9, 0),
    Metric("Fletching", 10, 0),
    Metric("Fishing", 11, 0),
    Metric("Firemaking", 12, 0),
    Metric("Crafting", 13, 0),
    Metric("Smithing", 14, 0),
    Metric("Mining", 15, 0),
    Metric("Herblore", 16, 0),
    Metric("Agility", 17, 0),
    Metric("Thieving", 18, 0),
    Metric("Slayer", 19, 0),
    Metric("Farming", 20, 0),
    Metric("Runecrafting", 21, 0),  # This is for you Ruben
    Metric("Hunter", 22, 0),
    Metric("Construction", 23, 0),
    Metric("Clue Scolls (all)", 7, 1),
    Metric("Clue Scolls (beginner)", 8, 1),
    Metric("Clue Scolls (easy)", 9, 1),
    Metric("Clue Scolls (medium)", 10, 1),
    Metric("Clue Scolls (hard)", 11, 1),
    Metric("Clue Scolls (elite)", 12, 1),
    Metric("Clue Scolls (master)", 13, 1),
    Metric("LMS - Rank", 14, 1),
    Metric("PVP Arena - Rank", 15, 1),
    Metric("Soul Wars Zeal", 16, 1),
    Metric("Rifts Closed", 17, 1),
    Metric("Colosseum Glory", 18, 1),
    Metric("Collections Logged", 19, 1),
    Metric("Abyssal Sire", 20, 1),
    Metric("Alchemical Hydra", 21, 1),
    Metric("Amoxliatl", 22, 1),
    Metric("Araxxor", 23, 1),
    Metric("Artio", 24, 1),
    Metric("Barrows Chests", 25, 1),
    Metric("Bryophyta", 26, 1),
    Metric("Callisto", 27, 1),
    Metric("Calvar'ion", 28, 1),
    Metric("Cerberus", 29, 1),
    Metric("Chambers of Xeric", 30, 1),
    Metric("Chambers of Xeric: Challenge Mode", 31, 1),
    Metric("Chaos Elemental", 32, 1),
    Metric("Chaos Fanatic", 33, 1),
    Metric("Commander Zilyana", 34, 1),
    Metric("Corporeal Beast", 35, 1),
    Metric("Crazy Archaeologist", 36, 1),
    Metric("Dagganoth Prime", 37, 1),
    Metric("Dagganoth Rex", 38, 1),
    Metric("Dagganoth Supreme", 39, 1),
    Metric("Deranged Archaeologist", 40, 1),
    Metric("Doom of Mokhaiotl", 41, 1),
    Metric("Duke Sucellus", 42, 1),
    Metric("General Graardor", 43, 1),
    Metric("Giant Mole", 44, 1),
    Metric("Grotesque Guardians", 45, 1),
    Metric("Hespori", 46, 1),
    Metric("Kalphite Queen", 47, 1),
    Metric("King Black Dragon", 48, 1),
    Metric("Kraken", 49, 1),
    Metric("Kree'arra", 50, 1),
    Metric("K'ril Tsutsaroth", 51, 1),
    Metric("Lunar Chests", 52, 1),
    Metric("Mimic", 53, 1),
    Metric("Nex", 54, 1),
    Metric("Nightmare", 55, 1),
    Metric("Phosani's Nightmare", 56, 1),
    Metric("Obor", 57, 1),
    Metric("Phantom Muspah", 58, 1),
    Metric("Sarachnis", 59, 1),
    Metric("Scorpia", 60, 1),
    Metric("Scurrius", 61,),
    Metric("Skotizo", 62, 1),
    Metric("Sol Heredit", 63, 1),
    Metric("Spindel", 64, 1),
    Metric("Tempoross", 65, 1),
    Metric("The Gauntlet", 66, 1),
    Metric("The Corrupted Gauntlet", 67, 1),
    Metric("The Hueycoatl", 68, 1),
    Metric("The Leviathan", 69, 1),
    Metric("The Royal Titans", 70, 1),
    Metric("The Whisperer", 71, 1),
    Metric("Theatre of Blood", 72, 1),
    Metric("Theatre of Blood: Hard Mode", 73, 1),
    Metric("Themonuclear Smoke Devil", 74, 1),
    Metric("Tombs of Amascut", 75, 1),
    Metric("Tombs of Amascut: Expert Mode", 76, 1),
    Metric("TzKal-Zuk", 77, 1),
    Metric("TzTok-Jad", 78, 1),
    Metric("Vardorvis", 79, 1),
    Metric("Venenatis", 80, 1),
    Metric("Vet'ion", 81, 1),
    Metric("Vorkath", 82, 1),
    Metric("Wintertodt", 83, 1),
    Metric("Yama", 84, 1),
    Metric("Zalcano", 85, 1),
    Metric("Zulrah", 86, 1),
]

if ENABLE_SEASONAL:
    # Add league points
    METRICS.insert(0, Metric("League Points", 1, 1))
else:
    # Add bounty hunter
    METRICS.extend(
        {
            Metric("Bounty Hunter Hunter", 3, 1),
            Metric("Bounty Hunter Rogue", 4, 1),
            Metric("Bounty Hunter Hunter (Legacy)", 5, 1),
            Metric("Bounty Hunter Rogue (Legacy)", 6, 1),
        }
    )

#########################################################
# END Metrics
#########################################################

#########################################################
# START Utilities
#########################################################

def build_url(metric: Metric, rank: int) -> str:
    """Builds the URL to use to fetch leaders for the given metric."""
    params = {"category": metric.category, "table": metric.table, "size": PLAYERS_PER_PAGE, "toprank": rank}
    query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    mode = "hiscore_oldschool"

    if ENABLE_SEASONAL:
        mode += "_seasonal"

    return f"{BASE_URL}/m={mode}/ranking.json?{query}"

#########################################################
# END Utilities
#########################################################

async def fetch_hiscore_players(session: ClientSession, metric: Metric, rank: int = 0) -> t.List[HiscorePlayer]:
    url = build_url(metric, rank)

    response = await session.get(url)
    if response.status != 200:
        return
    
    json_data = await response.json()
    hiscores_players = [
        HiscorePlayer(
            name=player_data["name"],
            score=int(player_data["score"].replace(',', '')),
            rank=int(player_data["rank"].replace(',', ''))
        )
        for player_data in json_data
    ] 

    return hiscores_players

async def fetch_all_leaders(session: ClientSession) -> t.List[HiscorePlayer]:
    metric_limit = METRIC_LIMIT if METRIC_LIMIT else len(METRICS)
    metric_leaders: t.List[HiscorePlayer] = []
    is_unique: t.Callable[[HiscorePlayer], bool] = lambda l: not any(
        l.name == m.name for m in metric_leaders
    )

    for i in range(metric_limit):
        metric = METRICS[i]
        LOGGER.info(f"Fetching leaders for {metric}")

        try:
            # Fetch leaders in this metric
            leaders = await fetch_hiscore_players(session, metric)
            LOGGER.info(f"Found {len(leaders)} leaders for {metric}")

            if LOG_LEVEL <= logging.DEBUG:
                LOGGER.debug([leader.name for leader in leaders])

            # Filter out players we have already seen
            leaders = list(filter(is_unique, leaders))
            LOGGER.info(f"Of those, {len(leaders)} were unique")

            metric_leaders.extend(leaders)
        except Exception as e:
            LOGGER.error(e)
            LOGGER.error(f"Failed to parse leaders for {metric} due to previous error")
        finally:
            if i < metric_limit - 1:
                # Dont sleep on the final iteration
                LOGGER.info(f"Sleeping for {DELAY} seconds...")
                await asyncio.sleep(DELAY)

    return metric_leaders


async def submit_updates(leaders: t.List[HiscorePlayer]) -> None:
    client = wom.Client(user_agent=WOM_USER_AGENT)
    await client.start()

    if WOM_API_KEY:
        client.set_api_key(WOM_API_KEY)

    if ENABLE_SEASONAL:
        client.set_api_base_url("https://api.wiseoldman.net/league")

    try:
        # Create the group
        group = await Group.create(client, leaders)
        await asyncio.sleep(1)

        # Update the group members
        await group.update()
        await asyncio.sleep(1)

        # Delete the group
        await group.delete()
    except Exception as e:
        LOGGER.error(e)
    finally:
        await client.close()


async def main() -> None:
    LOGGER.info("*" * 64)
    LOGGER.info("WOM Leagues Scraper starting...")

    session = ClientSession(headers={"User-Agent": BROWSER_USER_AGENT})
    leaders = await fetch_all_leaders(session)
    await session.close()
    LOGGER.info("Scrape complete")

    await submit_updates(leaders)
    LOGGER.info("*" * 64)


if __name__ == "__main__":
    asyncio.run(main())
