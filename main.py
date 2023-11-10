#!/usr/bin/env python3

import abc
import asyncio
import logging
import sys
import typing as t
from logging.handlers import RotatingFileHandler

from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag
from wom import Client, models

#########################################################
# START Configuration
#########################################################

LOG_LEVEL: t.Final[int] = logging.DEBUG
"""The logging level to use, either DEBUG or INFO."""

METRIC_LIMIT: t.Final[t.Optional[int]] = 3
"""The maximum number of metrics to fetch (set to `None` to fetch all).

This can ease testing without waiting for all metrics to be fetched.
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
USER_AGENT: t.Final[str] = "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
"""The User-Agent to send with requests."""
# fmt: on

API_KEY: t.Final[str] = ""
"""API Key for Wise Old Man"""

LEADER_GROUP_NAME: t.Final[str] = "Faab Testing"
"""The name for the group on WOM"""


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
# START WOM Client
#########################################################

client = Client()

if ENABLE_SEASONAL is True: client.set_api_base_url("https://api.wiseoldman.net/league")
else: client.set_api_base_url("https://api.wiseoldman.net/v2")

#########################################################
# END WOM Client
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
    def category(self) -> t.Optional[int]:
        """The category of the metric, it it has one.

        All skills have no category, and everything else is category 1.
        """
        return self._category


class MetricLeader(abc.ABC):
    def __init__(self, metric: Metric, username: str, rank: int) -> None:
        self._username = username
        self._metric = metric
        self._rank = rank

    @property
    def metric(self) -> Metric:
        """The metric this player leads in."""
        return self._metric

    @property
    def username(self) -> str:
        """The username of the player leading."""
        return self._username

    @property
    def rank(self) -> int:
        """The players rank in this metric."""
        return self._rank

    @abc.abstractmethod
    def __str__(self) -> str:
        """Returns a pretty string representing this metric leader."""
        ...


class SkillLeader(MetricLeader):
    def __init__(
        self, metric: Metric, username: str, rank: int, level: int, exp: int
    ) -> None:
        super().__init__(metric, username, rank)
        self._level = level
        self._exp = exp

    @property
    def level(self) -> int:
        """The players level in this skill."""
        return self._level

    @property
    def exp(self) -> int:
        """The players experience in this skill."""
        return self._exp

    def __str__(self) -> str:
        return (
            f"{self.metric.name}: Rank {self.rank} -> {self.username} "
            f"at level {self.level} with {self.exp} xp"
        )


class NonSkillLeader(MetricLeader):
    def __init__(self, metric: Metric, username: str, rank: int, score: int) -> None:
        super().__init__(metric, username, rank)
        self._score = score

    @property
    def score(self) -> int:
        """The players (score/kills/points etc) in this metric."""
        return self._score

    def __str__(self) -> str:
        return f"{self.metric.name}: Rank {self.rank} -> {self.username} with score {self.score}"
    
class Group():
    def __init__(self, name: str, members: list) -> None:
        self._name = name
        self._members = members
        self._count = len(members)
    
    def set_group_details(self, id: int, verification_code: str):
        self._id = id
        self._verification_code = verification_code

    def get_members(self) -> list:
        return self._members

    @property
    def name(self) -> str:
        """The group name as defined in the config"""
        return self._name

    @property
    def count(self) -> int:
        """The amount of leaders"""
        return self._count

    @property
    def id(self) -> id:
        """The ID of the group on Wise Old Man"""
        return self._id
        
    @property
    def verification_code(self) -> str:
        """The verification cope of the group on Wise Old Man"""
        return self._verification_code

    def __str__(self) -> str:
        return f"There are a total of {self.count} leaders in the group called {self.name}"


#########################################################
# END Models
#########################################################

#########################################################
# START Metrics
#########################################################

METRICS: t.Final[t.List[Metric]] = [
    Metric("Overall", 0),
    Metric("Attack", 1),
    Metric("Defence", 2),
    Metric("Strength", 3),
    Metric("Hitpoints", 4),
    Metric("Ranged", 5),
    Metric("Prayer", 6),
    Metric("Magic", 7),
    Metric("Cooking", 8),
    Metric("Woodcutting", 9),
    Metric("Fletching", 10),
    Metric("Fishing", 11),
    Metric("Firemaking", 12),
    Metric("Crafting", 13),
    Metric("Smithing", 14),
    Metric("Mining", 15),
    Metric("Herblore", 16),
    Metric("Agility", 17),
    Metric("Thieving", 18),
    Metric("Slayer", 19),
    Metric("Farming", 20),
    Metric("Runecrafting", 21),  # This is for you Ruben
    Metric("Hunter", 22),
    Metric("Construction", 23),
    Metric("Clue Scolls (all)", 6, 1),
    Metric("Clue Scolls (beginner)", 7, 1),
    Metric("Clue Scolls (easy)", 8, 1),
    Metric("Clue Scolls (medium)", 9, 1),
    Metric("Clue Scolls (hard)", 10, 1),
    Metric("Clue Scolls (elite)", 11, 1),
    Metric("Clue Scolls (master)", 12, 1),
    Metric("LMS - Rank", 13, 1),
    Metric("PVP Arena - Rank", 14, 1),
    Metric("Soul Wars Zeal", 15, 1),
    Metric("Rifts Closed", 16, 1),
    Metric("Abyssal Sire", 17, 1),
    Metric("Alchemical Hydra", 18, 1),
    Metric("Artio", 19, 1),
    Metric("Barrows Chests", 20, 1),
    Metric("Bryophyta", 21, 1),
    Metric("Callisto", 22, 1),
    Metric("Calvar'ion", 23, 1),
    Metric("Cerberus", 24, 1),
    Metric("Chambers of Xeric", 25, 1),
    Metric("Chambers of Xeric: Challenge Mode", 26, 1),
    Metric("Chaos Elemental", 27, 1),
    Metric("Chaos Fanatic", 28, 1),
    Metric("Commander Zilyana", 29, 1),
    Metric("Corporeal Beast", 30, 1),
    Metric("Crazy Archaeologist", 31, 1),
    Metric("Dagganoth Prime", 32, 1),
    Metric("Dagganoth Rex", 33, 1),
    Metric("Dagganoth Supreme", 34, 1),
    Metric("Deranged Archaeologist", 35, 1),
    Metric("Duke Sucellus", 36, 1),
    Metric("General Graardor", 37, 1),
    Metric("Giant Mole", 38, 1),
    Metric("Grotesque Guardians", 39, 1),
    Metric("Hespori", 40, 1),
    Metric("Kalphite Queen", 41, 1),
    Metric("King Black Dragon", 42, 1),
    Metric("Kraken", 43, 1),
    Metric("Kree'arra", 44, 1),
    Metric("K'ril Tsutsaroth", 45, 1),
    Metric("Mimic", 46, 1),
    Metric("Nex", 47, 1),
    Metric("Nightmare", 48, 1),
    Metric("Phosani's Nightmare", 49, 1),
    Metric("Obor", 50, 1),
    Metric("Phantom Muspah", 51, 1),
    Metric("Sarachnis", 52, 1),
    Metric("Scorpia", 53, 1),
    Metric("Skotizo", 54, 1),
    Metric("Spindel", 55, 1),
    Metric("Tempoross", 56, 1),
    Metric("The Gauntlet", 57, 1),
    Metric("The Corrupted Gauntlet", 58, 1),
    Metric("The Leviathan", 59, 1),
    Metric("The Whisperer", 60, 1),
    Metric("Theatre of Blood", 61, 1),
    Metric("Theatre of Blood: Hard Mode", 62, 1),
    Metric("Themonuclear Smoke Devil", 63, 1),
    Metric("Tombs of Amascut", 64, 1),
    Metric("Tombs of Amascut: Expert Mode", 65, 1),
    Metric("TzKal-Zuk", 66, 1),
    Metric("TzTok-Jad", 67, 1),
    Metric("Vardorvis", 68, 1),
    Metric("Venenatis", 69, 1),
    Metric("Vet'ion", 70, 1),
    Metric("Vorkath", 71, 1),
    Metric("Wintertodt", 72, 1),
    Metric("Zalcano", 73, 1),
    Metric("Zulrah", 74, 1),
]

if ENABLE_SEASONAL:
    # Add league points
    METRICS.insert(0, Metric("League Points", 0, 1))
else:
    # Add bounty hunter
    METRICS.extend(
        {
            Metric("Bounty Hunter Hunter", 2, 1),
            Metric("Bounty Hunter Rogue", 3, 1),
            Metric("Bounty Hunter Hunter (Legacy)", 4, 1),
            Metric("Bounty Hunter Rogue (Legacy)", 5, 1),
        }
    )

#########################################################
# END Metrics
#########################################################

#########################################################
# START Utilities
#########################################################


def clean_table_data(data: str) -> str:
    """Strips and removes commas from the strings to integers can be parsed."""
    return data.strip().replace(",", "")


def clean_username(username: str) -> str:
    """Strips and replaces &nbsp; with a space."""
    return username.strip().replace("\\xa0", " ")


def parse_leader(metric: Metric, row: tuple[str, ...]) -> MetricLeader:
    """Parses a metric leader out of a tuple of strings holding the necessary
    data points.
    """
    if metric.category:
        # This is an activity or boss
        rank, username, score = row
        return NonSkillLeader(
            metric,
            clean_username(username),
            int(clean_table_data(rank)),
            int(clean_table_data(score)),
        )
    else:
        # This is a skill
        rank, username, level, exp = row
        return SkillLeader(
            metric,
            clean_username(username),
            int(clean_table_data(rank)),
            int(clean_table_data(level)),
            int(clean_table_data(exp)),
        )


def parse_leaders(metric: Metric, data: Tag) -> t.List[MetricLeader]:
    """Transforms the parsed html table data into a list of metric leaders."""
    # Skills have 4 columns, bosses and activities have 3
    columns = 3 if metric.category else 4

    # Remove empty columns (bad data at the beginning of each table)
    # and group the necessary number of columns based on metric
    rows = zip(*[iter(row.text for row in data if row.text)] * columns)

    # Parse and return the metric leaders
    return [parse_leader(metric, row) for row in rows]


def build_url(metric: Metric) -> str:
    """Builds the URL to use to fetch leaders for the given metric."""
    params = {"category_type": metric.category, "table": metric.table}
    query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    mode = "hiscore_oldschool"

    if ENABLE_SEASONAL:
        mode += "_seasonal"

    return f"{BASE_URL}/m={mode}/overall?{query}"


#########################################################
# END Utilities
#########################################################


async def fetch_leaders(session: ClientSession, metric: Metric) -> t.List[MetricLeader]:
    url = build_url(metric)

    # Fetch the hiscores data for this metric
    response = await session.get(url)
    text = await response.text()

    # Parse the HTML down to the table data
    soup = BeautifulSoup(text, "html.parser")
    table = t.cast(Tag, soup.findChild("table"))
    rows = t.cast(Tag, table.findChildren("td"))

    # Parse and return the metric leaders
    return parse_leaders(metric, rows)

async def createGroup(members: list) -> Group:
    group = Group(LEADER_GROUP_NAME, members)
    LOGGER.debug(f"Group object created with {group.count} members")
    members = group.get_members()
    member_fragments = [models.GroupMemberFragment(username, None) for username in members]
    try:
        result = await client.groups.create_group(group.name, *member_fragments)
    except result.is_err is True:
        error = result.unwrap_err()
        LOGGER.error(error)
        raise ConnectionError("Something went wrong creating the group on Wise Old man, see error above")
    else:
        wom_group = result.unwrap()
        group.set_group_details(wom_group.group.id, wom_group.verification_code)
        LOGGER.info(f"Group created on Wise Old Man with ID {group.id}")
        LOGGER.debug(f"Verification code: {group.verification_code}")
    return group
    
async def updateGroup(group: Group) -> None:
    if hasattr(group, '_verification_code') is False or hasattr(group, '_id') is False:
        LOGGER.error("This group doesn't exist on Wise Old Man!")
    else:
        result = await client.groups.update_outdated_members(group.id, group.verification_code)
        if result.is_err is True:
            error = result.unwrap_err()
            LOGGER.error("Something went wrong updating the group members, see error below")
            LOGGER.error(error)
        else:
            http_result = result.unwrap()
            LOGGER.debug(f"{http_result.status}: {http_result.message}")
            LOGGER.info("All outdated group members have been added to the update queue")

async def deleteGroup(group: Group) -> None:
    if hasattr(group, '_verification_code') is False or hasattr(group, '_id') is False:
        LOGGER.error("This group doesn't exist on Wise Old Man!")
    else: 
        result = await client.groups.delete_group(group.id, group.verification_code)
        if result.is_err is True:
            error = result.unwrap_err()
            LOGGER.error("Something went wrong deleting the group, see error below")
            LOGGER.error(error)
        else:
            http_result = result.unwrap()
            LOGGER.debug(f"{http_result.status}: {http_result.message}")
            LOGGER.info("The group has been deleted")
    
async def main() -> None:
    await client.start()
    LOGGER.info("*" * 64)
    LOGGER.info("WOM Leagues Scraper starting...")
    session = ClientSession(headers={"User-Agent": USER_AGENT})
    metric_limit = METRIC_LIMIT if METRIC_LIMIT else len(METRICS)
    metric_leaders: t.List[MetricLeader] = []
    is_unique: t.Callable[[MetricLeader], bool] = lambda l: not any(
        l.username == m.username for m in metric_leaders
    )

    for i in range(metric_limit):
        metric = METRICS[i]
        LOGGER.info(f"Fetching leaders for {metric}")

        try:
            leaders = await fetch_leaders(session, metric)
            LOGGER.info(f"Found {len(leaders)} leaders for {metric}")

            if LOG_LEVEL <= logging.DEBUG:
                for leader in leaders:
                    LOGGER.debug(leader)

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


    # Get just the usernames from leaders
    leaders_usernames = [leader.username for leader in metric_leaders]
    LOGGER.info("Creating group on Wise Old Man...")
    group = await createGroup(leaders_usernames)
    LOGGER.info("Updating all outdated group members...")
    await updateGroup(group)
    LOGGER.info("Deleting the group from Wise Old Man...")
    await deleteGroup(group)


    await session.close()
    LOGGER.info("Scrape complete, exiting...")
    LOGGER.info("*" * 64)




if __name__ == "__main__":
    asyncio.run(main())
