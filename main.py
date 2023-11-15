#!/usr/bin/env python3

import abc
import asyncio
import logging
import secrets
import sys
import typing as t
from logging.handlers import RotatingFileHandler

import wom
from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag

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


class Group:
    def __init__(self, client: wom.Client, details: wom.GroupDetail) -> None:
        self._details = details
        self._client = client

    @property
    def members(self) -> t.List[wom.GroupMembership]:
        """The members of this group."""
        return self._details.memberships

    @property
    def name(self) -> str:
        """The group name."""
        return self._details.group.name

    @property
    def count(self) -> int:
        """The amount of leaders in the group."""
        return len(self._details.memberships)

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
    async def create(cls, client: wom.Client, members: t.List[MetricLeader]) -> "Group":
        """Creates the group on WOM."""
        LOGGER.info("Creating group")
        result = await client.groups.create_group(
            LEADER_GROUP_NAME, *(wom.GroupMemberFragment(m.username) for m in members)
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


def parse_leader(metric: Metric, row: t.Tuple[str, ...]) -> MetricLeader:
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


def build_url(metric: Metric, page: int) -> str:
    """Builds the URL to use to fetch leaders for the given metric."""
    params = {"category_type": metric.category, "table": metric.table, "page": page}
    query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    mode = "hiscore_oldschool"

    if ENABLE_SEASONAL:
        mode += "_seasonal"

    return f"{BASE_URL}/m={mode}/overall?{query}"


#########################################################
# END Utilities
#########################################################


async def fetch_leaders(session: ClientSession, metric: Metric, page: int = 1) -> t.List[MetricLeader]:
    url = build_url(metric, page)

    # Fetch the hiscores data for this metric
    response = await session.get(url)
    text = await response.text()

    # Parse the HTML down to the table data
    soup = BeautifulSoup(text, "html.parser")
    table = t.cast(Tag, soup.findChild("table"))
    rows = t.cast(Tag, table.findChildren("td"))

    # Parse and return the metric leaders
    return parse_leaders(metric, rows)


async def fetch_all_leaders(session: ClientSession) -> t.List[MetricLeader]:
    metric_limit = METRIC_LIMIT if METRIC_LIMIT else len(METRICS)
    metric_leaders: t.List[MetricLeader] = []
    is_unique: t.Callable[[MetricLeader], bool] = lambda l: not any(
        l.username == m.username for m in metric_leaders
    )

    for i in range(metric_limit):
        metric = METRICS[i]
        LOGGER.info(f"Fetching leaders for {metric}")

        try:
            # Fetch leaders in this metric
            leaders = await fetch_leaders(session, metric)
            LOGGER.info(f"Found {len(leaders)} leaders for {metric}")

            if LOG_LEVEL <= logging.DEBUG:
                for leader in leaders:
                    LOGGER.debug(leader)

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


async def submit_updates(leaders: t.List[MetricLeader]) -> None:
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
