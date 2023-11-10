# WOM Leagues Scraper

A quick and dirty python script to run periodically to scrape the hiscores and keep
the top players updated on [WOM Trailblazer Reloaded](https://league.wiseoldman.net).

## Setup

Python 3.8 or higher is required to use the `wom.py` dependency. This shouldn't be
an issue as I think ubuntu ships with 3.8 by default.

- Clone the repo if you plan to only run the script
- Fork the repo if you plan to contribute to it

Create a virtual environment and activate it

```bash
$ python -m venv .venv
# ^ this will likely need to be `python3` on ubuntu
$ source .venv/bin/activate
```

Install dependencies

```bash
$ pip install -r requirements.txt
# ^ this will likely need to be `pip3` on ubuntu
```

Read through the constants declared in `main.py` in the `Configuration` section.
Change any values as you see fit for development.

Recommended values for production:

- LOG_LEVEL: DEBUG or INFO depending if you want to log all the metric leaders.
- METRIC_LIMIT: None
- ENABLE_SEASONAL: True
- BASE_URL: Leave this untouched
- DELAY: 60
- USER_AGENT: Leave this untouched

Run the script

```bash
$ python main.py
# ^ Again, likely need to be `python3` on ubuntu
```

## License

WOM Leagues Scraper is licensed under the
[MIT License](https://github.com/Jonxslays/wom-leagues-scraper/blob/master/LICENSE).
