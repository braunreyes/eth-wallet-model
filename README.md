# ETH Wallet Model

A dbt project that turns public Ethereum ERC-20 transfers into a tested dimensional model for wallet investigations: balances, counterparties, and exposure to sanctioned wallets **at the time of each transfer**.

## Quickstart

```bash
make install
make extract          # 7 days from the AWS public bucket into data/raw
make build            # dbt build on DuckDB; every test must pass
```

CI runs `make ci-build` against a committed 1-day fixture in `tests/fixtures/raw/`.

## Lineage

Published at M5.

## Grain of Each Core Model

Written by the author at M5.

## Decisions and Tradeoffs

Written by the author as each milestone lands.

## What Changes at 100×

Written by the author at M5.
