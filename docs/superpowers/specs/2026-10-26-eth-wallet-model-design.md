# ETH Wallet Model: v1 Design

**Date:** 2026-10-26 · **Status:** draft for review

## Goal

A portfolio dbt project that turns public Ethereum token transfers into a tested dimensional model for wallet investigations: balances, counterparties, and exposure to sanctioned wallets **at the time of each transfer**. Built for data-infra applications (TRM Labs, Block, Cohere, Netflix).

What it shows: stating the grain, exact money handling when the source has lost precision, SCD2 with point-in-time joins, incremental models that survive chain reorgs, and data quality as part of the design.

## Non-Goals (v1)

- Orchestration (Airflow or Dagster). v1 runs through `make` targets
- USD prices, NFTs (ERC-721), native ETH transfers, internal transfers (`traces`)
- Running BigQuery in CI. The BigQuery profile must compile and is run by hand

## Success Criteria

1. `make extract && make build` on a clean laptop: extracts the default date range, runs `dbt build` on DuckDB, and every test passes.
2. CI is green on every PR: lint, then `dbt build` on a committed 1-day fixture.
3. `dbt compile --target bigquery` succeeds, and the README shows one hand-run BigQuery build.
4. `dbt docs` is published to GitHub Pages, and the README links to the lineage graph.
5. The README has a decisions and tradeoffs section, the grain of every core model, and a "what changes at 100×" section.
6. The author can explain every model and test without notes.

## Source Data

`s3://aws-public-blockchain/v1.0/eth/`: anonymous Parquet in `us-east-2`, Hive-partitioned by `date=YYYY-MM-DD`. A recent day has about 3.5M token transfers and 2M transactions.

| Table | Used for | Key columns |
|---|---|---|
| `token_transfers` | Grain, addresses, the `DOUBLE` cross-check | `token_address`, `from_address`, `to_address`, `value DOUBLE`, `transaction_hash`, `log_index`, `block_number`, `block_timestamp`, `last_modified` |
| `logs` | Exact amounts | `address`, `topics VARCHAR[]`, `data VARCHAR` (hex), `transaction_hash`, `log_index`, `block_hash`, `last_modified` |

**The precision problem:** token amounts are 256-bit unsigned integers, but `token_transfers.value` is a `DOUBLE`, so it holds about 15–17 significant digits. The exact amount lives in the ERC-20 `Transfer` event log: topic0 (`topics[1]` in DuckDB, whose arrays start at 1) is `0xddf252ad…`, there are three topics (topic0 plus the indexed `from` and `to`), and `data` is the amount as 32-byte hex.

**Scope filter:** a `tokens` seed of 5–10 major ERC-20 tokens (USDT, USDC, DAI, WETH, …), each with its `decimals`. The extract only keeps rows for these tokens, which keeps local runs small.

## Extract (Coach)

`make extract START=YYYY-MM-DD END=YYYY-MM-DD` uses DuckDB `httpfs` to copy the filtered rows of both tables into `data/raw/<table>/date=…/*.parquet`. It is idempotent per date: each date's partition is rewritten in full. dbt reads the raw Parquet as external sources, so rebuilding never touches S3.

A small 1-day fixture under `tests/fixtures/raw/` is committed for CI.

## Labels

| Source | Content |
|---|---|
| OFAC-sanctioned ETH addresses | Public list, saved as a dated seed file |
| `curated_labels` seed | Hand-picked exchange and bridge addresses, cited in the README |

A label row is `(address, label, source, as_of_date)`. `dbt snapshot` keeps the label history as SCD2. The repo keeps 2–3 dated versions of each label file so the history is real and the snapshot can be shown working: `make snapshot` replays them in order.

## Models

| Layer | Model | Grain | Notes |
|---|---|---|---|
| staging | `stg_eth__token_transfers` | One row per (`transaction_hash`, `log_index`) | Renamed and typed; addresses lowercased |
| staging | `stg_eth__transfer_logs` | Same | ERC-20 `Transfer` logs only (topic0 match, 3 topics) |
| staging | `stg_labels` | One row per (`address`, `label`, `as_of_date`) | Union of both seeds |
| intermediate | `int_transfer_amounts_exact` | One row per (`transaction_hash`, `log_index`) | Decodes `data` hex to an exact integer |
| core | `dim_token` | One row per token | From the seed: symbol, decimals |
| core | `dim_wallet` | One row per wallet version | From the snapshot: `wallet_key`, `address`, `label`, `valid_from`, `valid_to` |
| core | `dim_date` | One row per day | |
| core | `fct_token_transfer` | One row per (`transaction_hash`, `log_index`) | Exact raw amount, decimal-adjusted amount, from/to `wallet_key` as of `block_timestamp` |
| core | `fct_wallet_daily` | One row per wallet × token × day | Inflow, outflow, net, transfer count |
| marts | `mart_wallet_exposure` | One row per wallet × token | Amount received from wallets labeled sanctioned **at the time of the transfer** |
| marts | `mart_token_daily_volume` | One row per token × day | Volume, transfer count, active wallets |

### Exact Amounts

- The raw amount is a 256-bit integer, which overflows `DECIMAL(38, 0)`.
- **DuckDB:** keep the raw amount as a decimal string, and the decimal-adjusted amount as `DECIMAL(38, 18)` when it fits. Rows that don't fit go to a quarantine model and are counted in a test, never silently cast.
- **BigQuery:** `BIGNUMERIC` holds the raw amount.
- The `DOUBLE` value is kept only for the cross-check test.

### Incremental and Reorgs

- `fct_token_transfer` is incremental by `date`, merging on (`transaction_hash`, `log_index`).
- Every run reprocesses a lookback window (default 2 days, a dbt var), because a chain reorg can replace recent blocks. The source shows a rewrite through `last_modified` and a changed `block_hash`.
- A full refresh and an incremental run over the same dates must produce identical rows, which a test checks.

## Tests

| Kind | Examples |
|---|---|
| Generic | `unique` + `not_null` on every grain key; `relationships` from facts to dims; accepted values on labels |
| Contracts | Enforced on all core models: names, types, constraints |
| dbt unit tests | Hex decode (0, 1, max uint256, leading zeros); point-in-time join (a transfer before the first label, at exactly `valid_from`, after a label change) |
| Custom | Exact vs. `DOUBLE`: within 1e-12 relative; conservation per token per day (sum of inflows = sum of outflows, with mints and burns through the zero address); every row has exactly one current `dim_wallet` version; full refresh equals incremental |
| Freshness | Raw sources: the latest `date` is within N days of the extract range |

## Ownership

| Author writes (core) | Coach writes (plumbing) |
|---|---|
| Every model, snapshot, and macro | Extract script |
| Every test (generic, unit, custom) | `profiles.yml` (DuckDB + BigQuery), Makefile |
| Seeds' content and citations | CI workflow, the GitHub Pages deploy |
| README decisions, grain table, 100× section | Reviews, mutation tests (break a model, see whether a test catches it) |

## Repo Layout

```text
eth-wallet-model/
├── Makefile                  # extract, build, snapshot, docs, check
├── scripts/extract.py        # coach
├── dbt_project.yml
├── profiles.yml              # duckdb (default), bigquery
├── models/{staging,intermediate,core,marts}/
├── snapshots/
├── seeds/                    # tokens, ofac_*, curated_labels_*
├── macros/
├── tests/                    # custom data tests; fixtures/raw/
└── README.md
```

## CI

| Job | When | Runs |
|---|---|---|
| `check` | Every push | ruff and mypy on `scripts/`, sqlfluff on models, markdownlint |
| `build` | Every PR | `dbt build` on DuckDB against the 1-day fixture |
| `docs` | Merge to `main` | `dbt docs generate` → GitHub Pages |

## Milestones

| # | Deliverable | Done when |
|---|---|---|
| M1 | Extract + staging | Both staging models build on 1 day with grain tests green |
| M2 | Exact amounts | Decode unit tests and the `DOUBLE` cross-check pass |
| M3 | Labels + SCD2 | Snapshot replay gives 2+ versions per changed address; point-in-time unit tests pass |
| M4 | Core facts + marts | Conservation and full-refresh-equals-incremental tests pass |
| M5 | BigQuery + docs + publish | Success criteria 1–5 met; repo public |

## v2 Backlog

- Airflow or Dagster DAG with per-date backfill
- USD prices via a daily price seed
- Native ETH and internal transfers from `traces`
- Multi-hop exposure (2 hops from a sanctioned wallet)
- Export marts to project 3 (`olap-serving-bench`) on a schedule

## Risks

| Risk | Mitigation |
|---|---|
| OFAC list format or hosting changes | Commit dated copies; never fetch at build time |
| Hex decode differs between DuckDB and BigQuery | One macro with a per-adapter branch; unit tests run on DuckDB; BigQuery checked by hand at M5 |
| A token's `Transfer` event isn't standard (fee-on-transfer, rebasing) | Pick plain tokens for the seed; record exclusions in the README |
| Laptop runs get slow on wide date ranges | Default range is 7 days; the token filter is applied in the extract |
