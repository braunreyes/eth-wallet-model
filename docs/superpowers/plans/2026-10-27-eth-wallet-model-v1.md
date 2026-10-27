# ETH Wallet Model v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Roles override the default executor flow.** Read [`AGENTS.md`](../../../AGENTS.md) first. Steps tagged **(Coach)** are done by the agent in full. Steps tagged **(Learner)** are written by the human: the agent writes only the stub and brief, answers hints on request (`hint 1` → `hint 2` → `hint 3` → `show me`), then reviews and mutation-tests. An agent must never write learner SQL or tests unprompted.

**Goal:** A dbt project that turns public Ethereum ERC-20 transfers into a tested dimensional model for wallet investigations, including exposure to sanctioned wallets at the time of each transfer.

**Architecture:** A Python extract copies token-filtered Parquet from the AWS public bucket to `data/raw/`. dbt (DuckDB locally and in CI, BigQuery by hand) reads it as external sources and builds staging → intermediate → core → marts. Labels are dated seeds replayed through `dbt snapshot` into SCD2, and facts join to the wallet version valid at `block_timestamp`.

**Tech Stack:** uv, Python 3.12, DuckDB 1.4+, dbt-core 1.10+ with dbt-duckdb and dbt-bigquery, dbt_utils, Typer, loguru, pendulum, pytest, ruff, mypy (strict), sqlfluff, markdownlint-cli2, GitHub Actions + Pages.

**Spec:** [`docs/superpowers/specs/2026-10-26-eth-wallet-model-design.md`](../specs/2026-10-26-eth-wallet-model-design.md)

## Global Constraints

- Source: `s3://aws-public-blockchain/v1.0/eth/`, anonymous, region `us-east-2`, Hive-partitioned `date=YYYY-MM-DD`.
- Default extract range is 7 days: `START=2026-10-19 END=2026-10-25`. CI fixture date: `2026-10-20`.
- Amounts come from the exact raw integer in `logs.data`. `token_transfers.value` (`DOUBLE`) is used **only** for the cross-check test.
- Addresses are lowercased once, in staging.
- Every core model states its grain in its YAML description and has `contract: {enforced: true}`.
- Grain keys: `unique` (or `dbt_utils.unique_combination_of_columns`) + `not_null`.
- Lookback window: dbt var `lookback_days`, default `2`.
- No credentials in `profiles.yml`. BigQuery auth is `gcloud` application-default credentials.
- Always call `uv run dbt`, never bare `dbt`: the global `dbt` on this laptop is dbt-fusion, not dbt-core.
- Gate before every commit: `make check`. Branch per task, squash-merged PRs.
- New ideas go to the spec's v2 Backlog, not into the current milestone.

## Findings From Probing the Source (2026-10-27)

These came from querying `date=2026-10-20` and shape several tasks.

| Finding | Evidence | Impact |
|---|---|---|
| `token_transfers.from_address` / `to_address` are **32-byte padded topics** | `0x000000000000000000000000943e254b…` (66 chars) | Staging takes the last 40 hex chars. A `length = 42` test guards it |
| `token_transfers` also has `block_hash` | `DESCRIBE` | Use it for reorg detection on both tables |
| `logs` for a token include `Approval` events (topic0 `0x8c5be1e5…`) | Sample USDT rows | The topic0 filter in `stg_eth__transfer_logs` really removes rows, so a mutation test can catch a dropped filter |
| `value` is the raw integer as a `DOUBLE`, not decimal-adjusted | USDT `16254101.0` = 16.254101 USDT | The cross-check compares to the raw amount |
| One Parquet file per day per table | `glob` returned 1 file | Extract is one read per table per date |
| DuckDB `BIGNUM` holds 256-bit integers but arithmetic falls back to `DOUBLE` | `2::BIGNUM * 3::BIGNUM` → `6.0` | `BIGNUM` can store an amount but not decode or sum it |
| **BigQuery `BIGNUMERIC` holds about 38.7 integer digits (~2^128), not 2^256** | BigQuery docs: max ≈ 5.79e38 | **Spec bug.** Both engines are exact only below ~2^128. See Task 6, Step 1 |

---

## File Structure

| Path | Owner | Responsibility |
|---|---|---|
| `pyproject.toml`, `uv.lock` | Coach | Python deps and tool config (ruff, mypy, pytest) |
| `Makefile` | Coach | `install extract fixture build ci-build snapshot audit-incremental docs lint test-py check clean` |
| `profiles.yml` | Coach | `duckdb` (default) and `bigquery` targets; paths from env vars |
| `dbt_project.yml`, `packages.yml` | Coach | Layer configs, vars, dbt_utils |
| `.sqlfluff`, `.sqlfluffignore`, `.gitignore` | Coach | Lint and ignore config |
| `scripts/extract.py` | Coach | S3 → `data/raw/<table>/date=…/part-0.parquet`, idempotent per date |
| `scripts/tests/test_extract.py` | Coach | pytest for the extract, run against a local fake bucket |
| `scripts/replay_labels.sh` | Coach | Replays dated label versions through `dbt snapshot` in order |
| `scripts/audit_incremental.sh` | Coach | Builds full refresh vs. day-by-day incremental, then runs the equality test |
| `.github/workflows/ci.yml`, `docs.yml` | Coach | `check` (push), `build` (PR), docs → Pages (main) |
| `models/staging/_eth__sources.yml` | Coach | External Parquet sources (DuckDB) / public dataset (BigQuery) |
| `models/**/_*.yml` (stubs) | Coach writes stub, Learner adds tests | Contracts: grain, columns, types |
| `models/**/*.sql`, `snapshots/*`, `macros/*` | Learner | All transformation logic |
| `seeds/*.csv` | Learner | Token list, OFAC versions, curated labels |
| `tests/*.sql`, unit tests in YAML | Learner | Custom and unit tests |
| `tests/fixtures/raw/` | Coach (generated) | Committed 1-day fixture for CI |
| `README.md` | Coach skeleton + quickstart; Learner decisions, grain table, 100× | Portfolio front page |

### Learner Task Template

Every **(Learner)** task follows the same loop. Steps reference it by name.

1. **Stub (Coach):** write the YAML contract and a stub `.sql` that returns zero typed rows, so `make check` stays green.
2. **Brief (Coach):** in chat, explain the task in plain words with one example row.
3. **Build (Learner):** replace the stub SQL. Ask for hints by tier.
4. **Tests (Learner):** add the tests listed under "Tests to write".
5. **Gate:** `make check` passes.
6. **Review (Coach):** AGENTS.md order: trace a row → grain → contract → scale ("Let N be…") → tradeoffs. End with one interviewer follow-up question.
7. **Mutation test (Coach):** copy the repo to the scratchpad, apply each listed mutation one at a time, run `make ci-build`, and report a caught/survived table. Each survivor names a missing test, and the learner adds it.
8. **Commit:** learner's work, learner's message. Add "assisted" to the body if `show me` was used.

---

## Milestone M1: Extract + Staging

Branch per task: `chore/project-setup`, `feat/tokens-seed`, `feat/extract`, `feat/stg-token-transfers`, `feat/stg-transfer-logs`.

### Task 1: Project Scaffold and Gate (Coach)

**Files:**

- Create: `pyproject.toml`, `Makefile`, `profiles.yml`, `dbt_project.yml`, `packages.yml`, `.sqlfluff`, `.sqlfluffignore`, `.gitignore`, `README.md`, `.github/workflows/ci.yml`
- Create: `models/{staging,intermediate,core,marts}/.gitkeep`, `seeds/.gitkeep`, `snapshots/.gitkeep`, `macros/.gitkeep`, `tests/.gitkeep`
- Create: `scripts/__init__.py` (empty), `scripts/tests/__init__.py` (empty)
- Modify: `AGENTS.md` (Gotchas)

**Interfaces:**

- Produces: env vars `ETH_RAW_ROOT` (default `data/raw`), `ETH_DUCKDB_PATH` (default `data/eth.duckdb`), `ETH_EXTRACT_END` (default `2026-10-25`), `ETH_BQ_PROJECT`, `ETH_BQ_DATASET`.
- Produces: dbt vars `lookback_days: 2`, `extract_end`, `freshness_max_days: 1`, `process_until: '9999-12-31'`, `labels_as_of: '9999-12-31'`, `incremental_audit: false`.
- Produces: Make targets `install`, `lint`, `test-py`, `build`, `ci-build`, `check`, `clean`.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "eth-wallet-model"
version = "0.1.0"
description = "dbt model of Ethereum ERC-20 transfers for wallet investigations"
requires-python = ">=3.12"
dependencies = [
    "dbt-core>=1.10",
    "dbt-duckdb>=1.9",
    "dbt-bigquery>=1.10",
    "duckdb>=1.4",
    "loguru>=0.7",
    "pendulum>=3.0",
    "typer>=0.15",
]

[dependency-groups]
dev = [
    "mypy>=1.13",
    "pytest>=8.3",
    "pytest-cov>=6.0",
    "ruff>=0.8",
    "sqlfluff>=3.3",
]

[tool.uv]
package = false

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["scripts"]

[tool.ruff.lint]
select = ["E", "F", "W", "I", "B", "UP", "S", "SIM", "RUF", "ANN"]

[tool.ruff.lint.per-file-ignores]
"scripts/tests/**" = ["S101"]
# Column lists are module constants, never user input
"scripts/extract.py" = ["S608"]

[tool.mypy]
strict = true
python_version = "3.12"
files = ["scripts"]

[tool.pytest.ini_options]
testpaths = ["scripts/tests"]
pythonpath = ["scripts"]
addopts = "--cov=extract --cov-report=term-missing --cov-fail-under=80"
```

- [ ] **Step 2: Write `profiles.yml`**

```yaml
# No credentials here. BigQuery uses gcloud application-default credentials.
eth_wallet_model:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: "{{ env_var('ETH_DUCKDB_PATH', 'data/eth.duckdb') }}"
      threads: 4
    bigquery:
      type: bigquery
      method: oauth
      project: "{{ env_var('ETH_BQ_PROJECT', 'set-ETH_BQ_PROJECT') }}"
      dataset: "{{ env_var('ETH_BQ_DATASET', 'eth_wallet_model') }}"
      location: US
      threads: 4
      job_execution_timeout_seconds: 600
```

- [ ] **Step 3: Write `dbt_project.yml` and `packages.yml`**

```yaml
name: eth_wallet_model
version: "1.0.0"
profile: eth_wallet_model

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]
test-paths: ["tests"]
clean-targets: ["target", "dbt_packages"]

vars:
  lookback_days: 2
  extract_end: "{{ env_var('ETH_EXTRACT_END', '2026-10-25') }}"
  freshness_max_days: 1
  # Upper bound on block_date; the incremental audit steps it day by day.
  process_until: "9999-12-31"
  # Label state as of this date; replay_labels.sh steps it through each version.
  labels_as_of: "9999-12-31"
  incremental_audit: false

models:
  eth_wallet_model:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    core:
      +materialized: table
      +contract:
        enforced: true
    marts:
      +materialized: table
```

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
```

- [ ] **Step 4: Write `.sqlfluff` and `.sqlfluffignore`**

```ini
[sqlfluff]
dialect = duckdb
templater = jinja
max_line_length = 100
exclude_rules = structure.column_order

[sqlfluff:templater:jinja]
apply_dbt_builtins = True

[sqlfluff:indentation]
tab_space_size = 4

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = lower

[sqlfluff:rules:capitalisation.functions]
extended_capitalisation_policy = lower

[sqlfluff:rules:capitalisation.literals]
capitalisation_policy = lower

[sqlfluff:rules:capitalisation.types]
extended_capitalisation_policy = lower
```

```text
target/
dbt_packages/
logs/
```

- [ ] **Step 5: Write `.gitignore`**

```text
.venv/
__pycache__/
.coverage
.mypy_cache/
.pytest_cache/
.ruff_cache/
target/
dbt_packages/
logs/
data/
.user.yml
```

- [ ] **Step 6: Write `Makefile`**

```make
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

export DBT_PROFILES_DIR := $(CURDIR)

START ?= 2026-10-19
END ?= 2026-10-25
FIXTURE_DATE ?= 2026-10-20
FIXTURE_BLOCKS ?= 300
MDLINT ?= markdownlint-cli2

export ETH_EXTRACT_END ?= $(END)
DBT := uv run dbt
CI_ENV := ETH_RAW_ROOT=tests/fixtures/raw ETH_DUCKDB_PATH=data/ci.duckdb ETH_EXTRACT_END=$(FIXTURE_DATE)

.PHONY: help install extract fixture deps build ci-build lint lint-py lint-sql lint-md test-py check clean

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

install: ## Install Python deps and dbt packages
	uv sync
	$(DBT) deps

extract: ## Copy START..END from S3 into data/raw
	uv run python scripts/extract.py --start $(START) --end $(END) --out data/raw

fixture: ## Rebuild the committed 1-day CI fixture
	uv run python scripts/extract.py --start $(FIXTURE_DATE) --end $(FIXTURE_DATE) \
		--out tests/fixtures/raw --max-blocks $(FIXTURE_BLOCKS)

deps:
	@mkdir -p data
	$(DBT) deps --quiet

build: deps ## dbt build on DuckDB against ETH_RAW_ROOT (default data/raw)
	$(DBT) build

ci-build: ## Clean dbt build against the committed fixture
	rm -f data/ci.duckdb
	$(CI_ENV) $(MAKE) build

lint-py:
	uv run ruff format --check scripts
	uv run ruff check scripts
	uv run mypy

lint-sql:
	uv run sqlfluff lint models snapshots macros tests

lint-md:
	$(MDLINT) "**/*.md"

lint: lint-py lint-sql lint-md ## All linters

test-py: ## pytest with coverage gate
	uv run pytest

check: lint ci-build ## The gate: run before every commit

clean: ## Remove build artifacts (keeps data/raw)
	rm -rf target dbt_packages logs data/*.duckdb
```

- [ ] **Step 7: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
  pull_request:

permissions:
  contents: read

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: astral-sh/setup-uv@v10.3.2
      - run: uv sync --locked
      - run: make lint-py lint-sql
      - uses: DavidAnson/markdownlint-cli2-action@v25.0.0
        with:
          globs: "**/*.md"

  build:
    if: github.event_name == 'pull_request'
    needs: check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: astral-sh/setup-uv@v10.3.2
      - run: uv sync --locked
      - run: make ci-build
```

- [ ] **Step 8: Write the `README.md` skeleton**

````markdown
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
````

- [ ] **Step 9: Add the probe findings to `AGENTS.md` Gotchas**

Append these bullets under `## Gotchas`:

```markdown
- `token_transfers.from_address` / `to_address` are 32-byte padded topics (66 chars). Keep the last 40 hex chars.
- Run `uv run dbt`, never bare `dbt`: the global `dbt` here is dbt-fusion.
- BigQuery `BIGNUMERIC` holds about 38.7 integer digits (~2^128), not a full uint256.
```

Also add `dbt_packages/**`, `target/**`, and `logs/**` to `ignores` in `.markdownlint-cli2.jsonc`, so vendored package docs aren't linted.

- [ ] **Step 10: Create empty dirs, install, and run the gate**

```bash
mkdir -p models/{staging,intermediate,core,marts} seeds snapshots macros tests scripts/tests
touch models/{staging,intermediate,core,marts}/.gitkeep seeds/.gitkeep snapshots/.gitkeep macros/.gitkeep tests/.gitkeep
touch scripts/__init__.py scripts/tests/__init__.py
uv lock && make install
make lint-sql lint-md
uv run dbt parse
```

Expected: `uv lock` resolves, `dbt parse` prints no errors, and `make check` passes. `test-py` stays out of `check` and CI until Task 3 adds the first tests.

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml uv.lock Makefile profiles.yml dbt_project.yml packages.yml package-lock.yml \
  .markdownlint-cli2.jsonc \
  .sqlfluff .sqlfluffignore .gitignore README.md AGENTS.md .github/workflows/ci.yml \
  models seeds snapshots macros tests scripts/__init__.py scripts/tests/__init__.py
git commit -m "chore: scaffold dbt project and gate"
```

### Task 2: Tokens Seed (Learner)

**Files:**

- Create (Learner): `seeds/tokens.csv`
- Create (Coach stub): `seeds/_seeds.yml`

**Interfaces:**

- Produces: seed `tokens` with columns `token_address varchar` (lowercase), `symbol varchar`, `name varchar`, `decimals integer`. The extract (Task 3) reads `token_address` from this file.

- [ ] **Step 1: Stub (Coach)** — write `seeds/_seeds.yml`:

```yaml
seeds:
  - name: tokens
    description: >
      Grain: one row per ERC-20 token in scope. The extract keeps only these
      tokens. Each row is cited in the README.
    config:
      column_types:
        token_address: varchar
        symbol: varchar
        name: varchar
        decimals: integer
    columns:
      - name: token_address
        description: Contract address, lowercase, 0x + 40 hex chars.
      - name: symbol
      - name: name
      - name: decimals
        description: Digits after the decimal point (USDT 6, DAI 18).
```

- [ ] **Step 2: Brief (Coach).** Pick 5–10 plain ERC-20 tokens: no fee-on-transfer, no rebasing (so not stETH or AMPL). Suggested: USDT, USDC, DAI, WETH, WBTC, LINK, UNI. Cite each address from Etherscan or the issuer's docs. Record exclusions in the README Decisions.
- [ ] **Step 3: Build (Learner).** Write `seeds/tokens.csv`.
- [ ] **Step 4: Tests to write (Learner)** in `seeds/_seeds.yml`: `unique` + `not_null` on `token_address`; `not_null` on `decimals`; `accepted_values` or a range check (`dbt_utils.accepted_range`, 0–18) on `decimals`; a 42-char lowercase check on `token_address` (`dbt_utils.expression_is_true`).
- [ ] **Step 5: Gate.** `make lint-sql lint-md && uv run dbt seed && uv run dbt test -s tokens`
- [ ] **Step 6: Review + commit** per the Learner Task Template. Mutations: uppercase one address; duplicate a row; set USDC decimals to 18.

### Task 3: Extract Script, Sources, and Fixture (Coach)

**Files:**

- Create: `scripts/extract.py`, `scripts/tests/test_extract.py`, `models/staging/_eth__sources.yml`
- Create (generated): `tests/fixtures/raw/{token_transfers,logs}/date=2026-10-20/part-0.parquet`

**Interfaces:**

- Consumes: `seeds/tokens.csv` (Task 2).
- Produces: `uv run python scripts/extract.py --start D --end D --out DIR [--max-blocks N] [--source-root URI] [--tokens-csv PATH] [--dry-run]`.
- Produces: layout `DIR/<table>/date=YYYY-MM-DD/part-0.parquet` for `token_transfers` and `logs`. The `date` column comes from the path, not the file.
- Produces: dbt sources `source('eth', 'token_transfers')` and `source('eth', 'logs')`, both with a `date` column.

- [ ] **Step 1: Write the failing tests** — `scripts/tests/test_extract.py`:

```python
"""Tests for the extract, run against a fake local bucket."""

from pathlib import Path

import duckdb
import pendulum
import pytest
from typer.testing import CliRunner

import extract

DAY = "2026-10-20"
TOKEN = "0x" + "a" * 40
TOKEN_MIXED_CASE = "0x" + "A" * 40
OTHER_TOKEN = "0x" + "b" * 40
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def write_source_day(root: Path, day: str) -> None:
    """Write one day of fake source data: 2 in-scope rows (blocks 100, 500), 1 other token."""
    con = duckdb.connect()
    transfers_dir = root / "token_transfers" / f"date={day}"
    logs_dir = root / "logs" / f"date={day}"
    transfers_dir.mkdir(parents=True)
    logs_dir.mkdir(parents=True)
    # Mixed case on one row proves the extract matches addresses case-insensitively.
    con.execute(
        """
        CREATE TABLE src AS
        SELECT token, block FROM (VALUES (?, 100), (?, 101), (?, 500)) AS t(token, block)
        """,
        [TOKEN_MIXED_CASE, OTHER_TOKEN, TOKEN],
    )
    con.execute(
        f"""
        COPY (
            SELECT
                token AS token_address,
                '0x' || repeat('0', 64) AS from_address,
                '0x' || repeat('0', 64) AS to_address,
                1.0::DOUBLE AS value,
                '0x' || block::VARCHAR AS transaction_hash,
                block::BIGINT AS log_index,
                block::BIGINT AS block_number,
                '0xblock' AS block_hash,
                TIMESTAMP '{day} 00:00:00' AS block_timestamp,
                TIMESTAMP '{day} 01:00:00' AS last_modified
            FROM src
        ) TO '{transfers_dir / "part.parquet"}' (FORMAT parquet)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT
                token AS address,
                ['{TRANSFER_TOPIC}', '0x01', '0x02'] AS topics,
                '0x' || repeat('0', 63) || '1' AS data,
                0::BIGINT AS transaction_index,
                block::BIGINT AS log_index,
                '0x' || block::VARCHAR AS transaction_hash,
                block::BIGINT AS block_number,
                '0xblock' AS block_hash,
                TIMESTAMP '{day} 00:00:00' AS block_timestamp,
                TIMESTAMP '{day} 01:00:00' AS last_modified
            FROM src
        ) TO '{logs_dir / "part.parquet"}' (FORMAT parquet)
        """
    )


@pytest.fixture
def source_root(tmp_path: Path) -> Path:
    root = tmp_path / "bucket"
    write_source_day(root, DAY)
    return root


@pytest.fixture
def tokens_csv(tmp_path: Path) -> Path:
    path = tmp_path / "tokens.csv"
    path.write_text(f"token_address,symbol,name,decimals\n{TOKEN_MIXED_CASE},AAA,Token A,18\n")
    return path


def count_rows(path: Path) -> int:
    result = duckdb.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()
    assert result is not None
    return int(result[0])


def test_load_token_addresses_lowercases(tokens_csv: Path) -> None:
    assert extract.load_token_addresses(tokens_csv) == [TOKEN]


def test_load_token_addresses_rejects_bad_address(tmp_path: Path) -> None:
    path = tmp_path / "tokens.csv"
    path.write_text("token_address,symbol,name,decimals\n0x123,BAD,Bad,18\n")
    with pytest.raises(ValueError, match="Invalid token addresses"):
        extract.load_token_addresses(path)


def test_load_token_addresses_rejects_empty(tmp_path: Path) -> None:
    path = tmp_path / "tokens.csv"
    path.write_text("token_address,symbol,name,decimals\n")
    with pytest.raises(ValueError, match="No token addresses"):
        extract.load_token_addresses(path)


def test_dates_between_is_inclusive() -> None:
    days = extract.dates_between(pendulum.date(2026, 10, 19), pendulum.date(2026, 10, 21))
    assert [d.isoformat() for d in days] == ["2026-10-19", "2026-10-20", "2026-10-21"]


def test_dates_between_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match="before"):
        extract.dates_between(pendulum.date(2026, 10, 21), pendulum.date(2026, 10, 19))


def test_extract_keeps_only_scope_tokens(source_root: Path, tmp_path: Path) -> None:
    out = tmp_path / "raw"
    counts = extract.extract_day(
        source_root=str(source_root), out_root=out, day=pendulum.date(2026, 10, 20),
        tokens=[TOKEN], max_blocks=None,
    )
    assert counts == {"token_transfers": 2, "logs": 2}
    assert count_rows(out / "logs" / f"date={DAY}" / "part-0.parquet") == 2


def test_extract_is_idempotent(source_root: Path, tmp_path: Path) -> None:
    out = tmp_path / "raw"
    for _ in range(2):
        extract.extract_day(
            source_root=str(source_root), out_root=out, day=pendulum.date(2026, 10, 20),
            tokens=[TOKEN], max_blocks=None,
        )
    partition = out / "token_transfers" / f"date={DAY}"
    assert [p.name for p in partition.iterdir()] == ["part-0.parquet"]
    assert count_rows(partition / "part-0.parquet") == 2
    assert not (out / "token_transfers" / f"date={DAY}.tmp").exists()


def test_extract_max_blocks_keeps_first_blocks(source_root: Path, tmp_path: Path) -> None:
    counts = extract.extract_day(
        source_root=str(source_root), out_root=tmp_path / "raw",
        day=pendulum.date(2026, 10, 20), tokens=[TOKEN], max_blocks=300,
    )
    assert counts == {"token_transfers": 1, "logs": 1}


def test_cli_dry_run_writes_nothing(source_root: Path, tokens_csv: Path, tmp_path: Path) -> None:
    out = tmp_path / "raw"
    result = CliRunner().invoke(
        extract.app,
        ["--start", DAY, "--end", DAY, "--out", str(out), "--tokens-csv", str(tokens_csv),
         "--source-root", str(source_root), "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert not out.exists()


def test_cli_extracts_range(source_root: Path, tokens_csv: Path, tmp_path: Path) -> None:
    out = tmp_path / "raw"
    result = CliRunner().invoke(
        extract.app,
        ["--start", DAY, "--end", DAY, "--out", str(out), "--tokens-csv", str(tokens_csv),
         "--source-root", str(source_root)],
    )
    assert result.exit_code == 0, result.output
    assert count_rows(out / "token_transfers" / f"date={DAY}" / "part-0.parquet") == 2


def test_cli_rejects_bad_date(tokens_csv: Path, tmp_path: Path) -> None:
    result = CliRunner().invoke(
        extract.app,
        ["--start", "10/20/2026", "--end", DAY, "--out", str(tmp_path), "--tokens-csv",
         str(tokens_csv)],
    )
    assert result.exit_code != 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest -q --no-cov`
Expected: FAIL with `ModuleNotFoundError: No module named 'extract'`.

- [ ] **Step 3: Write `scripts/extract.py`**

```python
"""Copy token-filtered Ethereum tables from the AWS public bucket to local Parquet.

Each date's partition is rewritten in full, so re-running a date is safe.
"""

import csv
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import duckdb
import pendulum
import typer
from loguru import logger

S3_ROOT = "s3://aws-public-blockchain/v1.0/eth"
S3_REGION = "us-east-2"
ADDRESS_PATTERN = re.compile(r"^0x[0-9a-f]{40}$")


@dataclass(frozen=True)
class SourceTable:
    """A source table: its name, the column holding the token address, and the columns kept."""

    name: str
    token_column: str
    columns: tuple[str, ...]


# `date` is left out on purpose: it comes back from the Hive path on read.
TABLES = (
    SourceTable(
        name="token_transfers",
        token_column="token_address",
        columns=(
            "token_address", "from_address", "to_address", "value", "transaction_hash",
            "log_index", "block_number", "block_hash", "block_timestamp", "last_modified",
        ),
    ),
    SourceTable(
        name="logs",
        token_column="address",
        columns=(
            "address", "topics", "data", "transaction_index", "log_index", "transaction_hash",
            "block_number", "block_hash", "block_timestamp", "last_modified",
        ),
    ),
)

app = typer.Typer(add_completion=False)


def load_token_addresses(tokens_csv: Path) -> list[str]:
    """Read the in-scope token addresses from the tokens seed.

    Args:
        tokens_csv: Path to a CSV with a `token_address` column.

    Returns:
        Lowercased token addresses, in file order.

    Raises:
        ValueError: If the file has no rows or an address is malformed.
    """
    with tokens_csv.open(newline="") as handle:
        addresses = [row["token_address"].strip().lower() for row in csv.DictReader(handle)]
    if not addresses:
        raise ValueError(f"No token addresses in {tokens_csv}")
    invalid = [address for address in addresses if not ADDRESS_PATTERN.match(address)]
    if invalid:
        raise ValueError(f"Invalid token addresses in {tokens_csv}: {invalid}")
    return addresses


def dates_between(start: pendulum.Date, end: pendulum.Date) -> list[pendulum.Date]:
    """Return every date from start to end, inclusive.

    Raises:
        ValueError: If end is before start.
    """
    if end < start:
        raise ValueError(f"End {end} is before start {start}")
    return [start.add(days=offset) for offset in range((end - start).days + 1)]


def parse_date(value: str) -> pendulum.Date:
    """Parse a YYYY-MM-DD string for the CLI.

    Raises:
        typer.BadParameter: If the string isn't YYYY-MM-DD.
    """
    try:
        return pendulum.from_format(value, "YYYY-MM-DD").date()
    except ValueError as error:
        raise typer.BadParameter(f"{value!r} is not YYYY-MM-DD") from error


def connect(source_root: str) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB, with anonymous S3 access when reading the bucket."""
    con = duckdb.connect()
    if source_root.startswith("s3://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET s3_region = ?", [S3_REGION])
    return con


def first_block(con: duckdb.DuckDBPyConnection, source_root: str, day: pendulum.Date) -> int:
    """Return the day's first block, read from the unfiltered token_transfers partition."""
    glob = f"{source_root}/token_transfers/date={day.isoformat()}/*.parquet"
    row = con.execute("SELECT min(block_number) FROM read_parquet(?)", [glob]).fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"No token_transfers rows for {day}")
    return int(row[0])


def write_partition(
    con: duckdb.DuckDBPyConnection,
    table: SourceTable,
    source_root: str,
    out_root: Path,
    day: pendulum.Date,
    tokens: list[str],
    block_cutoff: int | None,
) -> int:
    """Rewrite one table's partition for one day, swapping it in only after the write succeeds.

    Returns:
        Rows written.
    """
    target = out_root / table.name / f"date={day.isoformat()}"
    staging = target.with_name(f"{target.name}.tmp")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    query = f"""
        SELECT {", ".join(table.columns)}
        FROM read_parquet($source_glob)
        WHERE list_contains($tokens, lower({table.token_column}))
          AND ($block_cutoff IS NULL OR block_number < $block_cutoff)
    """
    params = {
        "source_glob": f"{source_root}/{table.name}/date={day.isoformat()}/*.parquet",
        "tokens": tokens,
        "block_cutoff": block_cutoff,
    }
    output = staging / "part-0.parquet"
    con.sql(query, params=params).write_parquet(str(output))
    row = con.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchone()
    shutil.rmtree(target, ignore_errors=True)
    staging.rename(target)
    return int(row[0]) if row else 0


def extract_day(
    source_root: str,
    out_root: Path,
    day: pendulum.Date,
    tokens: list[str],
    max_blocks: int | None,
) -> dict[str, int]:
    """Extract both tables for one day.

    Args:
        source_root: Bucket root, e.g. `s3://aws-public-blockchain/v1.0/eth`.
        out_root: Local root; partitions land in `out_root/<table>/date=…`.
        day: The date partition to copy.
        tokens: Lowercased in-scope token addresses.
        max_blocks: Keep only the day's first N blocks (for the CI fixture), or None for all.

    Returns:
        Rows written per table.
    """
    con = connect(source_root)
    cutoff = first_block(con, source_root, day) + max_blocks if max_blocks else None
    return {
        table.name: write_partition(con, table, source_root, out_root, day, tokens, cutoff)
        for table in TABLES
    }


@app.command()
def main(
    start: Annotated[str, typer.Option(help="First date, YYYY-MM-DD")],
    end: Annotated[str, typer.Option(help="Last date, YYYY-MM-DD (inclusive)")],
    out: Annotated[Path, typer.Option(help="Local output root")] = Path("data/raw"),
    tokens_csv: Annotated[Path, typer.Option(help="Tokens seed")] = Path("seeds/tokens.csv"),
    source_root: Annotated[str, typer.Option(help="Source bucket root")] = S3_ROOT,
    max_blocks: Annotated[int | None, typer.Option(help="Keep the first N blocks per day")] = None,
    dry_run: Annotated[bool, typer.Option(help="Log the plan; write nothing")] = False,
) -> None:
    """Copy START..END from the public bucket into OUT, one partition per table per date."""
    days = dates_between(parse_date(start), parse_date(end))
    tokens = load_token_addresses(tokens_csv)
    logger.info("Plan: {} day(s), {} token(s), into {}", len(days), len(tokens), out)
    if dry_run:
        for day in days:
            logger.info("Would rewrite {}/*/date={}", out, day.isoformat())
        return
    for day in days:
        counts = extract_day(source_root, out, day, tokens, max_blocks)
        logger.info("{} rows={}", day.isoformat(), counts)


if __name__ == "__main__":
    app()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest`
Expected: all pass, coverage ≥ 80%. Now add `test-py` back to the gate: `check: lint test-py ci-build` in the Makefile, and `make lint-py lint-sql test-py` in the CI `check` job. Then `make lint-py` passes. If mypy flags pendulum's `.date()` return types, narrow with `assert isinstance(...)`; don't add `# type: ignore`.

- [ ] **Step 5: Write `models/staging/_eth__sources.yml`**

```yaml
sources:
  - name: eth
    description: >
      Ethereum tables from s3://aws-public-blockchain/v1.0/eth/, filtered to the
      tokens seed by scripts/extract.py. DuckDB reads the local Parquet copy.
    config:
      external_location: >-
        read_parquet('{{ env_var("ETH_RAW_ROOT", "data/raw") }}/{name}/*/*.parquet',
        hive_partitioning = true)
    tables:
      - name: token_transfers
        description: One row per ERC-20 transfer, decoded by the publisher. `value` is a lossy DOUBLE.
      - name: logs
        description: Raw event logs for the in-scope token contracts, including non-Transfer events.
```

If this dbt-duckdb version only reads `external_location` from `meta`, move it under `meta:`.

- [ ] **Step 6: Generate the fixture and smoke-test the sources**

```bash
make fixture
du -sh tests/fixtures/raw
ETH_RAW_ROOT=tests/fixtures/raw uv run dbt show --inline \
  "select count(*) as n, min(date) as d from {{ source('eth', 'logs') }}"
```

Expected: the fixture is under 5 MB (if not, lower `FIXTURE_BLOCKS`), and `dbt show` returns `n > 0` with `d = 2026-10-20`.

- [ ] **Step 7: Run the full extract once**

Run: `make extract` (7 days; takes a few minutes). Expected: one log line per day with row counts. Re-run one day and confirm the counts match (idempotent).

- [ ] **Step 8: Commit**

```bash
git add scripts/extract.py scripts/tests/test_extract.py models/staging/_eth__sources.yml tests/fixtures/raw \
  Makefile .github/workflows/ci.yml
git commit -m "feat(extract): copy token-filtered parquet from s3"
```

### Task 4: `stg_eth__token_transfers` and Source Freshness (Learner)

**Files:**

- Create (Coach stub): `models/staging/_eth__models.yml`, `models/staging/stg_eth__token_transfers.sql`
- Create (Learner): `tests/assert_raw_sources_fresh.sql`

**Interfaces:**

- Consumes: `source('eth', 'token_transfers')`.
- Produces: `stg_eth__token_transfers` with the columns below. Later tasks rely on these exact names.

- [ ] **Step 1: Stub (Coach)** — `models/staging/_eth__models.yml`:

```yaml
models:
  - name: stg_eth__token_transfers
    description: >
      Grain: one row per (transaction_hash, log_index). Renamed and typed from
      source token_transfers. Addresses are lowercased and unpadded to 20 bytes here,
      and nowhere else.
    columns:
      - name: transaction_hash
        data_type: varchar
      - name: log_index
        data_type: bigint
      - name: block_number
        data_type: bigint
      - name: block_hash
        data_type: varchar
      - name: block_timestamp
        data_type: timestamp
      - name: block_date
        data_type: date
        description: From the Hive `date` partition. Must equal cast(block_timestamp as date).
      - name: token_address
        data_type: varchar
      - name: from_address
        data_type: varchar
        description: 0x + 40 hex chars. The source pads it to 32 bytes.
      - name: to_address
        data_type: varchar
      - name: value_double
        data_type: double
        description: Lossy. Only for the cross-check test.
      - name: last_modified
        data_type: timestamp
```

`models/staging/stg_eth__token_transfers.sql` stub:

```sql
-- Stub: zero typed rows. The author replaces this.
select
    cast(null as varchar) as transaction_hash,
    cast(null as bigint) as log_index,
    cast(null as bigint) as block_number,
    cast(null as varchar) as block_hash,
    cast(null as timestamp) as block_timestamp,
    cast(null as date) as block_date,
    cast(null as varchar) as token_address,
    cast(null as varchar) as from_address,
    cast(null as varchar) as to_address,
    cast(null as double) as value_double,
    cast(null as timestamp) as last_modified
from {{ source('eth', 'token_transfers') }}
where false
```

- [ ] **Step 2: Brief (Coach).** Rename, cast, lowercase, and unpad `from_address`/`to_address` (`0x` + the last 40 chars). Apply `block_date <= var('process_until')` here so every downstream model respects it. Example row: `from_address = 0x000000000000000000000000943e254b…` → `0x943e254b…`.
- [ ] **Step 3: Build (Learner).**
- [ ] **Step 4: Tests to write (Learner):**
  - Grain: `dbt_utils.unique_combination_of_columns` on (`transaction_hash`, `log_index`); `not_null` on both.
  - `from_address` and `to_address`: `not_null`, plus `expression_is_true` for length 42 and lowercase.
  - `relationships` from `token_address` to `ref('tokens')`.
  - `block_date = cast(block_timestamp as date)` (`expression_is_true`).
  - `tests/assert_raw_sources_fresh.sql`: fails when `max(date)` of either source is more than `var('freshness_max_days')` days before `var('extract_end')`.
- [ ] **Step 5: Gate.** `make check`
- [ ] **Step 6: Review + mutation + commit** per the template. Mutations: drop the unpadding; drop `lower()`; `union all` the source twice (a double-read of one partition); freshness compare `>` → `>=` off by one; drop the `process_until` filter (caught later by the incremental audit; note it as survived for now if nothing catches it).

### Task 5: `stg_eth__transfer_logs` (Learner)

**Files:**

- Modify (Coach stub): `models/staging/_eth__models.yml`
- Create (Coach stub): `models/staging/stg_eth__transfer_logs.sql`

**Interfaces:**

- Consumes: `source('eth', 'logs')`.
- Produces: `stg_eth__transfer_logs` with `transaction_hash`, `log_index`, `block_number`, `block_hash`, `block_timestamp`, `block_date`, `token_address`, `from_address`, `to_address`, `amount_hex`, `last_modified`.

- [ ] **Step 1: Stub (Coach)** — append to `_eth__models.yml`:

```yaml
  - name: stg_eth__transfer_logs
    description: >
      Grain: one row per (transaction_hash, log_index). ERC-20 Transfer logs only:
      topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef and
      exactly 3 topics. DuckDB arrays start at 1, so topic0 is topics[1].
    columns:
      - name: transaction_hash
        data_type: varchar
      - name: log_index
        data_type: bigint
      - name: block_number
        data_type: bigint
      - name: block_hash
        data_type: varchar
      - name: block_timestamp
        data_type: timestamp
      - name: block_date
        data_type: date
      - name: token_address
        data_type: varchar
        description: The log's emitting contract (`address`), lowercased.
      - name: from_address
        data_type: varchar
        description: From topics[2], unpadded to 20 bytes.
      - name: to_address
        data_type: varchar
        description: From topics[3], unpadded to 20 bytes.
      - name: amount_hex
        data_type: varchar
        description: The `data` field, 0x + 64 hex chars, the exact uint256 amount.
      - name: last_modified
        data_type: timestamp
```

`models/staging/stg_eth__transfer_logs.sql` stub:

```sql
-- Stub: zero typed rows. The author replaces this.
select
    cast(null as varchar) as transaction_hash,
    cast(null as bigint) as log_index,
    cast(null as bigint) as block_number,
    cast(null as varchar) as block_hash,
    cast(null as timestamp) as block_timestamp,
    cast(null as date) as block_date,
    cast(null as varchar) as token_address,
    cast(null as varchar) as from_address,
    cast(null as varchar) as to_address,
    cast(null as varchar) as amount_hex,
    cast(null as timestamp) as last_modified
from {{ source('eth', 'logs') }}
where false
```

- [ ] **Step 2: Brief (Coach).** Filter to Transfer events, then shape like the transfers model. The raw table also holds `Approval` logs for the same tokens (seen in the probe), so the filter really removes rows. Ask: what does a 4-topic log with the same topic0 mean? (ERC-721 `Transfer`: the amount is a token ID.)
- [ ] **Step 3: Build (Learner).**
- [ ] **Step 4: Tests to write (Learner):** grain (`unique_combination_of_columns` + `not_null`); address length/lowercase; `amount_hex` length 66; and a singular test `tests/assert_transfer_logs_match_token_transfers.sql` that returns keys present in one staging model and not the other (both directions).
- [ ] **Step 5: Gate.** `make check`
- [ ] **Step 6: Review + mutation + commit** per the template. Mutations: `topics[0]` instead of `topics[1]`; drop the 3-topic check; drop the topic0 filter; read `to` from `topics[2]`.

**M1 done when:** both staging models build on the fixture and 7-day extract with grain tests green.

---

## Milestone M2: Exact Amounts

Branch: `feat/exact-amounts`, `feat/double-cross-check`.

### Task 6: Hex Decode Macro and `int_transfer_amounts_exact` (Learner)

**Files:**

- Modify (Coach): `docs/superpowers/specs/2026-10-26-eth-wallet-model-design.md` (Exact Amounts), after the learner agrees
- Create (Coach stub): `models/intermediate/_int_models.yml`, `models/intermediate/int_transfer_amounts_exact.sql`, `models/intermediate/int_transfer_amounts_quarantine.sql`
- Create (Learner): `macros/hex_to_uint.sql`, unit tests in `_int_models.yml`, `tests/assert_quarantine_is_empty.sql`

**Interfaces:**

- Consumes: `stg_eth__transfer_logs`, `ref('tokens')`.
- Produces: macro `hex_to_uint(hex_column)` with a per-adapter branch (`adapter.dispatch`). It returns an exact integer, or null when the value is ≥ 2^128.
- Produces: `int_transfer_amounts_exact` with `transaction_hash`, `log_index`, `block_date`, `token_address`, `amount_hex`, `amount_raw varchar`, `amount decimal(38,18)`, `is_quarantined boolean`.

- [ ] **Step 1: Spec decision (Coach + Learner).** The spec says "BigQuery: `BIGNUMERIC` holds the raw amount", but `BIGNUMERIC` tops out near 5.79e38 (~2^128), and DuckDB's `UHUGEINT` stops at 2^128 − 1. Proposed one policy for both engines: decode exactly below 2^128; at or above it, keep `amount_hex`, set `amount_raw` null, and quarantine the row. Real transfers of seed tokens never come close. Update the spec's Exact Amounts section once agreed, and record the decision in the README.
- [ ] **Step 2: Stub (Coach)** — `models/intermediate/_int_models.yml`:

```yaml
models:
  - name: int_transfer_amounts_exact
    description: >
      Grain: one row per (transaction_hash, log_index). Decodes amount_hex to an exact
      integer string and a decimal-adjusted amount. Never casts silently: a row whose
      value does not fit is flagged is_quarantined.
    columns:
      - name: transaction_hash
        data_type: varchar
      - name: log_index
        data_type: bigint
      - name: block_date
        data_type: date
      - name: token_address
        data_type: varchar
      - name: amount_hex
        data_type: varchar
      - name: amount_raw
        data_type: varchar
        description: Exact base-10 integer; null when quarantined.
      - name: amount
        data_type: decimal(38,18)
        description: amount_raw / 10^decimals; null when quarantined.
      - name: is_quarantined
        data_type: boolean

  - name: int_transfer_amounts_quarantine
    description: >
      Grain: one row per (transaction_hash, log_index). Rows from
      int_transfer_amounts_exact whose amount does not fit. Expected to be empty for
      the seed tokens; a test counts it.
```

Stubs for both `.sql` files follow the zero-typed-rows pattern from Task 4 (`from {{ ref('stg_eth__transfer_logs') }} where false`, and `from {{ ref('int_transfer_amounts_exact') }} where false`).

- [ ] **Step 3: Brief (Coach).** A 32-byte hex string is two 16-byte halves. If the high half is all zeros, the low half fits in 128 bits. Example: `0x…0162ae` → `90798`. With USDT `decimals = 6`, that is `0.090798`.
- [ ] **Step 4: Build (Learner).** Write the macro, then the two models.
- [ ] **Step 5: Tests to write (Learner):**
  - dbt unit tests on `int_transfer_amounts_exact` (given rows in `stg_eth__transfer_logs` and `tokens`): `0x00…00` → `"0"`; `0x00…01` → `"1"`; leading zeros (`0x00…0162ae` → `"90798"`); 2^128 − 1 → `"340282366920938463463374607431768211455"`, not quarantined; max uint256 (`0xff…ff`) → quarantined, `amount_raw` null; 18-decimal adjust (`1000000000000000000` → `1.000000000000000000`).
  - Grain + `not_null` on keys; `expression_is_true`: `is_quarantined = (amount_raw is null)`, so the flag and the null can never disagree.
  - `tests/assert_quarantine_is_empty.sql`: returns quarantined rows, `severity: warn` above 0, `error` above a `var('quarantine_max_rows', 0)` threshold. Explain the choice in the README.
- [ ] **Step 6: Gate.** `make check`
- [ ] **Step 7: Review + mutation + commit.** Mutations: drop the high-half check (overflows silently); off-by-one on the hex substring; decimals `10^(decimals - 1)`; cast to `double` mid-decode.

### Task 7: `DOUBLE` Cross-Check Test (Learner)

**Files:**

- Create (Learner): `tests/assert_exact_matches_double.sql`

**Interfaces:**

- Consumes: `int_transfer_amounts_exact.amount_raw`, `stg_eth__token_transfers.value_double`, joined on (`transaction_hash`, `log_index`).

- [ ] **Step 1: Brief (Coach).** Return rows where `abs(exact − double) / greatest(exact, 1) > 1e-12`. Ask why it is relative, not absolute (a 1e24 wei amount in a `DOUBLE` is off by about 1e8 wei and still correct to 16 digits).
- [ ] **Step 2: Build (Learner).**
- [ ] **Step 3: Gate.** `make check`
- [ ] **Step 4: Review + mutation + commit.** Mutations: swap the decode's halves; join on `transaction_hash` only; drop one hex digit.

**M2 done when:** decode unit tests and the `DOUBLE` cross-check pass on the fixture and the 7-day extract.

---

## Milestone M3: Labels + SCD2

Branch: `feat/label-seeds`, `feat/wallet-snapshot`, `feat/dim-wallet-pit`.

### Task 8: Dated Label Seeds (Learner)

**Files:**

- Create (Learner): `seeds/labels/ofac_eth_YYYY_MM_DD.csv` (2–3 versions), `seeds/labels/curated_labels_YYYY_MM_DD.csv` (2–3 versions)
- Modify (Coach stub): `seeds/_seeds.yml`

**Interfaces:**

- Produces: every label seed has columns `address varchar` (lowercase), `label varchar`, `source varchar`, `as_of_date date`. The file-name date equals `as_of_date`. `replay_labels.sh` (Task 9) parses the date from the file name.

- [ ] **Step 1: Stub (Coach)** — add a `seeds/labels/` block to `dbt_project.yml`:

```yaml
seeds:
  eth_wallet_model:
    labels:
      +column_types:
        address: varchar
        label: varchar
        source: varchar
        as_of_date: date
```

- [ ] **Step 2: Brief (Coach).** OFAC's SDN list tags addresses as "Digital Currency Address - ETH". Use real dated snapshots of that list: Tornado Cash addresses were added on 2022-08-08 and removed on 2025-03-21, which gives a real add-then-remove history. Allowed `label` values: `sanctioned`, `exchange`, `bridge`. Cite each curated address in the README. Note: the label dates will likely all fall before the extract window, so the real data rarely shows a mid-window change. The unit tests in Task 10 prove the point-in-time logic, and the README should say so.
- [ ] **Step 3: Build (Learner).**
- [ ] **Step 4: Tests to write (Learner):** `accepted_values` on `label` and `source`; `not_null` on all four columns; address length 42 + lowercase.
- [ ] **Step 5: Gate + review + commit.**

### Task 9: Labels Staging, As-Of Model, Snapshot, and Replay (Learner + Coach)

**Files:**

- Create (Coach): `scripts/replay_labels.sh`; modify `Makefile` (`snapshot` target; `build` runs it)
- Create (Coach stub): `models/staging/_labels_models.yml`, `models/staging/stg_labels.sql`, `models/intermediate/int_wallet_labels_as_of.sql`, `snapshots/_snapshots.yml`
- Create (Learner): SQL for both models and the snapshot config

**Interfaces:**

- Produces: `stg_labels` (grain `address`, `label`, `as_of_date`).
- Produces: `int_wallet_labels_as_of`: one row per address, as of `var('labels_as_of')`, with `address`, `is_sanctioned boolean`, `entity_label varchar`, `labels_as_of date` (the latest version date included).
- Produces: snapshot `snap_wallet_labels` (`relation: ref('int_wallet_labels_as_of')`, `unique_key: address`).

- [ ] **Step 1: Write `scripts/replay_labels.sh` (Coach)**

```bash
#!/usr/bin/env bash
# Replays each dated label version through dbt snapshot, oldest first, so the
# SCD2 history matches the real order of list changes. Safe to re-run: an
# unchanged version adds no snapshot rows.
set -euo pipefail

dates=$(find seeds/labels -name '*.csv' \
  | sed -E 's/.*_([0-9]{4})_([0-9]{2})_([0-9]{2})\.csv$/\1-\2-\3/' \
  | sort -u)

if [[ -z "${dates}" ]]; then
  echo "No label versions under seeds/labels; skipping replay."
  exit 0
fi

uv run dbt seed --select path:seeds/labels
for as_of in ${dates}; do
  echo "Replaying labels as of ${as_of}"
  vars="{labels_as_of: '${as_of}'}"
  uv run dbt run --select +int_wallet_labels_as_of --vars "${vars}"
  uv run dbt snapshot --select snap_wallet_labels --vars "${vars}"
done
```

Makefile changes:

```make
snapshot: deps ## Replay dated label versions into the SCD2 snapshot
	./scripts/replay_labels.sh

build: deps ## dbt build on DuckDB against ETH_RAW_ROOT (default data/raw)
	$(DBT) seed
	./scripts/replay_labels.sh
	$(DBT) build
```

- [ ] **Step 2: Stub (Coach)** — `snapshots/_snapshots.yml`:

```yaml
snapshots:
  - name: snap_wallet_labels
    description: >
      SCD2 history of each address's labels. dbt_valid_from must be the label
      version's as_of date, not the run time.
    relation: ref('int_wallet_labels_as_of')
    config:
      unique_key: address
      # Author fills in strategy, check_cols / updated_at, and hard_deletes.
```

Model stubs follow the zero-typed-rows pattern with the columns in Interfaces.

- [ ] **Step 3: Brief (Coach).** Spike first: does the `check` strategy with `updated_at: labels_as_of` set `dbt_valid_from` to the as-of date on this dbt version? If not, use the `timestamp` strategy with an `updated_at` that changes only when a row's state changes. Removals: keep every address ever seen in `int_wallet_labels_as_of`, so a delisting is a changed row (`is_sanctioned` true → false), not a hard delete that would stamp the run time.
- [ ] **Step 4: Build (Learner).**
- [ ] **Step 5: Tests to write (Learner):** grain on `stg_labels` and `int_wallet_labels_as_of`; on the snapshot, `unique` on `dbt_scd_id`; a singular test `tests/assert_snapshot_has_history.sql` that fails unless at least one address has 2+ versions.
- [ ] **Step 6: Gate.** `make check` (now replays labels inside `ci-build`).
- [ ] **Step 7: Review + mutation + commit.** Mutations: sort dates descending in the replay (Coach edits the script copy); `<` for `<=` in the as-of filter; `check_cols` missing `is_sanctioned`.

### Task 10: `dim_wallet` and the Point-in-Time Join (Learner)

**Files:**

- Create (Coach stub): `models/core/_core_models.yml`, `models/core/dim_wallet.sql`, `models/core/fct_token_transfer.sql` (table for now; incremental in Task 12)
- Create (Learner): SQL, unit tests, `tests/assert_one_current_wallet_version.sql`

**Interfaces:**

- Consumes: `snap_wallet_labels`, `stg_eth__token_transfers`, `int_transfer_amounts_exact`.
- Produces: `dim_wallet` with `wallet_key varchar`, `address varchar`, `is_sanctioned boolean`, `entity_label varchar`, `valid_from timestamp`, `valid_to timestamp`, `is_current boolean`.
- Produces: `fct_token_transfer` with `transaction_hash varchar`, `log_index bigint`, `block_date date`, `block_number bigint`, `block_timestamp timestamp`, `block_hash varchar`, `token_address varchar`, `from_address varchar`, `to_address varchar`, `from_wallet_key varchar`, `to_wallet_key varchar`, `amount_raw varchar`, `amount decimal(38,18)`, `amount_double double`, `last_modified timestamp`.

- [ ] **Step 1: Stub (Coach)** — `models/core/_core_models.yml`:

```yaml
models:
  - name: dim_wallet
    description: >
      Grain: one row per wallet version. Every address seen in a transfer has
      versions covering all time, so every transfer finds exactly one version.
    constraints:
      - type: primary_key
        columns: [wallet_key]
    columns:
      - name: wallet_key
        data_type: varchar
        constraints: [{type: not_null}]
      - name: address
        data_type: varchar
        constraints: [{type: not_null}]
      - name: is_sanctioned
        data_type: boolean
        constraints: [{type: not_null}]
      - name: entity_label
        data_type: varchar
      - name: valid_from
        data_type: timestamp
        constraints: [{type: not_null}]
      - name: valid_to
        data_type: timestamp
        description: Exclusive. The current version ends at 9999-12-31.
        constraints: [{type: not_null}]
      - name: is_current
        data_type: boolean
        constraints: [{type: not_null}]

  - name: fct_token_transfer
    description: >
      Grain: one row per (transaction_hash, log_index). Exact amounts only;
      quarantined rows are excluded. Wallet keys are the versions valid at
      block_timestamp.
    constraints:
      - type: primary_key
        columns: [transaction_hash, log_index]
    columns:
      - name: transaction_hash
        data_type: varchar
        constraints: [{type: not_null}]
      - name: log_index
        data_type: bigint
        constraints: [{type: not_null}]
      - name: block_date
        data_type: date
        constraints: [{type: not_null}]
      - name: block_number
        data_type: bigint
      - name: block_timestamp
        data_type: timestamp
      - name: block_hash
        data_type: varchar
      - name: token_address
        data_type: varchar
      - name: from_address
        data_type: varchar
      - name: to_address
        data_type: varchar
      - name: from_wallet_key
        data_type: varchar
      - name: to_wallet_key
        data_type: varchar
      - name: amount_raw
        data_type: varchar
      - name: amount
        data_type: decimal(38,18)
      - name: amount_double
        data_type: double
      - name: last_modified
        data_type: timestamp
```

- [ ] **Step 2: Brief (Coach).** `dim_wallet` = snapshot versions + an "unlabeled" version for each address before its first label + one version for each address that never had a label. The join rule is `valid_from <= block_timestamp and block_timestamp < valid_to`. Ask: why half-open? (So a transfer at exactly `valid_from` matches one version, not two.)
- [ ] **Step 3: Build (Learner).**
- [ ] **Step 4: Tests to write (Learner):**
  - dbt unit tests on `fct_token_transfer`: transfer before the first label → the unlabeled version; at exactly `valid_from` → the new version; after a label change → the new version; after a delisting → `is_sanctioned = false`.
  - `relationships`: `from_wallet_key` / `to_wallet_key` → `dim_wallet.wallet_key`; `token_address` → `ref('tokens')`.
  - `tests/assert_one_current_wallet_version.sql`: every address has exactly one `is_current` row.
  - A singular test that version windows for the same address don't overlap or leave gaps.
- [ ] **Step 5: Gate.** `make check`
- [ ] **Step 6: Review + mutation + commit.** Mutations: `<=` for `<` on `valid_to`; join on `is_current` instead of the time window; drop the pre-first-label version; `to_wallet_key` joined on `from_address`.

**M3 done when:** the snapshot replay gives 2+ versions per changed address and the point-in-time unit tests pass.

---

## Milestone M4: Core Facts + Marts

Branch: `feat/dim-token-date`, `feat/incremental-fct`, `feat/fct-wallet-daily`, `feat/marts`.

### Task 11: `dim_token` and `dim_date` (Learner)

**Files:**

- Modify (Coach stub): `models/core/_core_models.yml`
- Create (Coach stub → Learner): `models/core/dim_token.sql`, `models/core/dim_date.sql`

**Interfaces:**

- Produces: `dim_token(token_address varchar PK, symbol varchar, name varchar, decimals integer)`.
- Produces: `dim_date(date_day date PK, year integer, month integer, day_of_week integer, is_weekend boolean)`, covering every `block_date` in `fct_token_transfer`.

- [ ] **Step 1: Stub (Coach)** — append to `_core_models.yml`:

```yaml
  - name: dim_token
    description: "Grain: one row per token. From the tokens seed."
    constraints:
      - type: primary_key
        columns: [token_address]
    columns:
      - name: token_address
        data_type: varchar
        constraints: [{type: not_null}]
      - name: symbol
        data_type: varchar
        constraints: [{type: not_null}]
      - name: name
        data_type: varchar
      - name: decimals
        data_type: integer
        constraints: [{type: not_null}]

  - name: dim_date
    description: "Grain: one row per day. Spans the transfer date range with no gaps."
    constraints:
      - type: primary_key
        columns: [date_day]
    columns:
      - name: date_day
        data_type: date
        constraints: [{type: not_null}]
      - name: year
        data_type: integer
      - name: month
        data_type: integer
      - name: day_of_week
        data_type: integer
        description: ISO, Monday = 1.
      - name: is_weekend
        data_type: boolean
```

- [ ] **Step 2: Brief (Coach).** `dim_date` needs a date spine that works on both engines: `dbt_utils.date_spine` or a dispatched macro. Ask: why not derive days from the facts? (Days with zero transfers would vanish.)
- [ ] **Step 3: Build + tests (Learner):** PKs; `relationships` from `fct_token_transfer.block_date` → `dim_date.date_day` and `token_address` → `dim_token`; a no-gaps test on `dim_date`.
- [ ] **Step 4: Gate + review + mutation + commit.** Mutations: spine ends one day early; `day_of_week` 0-based.

### Task 12: Incremental `fct_token_transfer` with Reorg Lookback (Learner + Coach)

**Files:**

- Create (Coach): `scripts/audit_incremental.sh`; Makefile target `audit-incremental`; CI step in `build`
- Modify (Learner): `models/core/fct_token_transfer.sql`
- Create (Learner): `tests/assert_full_refresh_equals_incremental.sql`

**Interfaces:**

- Consumes: vars `lookback_days`, `process_until`, `incremental_audit`.
- Produces: table `fct_token_transfer__full` (written by the audit script, same schema) for the equality test.

- [ ] **Step 1: Write `scripts/audit_incremental.sh` (Coach)**

```bash
#!/usr/bin/env bash
# Proves a day-by-day incremental build equals one full refresh over the same dates.
# Usage: audit_incremental.sh START END   (dates YYYY-MM-DD, inclusive)
set -euo pipefail

start="$1"
end="$2"
db="${ETH_DUCKDB_PATH:-data/eth.duckdb}"
select="+fct_token_transfer"

run() { uv run dbt run --select "${select}" --vars "{process_until: '$1'}" "${@:2}"; }

echo "Full refresh through ${end}"
run "${end}" --full-refresh
uv run python - "${db}" <<'PY'
import sys

import duckdb

# A byte-for-byte copy is the point here, so every column is wanted.
with duckdb.connect(sys.argv[1]) as con:
    con.execute("create or replace table main.fct_token_transfer__full as from main.fct_token_transfer")
PY

echo "Incremental: full refresh through ${start}, then one day at a time"
run "${start}" --full-refresh
day="${start}"
while [[ "${day}" < "${end}" ]]; do
  day=$(uv run python -c "import pendulum, sys; print(pendulum.parse(sys.argv[1]).add(days=1).to_date_string())" "${day}")
  run "${day}"
done
# Re-run the last day: a no-op run must not change anything either.
run "${end}"

uv run dbt test --select assert_full_refresh_equals_incremental --vars "{incremental_audit: true}"
```

Makefile:

```make
audit-incremental: deps ## Full refresh vs. day-by-day incremental over START..END
	./scripts/audit_incremental.sh $(START) $(END)
```

Add to `ci-build` after `build`: `$(CI_ENV) ./scripts/audit_incremental.sh $(FIXTURE_DATE) $(FIXTURE_DATE)`.

- [ ] **Step 2: Brief (Coach).** Make the model incremental by `block_date`, reprocessing the last `var('lookback_days')` days on every run. The spec says merge on (`transaction_hash`, `log_index`). Question to answer before coding: a reorg drops a block, and its logs never come back. What does a key-based merge leave behind? Compare `merge` with `delete+insert` over the lookback window, and record the choice in the README.
- [ ] **Step 3: Build (Learner).** The equality test is `enabled: var('incremental_audit')`, compares both tables with `except` in both directions, and returns any differing rows.
- [ ] **Step 4: Gate.** `make check` and `make audit-incremental` on the 7-day extract.
- [ ] **Step 5: Review + mutation + commit.** Mutations: lookback `0`; `>` for `>=` in the incremental filter; drop `unique_key`; use `max(block_date)` without subtracting the lookback.

### Task 13: `fct_wallet_daily` and Conservation (Learner)

**Files:**

- Modify (Coach stub): `_core_models.yml`; create stub `models/core/fct_wallet_daily.sql`
- Create (Learner): `tests/assert_conservation_per_token_day.sql`

**Interfaces:**

- Produces: `fct_wallet_daily(wallet_address varchar, token_address varchar, date_day date, inflow_amount decimal(38,18), outflow_amount decimal(38,18), net_amount decimal(38,18), transfer_count bigint)`, PK (`wallet_address`, `token_address`, `date_day`).

- [ ] **Step 1: Stub (Coach)** — append to `_core_models.yml`:

```yaml
  - name: fct_wallet_daily
    description: >
      Grain: one row per wallet × token × day. A transfer adds to the receiver's
      inflow and the sender's outflow. The zero address stands for mints (sender)
      and burns (receiver).
    constraints:
      - type: primary_key
        columns: [wallet_address, token_address, date_day]
    columns:
      - name: wallet_address
        data_type: varchar
        constraints: [{type: not_null}]
      - name: token_address
        data_type: varchar
        constraints: [{type: not_null}]
      - name: date_day
        data_type: date
        constraints: [{type: not_null}]
      - name: inflow_amount
        data_type: decimal(38,18)
        constraints: [{type: not_null}]
      - name: outflow_amount
        data_type: decimal(38,18)
        constraints: [{type: not_null}]
      - name: net_amount
        data_type: decimal(38,18)
        constraints: [{type: not_null}]
      - name: transfer_count
        data_type: bigint
        constraints: [{type: not_null}]
```

- [ ] **Step 2: Brief (Coach).** Unpivot each transfer into two legs (+ for the receiver, − for the sender), then group. Self-transfers (from = to) are one wallet, two legs. Conservation: per token per day, sum of inflows = sum of outflows across all wallets, including the zero address.
- [ ] **Step 3: Build + tests (Learner):** PK; `expression_is_true` `net_amount = inflow_amount - outflow_amount`; conservation singular test; `relationships` to `dim_token` and `dim_date`.
- [ ] **Step 4: Gate + review + mutation + commit.** Mutations: drop the sender leg for the zero address; count a self-transfer once; group by `block_timestamp` instead of the day.

### Task 14: Marts (Learner)

**Files:**

- Create (Coach stub): `models/marts/_marts_models.yml`, `models/marts/mart_wallet_exposure.sql`, `models/marts/mart_token_daily_volume.sql`

**Interfaces:**

- Produces: `mart_wallet_exposure(wallet_address, token_address, sanctioned_inflow_amount decimal(38,18), sanctioned_transfer_count bigint, first_exposure_at timestamp, last_exposure_at timestamp)`.
- Produces: `mart_token_daily_volume(token_address, symbol, date_day, volume_amount decimal(38,18), transfer_count bigint, active_wallet_count bigint)`.

- [ ] **Step 1: Stub (Coach)** — `models/marts/_marts_models.yml`:

```yaml
models:
  - name: mart_wallet_exposure
    description: >
      Grain: one row per wallet × token with any inflow from a wallet labeled
      sanctioned at the time of the transfer (the sender's version valid at
      block_timestamp, not its current label).
    columns:
      - name: wallet_address
      - name: token_address
      - name: sanctioned_inflow_amount
      - name: sanctioned_transfer_count
      - name: first_exposure_at
      - name: last_exposure_at

  - name: mart_token_daily_volume
    description: >
      Grain: one row per token × day. Volume excludes mints and burns; the README
      says why.
    columns:
      - name: token_address
      - name: symbol
      - name: date_day
      - name: volume_amount
      - name: transfer_count
      - name: active_wallet_count
```

- [ ] **Step 2: Brief (Coach).** Exposure joins `fct_token_transfer.from_wallet_key` → `dim_wallet` and filters `is_sanctioned`. Ask: a wallet delisted in March 2025 sent funds in 2024. Is the receiver exposed? (Yes: at the time it was sanctioned.)
- [ ] **Step 3: Build + tests (Learner):** grain tests on both; a dbt unit test where the sender is delisted *after* the transfer (exposure counted) and one where it is sanctioned *after* (not counted); `transfer_count` in the volume mart equals the fact count per token-day.
- [ ] **Step 4: Gate + review + mutation + commit.** Mutations: join `dim_wallet` on `is_current`; count mints as volume; `count(*)` instead of `count(distinct wallet)`.

**M4 done when:** conservation and full-refresh-equals-incremental pass on the fixture and the 7-day extract.

---

## Milestone M5: BigQuery + Docs + Publish

Branch: `feat/bigquery`, `ci/docs-pages`, `docs/readme`.

### Task 15: BigQuery Target (Coach sources + Learner dispatch)

**Files:**

- Modify (Coach): `models/staging/_eth__sources.yml`
- Modify (Learner): any model or macro that needs `adapter.dispatch` (hex decode, date spine, array indexing)

- [ ] **Step 1: Point sources at the public dataset on BigQuery (Coach)**

```yaml
sources:
  - name: eth
    database: "{{ 'bigquery-public-data' if target.type == 'bigquery' else none }}"
    schema: "{{ 'crypto_ethereum' if target.type == 'bigquery' else 'main' }}"
```

Keep the DuckDB `external_location` config. The BigQuery tables differ: there's no `date` column (partitioned on `block_timestamp`), `token_transfers.value` is a STRING, and arrays are 0-based (`topics[OFFSET(0)]`). List each difference in the README.

- [ ] **Step 2: Dispatch (Learner).** Add `bigquery__` branches wherever compile fails. On BigQuery, always filter `block_timestamp` to the extract range so a run never scans the whole chain. Run `uv run dbt compile --target bigquery` (needs `gcloud auth application-default login` and `ETH_BQ_PROJECT`).
- [ ] **Step 3: One hand-run build (Learner).** `uv run dbt build --target bigquery --vars "{process_until: '2026-10-20'}"` over one day. Before running, check the bytes it will scan with a BigQuery dry run. Paste the result and the bytes billed into the README.
- [ ] **Step 4: Commit.**

### Task 16: Docs to GitHub Pages (Coach)

**Files:**

- Create: `.github/workflows/docs.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: docs

on:
  push:
    branches: [main]

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: astral-sh/setup-uv@v10.3.2
      - run: uv sync --locked
      - run: make ci-build
      - run: |
          ETH_RAW_ROOT=tests/fixtures/raw ETH_DUCKDB_PATH=data/ci.duckdb \
            DBT_PROFILES_DIR=. uv run dbt docs generate --static
          mkdir -p site && cp target/static_index.html site/index.html
      - uses: actions/upload-pages-artifact@v5
        with:
          path: site

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v5
```

- [ ] **Step 2: Add `make docs`** — `$(DBT) docs generate --static && open target/static_index.html`.
- [ ] **Step 3: Enable Pages (ask the user first):** `gh api -X POST repos/{owner}/{repo}/pages -f build_type=workflow`.
- [ ] **Step 4: Commit**, then link the lineage graph from the README once the first deploy succeeds.

### Task 17: README Final (Learner, Coach reviews)

- [ ] **Step 1 (Learner):** grain table for every core model; Decisions and Tradeoffs (one entry per decision made in Tasks 2–15); What Changes at 100× ("Let N be…" for each fact model: rows scanned per run, join sizes, what moves to BigQuery partitioning/clustering, when the snapshot replay stops scaling).
- [ ] **Step 2 (Coach):** review against spec Success Criteria 1–5; markdownlint clean.
- [ ] **Step 3: Commit.**

### Task 18: Final Verification

- [ ] **Step 1:** On a clean clone: `make install && make extract && make build` → all green (Success Criterion 1).
- [ ] **Step 2:** CI green on the last PR (2); `dbt compile --target bigquery` succeeds and the README shows the hand-run (3); Pages URL loads the lineage graph (4); README sections exist (5).
- [ ] **Step 3 (Learner):** explain every model and test aloud without notes (6). The coach asks one interviewer question per target role.
- [ ] **Step 4:** Ask the user before making the repo public: `gh repo edit --visibility public --accept-visibility-change-consequences`.
