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
