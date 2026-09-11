# US Stock Research Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, read-only Python research dashboard for a user-maintained US-stock watchlist, with source-linked fundamentals, valuation and technical indicators, earnings expectation comparisons, research judgments, scheduled refreshes, and bounded storage.

**Architecture:** A `stock_research` Python package separates provider adapters, SQLite persistence, deterministic calculations, job orchestration, and Streamlit pages. SEC EDGAR supplies filing evidence; Moomoo OpenD/API supplies market data for unattended jobs, while an adapter keeps the application independent of the installed Moomoo MCP's concrete tool schema. Every displayed item is classified as a source fact, derived calculation, or user judgment.

**Tech Stack:** Python 3.12+, SQLite/SQLAlchemy, Streamlit, Pandas, HTTPX, APScheduler, Moomoo `futu-api`, SEC EDGAR JSON/XBRL endpoints, Pytest, Ruff.

## Global Constraints

- Support US-listed common stocks and ETFs only in this release; all market-session dates use `America/New_York`.
- All broker and market-data interactions are read-only; do not import or expose order, account-transfer, or position-mutating operations.
- Use SEC EDGAR/XBRL and company IR links as primary evidence for financial facts; retain source URL, retrieval time, period start/end, filing date, unit, and raw taxonomy tag.
- Store 5 years of daily bars, 30 days of intraday bars, 2 years of changed-only estimate snapshots, 7 days of filing-file cache, and 90 days of detailed job/report logs.
- Preserve user judgments, peer groups, cited facts, provenance, aggregate run statistics, and cleanup audits indefinitely unless the user explicitly deletes them.
- Report stale or missing data in the UI; never replace it with fabricated values and never emit buy/sell recommendations.
- Every task begins with a failing Pytest assertion, then minimal code, then a passing test and a focused Git commit.

---

## Proposed File Structure

```text
pyproject.toml
.gitignore
.env.example
README.md
src/stock_research/
  __init__.py
  config.py
  db.py
  models.py
  repositories.py
  types.py
  providers/__init__.py
  providers/base.py
  providers/edgar.py
  providers/moomoo.py
  providers/fake.py
  services/sync.py
  services/metrics.py
  services/alerts.py
  services/retention.py
  services/__init__.py
  scheduler.py
  cli.py
  dashboard/Home.py
  dashboard/pages/__init__.py
  dashboard/pages/1_Watchlist.py
  dashboard/pages/2_Company.py
  dashboard/pages/3_Earnings.py
  dashboard/pages/4_Judgments.py
tests/
  conftest.py
  test_config.py
  test_repositories.py
  providers/test_edgar.py
  providers/test_moomoo.py
  services/test_metrics.py
  services/test_sync.py
  services/test_retention.py
  test_scheduler.py
  test_cli.py
  dashboard/test_view_models.py
```

### Task 1: Bootstrap the package, configuration, and test database

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `.env.example`, `src/stock_research/__init__.py`, `src/stock_research/config.py`, `src/stock_research/db.py`, `tests/conftest.py`, `tests/test_config.py`

**Interfaces:**
- Produces `Settings`, `load_settings()`, and `create_engine_from_settings(settings)`.

- [ ] **Step 1: Write the failing configuration test.**

```python
# tests/test_config.py
from stock_research.config import load_settings

def test_load_settings_uses_local_database_and_new_york_timezone(monkeypatch):
    monkeypatch.setenv("STOCK_RESEARCH_DB_PATH", "var/test.sqlite3")
    settings = load_settings()
    assert settings.database_path.as_posix() == "var/test.sqlite3"
    assert settings.market_timezone == "America/New_York"
    assert settings.edgar_user_agent.startswith("stock-research/")
```

- [ ] **Step 2: Run the test to verify it fails.**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL because `stock_research.config` does not exist.

- [ ] **Step 3: Create the minimal package and configuration implementation.**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "stock-research"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["APScheduler>=3.10", "httpx>=0.27", "pandas>=2.2", "SQLAlchemy>=2.0", "streamlit>=1.39", "typer>=0.12"]

[project.optional-dependencies]
market = ["futu-api>=9.3"]
dev = ["pytest>=8.3", "pytest-httpx>=0.30", "ruff>=0.6"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

```python
# src/stock_research/config.py
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Settings:
    database_path: Path
    market_timezone: str
    edgar_user_agent: str
    moomoo_host: str
    moomoo_port: int

def load_settings() -> Settings:
    return Settings(
        database_path=Path(os.getenv("STOCK_RESEARCH_DB_PATH", "var/stock_research.sqlite3")),
        market_timezone="America/New_York",
        edgar_user_agent=os.getenv("EDGAR_USER_AGENT", "stock-research/0.1 contact@example.com"),
        moomoo_host=os.getenv("MOOMOO_OPEND_HOST", "127.0.0.1"),
        moomoo_port=int(os.getenv("MOOMOO_OPEND_PORT", "11111")),
    )
```

```python
# src/stock_research/db.py
from sqlalchemy import create_engine
from .config import Settings

def create_engine_from_settings(settings: Settings):
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{settings.database_path}", future=True)
```

- [ ] **Step 4: Add `.gitignore` and `.env.example`.**

```gitignore
.env
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
var/
*.sqlite3
```

```dotenv
STOCK_RESEARCH_DB_PATH=var/stock_research.sqlite3
EDGAR_USER_AGENT=stock-research/0.1 your-email@example.com
MOOMOO_OPEND_HOST=127.0.0.1
MOOMOO_OPEND_PORT=11111
```

- [ ] **Step 5: Verify, format, and commit.**

Run: `python -m pytest tests/test_config.py -v && ruff check src tests`
Expected: 1 passed; Ruff exits 0.

```bash
git add pyproject.toml .gitignore .env.example src tests
git commit -m "chore: bootstrap stock research package"
```

### Task 2: Model provenance-aware SQLite entities and repositories

**Files:**
- Create: `src/stock_research/models.py`, `src/stock_research/repositories.py`, `tests/test_repositories.py`
- Modify: `src/stock_research/db.py`

**Interfaces:**
- Consumes `create_engine_from_settings()`.
- Produces `Base`, `initialize_database(engine)`, `Repository.add_security()`, `Repository.upsert_daily_bar()`, and `Repository.record_estimate_if_changed()`.

- [ ] **Step 1: Write the failing repository test.**

```python
def test_estimate_is_only_inserted_when_value_changes(repository):
    repository.add_security("US.NVDA", "NVDA", "NVIDIA CORP", "1045810")
    assert repository.record_estimate_if_changed("US.NVDA", "2026Q3", "eps", 1.25, "moomoo", "2026-09-10T20:00:00Z")
    assert not repository.record_estimate_if_changed("US.NVDA", "2026Q3", "eps", 1.25, "moomoo", "2026-09-11T20:00:00Z")
    assert repository.record_estimate_if_changed("US.NVDA", "2026Q3", "eps", 1.30, "moomoo", "2026-09-12T20:00:00Z")
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `python -m pytest tests/test_repositories.py::test_estimate_is_only_inserted_when_value_changes -v`
Expected: FAIL because `Repository` is undefined.

- [ ] **Step 3: Implement SQLAlchemy models and idempotent repository methods.**

```python
# src/stock_research/models.py (essential columns)
class EstimateSnapshot(Base):
    __tablename__ = "estimate_snapshot"
    id: Mapped[int] = mapped_column(primary_key=True)
    security_code: Mapped[str] = mapped_column(ForeignKey("security.code"), index=True)
    period: Mapped[str]
    metric: Mapped[str]
    value: Mapped[float]
    source: Mapped[str]
    retrieved_at: Mapped[datetime]

class ResearchJudgment(Base):
    __tablename__ = "research_judgment"
    id: Mapped[int] = mapped_column(primary_key=True)
    security_code: Mapped[str] = mapped_column(ForeignKey("security.code"), index=True)
    thesis: Mapped[str]
    growth_driver: Mapped[str]
    valuation_assumption: Mapped[str]
    target_weight: Mapped[float | None]
    invalidation_condition: Mapped[str]
    created_at: Mapped[datetime]
```

```python
# src/stock_research/repositories.py
def record_estimate_if_changed(self, code, period, metric, value, source, retrieved_at) -> bool:
    latest = self.session.scalar(
        select(EstimateSnapshot).where(
            EstimateSnapshot.security_code == code,
            EstimateSnapshot.period == period,
            EstimateSnapshot.metric == metric,
        ).order_by(EstimateSnapshot.retrieved_at.desc())
    )
    if latest is not None and latest.value == value:
        return False
    self.session.add(EstimateSnapshot(
        security_code=code, period=period, metric=metric, value=value,
        source=source, retrieved_at=datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")),
    ))
    self.session.commit()
    return True
```

- [ ] **Step 4: Add an in-memory `repository` fixture and pass tests.**

Run: `python -m pytest tests/test_repositories.py -v`
Expected: PASS, including repeated-bar upsert and provenance assertions.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/models.py src/stock_research/repositories.py src/stock_research/db.py tests
git commit -m "feat: add provenance-aware research storage"
```

### Task 3: Define read-only provider contracts and implement SEC EDGAR ingestion

**Files:**
- Create: `src/stock_research/types.py`, `src/stock_research/providers/__init__.py`, `src/stock_research/providers/base.py`, `src/stock_research/providers/edgar.py`, `src/stock_research/providers/fake.py`, `tests/providers/test_edgar.py`

**Interfaces:**
- Produces `FinancialFact`, `DailyBar`, `EarningsConsensus`, `MarketDataProvider`, and `EdgarProvider.fetch_company_facts(cik)`.
- `EdgarProvider` returns normalized facts with source URL, raw XBRL tag, unit, fiscal period, and retrieval time.

- [ ] **Step 1: Write the failing EDGAR normalization test using a fixed HTTP response.**

```python
def test_edgar_provider_returns_revenue_with_provenance(httpx_mock):
    httpx_mock.add_response(json={"facts": {"us-gaap": {"RevenueFromContractWithCustomerExcludingAssessedTax": {"units": {"USD": [{"fy": 2025, "fp": "FY", "start": "2024-01-01", "end": "2024-12-31", "filed": "2025-02-20", "form": "10-K", "val": 1000}]}}}}})
    fact = EdgarProvider("stock-research/0.1 test@example.com").fetch_company_facts("0001045810")[0]
    assert (fact.metric, fact.value, fact.unit, fact.raw_tag) == ("revenue", 1000.0, "USD", "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax")
    assert fact.source_url.endswith("CIK0001045810.json")
```

- [ ] **Step 2: Run the focused test.**

Run: `python -m pytest tests/providers/test_edgar.py::test_edgar_provider_returns_revenue_with_provenance -v`
Expected: FAIL because `EdgarProvider` does not exist.

- [ ] **Step 3: Implement the provider contract and SEC HTTP client.**

```python
class MarketDataProvider(Protocol):
    def daily_bars(self, code: str, start: date, end: date) -> list[DailyBar]: ...
    def earnings_consensus(self, code: str) -> list[EarningsConsensus]: ...

class EdgarProvider:
    def __init__(self, user_agent: str, client: httpx.Client | None = None):
        self.client = client or httpx.Client(headers={"User-Agent": user_agent}, timeout=20)

    def fetch_company_facts(self, cik: str) -> list[FinancialFact]:
        padded = cik.zfill(10)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
        response = self.client.get(url)
        response.raise_for_status()
        return normalize_company_facts(response.json(), url)
```

- [ ] **Step 4: Add fixtures for duplicate annual facts, quarterly facts, and unsupported units.**

Run: `python -m pytest tests/providers/test_edgar.py -v`
Expected: PASS; only 10-K/10-Q/8-K values with accepted units are normalized.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/types.py src/stock_research/providers tests/providers
git commit -m "feat: ingest source-linked SEC company facts"
```

### Task 4: Implement the Moomoo OpenD adapter and a deterministic fake provider

**Files:**
- Create: `src/stock_research/providers/moomoo.py`, `tests/providers/test_moomoo.py`
- Modify: `src/stock_research/providers/fake.py`

**Interfaces:**
- Consumes `MarketDataProvider` and `Settings.moomoo_host/port`.
- Produces `MoomooProvider.daily_bars()`, `MoomooProvider.earnings_consensus()`, and `MoomooProvider.earnings_calendar()`.

- [ ] **Step 1: Write failing mapping tests against a fake quote context.**

```python
def test_moomoo_maps_earnings_actual_and_estimate_to_consensus(fake_quote_context):
    provider = MoomooProvider("127.0.0.1", 11111, context_factory=lambda **_: fake_quote_context)
    item = provider.earnings_consensus("US.NVDA")[0]
    assert item.security_code == "US.NVDA"
    assert item.period == "2026Q3"
    assert item.eps_estimate == 1.25
    assert item.revenue_estimate == 1000.0
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `python -m pytest tests/providers/test_moomoo.py::test_moomoo_maps_earnings_actual_and_estimate_to_consensus -v`
Expected: FAIL because `MoomooProvider` is undefined.

- [ ] **Step 3: Implement a read-only context wrapper with explicit close handling.**

```python
class MoomooProvider:
    def __init__(self, host: str, port: int, context_factory=OpenQuoteContext):
        self.host, self.port, self.context_factory = host, port, context_factory

    def earnings_consensus(self, code: str) -> list[EarningsConsensus]:
        context = self.context_factory(host=self.host, port=self.port)
        try:
            ret, frame = context.get_earnings_calendar(market=Market.US)
            if ret != RET_OK:
                raise ProviderUnavailableError(str(frame))
            rows = frame.loc[frame["security"] == code]
            return [EarningsConsensus.from_moomoo_row(row) for _, row in rows.iterrows()]
        finally:
            context.close()
```

- [ ] **Step 4: Test failed OpenD calls, empty rows, and daily-bar mapping.**

Run: `python -m pytest tests/providers/test_moomoo.py -v`
Expected: PASS; tests prove no trading context is constructed.

- [ ] **Step 5: Add OpenD setup instructions to README and commit.**

```bash
git add src/stock_research/providers/moomoo.py src/stock_research/providers/fake.py tests/providers README.md
git commit -m "feat: add read-only Moomoo market data adapter"
```

### Task 5: Synchronize facts, prices, estimates, and earnings events idempotently

**Files:**
- Create: `src/stock_research/services/__init__.py`, `src/stock_research/services/sync.py`, `tests/services/test_sync.py`
- Modify: `src/stock_research/repositories.py`

**Interfaces:**
- Consumes `EdgarProvider`, `MarketDataProvider`, and `Repository`.
- Produces `SyncService.sync_security(code, today) -> SyncResult`, which backfills five years of daily bars on a security's first successful sync and requests only the latest eight calendar days on later syncs.

- [ ] **Step 1: Write a failing full-flow sync test.**

```python
def test_sync_saves_changed_consensus_and_creates_earnings_review_alert(repository, fake_market, fake_edgar):
    service = SyncService(repository, fake_market, fake_edgar)
    result = service.sync_security("US.NVDA", date(2026, 9, 10))
    assert result.bars_upserted == 1
    assert result.facts_upserted == 2
    assert result.estimates_inserted == 1
    assert repository.open_alert_titles("US.NVDA") == ["Review earnings guidance and management commentary"]

def test_first_sync_requests_five_year_daily_history(repository, fake_market, fake_edgar):
    SyncService(repository, fake_market, fake_edgar).sync_security("US.NVDA", date(2026, 9, 10))
    assert fake_market.daily_bar_requests == [("US.NVDA", date(2021, 9, 10), date(2026, 9, 10))]
```

- [ ] **Step 2: Run the test.**

Run: `python -m pytest tests/services/test_sync.py::test_sync_saves_changed_consensus_and_creates_earnings_review_alert -v`
Expected: FAIL because `SyncService` is undefined.

- [ ] **Step 3: Implement synchronizer stages with a per-security transaction boundary.**

```python
def sync_security(self, code: str, today: date) -> SyncResult:
    self.repository.mark_sync_started(code, today)
    try:
        start = today - timedelta(days=365 * 5) if not self.repository.has_daily_bars(code) else today - timedelta(days=8)
        bars = self.market.daily_bars(code, start, today)
        facts = self.edgar.fetch_company_facts(self.repository.cik_for(code))
        consensus = self.market.earnings_consensus(code)
        result = self.repository.persist_sync_batch(code, bars, facts, consensus, today)
        self.repository.mark_sync_succeeded(code, today)
        return result
    except Exception as exc:
        self.repository.mark_sync_failed(code, today, str(exc))
        return SyncResult.failed(code, str(exc))
```

- [ ] **Step 4: Verify idempotency, stale data, and error retention.**

Run: `python -m pytest tests/services/test_sync.py -v`
Expected: PASS; rerunning a successful date adds no duplicate bar, alert, or unchanged estimate.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/services/sync.py src/stock_research/repositories.py tests/services/test_sync.py
git commit -m "feat: synchronize research data with audit status"
```

### Task 6: Calculate traceable fundamental, valuation, and technical metrics

**Files:**
- Create: `src/stock_research/services/metrics.py`, `tests/services/test_metrics.py`

**Interfaces:**
- Produces `calculate_yoy()`, `calculate_ttm()`, `calculate_free_cash_flow()`, `calculate_sma()`, `calculate_rsi_wilder()`, `calculate_macd()`, and `MetricValue` with `formula_version="v1"`.

- [ ] **Step 1: Write failing calculation tests with known values.**

```python
def test_calculate_rsi_wilder_returns_100_for_fourteen_consecutive_gains():
    closes = pd.Series(range(1, 17), dtype=float)
    assert calculate_rsi_wilder(closes, 14).iloc[-1] == 100.0

def test_free_cash_flow_is_operating_cash_flow_less_capex():
    assert calculate_free_cash_flow(250.0, 70.0) == 180.0
```

- [ ] **Step 2: Run the metric tests.**

Run: `python -m pytest tests/services/test_metrics.py -v`
Expected: FAIL because the metric functions are undefined.

- [ ] **Step 3: Implement explicit, non-predictive formulas.**

```python
def calculate_free_cash_flow(operating_cash_flow: float, capex: float) -> float:
    return operating_cash_flow - abs(capex)

def calculate_sma(closes: pd.Series, window: int) -> pd.Series:
    return closes.rolling(window=window, min_periods=window).mean()

def calculate_macd(closes: pd.Series) -> pd.DataFrame:
    macd = closes.ewm(span=12, adjust=False).mean() - closes.ewm(span=26, adjust=False).mean()
    signal = macd.ewm(span=9, adjust=False).mean()
    return pd.DataFrame({"macd": macd, "signal": signal, "histogram": macd - signal})
```

- [ ] **Step 4: Add tests for TTM, year-over-year, 20/50/200 SMA, volume ratio, P/E/P/S, negative earnings, and missing observations.**

Run: `python -m pytest tests/services/test_metrics.py -v`
Expected: PASS; negative EPS yields `None` for P/E rather than a misleading multiple.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/services/metrics.py tests/services/test_metrics.py
git commit -m "feat: calculate evidence-linked research metrics"
```

### Task 7: Add research judgments, dashboard-only alerts, and retention cleanup

**Files:**
- Create: `src/stock_research/services/alerts.py`, `src/stock_research/services/retention.py`, `tests/services/test_retention.py`
- Modify: `src/stock_research/repositories.py`

**Interfaces:**
- Produces `AlertService.create_earnings_alert()`, `RetentionService.preview(now)`, and `RetentionService.execute(now) -> CleanupResult`.

- [ ] **Step 1: Write failing retention tests.**

```python
def test_retention_deletes_old_intraday_and_logs_but_keeps_judgments(repository):
    repository.seed_intraday_bar("US.NVDA", "2026-07-01T14:00:00Z")
    repository.seed_job_log("2026-06-01T14:00:00Z")
    judgment_id = repository.add_judgment("US.NVDA", "demand expands", "data center", "35x FCF", None, "revenue growth below 15%")
    result = RetentionService(repository).execute(datetime(2026, 9, 10, tzinfo=UTC))
    assert result.deleted["intraday_bar"] == 1
    assert repository.get_judgment(judgment_id).thesis == "demand expands"
```

- [ ] **Step 2: Run the focused test.**

Run: `python -m pytest tests/services/test_retention.py::test_retention_deletes_old_intraday_and_logs_but_keeps_judgments -v`
Expected: FAIL because `RetentionService` is undefined.

- [ ] **Step 3: Implement preview and execute using fixed retention cutoffs.**

```python
RETENTION_DAYS = {"daily_bar": 365 * 5, "intraday_bar": 30, "estimate_snapshot": 365 * 2, "filing_cache": 7, "job_log": 90, "report": 90}

def preview(self, now: datetime) -> dict[str, int]:
    return {kind: self.repository.count_before(kind, now - timedelta(days=days)) for kind, days in RETENTION_DAYS.items()}

def execute(self, now: datetime) -> CleanupResult:
    planned = self.preview(now)
    deleted = {kind: self.repository.delete_before(kind, now - timedelta(days=days)) for kind, days in RETENTION_DAYS.items()}
    self.repository.add_cleanup_audit(now, planned, deleted)
    return CleanupResult(planned=planned, deleted=deleted)
```

- [ ] **Step 4: Test every cutoff, cleanup audit, and storage summary.**

Run: `python -m pytest tests/services/test_retention.py -v`
Expected: PASS; `research_judgment`, `financial_fact`, `peer_group`, and `cleanup_audit` have no delete path.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/services src/stock_research/repositories.py tests/services/test_retention.py
git commit -m "feat: add dashboard alerts and bounded retention"
```

### Task 8: Schedule market-close, earnings, weekly, and cleanup jobs

**Files:**
- Create: `src/stock_research/scheduler.py`, `src/stock_research/cli.py`, `tests/test_scheduler.py`

**Interfaces:**
- Consumes `SyncService`, `AlertService`, `RetentionService`, and `Repository`.
- Produces `build_scheduler(services)`, `run_daily_sync()`, `run_weekly_review()`, and CLI commands `init-db`, `sync`, `run-scheduler`, and `cleanup`.

- [ ] **Step 1: Write failing scheduler registration tests.**

```python
def test_scheduler_registers_only_dashboard_jobs_in_new_york_timezone(services):
    scheduler = build_scheduler(services)
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert set(jobs) == {"daily-sync", "earnings-review", "weekly-review", "weekly-cleanup"}
    assert str(scheduler.timezone) == "America/New_York"
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `python -m pytest tests/test_scheduler.py::test_scheduler_registers_only_dashboard_jobs_in_new_york_timezone -v`
Expected: FAIL because `build_scheduler` is undefined.

- [ ] **Step 3: Implement explicit job schedules and CLI entry points.**

```python
def build_scheduler(services: Services) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="America/New_York")
    scheduler.add_job(services.run_daily_sync, "cron", day_of_week="mon-fri", hour=18, minute=10, id="daily-sync", replace_existing=True)
    scheduler.add_job(services.create_earnings_reviews, "cron", day_of_week="mon-fri", hour=8, minute=0, id="earnings-review", replace_existing=True)
    scheduler.add_job(services.run_weekly_review, "cron", day_of_week="sat", hour=10, minute=0, id="weekly-review", replace_existing=True)
    scheduler.add_job(services.retention.execute_now, "cron", day_of_week="sun", hour=3, minute=0, id="weekly-cleanup", replace_existing=True)
    return scheduler
```

- [ ] **Step 4: Test job failure recording and one-shot CLI sync.**

Run: `python -m pytest tests/test_scheduler.py -v`
Expected: PASS; failed providers create a `job_run` failure and preserve last valid data.

- [ ] **Step 5: Commit.**

```bash
git add src/stock_research/scheduler.py src/stock_research/cli.py tests/test_scheduler.py
git commit -m "feat: schedule local research refresh jobs"
```

### Task 9: Build the Streamlit watchlist, company, earnings, judgments, and storage views

**Files:**
- Create: `src/stock_research/dashboard/Home.py`, `src/stock_research/dashboard/view_models.py`, `src/stock_research/dashboard/pages/__init__.py`, `src/stock_research/dashboard/pages/1_Watchlist.py`, `src/stock_research/dashboard/pages/2_Company.py`, `src/stock_research/dashboard/pages/3_Earnings.py`, `src/stock_research/dashboard/pages/4_Judgments.py`, `tests/dashboard/test_view_models.py`

**Interfaces:**
- Consumes repository queries and `build_company_view(code) -> CompanyView`.
- Produces source-linked display models with `kind` equal to `fact`, `calculation`, or `judgment`.

- [ ] **Step 1: Write failing display-model tests.**

```python
def test_company_view_labels_facts_calculations_and_judgments(repository):
    view = build_company_view(repository, "US.NVDA")
    assert {item.kind for item in view.items} == {"fact", "calculation", "judgment"}
    assert all(item.source_url for item in view.items if item.kind == "fact")
```

- [ ] **Step 2: Run the test.**

Run: `python -m pytest tests/dashboard/test_view_models.py -v`
Expected: FAIL because `build_company_view` is undefined.

- [ ] **Step 3: Implement view models before page rendering.**

```python
@dataclass(frozen=True)
class DisplayItem:
    label: str
    value: str
    kind: Literal["fact", "calculation", "judgment"]
    source_url: str | None
    as_of: datetime | None
    status: Literal["current", "stale", "missing"]
```

- [ ] **Step 4: Implement pages with the defined view-model contract.**

```python
# Company page rendering pattern
st.subheader("Facts")
for item in (x for x in view.items if x.kind == "fact"):
    st.markdown(f"{item.label}: **{item.value}** · [source]({item.source_url})")
st.subheader("Calculations")
st.dataframe(view.calculation_frame)
st.subheader("My judgment")
st.dataframe(view.judgment_frame)
```

The Watchlist page must include price, valuation, growth, cash/debt, next earnings date, estimate-change state, peer range, technical state, and stale-data marker. The Earnings page must show expected versus actual EPS/revenue/EBIT and 1/5/20 trading-day post-earnings returns. The Judgments page must create and list thesis, growth driver, valuation assumption, target weight, invalidation condition, and review outcome. Home must show open dashboard alerts, job health, storage usage, and a cleanup preview/action.

- [ ] **Step 5: Run tests and start a manual local acceptance session.**

Run: `python -m pytest tests/dashboard/test_view_models.py -v && streamlit run src/stock_research/dashboard/Home.py`
Expected: tests pass; browser shows no buy/sell wording and every financial fact has a source link.

- [ ] **Step 6: Commit.**

```bash
git add src/stock_research/dashboard tests/dashboard
git commit -m "feat: add source-linked investment research dashboard"
```

### Task 10: Document local operation, validate the full workflow, and prepare the first release

**Files:**
- Create: `docs/operations.md`, `docs/data-dictionary.md`, `tests/test_cli.py`
- Modify: `README.md`

**Interfaces:**
- Documents exact installation, OpenD connection, backup, restore, retention, and scheduled-operation commands.

- [ ] **Step 1: Write a failing CLI smoke test.**

```python
from typer.testing import CliRunner
from stock_research.cli import app

runner = CliRunner()

def test_init_db_command_creates_all_tables(tmp_path):
    result = runner.invoke(app, ["init-db", "--database", str(tmp_path / "research.sqlite3")])
    assert result.exit_code == 0
    assert "database initialized" in result.stdout.lower()
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `python -m pytest tests/test_cli.py::test_init_db_command_creates_all_tables -v`
Expected: FAIL until the CLI exposes `init-db`.

- [ ] **Step 3: Implement the smallest command path and write operating documentation.**

README must include: Python virtual-environment setup, `pip install -e ".[market,dev]"`, `.env` creation, OpenD start and quote-permission check, `stock-research init-db`, `stock-research sync --watchlist core`, `stock-research run-scheduler`, `streamlit run ...`, local database backup via `sqlite3 var/stock_research.sqlite3 ".backup backup.sqlite3"`, and restore procedure. `docs/data-dictionary.md` must define all stored metrics, calculation formulas, source fields, freshness states, and retention periods.

- [ ] **Step 4: Run the full verification suite.**

Run: `python -m pytest -v && ruff check src tests && python -m stock_research.cli init-db --database /tmp/stock-research-smoke.sqlite3`
Expected: all tests pass; Ruff exits 0; CLI prints `database initialized`.

- [ ] **Step 5: Manually verify the user’s core workflow with a non-production database.**

Run: `STOCK_RESEARCH_DB_PATH=/tmp/stock-research-demo.sqlite3 streamlit run src/stock_research/dashboard/Home.py`
Expected: user can add an example ticker, inspect source-linked facts and calculations, save an invalidation condition, view an earnings-review alert, and preview retention cleanup without deleting a judgment.

- [ ] **Step 6: Commit.**

```bash
git add README.md docs src tests
git commit -m "docs: document research dashboard operation"
```

## Plan Self-Review

- Spec coverage: Tasks 1–4 establish local, read-only, SEC/Moomoo sources; Tasks 5–6 cover facts, calculations, valuations, technical indicators, and earnings expectations; Task 7 enforces every approved retention rule; Task 8 implements local schedules and dashboard-only reminders; Task 9 implements the full multi-stock dashboard and judgment workflow; Task 10 verifies, documents, backs up, and operates the release.
- No unbounded data path remains: every high-volume table has a documented cutoff, a previewable cleanup path, an audit record, and tests.
- Type consistency: `MarketDataProvider`, `Repository`, `SyncService`, `MetricValue`, `RetentionService`, and `DisplayItem` are introduced before their downstream consumers.
