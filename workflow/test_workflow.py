"""
Tests for the Chainalysis Media Monitor daily workflow.
Migrated to the new test pattern (post-Workflows launch, June 2026):
  - Uses _wrap_for_test() to apply @durable_execution
  - Injects test input via the wrapper, not runner.run(input=)
"""

import json

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestRunner,
    DurableFunctionTestResult,
)
from workflow import handler


def _wrap_for_test(test_input: dict):
    """Apply @durable_execution and substitute test_input on empty events."""
    @durable_execution
    def runner_handler(event, context):
        return handler(event if event else test_input, context)
    return runner_handler


def test_daily_full_workflow():
    """Test the default daily run with all outputs enabled."""
    runner_handler = _wrap_for_test({
        "days_back": 1,
        "include_html": True,
        "include_slack": True,
        "include_competitors": True,
        "include_email": True,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result: DurableFunctionTestResult = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED, f"Failed: {result.result}"
    output = json.loads(result.result)
    assert output["status"] in ("completed", "no_articles")
    assert output["period_days"] == 1

    if output["status"] == "completed":
        # Core fields
        assert "classification_stats" in output
        assert "executive_summary" in output
        assert "chainalysis_mentions" in output
        assert "narrative_coverage" in output

        # HTML + email digests
        assert "html_digest" in output
        assert "email_digest" in output
        assert "CHAINALYSIS NEWS DIGEST" in output["email_digest"]

        # Slack payload + delivery
        assert "slack_summary" in output
        assert "text" in output["slack_summary"]
        assert "blocks" in output["slack_summary"]
        assert "slack_delivery" in output
        # Without SLACK_WEBHOOK_URL configured, delivery should gracefully skip
        assert output["slack_delivery"]["sent"] is False
        assert "not configured" in output["slack_delivery"]["reason"]

        # Google News stats
        assert "google_news_publications" in output
        assert "google_news_publication_count" in output

        # Competitor intelligence
        assert "competitor_intelligence" in output
        comp = output["competitor_intelligence"]
        assert "summary" in comp
        assert "highlights" in comp
        assert "stats" in comp

        # HTML should include competitor section
        assert "Competitor Intelligence" in output["html_digest"]

        # Stats structure
        stats = output["classification_stats"]
        assert "total_articles_gnews" in stats
        assert "total_articles_rss" in stats
        assert "total_unique" in stats

        print(f"Mentions: {stats['chainalysis_mentions']}")
        print(f"Narrative: {stats['narrative_relevant']}")
        print(f"Slack blocks: {len(output['slack_summary']['blocks'])}")


def test_weekly_catchup():
    """Test weekly mode with strategic digest."""
    runner_handler = _wrap_for_test({
        "mode": "weekly",
        "include_html": True,
        "include_slack": True,
        "include_competitors": True,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)
    assert output["status"] in ("completed", "no_articles")
    assert output["mode"] == "weekly"
    assert output["period_days"] == 7  # weekly default

    if output["status"] == "completed":
        # Weekly email should be strategic briefing
        assert "email_digest" in output
        assert "WEEKLY BRIEFING" in output["email_digest"]

        # Weekly Slack should use strategic format
        assert "slack_summary" in output
        assert "Weekly Briefing" in output["slack_summary"]["text"]


def test_without_optional_outputs():
    """Test with all optional outputs disabled."""
    runner_handler = _wrap_for_test({
        "days_back": 1,
        "include_html": False,
        "include_slack": False,
        "include_competitors": False,
        "include_email": False,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)
    assert output["status"] in ("completed", "no_articles")

    if output["status"] == "completed":
        # Should still have core analysis
        assert "chainalysis_mentions" in output
        assert "narrative_coverage" in output
        assert "executive_summary" in output
        # No optional outputs
        assert "html_digest" not in output
        assert "slack_summary" not in output
        assert "email_digest" not in output
        assert "competitor_intelligence" not in output


def test_default_is_daily():
    """Verify that the default mode is daily with days_back=1."""
    runner_handler = _wrap_for_test({})
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)
    assert output["period_days"] == 1
    assert output.get("mode") == "daily"


def test_feed_stats_present():
    """Test that feed stats cover all configured feeds."""
    runner_handler = _wrap_for_test({
        "days_back": 1,
        "include_html": False,
        "include_slack": False,
        "include_email": False,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)
    assert "feed_stats" in output

    expected_feeds = [
        "coindesk", "cointelegraph", "the_block", "decrypt", "dl_news", "chainalysis_blog",
        "protos", "unchained", "beincrypto", "bitcoin_magazine", "crypto_briefing",
        "the_defiant", "cryptoslate", "cryptonews",
        "techcrunch_fintech", "finextra",
        "crowdfund_insider", "compliance_week",
        # Google News proxy feeds
        "blockworks_gn", "wsj_gn", "ledger_insights_gn",
        "forbes_crypto_gn", "fortune_crypto_gn",
    ]
    for feed in expected_feeds:
        assert feed in output["feed_stats"], f"Missing feed stats for {feed}"


def test_slack_payload_structure():
    """Verify that Slack payload has valid Block Kit structure."""
    runner_handler = _wrap_for_test({
        "days_back": 1,
        "include_slack": True,
        "include_html": False,
        "include_email": False,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)

    if output["status"] == "completed":
        slack = output["slack_summary"]
        assert isinstance(slack["text"], str)
        assert isinstance(slack["blocks"], list)
        assert len(slack["blocks"]) > 0

        # First block should be header
        assert slack["blocks"][0]["type"] == "header"

        # Should have context footer
        context_blocks = [b for b in slack["blocks"] if b["type"] == "context"]
        assert len(context_blocks) > 0

        # Verify all blocks have valid types
        valid_types = {"header", "section", "divider", "context", "actions", "image"}
        for block in slack["blocks"]:
            assert block["type"] in valid_types, f"Invalid block type: {block['type']}"


def test_trend_tracking_without_history():
    """Verify narratives get trend data even when no history is available."""
    runner_handler = _wrap_for_test({
        "days_back": 1,
        "include_html": False,
        "include_slack": False,
        "include_email": False,
    })
    with DurableFunctionTestRunner(handler=runner_handler) as runner:
        result = runner.run(timeout=180)

    assert result.status is InvocationStatus.SUCCEEDED
    output = json.loads(result.result)

    if output["status"] == "completed":
        narratives = output["key_narratives"]
        assert len(narratives) > 0

        for n in narratives:
            # Every narrative must have trend data
            assert "trend" in n, f"Missing trend data for: {n['title']}"
            trend = n["trend"]
            assert "appearances" in trend
            assert "cadence" in trend
            assert "momentum" in trend
            assert "is_trending" in trend
            assert isinstance(trend["is_trending"], bool)

            # With no history, all should be "new"
            assert trend["momentum"] == "new"
            assert trend["cadence"] == "new"
            assert trend["appearances"] == 1

            # Article links should still be present
            assert "articles" in n
            assert "chainalysis_relevance" in n

            print(f"  {n['title']}: cadence={trend['cadence']}, momentum={trend['momentum']}")
