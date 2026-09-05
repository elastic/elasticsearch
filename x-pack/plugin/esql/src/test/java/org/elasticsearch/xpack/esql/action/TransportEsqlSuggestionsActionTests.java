/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.suggestions.CursorOffset;
import org.elasticsearch.xpack.esql.action.suggestions.valuesampling.HotTierValueSampler;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the coordinator-only completion path of the transport action ({@link
 * TransportEsqlSuggestionsAction#suggest}) — the fallback used for a remote-qualified query, and the
 * unit-testable half of the remote-index detection ({@link TransportEsqlSuggestionsAction#hasRemoteTarget}) that
 * decides whether the real, analysis-wired path ({@link TransportEsqlSuggestionsAction#suggestFromAnalyzedPlan})
 * is reachable at all. The full analysis-wired path itself needs a real cluster to resolve indices against; see
 * {@code EsqlSuggestionsActionIT} for that.
 */
public class TransportEsqlSuggestionsActionTests extends ESTestCase {

    public void testPipePositionReturnsEmptyWarnings() {
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query("FROM foo | KEEP a\n")
            .cursor(CursorOffset.utf16("FROM foo | KEEP a\n".length()));
        EsqlSuggestionsResponse response = TransportEsqlSuggestionsAction.suggest(TEST_PARSER, request);
        assertNotNull(response.fields());
        assertThat(response.warnings(), matchesList());
    }

    public void testStringLiteralContextReturnsSkeleton() {
        String query = "FROM foo | WHERE agent == \"as\"";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(CursorOffset.utf16(query.indexOf("as\"") + 1));
        EsqlSuggestionsResponse response = TransportEsqlSuggestionsAction.suggest(TEST_PARSER, request);
        // Single-field literal context: no coordinator-side type is known, so the field map is empty
        // (values would come from a deferred data-node visit).
        assertMap(response.fields(), matchesMap());
        assertThat(response.warnings(), matchesList());
    }

    public void testHasRemoteTargetDetectsClusterQualifiedFrom() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM my_cluster:logs | KEEP a");
        assertThat(TransportEsqlSuggestionsAction.hasRemoteTarget(plan), equalTo(true));
    }

    public void testHasRemoteTargetIgnoresPlainLocalFrom() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM logs,other_logs | KEEP a");
        assertThat(TransportEsqlSuggestionsAction.hasRemoteTarget(plan), equalTo(false));
    }

    // Warnings wiring for the hot-tier value-sampling path.

    public void testWarningsForSampleResultEmptyWhenCompleteAndNoColdSkip() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, false, false, false);
        assertThat(TransportEsqlSuggestionsAction.warningsForSampleResult(result, false), matchesList());
    }

    public void testWarningsForSampleResultAddsShardsSkippedOnPartial() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, true, false, false);
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(result, false),
            matchesList(List.of(EsqlSuggestionsResponse.Warning.SHARDS_SKIPPED))
        );
    }

    public void testWarningsForSampleResultAddsDlsActive() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, false, true, false);
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(result, false),
            matchesList(List.of(EsqlSuggestionsResponse.Warning.DLS_ACTIVE))
        );
    }

    public void testWarningsForSampleResultAddsTimedOut() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, false, false, true);
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(result, false),
            matchesList(List.of(EsqlSuggestionsResponse.Warning.TIMED_OUT))
        );
    }

    public void testWarningsForSampleResultAddsSkippedColdWhenColdIndexSkipped() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, false, false, false);
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(result, true),
            matchesList(List.of(EsqlSuggestionsResponse.Warning.SKIPPED_COLD))
        );
    }

    public void testWarningsForSampleResultCombinesAllFour() {
        HotTierValueSampler.SampleResult result = new HotTierValueSampler.SampleResult(List.of(), 100L, true, true, true);
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(result, true),
            matchesList(
                List.of(
                    EsqlSuggestionsResponse.Warning.SKIPPED_COLD,
                    EsqlSuggestionsResponse.Warning.SHARDS_SKIPPED,
                    EsqlSuggestionsResponse.Warning.DLS_ACTIVE,
                    EsqlSuggestionsResponse.Warning.TIMED_OUT
                )
            )
        );
    }

    public void testNoHotNodesShortCircuitCarriesNoWarningsWithoutColdSkip() {
        // The no-fan-out short-circuit: SampleResult.NO_HOT_NODES itself carries no signals of its own;
        // whether skipped_cold attaches depends entirely on the caller's separate coldSkipped bit.
        assertThat(
            TransportEsqlSuggestionsAction.warningsForSampleResult(HotTierValueSampler.SampleResult.NO_HOT_NODES, false),
            matchesList()
        );
    }
}
