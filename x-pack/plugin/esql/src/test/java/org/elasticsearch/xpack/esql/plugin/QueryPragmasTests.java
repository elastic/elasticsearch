/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Verifies that the time-series target-chunk-rows pragma is read independently from the regular aggregation pragmas,
 * keeping the time-series chunking knob fully decoupled.
 */
public class QueryPragmasTests extends ESTestCase {

    public void testTimeSeriesTargetChunkRowsPragmaOverridesDefault() {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 4_096).build()
        );
        assertThat(pragmas.timeSeriesTargetChunkRows(10_000), equalTo(4_096));
    }

    public void testTimeSeriesTargetChunkRowsIsValidPragmaName() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey()));
    }

    public void testTimeSeriesTargetChunkRowsPragmaFallsBackToProvidedDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 1_000_000);
        assertThat(pragmas.timeSeriesTargetChunkRows(clusterDefault), equalTo(clusterDefault));
    }

    public void testMaxBranchCountPerMergeIsValidPragmaName() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE.getKey()));
    }

    public void testMaxBranchCountPerMergePragmaOverridesDefault() {
        int clusterDefault = 8;
        int override = 3;
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE.getKey(), override).build());
        assertThat(pragmas.maxBranchCountPerMerge(clusterDefault), equalTo(override));
        assertThat(
            pragmas.maxBranchCountPerMergeLimitSource("esql.query.max_branch_count_per_merge"),
            equalTo("[max_branch_count_per_merge] query pragma")
        );
    }

    public void testMaxBranchCountPerMergeFallsBackToClusterDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 50);
        assertThat(pragmas.maxBranchCountPerMerge(clusterDefault), equalTo(clusterDefault));
        assertThat(
            pragmas.maxBranchCountPerMergeLimitSource("esql.query.max_branch_count_per_merge"),
            equalTo("[esql.query.max_branch_count_per_merge] cluster setting")
        );
    }

    public void testShufflePragmas() {
        QueryPragmas defaults = QueryPragmas.EMPTY;
        assertThat(defaults.shufflePartitionsPerNode(), equalTo(0));
        assertFalse(defaults.shuffleAgg());
        assertFalse(defaults.shuffleJoin());
        assertThat(defaults.shuffleJoinStrategy(), equalTo("partitioned"));
        assertFalse(defaults.shuffleEnabled());

        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 3)
                .put(QueryPragmas.SHUFFLE_AGG.getKey(), true)
                .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
                .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast")
                .build()
        );
        assertThat(pragmas.shufflePartitionsPerNode(), equalTo(3));
        assertTrue(pragmas.shuffleAgg());
        assertTrue(pragmas.shuffleJoin());
        assertThat(pragmas.shuffleJoinStrategy(), equalTo("broadcast"));
        assertTrue(pragmas.shuffleEnabled());
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey()));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.SHUFFLE_AGG.getKey()));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.SHUFFLE_JOIN.getKey()));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey()));
    }

    public void testCboPragmas() {
        QueryPragmas defaults = QueryPragmas.EMPTY;
        assertFalse(defaults.cbo());
        assertFalse(defaults.cboTrace());
        assertFalse(defaults.shuffleAggExplicit());
        assertFalse(defaults.shuffleJoinExplicit());
        assertFalse(defaults.shuffleJoinStrategyExplicit());

        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.CBO.getKey(), true)
                .put(QueryPragmas.CBO_TRACE.getKey(), true)
                .put(QueryPragmas.SHUFFLE_AGG.getKey(), false)
                .put(QueryPragmas.SHUFFLE_JOIN.getKey(), false)
                .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast")
                .build()
        );
        assertTrue(pragmas.cbo());
        assertTrue(pragmas.cboTrace());
        assertTrue(pragmas.shuffleAggExplicit());
        assertTrue(pragmas.shuffleJoinExplicit());
        assertTrue(pragmas.shuffleJoinStrategyExplicit());
        assertThat(pragmas.shuffleJoinStrategy(), equalTo("broadcast"));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.CBO.getKey()));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.CBO_TRACE.getKey()));
    }

    public void testTimeSeriesPragmaIsDecoupledFromRegularAggregationPragmas() {
        // Only the regular pragma is set: it reaches the regular accessor and leaves the time-series one at its default.
        QueryPragmas regularOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 333).build()
        );
        assertThat(regularOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(333));
        assertThat(regularOnly.timeSeriesTargetChunkRows(10_000), equalTo(10_000));

        // Only the time-series pragma is set: it reaches the time-series accessor and leaves the regular one at its default.
        QueryPragmas timeSeriesOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 999).build()
        );
        assertThat(timeSeriesOnly.timeSeriesTargetChunkRows(10_000), equalTo(999));
        assertThat(timeSeriesOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(100_000));
    }
}
