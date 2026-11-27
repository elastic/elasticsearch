/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexingStatsTests extends ESTestCase {

    private static final double DOUBLE_TOLERANCE = 1.0e-10;

    /**
     * The in-place doc-values update counter is gated in the rendered response: with the feature enabled it is always present (carrying
     * its value, including zero), so the field ships with the feature.
     */
    public void testInPlaceDocValuesUpdateStatRenderedWhenFeatureEnabled() throws IOException {
        assumeTrue("requires the updatable doc-values feature flag", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());
        assertThat(renderedInPlaceCount(statsWithInPlaceCount(0)), equalTo(0L));
        assertThat(renderedInPlaceCount(statsWithInPlaceCount(7)), equalTo(7L));
    }

    /**
     * The rendering gate itself, exercised for both flag states in the normal unit suite (it takes the flag state as a parameter, so it
     * does not depend on the JVM-wide feature flag): with the feature enabled the field is always rendered, and with it disabled the field
     * is rendered only for a nonzero count, so a feature-disabled coordinating node still reports counts aggregated from feature-enabled
     * shard nodes rather than dropping them. Reverting the nonzero clause would flip the {@code (false, 7)} case and fail here.
     */
    public void testInPlaceDocValuesUpdateStatRenderingGate() {
        assertTrue(IndexingStats.Stats.includeInPlaceDocValuesUpdateStat(true, 0));
        assertTrue(IndexingStats.Stats.includeInPlaceDocValuesUpdateStat(true, 7));
        assertFalse(IndexingStats.Stats.includeInPlaceDocValuesUpdateStat(false, 0));
        assertTrue(IndexingStats.Stats.includeInPlaceDocValuesUpdateStat(false, 7));
    }

    private static IndexingStats.Stats statsWithInPlaceCount(long inPlaceCount) {
        return new IndexingStats.Stats(1, 2, 3, 4, 5, 0, 6, 7, 8, 9, inPlaceCount, false, 10, 11, 11, 12, 0.1357, 0.2468);
    }

    private static Long renderedInPlaceCount(IndexingStats.Stats stats) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> map = convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Object value = map.get("in_place_doc_values_update_total");
        return value == null ? null : ((Number) value).longValue();
    }

    public void testStatsGetWriteLoad() {
        IndexingStats.Stats stats = new IndexingStats.Stats(
            1,
            2,
            3,
            4,
            5,
            0,
            6,
            7,
            8,
            9,
            false,
            10,
            1_800_000_000L, // totalIndexingTimeSinceShardStartedInNanos - 1.8sec
            1_800_000_000L, // totalIndexingExecutionTimeSinceShardStartedInNanos - 1.8sec
            3_000_000_000L, // totalActiveTimeInNanos - 3sec
            0.1357,
            0.2468
        );
        double expectedWriteLoad = 0.6; // 1.8sec / 3sec
        assertThat(stats.getWriteLoad(), closeTo(expectedWriteLoad, DOUBLE_TOLERANCE));
    }

    public void testStatsAdd_indexCount() {
        IndexingStats.Stats stats1 = new IndexingStats.Stats(
            1001L, // indexCount
            2,
            3,
            4,
            5,
            0,
            6,
            7,
            8,
            9,
            false,
            10,
            11,
            11,
            12,
            0.1357,
            0.2468
        );
        IndexingStats.Stats stats2 = new IndexingStats.Stats(
            2001L, // indexCount
            2,
            3,
            4,
            5,
            0,
            6,
            7,
            8,
            9,
            false,
            10,
            11,
            11,
            12,
            0.1357,
            0.2468
        );
        IndexingStats.Stats statsAgg = sumOfStats(stats1, stats2);
        assertThat(statsAgg.getIndexCount(), equalTo(1001L + 2001L));
    }

    public void testStatsAdd_throttled() {
        IndexingStats.Stats statsFalse = new IndexingStats.Stats(1, 2, 3, 4, 5, 0, 6, 7, 8, 9, false, 10, 11, 11, 12, 0.1357, 0.2468);
        IndexingStats.Stats statsTrue = new IndexingStats.Stats(1, 2, 3, 4, 5, 0, 6, 7, 8, 9, true, 10, 11, 11, 12, 0.1357, 0.2468);
        assertThat(sumOfStats(statsFalse, statsFalse).isThrottled(), is(false));
        assertThat(sumOfStats(statsFalse, statsTrue).isThrottled(), is(true));
        assertThat(sumOfStats(statsTrue, statsFalse).isThrottled(), is(true));
        assertThat(sumOfStats(statsTrue, statsTrue).isThrottled(), is(true));
    }

    public void testStatsAdd_writeLoads() {
        IndexingStats.Stats stats1 = new IndexingStats.Stats(
            1,
            2,
            3,
            4,
            5,
            0,
            6,
            7,
            8,
            9,
            false,
            10,
            1_000_000_000L, // totalIndexingTimeSinceShardStartedInNanos - 1sec
            1_000_000_000L, // totalIndexingLoadSinceShardStartedInNanos - 1sec
            2_000_000_000L, // totalActiveTimeInNanos - 2sec
            0.1357, // recentWriteLoad
            0.3579 // peakWriteLoad
        );
        IndexingStats.Stats stats2 = new IndexingStats.Stats(
            2,
            2,
            3,
            4,
            5,
            0,
            6,
            7,
            8,
            9,
            false,
            10,
            2_100_000_000L, // totalIndexingTimeSinceShardStartedInNanos - 2.1sec
            2_100_000_000L, // totalIndexingTimeSinceShardStartedInNanos - 2.1sec
            3_000_000_000L, // totalActiveTimeInNanos - 3sec
            0.2468, // recentWriteLoad
            0.5791 // peakWriteLoad
        );
        IndexingStats.Stats statsAgg = sumOfStats(stats1, stats2);
        // The unweighted write loads for the two shards are 0.5 (1sec / 2sec) and 0.7 (2.1sec / 3sec) respectively.
        // The aggregated value should be the average weighted by the times, i.e. by 2sec and 3sec, giving weights of 0.4 and 0.6.
        double expectedWriteLoad = 0.4 * 0.5 + 0.6 * 0.7;
        // The aggregated value for the recent and peak write loads should be the average with the same weights.
        double expectedRecentWriteLoad = 0.4 * 0.1357 + 0.6 * 0.2468;
        double expectedPeakWriteLoad = 0.4 * 0.3579 + 0.6 * 0.5791;
        assertThat(statsAgg.getWriteLoad(), closeTo(expectedWriteLoad, DOUBLE_TOLERANCE));
        assertThat(statsAgg.getRecentWriteLoad(), closeTo(expectedRecentWriteLoad, DOUBLE_TOLERANCE));
        assertThat(statsAgg.getPeakWriteLoad(), closeTo(expectedPeakWriteLoad, DOUBLE_TOLERANCE));
    }

    private static IndexingStats.Stats sumOfStats(IndexingStats.Stats stats1, IndexingStats.Stats stats2) {
        IndexingStats.Stats statsAgg = new IndexingStats.Stats();
        statsAgg.add(stats1);
        statsAgg.add(stats2);
        return statsAgg;
    }
}
