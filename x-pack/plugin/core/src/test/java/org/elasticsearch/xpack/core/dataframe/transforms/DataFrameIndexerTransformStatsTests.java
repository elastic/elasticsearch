/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameIndexerTransformStatsTests extends AbstractSerializingTestCase<DataFrameIndexerTransformStats> {

    @Override
    protected DataFrameIndexerTransformStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<DataFrameIndexerTransformStats> instanceReader() {
        return DataFrameIndexerTransformStats::new;
    }

    @Override
    protected DataFrameIndexerTransformStats doParseInstance(XContentParser parser) {
        return DataFrameIndexerTransformStats.fromXContent(parser);
    }

    public static DataFrameIndexerTransformStats randomStats() {
        Long continuousCheckpointsProcessed = randomBoolean() ? null : randomLongBetween(0, 1000L);
        return new DataFrameIndexerTransformStats(randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), continuousCheckpointsProcessed,
            continuousCheckpointsProcessed != null && continuousCheckpointsProcessed > 0 && randomBoolean() ? randomDouble() : null,
            continuousCheckpointsProcessed != null && continuousCheckpointsProcessed > 0 && randomBoolean() ? randomDouble() : null,
            continuousCheckpointsProcessed != null && continuousCheckpointsProcessed > 0 && randomBoolean() ? randomDouble() : null);
    }

    public void testExpAvgIncrement() {
        DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();

        assertThat(stats.getContinuousCheckpointsProcessed(), equalTo(0L));
        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(0.0));

        stats.incrementCheckpointExponentialAverages(100, 20, 50);

        assertThat(stats.getContinuousCheckpointsProcessed(), equalTo(1L));
        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(100.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(20.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(50.0));

        stats.incrementCheckpointExponentialAverages(150, 23, 100);

        assertThat(stats.getContinuousCheckpointsProcessed(), equalTo(2L));
        assertThat(stats.getExpAvgCheckpointDurationMs(), closeTo(133.333333333, 0.0000001));
        assertThat(stats.getExpAvgDocumentsIndexed(), closeTo(22.0, 0.0000001));
        assertThat(stats.getExpAvgDocumentsProcessed(), closeTo(83.3333333333, 0.0000001));
    }
}
