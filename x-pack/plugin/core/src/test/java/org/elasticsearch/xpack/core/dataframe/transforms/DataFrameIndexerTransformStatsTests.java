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
        return new DataFrameIndexerTransformStats(randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null);
    }

    public void testExpAvgIncrement() {
        DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(0.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(0.0));

        stats.incrementCheckpointExponentialAverages(100, 20, 50);

        assertThat(stats.getExpAvgCheckpointDurationMs(), equalTo(100.0));
        assertThat(stats.getExpAvgDocumentsIndexed(), equalTo(20.0));
        assertThat(stats.getExpAvgDocumentsProcessed(), equalTo(50.0));

        stats.incrementCheckpointExponentialAverages(150, 23, 100);

        assertThat(stats.getExpAvgCheckpointDurationMs(), closeTo(109.090909, 0.0000001));
        assertThat(stats.getExpAvgDocumentsIndexed(), closeTo(20.54545454, 0.0000001));
        assertThat(stats.getExpAvgDocumentsProcessed(), closeTo(59.0909090, 0.0000001));
    }
}
