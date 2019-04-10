/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformCheckpointStatsTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpointStats>
{
    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformCheckpointStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformCheckpointStats createTestInstance() {
        return randomDataFrameTransformCheckpointStats();
    }

    @Override
    protected Reader<DataFrameTransformCheckpointStats> instanceReader() {
        return DataFrameTransformCheckpointStats::new;
    }

    public void testPercentageComplete() {
        DataFrameTransformCheckpointStats stats = new DataFrameTransformCheckpointStats(0L, 0L, 0L, 0L);
        assertThat(stats.getPercentageCompleted(), equalTo(1.0));

        stats = new DataFrameTransformCheckpointStats(0L, 0L, 100L, 0L);
        assertThat(stats.getPercentageCompleted(), equalTo(0.0));

        stats = new DataFrameTransformCheckpointStats(0L, 0L, 100L, 50L);
        assertThat(stats.getPercentageCompleted(), closeTo(0.5, 0.0000001));

        stats = new DataFrameTransformCheckpointStats(0L, 0L, 0L, 50L);
        assertThat(stats.getPercentageCompleted(), equalTo(1.0));
    }
}
