/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class DataFrameTransformCheckpointStatsTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpointStats>
{
    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomLongBetween(1, 1_000_000),
            DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomBoolean() ? null : DataFrameTransformProgressTests.randomDataFrameTransformProgress(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
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
}
