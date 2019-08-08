/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class DataFrameTransformStatsTests extends AbstractSerializingTestCase<DataFrameTransformStats> {

    public static DataFrameTransformStats randomDataFrameTransformStats() {
        return new DataFrameTransformStats(randomAlphaOfLength(10),
            randomFrom(DataFrameTransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            DataFrameIndexerTransformStatsTests.randomStats(),
            DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected DataFrameTransformStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStats.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformStats createTestInstance() {
        return randomDataFrameTransformStats();
    }

    @Override
    protected Reader<DataFrameTransformStats> instanceReader() {
        return DataFrameTransformStats::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "position" };
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }
}
