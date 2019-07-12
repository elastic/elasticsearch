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

public class DataFrameTransformStateAndStatsInfoTests extends AbstractSerializingTestCase<DataFrameTransformStateAndStatsInfo> {

    public static DataFrameTransformStateAndStatsInfo randomDataFrameTransformStateAndStatsInfo() {
        return new DataFrameTransformStateAndStatsInfo(randomAlphaOfLength(10),
            randomFrom(DataFrameTransformTaskState.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            DataFrameIndexerTransformStatsTests.randomStats(DataFrameIndexerTransformStats.DEFAULT_TRANSFORM_ID),
            DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected DataFrameTransformStateAndStatsInfo doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStateAndStatsInfo.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformStateAndStatsInfo createTestInstance() {
        return randomDataFrameTransformStateAndStatsInfo();
    }

    @Override
    protected Reader<DataFrameTransformStateAndStatsInfo> instanceReader() {
        return DataFrameTransformStateAndStatsInfo::new;
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
