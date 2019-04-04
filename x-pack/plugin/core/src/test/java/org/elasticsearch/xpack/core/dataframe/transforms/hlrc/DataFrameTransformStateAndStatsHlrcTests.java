/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStatsTests;

import java.io.IOException;
import java.util.function.Predicate;

public class DataFrameTransformStateAndStatsHlrcTests extends AbstractHlrcXContentTestCase<DataFrameTransformStateAndStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats> {

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats.fromXContent(parser);
    }

    @Override
    public DataFrameTransformStateAndStats convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformStateAndStats instance) {
        return new DataFrameTransformStateAndStats(instance.getId(),
                DataFrameTransformStateHlrcTests.fromHlrc(instance.getTransformState()),
                DataFrameIndexerTransformStatsHlrcTests.fromHlrc(instance.getTransformStats()),
                DataFrameTransformCheckpointingInfoHlrcTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    @Override
    protected DataFrameTransformStateAndStats createTestInstance() {
        // the transform id is not part of HLRC as it's only to a field for internal storage, therefore use a default id
        return DataFrameTransformStateAndStatsTests
                .randomDataFrameTransformStateAndStats(DataFrameIndexerTransformStats.DEFAULT_TRANSFORM_ID);
    }

    @Override
    protected DataFrameTransformStateAndStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStateAndStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("state.current_position");
    }
}

