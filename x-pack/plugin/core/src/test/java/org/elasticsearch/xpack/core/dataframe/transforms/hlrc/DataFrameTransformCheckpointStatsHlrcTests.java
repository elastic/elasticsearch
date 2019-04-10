/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStatsTests;

import java.io.IOException;

public class DataFrameTransformCheckpointStatsHlrcTests extends AbstractHlrcXContentTestCase<
        DataFrameTransformCheckpointStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats> {

    public static DataFrameTransformCheckpointStats fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats instance) {
        return new DataFrameTransformCheckpointStats(instance.getTimestampMillis(), instance.getTimeUpperBoundMillis());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    public DataFrameTransformCheckpointStats convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointStats instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameTransformCheckpointStats createTestInstance() {
        return DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats();
    }

    @Override
    protected DataFrameTransformCheckpointStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
