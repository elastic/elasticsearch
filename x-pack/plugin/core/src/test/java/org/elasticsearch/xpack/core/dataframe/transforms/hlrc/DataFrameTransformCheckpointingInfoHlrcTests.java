/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfoTests;

import java.io.IOException;

public class DataFrameTransformCheckpointingInfoHlrcTests  extends AbstractHlrcXContentTestCase<
        DataFrameTransformCheckpointingInfo,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo> {

    public static DataFrameTransformCheckpointingInfo fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo instance) {
        return new DataFrameTransformCheckpointingInfo(
                DataFrameTransformCheckpointStatsHlrcTests.fromHlrc(instance.getCurrent()),
                DataFrameTransformCheckpointStatsHlrcTests.fromHlrc(instance.getInProgress()),
                instance.getOperationsBehind());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    public DataFrameTransformCheckpointingInfo convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformCheckpointingInfo instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameTransformCheckpointingInfo createTestInstance() {
        return DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo();
    }

    @Override
    protected DataFrameTransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}