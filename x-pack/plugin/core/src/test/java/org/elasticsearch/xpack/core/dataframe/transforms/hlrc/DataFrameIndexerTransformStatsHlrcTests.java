/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStatsTests;

import java.io.IOException;

public class DataFrameIndexerTransformStatsHlrcTests extends AbstractHlrcXContentTestCase<
        DataFrameIndexerTransformStats,
        org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats> {

    public static DataFrameIndexerTransformStats fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats instance) {
        return DataFrameIndexerTransformStats.withDefaultTransformId(instance.getNumPages(), instance.getNumDocuments(),
                instance.getOutputDocuments(), instance.getNumInvocations(), instance.getIndexTime(), instance.getSearchTime(),
                instance.getIndexTotal(), instance.getSearchTotal(), instance.getIndexFailures(), instance.getSearchFailures());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats.fromXContent(parser);
    }

    @Override
    public DataFrameIndexerTransformStats convertHlrcToInternal(
            org.elasticsearch.client.dataframe.transforms.DataFrameIndexerTransformStats instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameIndexerTransformStats createTestInstance() {
        return DataFrameIndexerTransformStatsTests.randomStats(DataFrameIndexerTransformStats.DEFAULT_TRANSFORM_ID);
    }

    @Override
    protected DataFrameIndexerTransformStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameIndexerTransformStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

}
