/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.hlrc;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcXContentTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformState;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateTests;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformTaskState;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.function.Predicate;

public class DataFrameTransformStateHlrcTests extends AbstractHlrcXContentTestCase<DataFrameTransformState,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformState> {

    public static DataFrameTransformState fromHlrc(org.elasticsearch.client.dataframe.transforms.DataFrameTransformState instance) {
        return new DataFrameTransformState(DataFrameTransformTaskState.fromString(instance.getTaskState().value()),
                IndexerState.fromString(instance.getIndexerState().value()), instance.getPosition(), instance.getGeneration(),
                instance.getReason());
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformState doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformState.fromXContent(parser);
    }

    @Override
    public DataFrameTransformState convertHlrcToInternal(org.elasticsearch.client.dataframe.transforms.DataFrameTransformState instance) {
        return fromHlrc(instance);
    }

    @Override
    protected DataFrameTransformState createTestInstance() {
        return DataFrameTransformStateTests.randomDataFrameTransformState();
    }

    @Override
    protected DataFrameTransformState doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformState.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("current_position");
    }
}
