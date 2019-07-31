/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DataFrameAnalyticsTaskStateTests extends AbstractSerializingTestCase<DataFrameAnalyticsTaskState> {

    @Override
    protected DataFrameAnalyticsTaskState createTestInstance() {
        return new DataFrameAnalyticsTaskState(randomFrom(DataFrameAnalyticsState.values()), randomLong(), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsTaskState> instanceReader() {
        return DataFrameAnalyticsTaskState::new;
    }

    @Override
    protected DataFrameAnalyticsTaskState doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsTaskState.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
