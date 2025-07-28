/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataFrameAnalyticsTaskStateTests extends AbstractXContentSerializingTestCase<DataFrameAnalyticsTaskState> {

    @Override
    protected DataFrameAnalyticsTaskState createTestInstance() {
        return new DataFrameAnalyticsTaskState(
            randomFrom(DataFrameAnalyticsState.values()),
            randomLong(),
            randomAlphaOfLength(10),
            randomInstant()
        );
    }

    @Override
    protected DataFrameAnalyticsTaskState mutateInstance(DataFrameAnalyticsTaskState instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
