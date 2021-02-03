/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DataFrameAnalyticsDestTests extends AbstractXContentTestCase<DataFrameAnalyticsDest> {

    public static DataFrameAnalyticsDest randomDestConfig() {
        return DataFrameAnalyticsDest.builder()
            .setIndex(randomAlphaOfLengthBetween(1, 10))
            .setResultsField(randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10))
            .build();
    }

    @Override
    protected DataFrameAnalyticsDest doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsDest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected DataFrameAnalyticsDest createTestInstance() {
        return randomDestConfig();
    }
}
