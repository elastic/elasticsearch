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

public class DataFrameAnalyticsDestTests extends AbstractSerializingTestCase<DataFrameAnalyticsDest> {

    @Override
    protected DataFrameAnalyticsDest doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsDest.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsDest createTestInstance() {
        return createRandom();
    }

    public static DataFrameAnalyticsDest createRandom() {
        return new DataFrameAnalyticsDest(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsDest> instanceReader() {
        return DataFrameAnalyticsDest::new;
    }
}
