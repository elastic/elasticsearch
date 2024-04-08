/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;

public class DataFrameAnalyticsDestTests extends AbstractBWCSerializationTestCase<DataFrameAnalyticsDest> {

    @Override
    protected DataFrameAnalyticsDest doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsDest.createParser(false).apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsDest createTestInstance() {
        return createRandom();
    }

    @Override
    protected DataFrameAnalyticsDest mutateInstance(DataFrameAnalyticsDest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DataFrameAnalyticsDest createRandom() {
        String index = randomAlphaOfLength(10);
        String resultsField = randomBoolean() ? null : randomAlphaOfLength(10);
        return new DataFrameAnalyticsDest(index, resultsField);
    }

    public static DataFrameAnalyticsDest mutateForVersion(DataFrameAnalyticsDest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsDest> instanceReader() {
        return DataFrameAnalyticsDest::new;
    }

    @Override
    protected DataFrameAnalyticsDest mutateInstanceForVersion(DataFrameAnalyticsDest instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
