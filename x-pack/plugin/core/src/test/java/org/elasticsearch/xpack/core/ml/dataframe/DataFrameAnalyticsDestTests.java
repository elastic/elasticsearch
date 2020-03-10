/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
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

    public static DataFrameAnalyticsDest createRandom() {
        String index = randomAlphaOfLength(10);
        String resultsField = randomBoolean() ? null : randomAlphaOfLength(10);
        return new DataFrameAnalyticsDest(index, resultsField);
    }

    public static DataFrameAnalyticsDest mutateForVersion(DataFrameAnalyticsDest instance, Version version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsDest> instanceReader() {
        return DataFrameAnalyticsDest::new;
    }

    @Override
    protected DataFrameAnalyticsDest mutateInstanceForVersion(DataFrameAnalyticsDest instance, Version version) {
        return mutateForVersion(instance, version);
    }
}
