/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DataFrameAnalyticsConfigTests extends AbstractSerializingTestCase<DataFrameAnalyticsConfig> {

    @Override
    protected DataFrameAnalyticsConfig doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null).build();
    }

    @Override
    protected DataFrameAnalyticsConfig createTestInstance() {
        return createRandom(randomValidId());
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsConfig> instanceReader() {
        return DataFrameAnalyticsConfig::new;
    }

    public static DataFrameAnalyticsConfig createRandom(String id) {
        String source = randomAlphaOfLength(10);
        String dest = randomAlphaOfLength(10);
        List<DataFrameAnalysisConfig> analyses = Collections.singletonList(DataFrameAnalysisConfigTests.randomConfig());
        return new DataFrameAnalyticsConfig(id, source, dest, analyses);
    }

    public static String randomValidId() {
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }
}
