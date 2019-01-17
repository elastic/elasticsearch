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
import java.util.HashMap;
import java.util.Map;

public class DataFrameAnalysisConfigTests extends AbstractSerializingTestCase<DataFrameAnalysisConfig> {

    @Override
    protected DataFrameAnalysisConfig createTestInstance() {
        return randomConfig();
    }

    @Override
    protected DataFrameAnalysisConfig doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalysisConfig.parser().parse(parser, null);
    }

    @Override
    protected Writeable.Reader<DataFrameAnalysisConfig> instanceReader() {
        return DataFrameAnalysisConfig::new;
    }

    public static DataFrameAnalysisConfig randomConfig() {
        Map<String, Object> configParams = new HashMap<>();
        int count = randomIntBetween(1, 5);
        for (int i = 0; i < count; i++) {
            if (randomBoolean()) {
                configParams.put(randomAlphaOfLength(10), randomInt());
            } else {
                configParams.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
        }
        Map<String, Object> config = new HashMap<>();
        config.put(randomAlphaOfLength(10), configParams);
        return new DataFrameAnalysisConfig(config);
    }
}
