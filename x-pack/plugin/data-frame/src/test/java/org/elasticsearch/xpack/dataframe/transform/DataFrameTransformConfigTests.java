/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transform;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.dataframe.transform.DataFrameTransformConfig;
import org.junit.Before;

import java.io.IOException;

public class DataFrameTransformConfigTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfig> {

    private String transformId;

    public static DataFrameTransformConfig randomDataFrameTransformConfig() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), SourceConfigTests.randomSourceConfig(),
                AggregationConfigTests.randomAggregationConfig());
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected DataFrameTransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return DataFrameTransformConfig.fromXContent(parser, transformId);
        } else {
            return DataFrameTransformConfig.fromXContent(parser, null);
        }
    }

    @Override
    protected DataFrameTransformConfig createTestInstance() {
        return randomDataFrameTransformConfig();
    }

    @Override
    protected Reader<DataFrameTransformConfig> instanceReader() {
        return DataFrameTransformConfig::new;
    }
}
