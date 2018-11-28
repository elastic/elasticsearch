/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

public class DataFrameJobConfigTests extends AbstractSerializingFeatureIndexBuilderTestCase<DataFrameJobConfig> {

    private String jobId;

    public static DataFrameJobConfig randomDataFrameJobConfig() {
        return new DataFrameJobConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), SourceConfigTests.randomSourceConfig(),
                AggregationConfigTests.randomAggregationConfig());
    }

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected DataFrameJobConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return DataFrameJobConfig.fromXContent(parser, jobId);
        } else {
            return DataFrameJobConfig.fromXContent(parser, null);
        }
    }

    @Override
    protected DataFrameJobConfig createTestInstance() {
        return randomDataFrameJobConfig();
    }

    @Override
    protected Reader<DataFrameJobConfig> instanceReader() {
        return DataFrameJobConfig::new;
    }
}
