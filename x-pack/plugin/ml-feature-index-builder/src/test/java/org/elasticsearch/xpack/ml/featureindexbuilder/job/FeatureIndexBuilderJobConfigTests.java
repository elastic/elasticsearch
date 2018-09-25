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

public class FeatureIndexBuilderJobConfigTests extends AbstractSerializingFeatureIndexBuilderTestCase<FeatureIndexBuilderJobConfig> {

    private String jobId;

    public static FeatureIndexBuilderJobConfig randomFeatureIndexBuilderJobConfig() {
        return new FeatureIndexBuilderJobConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), null, AggregationConfigTests.randonAggregationConfig());
    }

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected FeatureIndexBuilderJobConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return FeatureIndexBuilderJobConfig.fromXContent(parser, jobId);
        } else {
            return FeatureIndexBuilderJobConfig.fromXContent(parser, null);
        }
    }

    @Override
    protected FeatureIndexBuilderJobConfig createTestInstance() {
        return randomFeatureIndexBuilderJobConfig();
    }

    @Override
    protected Reader<FeatureIndexBuilderJobConfig> instanceReader() {
        return FeatureIndexBuilderJobConfig::new;
    }
}
