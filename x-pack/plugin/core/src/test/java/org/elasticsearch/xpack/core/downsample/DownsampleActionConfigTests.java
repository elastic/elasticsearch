/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DownsampleActionConfigTests extends AbstractXContentSerializingTestCase<DownsampleConfig> {

    @Override
    protected DownsampleConfig createTestInstance() {
        return randomConfig();
    }

    @Override
    protected DownsampleConfig mutateInstance(DownsampleConfig instance) {
        var interval = instance.getFixedInterval();
        var samplingMode = instance.getSamplingMethod();
        switch (between(0, 1)) {
            case 0 -> interval = randomValueOtherThan(interval, ConfigTestHelpers::randomInterval);
            case 1 -> samplingMode = randomValueOtherThan(samplingMode, DownsampleActionConfigTests::randomSamplingMethod);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new DownsampleConfig(interval, samplingMode);
    }

    public static DownsampleConfig randomConfig() {
        return new DownsampleConfig(ConfigTestHelpers.randomInterval(), randomSamplingMethod());
    }

    private static DownsampleConfig.SamplingMethod randomSamplingMethod() {
        return randomBoolean() ? null : randomFrom(DownsampleConfig.SamplingMethod.AGGREGATE, DownsampleConfig.SamplingMethod.LAST_VALUE);
    }

    @Override
    protected Writeable.Reader<DownsampleConfig> instanceReader() {
        return DownsampleConfig::new;
    }

    @Override
    protected DownsampleConfig doParseInstance(final XContentParser parser) throws IOException {
        return DownsampleConfig.fromXContent(parser);
    }

    public void testEmptyFixedInterval() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new DownsampleConfig((DateHistogramInterval) null));
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        DownsampleConfig config = new DownsampleConfig(ConfigTestHelpers.randomInterval());
        assertEquals("UTC", config.getTimeZone());
    }
}
