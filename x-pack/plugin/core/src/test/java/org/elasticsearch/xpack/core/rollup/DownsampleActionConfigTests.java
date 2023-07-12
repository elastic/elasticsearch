/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class DownsampleActionConfigTests extends AbstractXContentSerializingTestCase<DownsampleConfig> {

    public static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    @Override
    protected DownsampleConfig createTestInstance() {
        return randomConfig();
    }

    @Override
    protected DownsampleConfig mutateInstance(DownsampleConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DownsampleConfig randomConfig() {
        return new DownsampleConfig(ConfigTestHelpers.randomInterval(), TIMEOUT);
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
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new DownsampleConfig((DateHistogramInterval) null, TIMEOUT)
        );
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimeout() {
        DownsampleConfig config = new DownsampleConfig(ConfigTestHelpers.randomInterval(), null);
        assertEquals(DownsampleConfig.DEFAULT_TIMEOUT, config.getTimeout());
    }

    public void testEmptyTimezone() {
        DownsampleConfig config = new DownsampleConfig(ConfigTestHelpers.randomInterval(), TIMEOUT);
        assertEquals("UTC", config.getTimeZone());
    }
}
