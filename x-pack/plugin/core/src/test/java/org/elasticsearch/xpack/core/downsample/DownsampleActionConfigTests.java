/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.Writeable;
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
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DownsampleConfig randomConfig() {
        Integer forceMergeMaxNumSegments = randomBoolean() ? null : randomIntBetween(-1, 128);
        return new DownsampleConfig(ConfigTestHelpers.randomInterval(), forceMergeMaxNumSegments);
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> new DownsampleConfig(null, null));
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        Integer forceMergeMaxNumSegments = randomBoolean() ? null : randomIntBetween(-1, 128);
        DownsampleConfig config = new DownsampleConfig(ConfigTestHelpers.randomInterval(), forceMergeMaxNumSegments);
        assertEquals("UTC", config.getTimeZone());
    }
}
