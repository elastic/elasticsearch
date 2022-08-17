/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class RollupActionConfigTests extends AbstractSerializingTestCase<RollupActionConfig> {

    @Override
    protected RollupActionConfig createTestInstance() {
        return randomConfig();
    }

    public static RollupActionConfig randomConfig() {
        return new RollupActionConfig(ConfigTestHelpers.randomInterval());
    }

    @Override
    protected Writeable.Reader<RollupActionConfig> instanceReader() {
        return RollupActionConfig::new;
    }

    @Override
    protected RollupActionConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupActionConfig.fromXContent(parser);
    }

    public void testEmptyFixedInterval() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new RollupActionConfig((DateHistogramInterval) null));
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        RollupActionConfig config = new RollupActionConfig(ConfigTestHelpers.randomInterval());
        assertEquals("UTC", config.getTimeZone());
    }
}
