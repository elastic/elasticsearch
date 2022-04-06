/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class RollupActionConfigTests extends AbstractSerializingTestCase<RollupActionConfig> {

    private static final String timezone = "UTC";

    @Override
    protected RollupActionConfig createTestInstance() {
        return randomConfig(random());
    }

    public static RollupActionConfig randomConfig(Random random) {
        return new RollupActionConfig(ConfigTestHelpers.randomInterval(), timezone);
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> new RollupActionConfig(null, randomBoolean() ? timezone : null));
        assertThat(e.getMessage(), equalTo("Parameter [fixed_interval] is required."));
    }

    public void testEmptyTimezone() {
        RollupActionConfig config = new RollupActionConfig(ConfigTestHelpers.randomInterval(), null);
        assertEquals("UTC", config.getTimeZone());
    }

    public void testUnsupportedTimezone() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new RollupActionConfig(ConfigTestHelpers.randomInterval(), "EET"));
        assertThat(e.getMessage(), equalTo("Parameter [time_zone] supports only [UTC]."));
    }
}
