/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TimeSyncConfigTests extends AbstractSerializingTestCase<TimeSyncConfig> {

    public static TimeSyncConfig randomTimeSyncConfig() {
        return new TimeSyncConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomNonNegativeLong()));
    }

    @Override
    protected TimeSyncConfig doParseInstance(XContentParser parser) throws IOException {
        return TimeSyncConfig.fromXContent(parser, false);
    }

    @Override
    protected TimeSyncConfig createTestInstance() {
        return randomTimeSyncConfig();
    }

    @Override
    protected Reader<TimeSyncConfig> instanceReader() {
        return TimeSyncConfig::new;
    }

    public void testDefaultDelay() {
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), null);
        assertThat(config.getDelay(), equalTo(TimeSyncConfig.DEFAULT_DELAY));
    }
}
