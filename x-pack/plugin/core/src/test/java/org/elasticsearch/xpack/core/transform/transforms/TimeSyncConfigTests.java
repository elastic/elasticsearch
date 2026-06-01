/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSyncConfigTests extends AbstractSerializingTransformTestCase<TimeSyncConfig> {

    public static TimeSyncConfig randomTimeSyncConfig() {
        TimeValue delay = new TimeValue(randomNonNegativeLong());
        if (randomBoolean()) {
            // exercise the default (initial_delay omitted, so it equals delay)
            return new TimeSyncConfig(randomAlphaOfLengthBetween(1, 10), delay);
        }
        // initial_delay must never be greater than delay
        TimeValue initialDelay = new TimeValue(randomLongBetween(0, delay.millis()));
        return new TimeSyncConfig(randomAlphaOfLengthBetween(1, 10), delay, initialDelay);
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
    protected TimeSyncConfig mutateInstance(TimeSyncConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TimeSyncConfig> instanceReader() {
        return TimeSyncConfig::new;
    }

    @Override
    protected TimeSyncConfig mutateInstanceForVersion(TimeSyncConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static TimeSyncConfig mutateForVersion(TimeSyncConfig instance, TransportVersion version) {
        if (version.supports(TimeSyncConfig.TRANSFORM_SYNC_INITIAL_DELAY)) {
            return instance;
        }
        // Older nodes do not know about initial_delay, so on read it falls back to delay.
        return new TimeSyncConfig(instance.getField(), instance.getDelay());
    }

    public void testDefaultDelay() {
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), null);
        assertThat(config.getDelay(), equalTo(TimeSyncConfig.DEFAULT_DELAY));
    }

    public void testInitialDelayDefaultsToDelay() {
        TimeValue delay = TimeValue.timeValueSeconds(30);
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), delay, null);
        assertThat(config.getInitialDelay(), equalTo(delay));
    }

    public void testInitialDelayDefaultsToDefaultDelayWhenDelayUnset() {
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), null, null);
        assertThat(config.getInitialDelay(), equalTo(TimeSyncConfig.DEFAULT_DELAY));
    }

    public void testInitialDelayGreaterThanDelayIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TimeSyncConfig(randomAlphaOfLength(10), TimeValue.timeValueSeconds(60), TimeValue.timeValueSeconds(61))
        );
        assertThat(e.getMessage(), containsString("initial_delay"));
        assertThat(e.getMessage(), containsString("must not be greater than"));
    }

    public void testInitialDelayEqualToDelayIsAllowed() {
        TimeValue delay = TimeValue.timeValueSeconds(60);
        TimeSyncConfig config = new TimeSyncConfig(randomAlphaOfLength(10), delay, delay);
        assertThat(config.getInitialDelay(), equalTo(delay));
    }
}
