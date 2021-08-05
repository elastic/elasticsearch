/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DelayedDataCheckConfigTests extends AbstractSerializingTestCase<DelayedDataCheckConfig> {

    @Override
    protected DelayedDataCheckConfig createTestInstance(){
        return createRandomizedConfig(100, randomBoolean() ? null : randomLongBetween(10, 10000L));
    }

    @Override
    protected Writeable.Reader<DelayedDataCheckConfig> instanceReader() {
        return DelayedDataCheckConfig::new;
    }

    @Override
    protected DelayedDataCheckConfig doParseInstance(XContentParser parser) {
        return DelayedDataCheckConfig.STRICT_PARSER.apply(parser, null);
    }

    public void testConstructor() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new DelayedDataCheckConfig(true, TimeValue.MINUS_ONE, TimeValue.timeValueMinutes(1))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new DelayedDataCheckConfig(true, TimeValue.timeValueMinutes(1), TimeValue.MINUS_ONE)
        );
    }

    public void testEnabledDelayedDataCheckConfig() {
        DelayedDataCheckConfig delayedDataCheckConfig = DelayedDataCheckConfig.enabledDelayedDataCheckConfig(
            TimeValue.timeValueHours(5),
            TimeValue.timeValueHours(1)
        );
        assertThat(delayedDataCheckConfig.isEnabled(), equalTo(true));
        assertThat(delayedDataCheckConfig.getCheckWindow(), equalTo(TimeValue.timeValueHours(5)));
        assertThat(delayedDataCheckConfig.getCheckFrequency(), equalTo(TimeValue.timeValueHours(1)));
    }

    public void testDisabledDelayedDataCheckConfig() {
        DelayedDataCheckConfig delayedDataCheckConfig = DelayedDataCheckConfig.disabledDelayedDataCheckConfig();
        assertThat(delayedDataCheckConfig.isEnabled(), equalTo(false));
        assertThat(delayedDataCheckConfig.getCheckWindow(), equalTo(null));
    }

    public void testDefaultDelayedDataCheckConfig() {
        DelayedDataCheckConfig delayedDataCheckConfig = DelayedDataCheckConfig.defaultDelayedDataCheckConfig();
        assertThat(delayedDataCheckConfig.isEnabled(), equalTo(true));
        assertThat(delayedDataCheckConfig.getCheckWindow(), is(nullValue()));
    }

    public static DelayedDataCheckConfig createRandomizedConfig(long bucketSpanMillis, Long frequency) {
        boolean enabled = randomBoolean();
        TimeValue timeWindow = null;
        Long checkWindow = null;
        if (enabled || randomBoolean()) {
            // time span is required to be at least 1 millis, so we use a custom method to generate a time value here
            timeWindow = new TimeValue(randomLongBetween(bucketSpanMillis,bucketSpanMillis*2));
            checkWindow = randomBoolean() ? null :
                frequency == null ? randomLongBetween(bucketSpanMillis,bucketSpanMillis*2) : randomLongBetween(frequency, frequency*2);
        }
        return new DelayedDataCheckConfig(
            enabled,
            timeWindow,
            checkWindow == null ? null : new TimeValue(checkWindow)
        );
    }

}
