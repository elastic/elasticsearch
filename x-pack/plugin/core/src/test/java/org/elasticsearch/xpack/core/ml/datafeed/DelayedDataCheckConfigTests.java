/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DelayedDataCheckConfigTests extends AbstractSerializingTestCase<DelayedDataCheckConfig> {

    @Override
    protected DelayedDataCheckConfig createTestInstance(){
        return createRandomizedConfig(100);
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
        expectThrows(IllegalArgumentException.class, () -> new DelayedDataCheckConfig(true, TimeValue.MINUS_ONE));
        expectThrows(IllegalArgumentException.class, () -> new DelayedDataCheckConfig(true, TimeValue.timeValueHours(25)));
    }

    public void testEnabledDelayedDataCheckConfig() {
        DelayedDataCheckConfig delayedDataCheckConfig = DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(5));
        assertThat(delayedDataCheckConfig.isEnabled(), equalTo(true));
        assertThat(delayedDataCheckConfig.getCheckWindow(), equalTo(TimeValue.timeValueHours(5)));
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

    public static DelayedDataCheckConfig createRandomizedConfig(long bucketSpanMillis) {
        boolean enabled = randomBoolean();
        TimeValue timeWindow = null;
        if (enabled || randomBoolean()) {
            // time span is required to be at least 1 millis, so we use a custom method to generate a time value here
            timeWindow = new TimeValue(randomLongBetween(bucketSpanMillis,bucketSpanMillis*2));
        }
        return new DelayedDataCheckConfig(enabled, timeWindow);
    }

    @Override
    protected DelayedDataCheckConfig mutateInstance(DelayedDataCheckConfig instance) throws IOException {
        boolean enabled = instance.isEnabled();
        TimeValue timeWindow = instance.getCheckWindow();
        switch (between(0, 1)) {
        case 0:
            enabled = !enabled;
            if (randomBoolean()) {
                timeWindow = TimeValue.timeValueMillis(randomLongBetween(1, 1000));
            } else {
                timeWindow = null;
            }
            break;
        case 1:
            if (timeWindow == null) {
                timeWindow = TimeValue.timeValueMillis(randomLongBetween(1, 1000));
            } else {
                timeWindow = new TimeValue(timeWindow.getMillis() + between(10, 100));
            }
            enabled = true;
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new DelayedDataCheckConfig(enabled, timeWindow);
    }
}
