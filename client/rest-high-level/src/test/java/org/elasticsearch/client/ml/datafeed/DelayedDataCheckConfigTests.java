/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DelayedDataCheckConfigTests extends AbstractXContentTestCase<DelayedDataCheckConfig> {

    @Override
    protected DelayedDataCheckConfig createTestInstance() {
        return createRandomizedConfig();
    }

    @Override
    protected DelayedDataCheckConfig doParseInstance(XContentParser parser) {
        return DelayedDataCheckConfig.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
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

    public static DelayedDataCheckConfig createRandomizedConfig() {
        boolean enabled = randomBoolean();
        TimeValue timeWindow = null;
        if (enabled || randomBoolean()) {
            timeWindow = TimeValue.timeValueMillis(randomLongBetween(1, 1_000));
        }
        return new DelayedDataCheckConfig(enabled, timeWindow);
    }
}

