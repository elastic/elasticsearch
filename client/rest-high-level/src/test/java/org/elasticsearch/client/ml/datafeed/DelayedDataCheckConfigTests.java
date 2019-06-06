/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.unit.TimeValue;
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

