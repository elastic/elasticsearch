/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class BreakerSettingsTests extends ESTestCase {

    public void testFromSettings() {
        Settings clusterSettings = Settings.builder()
            .put(BreakerSettings.breakerLimitSettingKey("foo"), "100b")
            .put(BreakerSettings.breakerLimitSettingKey("bar"), "150b")
            .put(BreakerSettings.breakerOverheadSettingKey("bar"), 2.5)
            .put(BreakerSettings.breakerTypeSettingKey("bar"), CircuitBreaker.Type.MEMORY)
            .build();

        BreakerSettings breakerFoo = BreakerSettings.updateFromSettings(new BreakerSettings("foo",
            10L,
            1.2d,
            CircuitBreaker.Type.NOOP,
            CircuitBreaker.Durability.TRANSIENT),
            clusterSettings);
        assertThat(breakerFoo.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        assertThat(breakerFoo.getLimit(), equalTo(100L));
        assertThat(breakerFoo.getOverhead(), equalTo(1.2));
        assertThat(breakerFoo.getType(), equalTo(CircuitBreaker.Type.NOOP));

        BreakerSettings breakerBar = BreakerSettings.updateFromSettings(new BreakerSettings("bar",
            5L,
            0.5d,
            CircuitBreaker.Type.NOOP,
            CircuitBreaker.Durability.PERMANENT),
            clusterSettings);
        assertThat(breakerBar.getDurability(), equalTo(CircuitBreaker.Durability.PERMANENT));
        assertThat(breakerBar.getLimit(), equalTo(150L));
        assertThat(breakerBar.getOverhead(), equalTo(2.5));
        assertThat(breakerBar.getType(), equalTo(CircuitBreaker.Type.MEMORY));
    }
}
