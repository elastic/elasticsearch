/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
