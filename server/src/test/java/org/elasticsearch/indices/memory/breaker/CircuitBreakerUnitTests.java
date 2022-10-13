/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.memory.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the circuit breaker
 */
public class CircuitBreakerUnitTests extends ESTestCase {
    public static long pctBytes(String percentString) {
        return Settings.EMPTY.getAsMemory("", percentString).getBytes();
    }

    public void testBreakerSettingsValidationWithValidSettings() {
        // parent: {:limit 70}, fd: {:limit 50}, request: {:limit 20}
        BreakerSettings fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("50%"), 1.0);
        BreakerSettings request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("20%"), 1.0);
        HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[] { fd, request });

        // parent: {:limit 70}, fd: {:limit 40}, request: {:limit 30}
        fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("40%"), 1.0);
        request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("30%"), 1.0);
        HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[] { fd, request });
    }

    public void testBreakerSettingsValidationNegativeOverhead() {
        // parent: {:limit 70}, fd: {:limit 50}, request: {:limit 20}
        BreakerSettings fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("50%"), -0.1);
        BreakerSettings request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("20%"), 1.0);
        try {
            HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[] { fd, request });
            fail("settings are invalid but validate settings did not throw an exception");
        } catch (Exception e) {
            assertThat("Incorrect message: " + e.getMessage(), e.getMessage().contains("must be non-negative"), equalTo(true));
        }
    }

}
