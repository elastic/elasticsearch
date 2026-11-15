/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

/** A handle has one reservation regardless of the number of pending/output references. */
public class SequencePayloadHandleTests extends ESTestCase {
    public void testCopiedBytesAndIndependentReferences() {
        CircuitBreaker breaker = breaker("1mb");
        BytesRef source = new BytesRef("payload");
        SequencePayloadHandle handle = SequencePayloadHandle.copyOf(source, breaker);
        source.bytes[source.offset] = 'X';
        assertEquals(new BytesRef("payload"), handle.bytes());
        assertEquals(handle.accountedBytes(), breaker.getUsed());
        handle.incRef();
        long bytes = breaker.getUsed();
        handle.decRef();
        assertEquals(bytes, breaker.getUsed());
        assertEquals(new BytesRef("payload"), handle.bytes());
        handle.decRef();
        expectThrows(IllegalStateException.class, handle::bytes);
        assertEquals(0, breaker.getUsed());
    }

    public void testRejectedAdmissionDoesNotLeaveAReservation() {
        CircuitBreaker breaker = breaker("1b");
        expectThrows(CircuitBreakingException.class, () -> SequencePayloadHandle.copyOf(new BytesRef("payload"), breaker));
        assertEquals(0, breaker.getUsed());
    }

    private static CircuitBreaker breaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
