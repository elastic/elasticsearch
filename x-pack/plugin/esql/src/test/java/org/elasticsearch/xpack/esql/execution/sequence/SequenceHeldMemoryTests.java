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

/** Source-held estimates combine simultaneous owners without borrowing unrelated node breaker totals. */
public class SequenceHeldMemoryTests extends ESTestCase {
    /** One combined timeline must distinguish simultaneous holdings from separate peaks and unrelated requests. */
    public void testCombinedPeakAndUnrelatedBreakerUsage() {
        CircuitBreaker delegate = breaker("1mb");
        SequenceHeldMemory memory = new SequenceHeldMemory();
        CircuitBreaker explicit = memory.newScope().breaker(delegate);
        var cells = memory.newScope();
        delegate.addEstimateBytesAndMaybeBreak(777, "unrelated");
        assertEquals(0, memory.currentBytes());
        explicit.addEstimateBytesAndMaybeBreak(100, "source");
        cells.recordHeldBytes(40);
        assertEquals(140, memory.currentBytes());
        explicit.addWithoutBreaking(-100, "source");
        explicit.addEstimateBytesAndMaybeBreak(80, "source");
        assertEquals(120, memory.currentBytes());
        assertEquals(140, memory.peakBytes());
        explicit.addWithoutBreaking(-80);
        cells.recordHeldBytes(-40);
        assertEquals(0, memory.currentBytes());
        assertEquals(777, delegate.getUsed());
        delegate.addWithoutBreaking(-777);
        assertEquals(0, delegate.getUsed());
    }

    /** Rejected real admissions never become held bytes or a new telemetry peak. */
    public void testRejectedAdmissionDoesNotChangeEstimate() {
        CircuitBreaker delegate = breaker("1b");
        SequenceHeldMemory memory = new SequenceHeldMemory();
        var scope = memory.newScope();
        CircuitBreaker observed = scope.breaker(delegate);
        expectThrows(CircuitBreakingException.class, () -> observed.addEstimateBytesAndMaybeBreak(2, "source"));
        assertEquals(0, memory.currentBytes());
        assertEquals(0, memory.peakBytes());
        assertEquals(0, delegate.getUsed());
        expectThrows(IllegalArgumentException.class, () -> scope.breaker(observed));
    }

    /** The existing two-buffer reservation overlaps one retained packet regardless of its reference count. */
    public void testPayloadReferencesAndExplicitSerializationOverlap() {
        CircuitBreaker delegate = breaker("1mb");
        SequenceHeldMemory memory = new SequenceHeldMemory();
        CircuitBreaker observed = memory.newScope().breaker(delegate);
        observed.addEstimateBytesAndMaybeBreak(96, "two-serialization-buffers");
        SequencePayloadHandle handle = SequencePayloadHandle.copyOf(new BytesRef("retained-output"), observed);
        long packetBytes = handle.accountedBytes();
        assertEquals(96 + packetBytes, memory.currentBytes());
        assertEquals(96 + packetBytes, memory.peakBytes());
        observed.addWithoutBreaking(-96, "two-serialization-buffers");
        handle.incRef();
        handle.incRef();
        assertEquals(packetBytes, memory.currentBytes());
        handle.decRef();
        handle.decRef();
        assertEquals(packetBytes, memory.currentBytes());
        handle.decRef();
        assertEquals(0, memory.currentBytes());
        assertEquals(0, delegate.getUsed());
    }

    /** Handing data downstream ends Source attribution while its physical reservation remains live. */
    public void testOwnershipHandoffDoesNotRefundTheRealBreaker() {
        CircuitBreaker delegate = breaker("1mb");
        SequenceHeldMemory memory = new SequenceHeldMemory();
        var outgoing = memory.newScope();
        CircuitBreaker observed = outgoing.breaker(delegate);
        observed.addEstimateBytesAndMaybeBreak(100, "result");
        outgoing.detach();
        outgoing.detach();
        assertEquals(0, memory.currentBytes());
        assertEquals(100, memory.peakBytes());
        assertEquals(100, delegate.getUsed());
        observed.addWithoutBreaking(-100, "consumer-release");
        assertEquals(0, delegate.getUsed());
        assertEquals(0, memory.currentBytes());
        assertEquals(100, memory.peakBytes());
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
