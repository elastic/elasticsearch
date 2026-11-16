/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/** Real request breakers and pooled-allocation APIs exercise admission, compensation and ownership transfer. */
public class SequenceMemoryBudgetTests extends ESTestCase {
    /** A query cap combines its owners without borrowing another request's existing breaker usage. */
    public void testCombinedLimitIgnoresUnrelatedRequestUsage() {
        CircuitBreaker request = breaker("1mb");
        SequenceMemoryBudget budget = new SequenceMemoryBudget(140);
        CircuitBreaker state = budget.newScope().breaker(request);
        request.addEstimateBytesAndMaybeBreak(777, "unrelated");
        state.addEstimateBytesAndMaybeBreak(100, "candidate");
        budget.admitForeign(40);
        CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, () -> budget.admitForeign(1));
        assertThat(failure.getMessage(), containsString("esql.sequence.max_memory"));
        assertEquals(141, failure.getBytesWanted());
        assertEquals(140, failure.getByteLimit());
        assertEquals(140, budget.currentBytes());
        assertEquals(140, budget.peakBytes());
        state.addWithoutBreaking(-100);
        budget.releaseForeign(40);
        assertEquals(0, budget.currentBytes());
        assertEquals(777, request.getUsed());
        request.addWithoutBreaking(-777);
    }

    /** Ordinary positive accounting calls must enforce the same limit as estimated admissions. */
    public void testPositiveUncheckedDeltaIsAnAdmission() {
        CircuitBreaker request = breaker("1mb");
        SequenceMemoryBudget budget = new SequenceMemoryBudget(10);
        CircuitBreaker state = budget.newScope().breaker(request);
        expectThrows(CircuitBreakingException.class, () -> state.addWithoutBreaking(11));
        assertEquals(0, request.getUsed());
        assertEquals(0, budget.currentBytes());
        state.addWithoutBreaking(10);
        state.addWithoutBreaking(-10);
        assertEquals(0, request.getUsed());
    }

    /** An already-created allocation can book its rejected delta solely so failure cleanup can refund it. */
    public void testRejectedEstimateAllowsExactFailureCompensationOnly() {
        CircuitBreaker request = breaker("1mb");
        SequenceMemoryBudget budget = new SequenceMemoryBudget(10);
        CircuitBreaker state = budget.newScope().breaker(request);
        expectThrows(CircuitBreakingException.class, () -> state.addEstimateBytesAndMaybeBreak(20, "already-created"));
        assertEquals(0, budget.currentBytes());
        state.addWithoutBreaking(20); // BigArrays failure compensation, not a successful allocation path.
        assertEquals(20, budget.currentBytes());
        assertEquals(20, request.getUsed());
        state.addWithoutBreaking(-20);
        assertEquals(0, budget.currentBytes());
        assertEquals(0, request.getUsed());
        expectThrows(CircuitBreakingException.class, () -> state.addWithoutBreaking(20));
    }

    /** Real BigArrays validation preserves the budget failure and refunds the already-created buffer. */
    public void testRealBigArraysRejectedValidationRefundsItsCompensation() {
        CircuitBreaker request = breaker("1mb");
        BlockFactory parent = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(request).build();
        SequenceMemoryBudget budget = new SequenceMemoryBudget(1);
        BlockFactory source = budget.newScope().blockFactory(parent);
        CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, () -> {
            try (var array = source.bigArrays().newByteArray(2_000)) {
                fail("an over-budget buffer must not be admitted");
            }
        });
        assertThat(failure.getMessage(), containsString("SEQUENCE memory limit"));
        assertEquals(0, budget.currentBytes());
        assertEquals(0, request.getUsed());
    }

    /** Output transfer ends source attribution while the consumer still owns the real buffer reservation. */
    public void testBigArrayBlocksKeepRequestChargeAfterOutputDetach() {
        CircuitBreaker request = breaker("1mb");
        BlockFactory parent = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(request)
            .maxPrimitiveArraySize(8)
            .bytesRefRamOverestimateThreshold(2048)
            .bytesRefRamOverestimateFactor(3)
            .build();
        SequenceMemoryBudget budget = new SequenceMemoryBudget(1_000_000);
        var output = budget.newScope();
        BlockFactory source = output.blockFactory(parent);
        assertEquals(parent.maxPrimitiveArrayBytes(), source.maxPrimitiveArrayBytes());
        assertEquals(parent.bytesRefRamOverestimateThreshold(), source.bytesRefRamOverestimateThreshold());
        assertEquals(parent.bytesRefRamOverestimateFactor(), source.bytesRefRamOverestimateFactor(), 0);
        Block block;
        try (var builder = source.newIntBlockBuilder(1_000)) {
            for (int value = 0; value < 1_000; value++) {
                builder.appendInt(value);
            }
            block = builder.build();
        }
        try (block) {
            assertTrue(budget.currentBytes() >= block.ramBytesUsed());
            assertEquals(request.getUsed(), budget.currentBytes());
            output.detach();
            output.detach();
            assertEquals(0, budget.currentBytes());
            assertTrue(request.getUsed() > 0);
        }
        assertEquals(0, request.getUsed());
        assertEquals(0, budget.currentBytes());
    }

    /** Forwarded source blocks stay charged once, while ordinary child blocks join on source ownership. */
    public void testForeignPageOwnershipAdmitsOnlyUnownedBlocks() {
        CircuitBreaker request = breaker("1mb");
        BlockFactory parent = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(request).build();
        SequenceMemoryBudget budget = new SequenceMemoryBudget(10_000);
        BlockFactory source = budget.newScope().blockFactory(parent);
        try (Page own = new Page(source.newConstantIntBlockWith(1, 1)); Page foreign = new Page(parent.newConstantIntBlockWith(2, 1))) {
            assertEquals(0, budget.foreignBytes(own));
            long bytes = budget.foreignBytes(foreign);
            assertEquals(foreign.ramBytesUsedByBlocks(), bytes);
            long current = budget.currentBytes();
            budget.admitForeign(bytes);
            assertEquals(current + bytes, budget.currentBytes());
            budget.releaseForeign(bytes);
            assertEquals(current, budget.currentBytes());
        }
        assertEquals(0, budget.currentBytes());
        assertEquals(0, request.getUsed());
    }

    /** A dynamic update changes future queries while each existing query retains its captured cap. */
    public void testDynamicSettingAndCapturedQueryLimit() {
        var setting = PlannerSettings.SEQUENCE_MAX_MEMORY;
        long expected = Math.min((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.02), 100L * 1024 * 1024);
        assertEquals(expected, setting.getDefault(Settings.EMPTY).getBytes());
        var cluster = new ClusterSettings(Settings.EMPTY, new HashSet<>(PlannerSettings.settings()));
        cluster.applySettings(Settings.builder().put(setting.getKey(), "8kb").build());
        var first = new SequenceMemoryBudget(cluster.get(setting).getBytes());
        cluster.applySettings(Settings.builder().put(setting.getKey(), "4kb").build());
        var later = new SequenceMemoryBudget(cluster.get(setting).getBytes());
        CircuitBreaker request = breaker("1mb");
        CircuitBreaker oldQuery = first.newScope().breaker(request);
        CircuitBreaker newQuery = later.newScope().breaker(request);
        oldQuery.addEstimateBytesAndMaybeBreak(5_000, "captured-before-update");
        expectThrows(CircuitBreakingException.class, () -> newQuery.addEstimateBytesAndMaybeBreak(5_000, "new-query"));
        oldQuery.addWithoutBreaking(-5_000);
        assertEquals(0, request.getUsed());
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
