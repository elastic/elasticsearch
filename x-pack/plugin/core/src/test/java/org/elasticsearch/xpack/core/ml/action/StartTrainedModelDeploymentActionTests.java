/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StartTrainedModelDeploymentActionTests extends ESTestCase {

    private static final long MODEL_BYTES = ByteSizeValue.ofMb(468).getBytes();

    public void testEstimateMemoryUsageBytes_ZeroAllocations() {
        assertThat(StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(".elser_model_2", MODEL_BYTES, 0, 0, 0), equalTo(0L));
    }

    /**
     * For ELSER v1/v2 when perAllocationMemoryBytes has been injected (new deployments), the estimate must scale
     * linearly with the number of allocations. This is the main fix for the OOM regression: at 1 allocation the
     * estimate approximates the old 2004 MB constant; at N allocations it grows proportionally.
     */
    public void testEstimateMemoryUsageBytes_ElserV2_ScalesWithAllocations() {
        long perAllocation = StartTrainedModelDeploymentAction.ELSER_1_OR_2_PER_ALLOCATION_MEMORY.getBytes();

        // 1 allocation: estimate ≈ perAllocation + MODEL_BYTES ≈ 2004 MB (close to the old flat constant).
        long oneAlloc = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(".elser_model_2", MODEL_BYTES, 0, perAllocation, 1);
        assertThat(oneAlloc, greaterThan(ByteSizeValue.ofMb(1900).getBytes()));

        // N allocations: estimate is strictly larger and proportional to N.
        long eightAllocs = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(".elser_model_2", MODEL_BYTES, 0, perAllocation, 8);
        assertThat(eightAllocs, greaterThan(oneAlloc * 5)); // at least 5× the 1-allocation estimate

        long thirtyTwoAllocs = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
            ".elser_model_2",
            MODEL_BYTES,
            0,
            perAllocation,
            32
        );
        assertThat(thirtyTwoAllocs, greaterThan(eightAllocs * 3)); // at least 3× the 8-allocation estimate
    }

    /**
     * For ELSER v1/v2 deployments persisted by older nodes (perAllocationMemoryBytes == 0 in task params),
     * the estimate falls back to the historical flat 2004 MB constant to keep behaviour stable for those
     * deployments until they are restarted.
     */
    public void testEstimateMemoryUsageBytes_ElserV2_FlatFallbackWhenNoPerAllocationMemory() {
        long expected = ByteSizeValue.ofMb(2004).getBytes();
        assertThat(StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(".elser_model_2", MODEL_BYTES, 0, 0, 1), equalTo(expected));
        // Flat even for multiple allocations — old behaviour preserved for BWC with old persisted task params.
        assertThat(StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(".elser_model_2", MODEL_BYTES, 0, 0, 8), equalTo(expected));
    }

    /** Non-ELSER models with no per-allocation metadata use the baseline 240 MB + 2×modelBytes formula. */
    public void testEstimateMemoryUsageBytes_NonElser_NoMetadata() {
        long memOverhead = ByteSizeValue.ofMb(240).getBytes();
        assertThat(
            StartTrainedModelDeploymentAction.estimateMemoryUsageBytes("some_model", MODEL_BYTES, 0, 0, 1),
            equalTo(memOverhead + 2 * MODEL_BYTES)
        );
    }

    /** Non-ELSER models with explicit per-allocation metadata scale with the number of allocations. */
    public void testEstimateMemoryUsageBytes_NonElser_WithMetadata_ScalesWithAllocations() {
        long perAllocation = ByteSizeValue.ofMb(500).getBytes();
        long oneAlloc = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes("some_model", MODEL_BYTES, 0, perAllocation, 1);
        long fourAllocs = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes("some_model", MODEL_BYTES, 0, perAllocation, 4);
        assertThat(fourAllocs, greaterThan(oneAlloc));
    }
}
