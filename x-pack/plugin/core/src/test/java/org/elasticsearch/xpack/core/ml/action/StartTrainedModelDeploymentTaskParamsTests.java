/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class StartTrainedModelDeploymentTaskParamsTests extends AbstractXContentSerializingTestCase<TaskParams> {

    @Override
    protected TaskParams doParseInstance(XContentParser parser) throws IOException {
        return TaskParams.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TaskParams> instanceReader() {
        return TaskParams::new;
    }

    @Override
    protected TaskParams createTestInstance() {
        return createRandom();
    }

    @Override
    protected TaskParams mutateInstance(TaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static StartTrainedModelDeploymentAction.TaskParams createRandom() {
        return new TaskParams(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            randomIntBetween(1, 8),
            randomIntBetween(1, 8),
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomNonNegativeLong()),
            randomFrom(Priority.values()),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    /**
     * Once real per-allocation / per-deployment memory has been observed for an ELSER v1/v2 model,
     * {@link StartTrainedModelDeploymentAction#estimateMemoryUsageBytes} must fall through to the linear
     * estimate (base + perAllocation * allocations + model size) rather than returning the flat
     * {@code ELSER_1_OR_2_MEMORY_USAGE} constant, so that memory scales with the number of allocations and
     * the planner memory guards engage. The flat estimate must only apply while no observed memory exists.
     */
    public void testEstimateMemoryUsageBytesForElserWithObservedMemoryUsesLinearEstimate() {
        long elserFlatEstimateBytes = ByteSizeValue.ofMb(2004).getBytes();
        long modelBytes = ByteSizeValue.ofMb(30).getBytes();
        long perDeploymentMemoryBytes = ByteSizeValue.ofMb(300).getBytes();
        long perAllocationMemoryBytes = ByteSizeValue.ofMb(400).getBytes();
        int numberOfAllocations = 3;

        long estimate = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
            ".elser_model_2",
            modelBytes,
            perDeploymentMemoryBytes,
            perAllocationMemoryBytes,
            numberOfAllocations
        );

        long expectedLinearEstimate = perDeploymentMemoryBytes + perAllocationMemoryBytes * numberOfAllocations + modelBytes;
        assertThat(estimate, equalTo(expectedLinearEstimate));
        assertThat(estimate, not(equalTo(elserFlatEstimateBytes)));

        // Control: with no observed memory the same ELSER model still uses the flat estimate.
        long flatEstimate = StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
            ".elser_model_2",
            modelBytes,
            0L,
            0L,
            numberOfAllocations
        );
        assertThat(flatEstimate, equalTo(elserFlatEstimateBytes));
    }
}
