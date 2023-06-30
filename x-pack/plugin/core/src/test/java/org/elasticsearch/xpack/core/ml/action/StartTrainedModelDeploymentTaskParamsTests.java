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
            randomFrom(Priority.values())
        );
    }

    public void testEstimateMemoryUsageBytes() {
        final long executableMemoryOverhead = ByteSizeValue.ofMb(240).getBytes(); // see StartTrainedModelDeploymentAction::MEMORY_OVERHEAD
        long modelSizeBytes = 1024L;
        int numAllocations = 1;

        {
            var task = new TaskParams(
                "not-elser",
                "not-elser",
                modelSizeBytes,
                numAllocations,
                randomIntBetween(1, 8),
                randomIntBetween(1, 10000),
                ByteSizeValue.ZERO, // no cache
                Priority.NORMAL
            );
            assertEquals(executableMemoryOverhead + 2 * modelSizeBytes, task.estimateMemoryUsageBytes());
        }
        {
            numAllocations = 2;
            var task = new TaskParams(
                "not-elser",
                "not-elser",
                modelSizeBytes,
                numAllocations,
                randomIntBetween(1, 8),
                randomIntBetween(1, 10000),
                ByteSizeValue.ZERO, // no cache
                Priority.NORMAL
            );
            assertEquals(executableMemoryOverhead + 3 * modelSizeBytes, task.estimateMemoryUsageBytes());
        }
        {
            numAllocations = 2;
            long cacheSize = modelSizeBytes / 2;
            var task = new TaskParams(
                "not-elser",
                "not-elser",
                modelSizeBytes,
                numAllocations,
                randomIntBetween(1, 8),
                randomIntBetween(1, 10000),
                ByteSizeValue.ofBytes(cacheSize), // no cache
                Priority.NORMAL
            );
            assertEquals(executableMemoryOverhead + 3 * modelSizeBytes, task.estimateMemoryUsageBytes());
        }
        {
            numAllocations = 2;
            long cacheSize = modelSizeBytes * 2;
            var task = new TaskParams(
                "not-elser",
                "not-elser",
                modelSizeBytes,
                numAllocations,
                randomIntBetween(1, 8),
                randomIntBetween(1, 10000),
                ByteSizeValue.ofBytes(cacheSize), // no cache
                Priority.NORMAL
            );
            assertEquals(executableMemoryOverhead + (3 * modelSizeBytes) + (cacheSize - modelSizeBytes), task.estimateMemoryUsageBytes());
        }
        {
            numAllocations = 4;
            var task = new TaskParams(
                ".elser_model_1",
                ".elser_model_1",
                modelSizeBytes,
                numAllocations,
                randomIntBetween(1, 8),
                randomIntBetween(1, 10000),
                ByteSizeValue.ZERO, // no cache
                Priority.NORMAL
            );
            // See StartTrainedModelDeploymentAction::ELSER_1_MEMORY_USAGE
            long elserMemUsage = ByteSizeValue.ofMb(1002).getBytes();
            long elserMemUsagePerAllocation = ByteSizeValue.ofMb(1002).getBytes();
            assertEquals(elserMemUsage + 4 * elserMemUsagePerAllocation, task.estimateMemoryUsageBytes());
        }
        {
            assertEquals(
                executableMemoryOverhead + 2 * modelSizeBytes,
                StartTrainedModelDeploymentAction.estimateMemoryUsageBytes("not-elser", modelSizeBytes)
            );
        }
    }
}
