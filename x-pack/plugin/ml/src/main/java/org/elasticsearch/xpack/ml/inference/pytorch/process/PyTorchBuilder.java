/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PyTorchBuilder {

    public static final String PROCESS_NAME = "pytorch_inference";
    private static final String PROCESS_PATH = "./" + PROCESS_NAME;

    private static final String LICENSE_KEY_VALIDATED_ARG = "--validElasticLicenseKeyConfirmed=";
    private static final String NUM_THREADS_PER_ALLOCATION_ARG = "--numThreadsPerAllocation=";
    private static final String NUM_ALLOCATIONS_ARG = "--numAllocations=";
    private static final String CACHE_MEMORY_LIMIT_BYTES_ARG = "--cacheMemorylimitBytes=";
    private static final String LOW_PRIORITY_ARG = "--lowPriority";

    private final NativeController nativeController;
    private final ProcessPipes processPipes;
    private final StartTrainedModelDeploymentAction.TaskParams taskParams;

    public PyTorchBuilder(
        NativeController nativeController,
        ProcessPipes processPipes,
        StartTrainedModelDeploymentAction.TaskParams taskParams
    ) {
        this.nativeController = Objects.requireNonNull(nativeController);
        this.processPipes = Objects.requireNonNull(processPipes);
        this.taskParams = Objects.requireNonNull(taskParams);
    }

    public void build() throws IOException, InterruptedException {
        List<String> command = buildCommand();
        processPipes.addArgs(command);
        nativeController.startProcess(command);
    }

    private List<String> buildCommand() {
        List<String> command = new ArrayList<>();
        command.add(PROCESS_PATH);

        // License was validated when the trained model was started
        command.add(LICENSE_KEY_VALIDATED_ARG + true);

        command.add(NUM_THREADS_PER_ALLOCATION_ARG + taskParams.getThreadsPerAllocation());
        command.add(NUM_ALLOCATIONS_ARG + taskParams.getNumberOfAllocations());
        if (taskParams.getCacheSizeBytes() > 0) {
            command.add(CACHE_MEMORY_LIMIT_BYTES_ARG + taskParams.getCacheSizeBytes());
        }
        if (taskParams.getPriority() == Priority.LOW) {
            command.add(LOW_PRIORITY_ARG);
        }

        return command;
    }
}
