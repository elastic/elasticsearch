/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

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

    private final NativeController nativeController;
    private final ProcessPipes processPipes;
    private final int threadsPerAllocation;
    private final int numberOfAllocations;
    private final long cacheMemoryLimitBytes;

    public PyTorchBuilder(
        NativeController nativeController,
        ProcessPipes processPipes,
        int threadPerAllocation,
        int numberOfAllocations,
        long cacheMemoryLimitBytes
    ) {
        this.nativeController = Objects.requireNonNull(nativeController);
        this.processPipes = Objects.requireNonNull(processPipes);
        this.threadsPerAllocation = threadPerAllocation;
        this.numberOfAllocations = numberOfAllocations;
        this.cacheMemoryLimitBytes = cacheMemoryLimitBytes;
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

        command.add(NUM_THREADS_PER_ALLOCATION_ARG + threadsPerAllocation);
        command.add(NUM_ALLOCATIONS_ARG + numberOfAllocations);
        if (cacheMemoryLimitBytes > 0) {
            command.add(CACHE_MEMORY_LIMIT_BYTES_ARG + cacheMemoryLimitBytes);
        }

        return command;
    }
}
