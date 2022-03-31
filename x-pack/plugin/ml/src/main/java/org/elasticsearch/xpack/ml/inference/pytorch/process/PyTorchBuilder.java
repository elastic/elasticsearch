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
    private static final String INFERENCE_THREADS_ARG = "--inferenceThreads=";
    private static final String MODEL_THREADS_ARG = "--modelThreads=";

    private final NativeController nativeController;
    private final ProcessPipes processPipes;
    private final int inferenceThreads;
    private final int modelThreads;

    public PyTorchBuilder(NativeController nativeController, ProcessPipes processPipes, int inferenceThreads, int modelThreads) {
        this.nativeController = Objects.requireNonNull(nativeController);
        this.processPipes = Objects.requireNonNull(processPipes);
        this.inferenceThreads = inferenceThreads;
        this.modelThreads = modelThreads;
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

        command.add(INFERENCE_THREADS_ARG + inferenceThreads);
        command.add(MODEL_THREADS_ARG + modelThreads);

        return command;
    }
}
