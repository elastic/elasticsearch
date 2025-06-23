/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public interface PyTorchProcessFactory {

    interface TimeoutRunnable {
        void run() throws TimeoutException;
    }

    PyTorchProcess createProcess(
        TrainedModelDeploymentTask task,
        ExecutorService executorService,
        TimeoutRunnable afterInStreamClose,
        Consumer<String> onProcessCrash
    );
}
