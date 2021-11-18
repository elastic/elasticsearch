/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Factory interface for creating implementations of {@link AutodetectProcess}
 */
public interface AutodetectProcessFactory {

    /**
     * Create an implementation of {@link AutodetectProcess}
     *
     * @param job               Job configuration for the analysis process
     * @param autodetectParams  Autodetect parameters including The model snapshot to restore from
     *                          and the quantiles to push to the native process
     * @param executorService   Executor service used to start the async tasks a job needs to operate the analytical process
     * @param onProcessCrash    Callback to execute if the process stops unexpectedly
     * @return The process
     */
    default AutodetectProcess createAutodetectProcess(
        Job job,
        AutodetectParams autodetectParams,
        ExecutorService executorService,
        Consumer<String> onProcessCrash
    ) {
        return createAutodetectProcess(job.getId(), job, autodetectParams, executorService, onProcessCrash);
    }

    AutodetectProcess createAutodetectProcess(
        String pipelineId,
        Job job,
        AutodetectParams autodetectParams,
        ExecutorService executorService,
        Consumer<String> onProcessCrash
    );
}
