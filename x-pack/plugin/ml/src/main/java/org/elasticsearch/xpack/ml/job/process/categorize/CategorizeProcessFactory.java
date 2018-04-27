/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.concurrent.ExecutorService;

public interface CategorizeProcessFactory {

    /**
     * Create an implementation of {@link CategorizeProcess}
     *
     * @param job               Job configuration for the analysis process
     * @param haveState         Is there already state that needs to be restored?
     * @param executorService   Executor service used to start the async tasks a job needs to operate the analytical process
     * @param onProcessCrash    Callback to execute if the process stops unexpectedly
     * @return The process
     */
    CategorizeProcess createCategorizeProcess(Job job, boolean haveState, ExecutorService executorService, Runnable onProcessCrash);
}
