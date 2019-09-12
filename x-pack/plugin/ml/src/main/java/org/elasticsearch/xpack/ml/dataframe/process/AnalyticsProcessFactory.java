/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface AnalyticsProcessFactory<ProcessResult> {

    /**
     * Create an implementation of {@link AnalyticsProcess}
     *
     * @param jobId             The job id
     * @param analyticsProcessConfig The process configuration
     * @param executorService   Executor service used to start the async tasks a job needs to operate the analytical process
     * @param onProcessCrash    Callback to execute if the process stops unexpectedly
     * @return The process
     */
    AnalyticsProcess<ProcessResult> createAnalyticsProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig,
                                                           ExecutorService executorService, Consumer<String> onProcessCrash);
}
