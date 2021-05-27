/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface AnalyticsProcessFactory<ProcessResult> {

    /**
     * Create an implementation of {@link AnalyticsProcess}
     *
     * @param config                 The data frame analytics config
     * @param analyticsProcessConfig The process configuration
     * @param hasState               Whether there is state to restore from
     * @param executorService        Executor service used to start the async tasks a job needs to operate the analytical process
     * @param onProcessCrash         Callback to execute if the process stops unexpectedly
     * @return The process
     */
    AnalyticsProcess<ProcessResult> createAnalyticsProcess(DataFrameAnalyticsConfig config, AnalyticsProcessConfig analyticsProcessConfig,
                                                           boolean hasState, ExecutorService executorService,
                                                           Consumer<String> onProcessCrash);
}
