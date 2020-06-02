/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface AnalyticsProcessFactory<ProcessResult> {

    /**
     * Create an implementation of {@link AnalyticsProcess}
     *
     * @param config                 The data frame analytics config
     * @param analyticsProcessConfig The process configuration
     * @param state                  The state document to restore from if there is one available
     * @param executorService        Executor service used to start the async tasks a job needs to operate the analytical process
     * @param onProcessCrash         Callback to execute if the process stops unexpectedly
     * @return The process
     */
    AnalyticsProcess<ProcessResult> createAnalyticsProcess(DataFrameAnalyticsConfig config, AnalyticsProcessConfig analyticsProcessConfig,
                                                           @Nullable BytesReference state, ExecutorService executorService,
                                                           Consumer<String> onProcessCrash);
}
