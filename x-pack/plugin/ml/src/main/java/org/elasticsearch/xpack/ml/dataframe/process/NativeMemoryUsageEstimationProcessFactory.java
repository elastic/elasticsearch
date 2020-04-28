/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class NativeMemoryUsageEstimationProcessFactory implements AnalyticsProcessFactory<MemoryUsageEstimationResult> {

    private static final Logger LOGGER = LogManager.getLogger(NativeMemoryUsageEstimationProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private volatile Duration processConnectTimeout;

    public NativeMemoryUsageEstimationProcessFactory(Environment env, NativeController nativeController, ClusterService clusterService) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(env.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            MachineLearning.PROCESS_CONNECT_TIMEOUT, this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public NativeMemoryUsageEstimationProcess createAnalyticsProcess(
            DataFrameAnalyticsConfig config,
            AnalyticsProcessConfig analyticsProcessConfig,
            @Nullable BytesReference state,
            ExecutorService executorService,
            Consumer<String> onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(
            env, NAMED_PIPE_HELPER, AnalyticsBuilder.ANALYTICS, config.getId(), true, false, false, true, false, false);

        createNativeProcess(config.getId(), analyticsProcessConfig, filesToDelete, processPipes);

        NativeMemoryUsageEstimationProcess process = new NativeMemoryUsageEstimationProcess(
            config.getId(),
            nativeController,
            processPipes.getLogStream().get(),
            // Memory estimation process does not use the input pipe, hence null.
            null,
            processPipes.getProcessOutStream().get(),
            null,
            0,
            filesToDelete,
            onProcessCrash,
            processConnectTimeout);

        try {
            process.start(executorService);
            return process;
        } catch (EsRejectedExecutionException e) {
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                LOGGER.error("Can't close data frame analytics memory usage estimation process", ioe);
            }
            throw e;
        }
    }

    private void createNativeProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig, List<Path> filesToDelete,
                                     ProcessPipes processPipes) {
        AnalyticsBuilder analyticsBuilder =
            new AnalyticsBuilder(env::tmpFile, nativeController, processPipes, analyticsProcessConfig, filesToDelete)
                .performMemoryUsageEstimationOnly();
        try {
            analyticsBuilder.build();
            processPipes.connectStreams(processConnectTimeout);
        } catch (IOException e) {
            String msg = "Failed to launch data frame analytics memory usage estimation process for job " + jobId;
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}
