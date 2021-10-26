/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class NativeMemoryUsageEstimationProcessFactory implements AnalyticsProcessFactory<MemoryUsageEstimationResult> {

    private static final Logger logger = LogManager.getLogger(NativeMemoryUsageEstimationProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private final String nodeName;
    private final AtomicLong counter;
    private volatile Duration processConnectTimeout;

    public NativeMemoryUsageEstimationProcessFactory(Environment env, NativeController nativeController, ClusterService clusterService) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.nodeName = clusterService.getNodeName();
        this.counter = new AtomicLong(0);
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
            boolean hasState,
            ExecutorService executorService,
            Consumer<String> onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        // Since memory estimation can be called many times in quick succession for the same config the config ID alone is not
        // sufficient to guarantee that the memory estimation process pipe names are unique.  Therefore an increasing counter
        // value is passed as well as the config ID to ensure uniqueness between calls.
        ProcessPipes processPipes = new ProcessPipes(
            env, NAMED_PIPE_HELPER, processConnectTimeout, AnalyticsBuilder.ANALYTICS, config.getId(), counter.incrementAndGet(),
            false, false, true, false, false);

        createNativeProcess(config.getId(), analyticsProcessConfig, filesToDelete, processPipes);

        NativeMemoryUsageEstimationProcess process = new NativeMemoryUsageEstimationProcess(
            config.getId(),
            nativeController,
            processPipes,
            0,
            filesToDelete,
            onProcessCrash);

        try {
            process.start(executorService);
            return process;
        } catch (IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to data frame analytics memory usage estimation process for job " + config.getId();
            logger.error(msg);
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                logger.error("Can't close data frame analytics memory usage estimation process", ioe);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    private void createNativeProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig, List<Path> filesToDelete,
                                     ProcessPipes processPipes) {
        AnalyticsBuilder analyticsBuilder =
            new AnalyticsBuilder(env::tmpFile, nativeController, processPipes, analyticsProcessConfig, filesToDelete)
                .performMemoryUsageEstimationOnly();
        try {
            analyticsBuilder.build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("[{}] Interrupted while launching data frame analytics memory usage estimation process", jobId);
        } catch (IOException e) {
            String msg = "[" + jobId + "] Failed to launch data frame analytics memory usage estimation process";
            logger.error(msg);
            throw ExceptionsHelper.serverError(msg + " on [" + nodeName + "]", e);
        }
    }
}
