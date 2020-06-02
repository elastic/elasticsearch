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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.process.IndexingStateProcessor;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class NativeAnalyticsProcessFactory implements AnalyticsProcessFactory<AnalyticsResult> {

    private static final Logger LOGGER = LogManager.getLogger(NativeAnalyticsProcessFactory.class);

    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final NativeController nativeController;
    private final NamedXContentRegistry namedXContentRegistry;
    private final ResultsPersisterService resultsPersisterService;
    private final DataFrameAnalyticsAuditor auditor;
    private volatile Duration processConnectTimeout;

    public NativeAnalyticsProcessFactory(Environment env,
                                         NativeController nativeController,
                                         ClusterService clusterService,
                                         NamedXContentRegistry namedXContentRegistry,
                                         ResultsPersisterService resultsPersisterService,
                                         DataFrameAnalyticsAuditor auditor) {
        this.env = Objects.requireNonNull(env);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.namedXContentRegistry = Objects.requireNonNull(namedXContentRegistry);
        this.auditor = auditor;
        this.resultsPersisterService = resultsPersisterService;
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(env.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.PROCESS_CONNECT_TIMEOUT,
            this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public NativeAnalyticsProcess createAnalyticsProcess(DataFrameAnalyticsConfig config, AnalyticsProcessConfig analyticsProcessConfig,
                                                         @Nullable BytesReference state, ExecutorService executorService,
                                                         Consumer<String> onProcessCrash) {
        String jobId = config.getId();
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, AnalyticsBuilder.ANALYTICS, jobId,
                false, true, true, state != null, config.getAnalysis().persistsState());

        // The extra 2 are for the checksum and the control field
        int numberOfFields = analyticsProcessConfig.cols() + 2;

        createNativeProcess(jobId, analyticsProcessConfig, filesToDelete, processPipes, executorService);

        NativeAnalyticsProcess analyticsProcess =
            new NativeAnalyticsProcess(
                jobId, nativeController, processPipes, numberOfFields, filesToDelete,
                onProcessCrash, processConnectTimeout, analyticsProcessConfig, namedXContentRegistry);

        try {
            startProcess(config, executorService, processPipes, analyticsProcess);
            return analyticsProcess;
        } catch (IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to data frame analytics process for job " + jobId;
            LOGGER.error(msg);
            try {
                IOUtils.close(analyticsProcess);
            } catch (IOException ioe) {
                LOGGER.error("Can't close data frame analytics process", ioe);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    private void startProcess(DataFrameAnalyticsConfig config, ExecutorService executorService, ProcessPipes processPipes,
                                                NativeAnalyticsProcess process) throws IOException {
        if (config.getAnalysis().persistsState()) {
            IndexingStateProcessor stateProcessor = new IndexingStateProcessor(config.getId(), resultsPersisterService, auditor);
            process.start(executorService, stateProcessor);
        } else {
            process.start(executorService);
        }
    }

    private void createNativeProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig, List<Path> filesToDelete,
                                     ProcessPipes processPipes, ExecutorService executorService) {
        AnalyticsBuilder analyticsBuilder =
            new AnalyticsBuilder(env::tmpFile, nativeController, processPipes, analyticsProcessConfig, filesToDelete);
        try {
            analyticsBuilder.build();
        } catch (IOException e) {
            String msg = "Failed to launch data frame analytics process for job " + jobId;
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}
