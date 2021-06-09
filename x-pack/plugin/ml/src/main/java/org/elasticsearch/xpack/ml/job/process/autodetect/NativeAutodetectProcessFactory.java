/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.IndexingStateProcessor;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;
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

public class NativeAutodetectProcessFactory implements AutodetectProcessFactory {

    private static final Logger LOGGER = LogManager.getLogger(NativeAutodetectProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();

    private final Environment env;
    private final Settings settings;
    private final NativeController nativeController;
    private final ClusterService clusterService;
    private final ResultsPersisterService resultsPersisterService;
    private final AnomalyDetectionAuditor auditor;
    private volatile Duration processConnectTimeout;

    public NativeAutodetectProcessFactory(Environment env,
                                          Settings settings,
                                          NativeController nativeController,
                                          ClusterService clusterService,
                                          ResultsPersisterService resultsPersisterService,
                                          AnomalyDetectionAuditor auditor) {
        this.env = Objects.requireNonNull(env);
        this.settings = Objects.requireNonNull(settings);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.clusterService = clusterService;
        this.resultsPersisterService = resultsPersisterService;
        this.auditor = auditor;
        setProcessConnectTimeout(MachineLearning.PROCESS_CONNECT_TIMEOUT.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.PROCESS_CONNECT_TIMEOUT,
            this::setProcessConnectTimeout);
    }

    void setProcessConnectTimeout(TimeValue processConnectTimeout) {
        this.processConnectTimeout = Duration.ofMillis(processConnectTimeout.getMillis());
    }

    @Override
    public AutodetectProcess createAutodetectProcess(String pipelineId,
                                                     Job job,
                                                     AutodetectParams params,
                                                     ExecutorService executorService,
                                                     Consumer<String> onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, processConnectTimeout, AutodetectBuilder.AUTODETECT,
            pipelineId, null, false, true, true, params.modelSnapshot() != null, true);
        createNativeProcess(job, params, processPipes, filesToDelete);
        boolean includeTokensField = MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA
            && job.getAnalysisConfig().getCategorizationFieldName() != null;
        // The extra 1 is the control field
        int numberOfFields = job.allInputFields().size() + (includeTokensField ? 1 : 0) + 1;

        IndexingStateProcessor stateProcessor = new IndexingStateProcessor(job.getId(), resultsPersisterService, auditor);
        ProcessResultsParser<AutodetectResult> resultsParser = new ProcessResultsParser<>(AutodetectResult.PARSER,
            NamedXContentRegistry.EMPTY);
        NativeAutodetectProcess autodetect = new NativeAutodetectProcess(
            job.getId(), nativeController, processPipes, numberOfFields,
            filesToDelete, resultsParser, onProcessCrash);
        try {
            autodetect.start(executorService, stateProcessor);
            return autodetect;
        } catch (IOException | EsRejectedExecutionException e) {
            String msg = "Failed to connect to autodetect for job " + job.getId();
            LOGGER.error(msg);
            try {
                IOUtils.close(autodetect);
            } catch (IOException ioe) {
                LOGGER.error("Can't close autodetect", ioe);
            }
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    void createNativeProcess(Job job, AutodetectParams autodetectParams, ProcessPipes processPipes,
                             List<Path> filesToDelete) {
        try {

            Settings updatedSettings = Settings.builder()
                .put(settings)
                .put(AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC.getKey(),
                    clusterService.getClusterSettings().get(AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC))
                .build();

            AutodetectBuilder autodetectBuilder = new AutodetectBuilder(job, filesToDelete, LOGGER, env,
                updatedSettings, nativeController, processPipes)
                    .referencedFilters(autodetectParams.filters())
                    .scheduledEvents(autodetectParams.scheduledEvents());

            // if state is null or empty it will be ignored
            // else it is used to restore the quantiles
            if (autodetectParams.quantiles() != null) {
                autodetectBuilder.quantiles(autodetectParams.quantiles());
            }
            autodetectBuilder.build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("[{}] Interrupted while launching autodetect", job.getId());
        } catch (IOException e) {
            String msg = "[" + job.getId() + "] Failed to launch autodetect";
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

}

