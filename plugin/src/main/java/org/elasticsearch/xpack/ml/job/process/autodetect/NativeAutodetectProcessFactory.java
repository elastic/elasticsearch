/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.ProcessPipes;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.StateProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class NativeAutodetectProcessFactory implements AutodetectProcessFactory {

    private static final Logger LOGGER = Loggers.getLogger(NativeAutodetectProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();
    private static final Duration PROCESS_STARTUP_TIMEOUT = Duration.ofSeconds(10);

    private final Client client;
    private final Environment env;
    private final Settings settings;
    private final JobProvider jobProvider;
    private final NativeController nativeController;

    public NativeAutodetectProcessFactory(JobProvider jobProvider, Environment env, Settings settings,
                                          NativeController nativeController, Client client) {
        this.env = Objects.requireNonNull(env);
        this.settings = Objects.requireNonNull(settings);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.client = client;
    }

    @Override
    public AutodetectProcess createAutodetectProcess(Job job, ModelSnapshot modelSnapshot,
                                                     Quantiles quantiles, Set<MlFilter> filters,
                                                     ExecutorService executorService,
                                                     Runnable onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, ProcessCtrl.AUTODETECT, job.getId(),
                true, false, true, true, modelSnapshot != null, !ProcessCtrl.DONT_PERSIST_MODEL_STATE_SETTING.get(settings));
        createNativeProcess(job, quantiles, filters, processPipes, filesToDelete);
        int numberOfAnalysisFields = job.getAnalysisConfig().analysisFields().size();

        StateProcessor stateProcessor = new StateProcessor(settings, client);
        AutodetectResultsParser resultsParser = new AutodetectResultsParser(settings);
        NativeAutodetectProcess autodetect = new NativeAutodetectProcess(
                job.getId(), processPipes.getLogStream().get(), processPipes.getProcessInStream().get(),
                processPipes.getProcessOutStream().get(), processPipes.getRestoreStream().orElse(null), numberOfAnalysisFields,
                filesToDelete, resultsParser, onProcessCrash);
        try {
            autodetect.start(executorService, stateProcessor, processPipes.getPersistStream().get());
            return autodetect;
        } catch (EsRejectedExecutionException e) {
            try {
                IOUtils.close(autodetect);
            } catch (IOException ioe) {
                LOGGER.error("Can't close autodetect", ioe);
            }
            throw e;
        }
    }

    private void createNativeProcess(Job job, Quantiles quantiles, Set<MlFilter> filters, ProcessPipes processPipes,
                                     List<Path> filesToDelete) {
        try {
            AutodetectBuilder autodetectBuilder = new AutodetectBuilder(job, filesToDelete, LOGGER, env,
                    settings, nativeController, processPipes)
                    .referencedFilters(filters);

            // if state is null or empty it will be ignored
            // else it is used to restore the quantiles
            if (quantiles != null) {
                autodetectBuilder.quantiles(quantiles);
            }

            autodetectBuilder.build();
            processPipes.connectStreams(PROCESS_STARTUP_TIMEOUT);
        } catch (IOException | TimeoutException e) {
            String msg = "Failed to launch autodetect for job " + job.getId();
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}

