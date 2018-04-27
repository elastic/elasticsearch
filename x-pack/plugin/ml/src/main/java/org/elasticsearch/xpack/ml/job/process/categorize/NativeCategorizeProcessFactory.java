/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.ProcessPipes;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.StateProcessor;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class NativeCategorizeProcessFactory implements CategorizeProcessFactory {

    private static final Logger LOGGER = Loggers.getLogger(NativeCategorizeProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();
    public static final Duration PROCESS_STARTUP_TIMEOUT = Duration.ofSeconds(10);

    private final Client client;
    private final Environment env;
    private final Settings settings;
    private final NativeController nativeController;

    public NativeCategorizeProcessFactory(Environment env, Settings settings, NativeController nativeController, Client client) {
        this.env = Objects.requireNonNull(env);
        this.settings = Objects.requireNonNull(settings);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.client = client;
    }

    @Override
    public CategorizeProcess createCategorizeProcess(Job job,
                                                     boolean haveState,
                                                     ExecutorService executorService,
                                                     Runnable onProcessCrash) {
        List<Path> filesToDelete = new ArrayList<>();
        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, ProcessCtrl.CATEGORIZE, job.getId(),
                true, false, true, true, haveState, !ProcessCtrl.DONT_PERSIST_MODEL_STATE_SETTING.get(settings));
        createNativeProcess(job, processPipes, filesToDelete);
        boolean includeTokensField = MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA
                && job.getAnalysisConfig().getCategorizationFieldName() != null;
        // The extra 1 is the control field
        int numberOfFields = job.allInputFields().size() + (includeTokensField ? 1 : 0) + 1;

        StateProcessor stateProcessor = new StateProcessor(settings, client);
        AutodetectResultsParser resultsParser = new AutodetectResultsParser(settings);
        NativeCategorizeProcess categorize = new NativeCategorizeProcess(
                job.getId(), processPipes.getLogStream().get(), processPipes.getProcessInStream().get(),
                processPipes.getProcessOutStream().get(), processPipes.getRestoreStream().orElse(null), numberOfFields,
                filesToDelete, resultsParser, onProcessCrash);
        try {
            categorize.start(executorService, stateProcessor, processPipes.getPersistStream().get());
            return categorize;
        } catch (EsRejectedExecutionException e) {
            try {
                IOUtils.close(categorize);
            } catch (IOException ioe) {
                LOGGER.error("Can't close categorize", ioe);
            }
            throw e;
        }
    }

    private void createNativeProcess(Job job, ProcessPipes processPipes, List<Path> filesToDelete) {

        try {
            CategorizeBuilder categorizeBuilder = new CategorizeBuilder(job, filesToDelete, LOGGER, env,
                    settings, nativeController, processPipes);

            categorizeBuilder.build();
            processPipes.connectStreams(PROCESS_STARTUP_TIMEOUT);
        } catch (IOException e) {
            String msg = "Failed to launch categorize for job " + job.getId();
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }
}
