/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.process.NativeController;
import org.elasticsearch.xpack.prelert.job.process.ProcessCtrl;
import org.elasticsearch.xpack.prelert.job.process.ProcessPipes;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.lists.ListDocument;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.elasticsearch.xpack.prelert.utils.NamedPipeHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class NativeAutodetectProcessFactory implements AutodetectProcessFactory {

    private static final Logger LOGGER = Loggers.getLogger(NativeAutodetectProcessFactory.class);
    private static final NamedPipeHelper NAMED_PIPE_HELPER = new NamedPipeHelper();
    private static final Duration PROCESS_STARTUP_TIMEOUT = Duration.ofSeconds(2);
    private final JobProvider jobProvider;
    private Environment env;
    private Settings settings;
    private NativeController nativeController;

    public NativeAutodetectProcessFactory(JobProvider jobProvider, Environment env, Settings settings, NativeController nativeController) {
        this.env = Objects.requireNonNull(env);
        this.settings = Objects.requireNonNull(settings);
        this.jobProvider = Objects.requireNonNull(jobProvider);
        this.nativeController = Objects.requireNonNull(nativeController);
    }

    @Override
    public AutodetectProcess createAutodetectProcess(Job job, boolean ignoreDowntime) {
        List<Path> filesToDelete = new ArrayList<>();
        List<ModelSnapshot> modelSnapshots = jobProvider.modelSnapshots(job.getId(), 0, 1).results();
        ModelSnapshot modelSnapshot = (modelSnapshots != null && !modelSnapshots.isEmpty()) ? modelSnapshots.get(0) : null;

        ProcessPipes processPipes = new ProcessPipes(env, NAMED_PIPE_HELPER, ProcessCtrl.AUTODETECT, job.getId(),
                true, false, true, true, modelSnapshot != null, !ProcessCtrl.DONT_PERSIST_MODEL_STATE_SETTING.get(settings));
        createNativeProcess(job, processPipes, ignoreDowntime, filesToDelete);
        int numberOfAnalysisFields = job.getAnalysisConfig().analysisFields().size();
        NativeAutodetectProcess autodetect = new NativeAutodetectProcess(job.getId(), processPipes.getLogStream().get(),
                processPipes.getProcessInStream().get(), processPipes.getProcessOutStream().get(),
                processPipes.getPersistStream().get(), numberOfAnalysisFields, filesToDelete);
        autodetect.tailLogsInThread();
        if (modelSnapshot != null) {
            restoreStateInThread(job.getId(), modelSnapshot, processPipes.getRestoreStream().get());
        }
        return autodetect;
    }

    private void restoreStateInThread(String jobId, ModelSnapshot modelSnapshot, OutputStream restoreStream) {
        new Thread(() -> {
            try {
                jobProvider.restoreStateToStream(jobId, modelSnapshot, restoreStream);
            } catch (Exception e) {
                LOGGER.error("Error restoring model state for job " + jobId, e);
            }
            // The restore stream will not be needed again.  If an error occurred getting state to restore then
            // it's critical to close the restore stream so that the C++ code can realise that it will never
            // receive any state to restore.  If restoration went smoothly then this is just good practice.
            try {
                restoreStream.close();
            } catch (IOException e) {
                LOGGER.error("Error closing restore stream for job " + jobId, e);
            }
        }).start();
    }

    private void createNativeProcess(Job job, ProcessPipes processPipes, boolean ignoreDowntime, List<Path> filesToDelete) {

        String jobId = job.getId();
        Optional<Quantiles> quantiles = jobProvider.getQuantiles(jobId);

        try {
            AutodetectBuilder autodetectBuilder = new AutodetectBuilder(job, filesToDelete, LOGGER, env,
                    settings, nativeController, processPipes)
                    .ignoreDowntime(ignoreDowntime)
                    .referencedLists(resolveLists(job.getAnalysisConfig().extractReferencedLists()));

            // if state is null or empty it will be ignored
            // else it is used to restore the quantiles
            if (quantiles != null) {
                autodetectBuilder.quantiles(quantiles);
            }

            autodetectBuilder.build();
            processPipes.connectStreams(PROCESS_STARTUP_TIMEOUT);
        } catch (IOException e) {
            String msg = "Failed to launch process for job " + job.getId();
            LOGGER.error(msg);
            throw ExceptionsHelper.serverError(msg, e);
        }
    }

    private Set<ListDocument> resolveLists(Set<String> listIds) {
        Set<ListDocument> resolved = new HashSet<>();
        for (String listId : listIds) {
            Optional<ListDocument> list = jobProvider.getList(listId);
            if (list.isPresent()) {
                resolved.add(list.get());
            } else {
                LOGGER.warn("List '" + listId + "' could not be retrieved.");
            }
        }
        return resolved;
    }
}

