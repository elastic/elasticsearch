/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.AbstractProcessCommunicator;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AutodetectCommunicator extends AbstractProcessCommunicator {

    private static final Logger LOGGER = Loggers.getLogger(AutodetectCommunicator.class);

    private final AutodetectProcess autodetectProcess;

    AutodetectCommunicator(Job job, Environment environment, AutodetectProcess autodetectProcess, StateStreamer stateStreamer,
                           DataCountsReporter dataCountsReporter, AutoDetectResultProcessor autoDetectResultProcessor,
                           Consumer<Exception> onFinishHandler, NamedXContentRegistry xContentRegistry,
                           ExecutorService executorService) {
        super(LOGGER, job, environment, autodetectProcess, stateStreamer, dataCountsReporter, autoDetectResultProcessor, onFinishHandler,
                xContentRegistry, executorService);
        this.autodetectProcess = autodetectProcess;
    }

    public void init(ModelSnapshot modelSnapshot) throws IOException {
        process.restoreState((modelSnapshot != null) ? r -> stateStreamer.restoreStateToStream(job.getId(), modelSnapshot, r) : null);
        createProcessWriter(Optional.empty()).writeHeader();
    }

    public void writeUpdateProcessMessage(UpdateParams updateParams, List<ScheduledEvent> scheduledEvents,
                                          BiConsumer<Void, Exception> handler) {
        submitOperation(() -> {
            if (updateParams.getModelPlotConfig() != null) {
                autodetectProcess.writeUpdateModelPlotMessage(updateParams.getModelPlotConfig());
            }

            // Filters have to be written before detectors
            if (updateParams.getFilter() != null) {
                autodetectProcess.writeUpdateFiltersMessage(Collections.singletonList(updateParams.getFilter()));
            }

            // Add detector rules
            if (updateParams.getDetectorUpdates() != null) {
                for (JobUpdate.DetectorUpdate update : updateParams.getDetectorUpdates()) {
                    if (update.getRules() != null) {
                        autodetectProcess.writeUpdateDetectorRulesMessage(update.getDetectorIndex(), update.getRules());
                    }
                }
            }

            // Add scheduled events; null means there's no update but an empty list means we should clear any events in the process
            if (scheduledEvents != null) {
                autodetectProcess.writeUpdateScheduledEventsMessage(scheduledEvents, job.getAnalysisConfig().getBucketSpan());
            }

            return null;
        }, handler);
    }

    protected void extraParamProcessing(DataLoadParams params) throws IOException {
        if (params.isResettingBuckets()) {
            autodetectProcess.writeResetBucketsControlMessage(params);
        }
    }

    public void forecastJob(ForecastParams params, BiConsumer<Void, Exception> handler) {
        BiConsumer<Void, Exception> forecastConsumer = (aVoid, e) -> {
            if (e == null) {
                FlushJobParams flushParams = FlushJobParams.builder().build();
                flushJob(flushParams, (flushAcknowledgement, flushException) -> {
                    if (flushException != null) {
                        String msg = String.format(Locale.ROOT, "[%s] exception while flushing job", job.getId());
                        handler.accept(null, ExceptionsHelper.serverError(msg, e));
                    } else {
                        handler.accept(null, null);
                    }
                });
            } else {
                handler.accept(null, e);
            }
        };
        submitOperation(() -> {
            autodetectProcess.forecastJob(params);
            return null;
        }, forecastConsumer);
    }

    public void persistJob(BiConsumer<Void, Exception> handler) {
        submitOperation(() -> {
            autodetectProcess.persistJob();
            return null;
        }, handler);
    }
}
