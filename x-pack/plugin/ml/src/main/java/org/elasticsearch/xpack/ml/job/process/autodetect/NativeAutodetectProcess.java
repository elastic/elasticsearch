/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.AutodetectControlMsgWriter;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;
import org.elasticsearch.xpack.ml.process.NativeController;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Autodetect process using native code.
 */
class NativeAutodetectProcess extends AbstractNativeProcess implements AutodetectProcess {

    private static final Logger LOGGER = LogManager.getLogger(NativeAutodetectProcess.class);

    private static final String NAME = "autodetect";

    private final ProcessResultsParser<AutodetectResult> resultsParser;

    NativeAutodetectProcess(String jobId, NativeController nativeController, ProcessPipes processPipes,
                            int numberOfFields, List<Path> filesToDelete, ProcessResultsParser<AutodetectResult> resultsParser,
                            Consumer<String> onProcessCrash, Duration processConnectTimeout) {
        super(jobId, nativeController, processPipes, numberOfFields, filesToDelete, onProcessCrash, processConnectTimeout);
        this.resultsParser = resultsParser;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void restoreState(StateStreamer stateStreamer, ModelSnapshot modelSnapshot) {
        if (modelSnapshot != null) {
            try (OutputStream r = processRestoreStream()) {
                stateStreamer.restoreStateToStream(jobId(), modelSnapshot, r);
            } catch (Exception e) {
                // TODO: should we fail to start?
                if (isProcessKilled() == false) {
                    LOGGER.error("Error restoring model state for job " + jobId(), e);
                }
            }
        }
        setReady();
    }

    @Override
    public void writeResetBucketsControlMessage(DataLoadParams params) throws IOException {
        newMessageWriter().writeResetBucketsMessage(params);
    }

    @Override
    public void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) throws IOException {
        newMessageWriter().writeUpdateModelPlotMessage(modelPlotConfig);
    }

    @Override
    public void writeUpdatePerPartitionCategorizationMessage(PerPartitionCategorizationConfig perPartitionCategorizationConfig)
        throws IOException {
        newMessageWriter().writeCategorizationStopOnWarnMessage(perPartitionCategorizationConfig.isStopOnWarn());
    }

    @Override
    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException {
        newMessageWriter().writeUpdateDetectorRulesMessage(detectorIndex, rules);
    }

    @Override
    public void writeUpdateFiltersMessage(List<MlFilter> filters) throws IOException {
        newMessageWriter().writeUpdateFiltersMessage(filters);
    }

    @Override
    public void writeUpdateScheduledEventsMessage(List<ScheduledEvent> events, TimeValue bucketSpan) throws IOException {
        newMessageWriter().writeUpdateScheduledEventsMessage(events, bucketSpan);
    }

    @Override
    public String flushJob(FlushJobParams params) throws IOException {
        AutodetectControlMsgWriter writer = newMessageWriter();
        writer.writeFlushControlMessage(params);
        return writer.writeFlushMessage();
    }

    @Override
    public void forecastJob(ForecastParams params) throws IOException {
        newMessageWriter().writeForecastMessage(params);
    }

    @Override
    public void persistState() throws IOException {
        newMessageWriter().writeStartBackgroundPersistMessage();
    }

    @Override
    public Iterator<AutodetectResult> readAutodetectResults() {
        return resultsParser.parseResults(processOutStream());
    }

    private AutodetectControlMsgWriter newMessageWriter() {
        return new AutodetectControlMsgWriter(recordWriter(), numberOfFields());
    }
}
