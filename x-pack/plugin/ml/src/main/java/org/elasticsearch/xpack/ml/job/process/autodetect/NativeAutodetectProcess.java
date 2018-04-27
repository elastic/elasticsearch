/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.process.writer.ControlMsgToProcessWriter;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * Autodetect process using native code.
 */
class NativeAutodetectProcess extends AbstractNativeProcess implements AutodetectProcess {
    private static final Logger LOGGER = Loggers.getLogger(NativeAutodetectProcess.class);

    private final AutodetectResultsParser resultsParser;

    NativeAutodetectProcess(String jobId, InputStream logStream, OutputStream processInStream, InputStream processOutStream,
                            OutputStream processRestoreStream, int numberOfFields, List<Path> filesToDelete,
                            AutodetectResultsParser resultsParser, Runnable onProcessCrash) {
        super(ProcessCtrl.AUTODETECT, LOGGER, jobId, logStream, processInStream, processOutStream, processRestoreStream, numberOfFields,
                filesToDelete, onProcessCrash);
        this.resultsParser = resultsParser;
    }

    @Override
    public void writeResetBucketsControlMessage(DataLoadParams params) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeResetBucketsMessage(params);
    }

    @Override
    public void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeUpdateModelPlotMessage(modelPlotConfig);
    }

    @Override
    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeUpdateDetectorRulesMessage(detectorIndex, rules);
    }

    @Override
    public void writeUpdateFiltersMessage(List<MlFilter> filters) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeUpdateFiltersMessage(filters);
    }

    @Override
    public void writeUpdateScheduledEventsMessage(List<ScheduledEvent> events, TimeValue bucketSpan) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeUpdateScheduledEventsMessage(events, bucketSpan);
    }

    @Override
    public void forecastJob(ForecastParams params) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeForecastMessage(params);
    }

    @Override
    public void persistJob() throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfFields);
        writer.writeStartBackgroundPersistMessage();
    }

    @Override
    public Iterator<AutodetectResult> readResults() {
        return resultsParser.parseResults(processOutStream);
    }
}
