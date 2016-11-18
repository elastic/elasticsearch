/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.parsing.AutoDetectResultProcessor;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.parsing.StateReader;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.writer.DataToProcessWriter;
import org.elasticsearch.xpack.prelert.job.process.autodetect.writer.DataToProcessWriterFactory;
import org.elasticsearch.xpack.prelert.job.status.CountingInputStream;
import org.elasticsearch.xpack.prelert.job.status.StatusReporter;
import org.elasticsearch.xpack.prelert.job.transform.TransformConfigs;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;

public class AutodetectCommunicator implements Closeable {

    private static final int DEFAULT_TRY_COUNT = 5;
    private static final int DEFAULT_TRY_TIMEOUT_SECS = 6;

    private final Logger jobLogger;
    private final StatusReporter statusReporter;
    private final AutodetectProcess autodetectProcess;
    private final DataToProcessWriter autoDetectWriter;
    private final AutoDetectResultProcessor autoDetectResultProcessor;

    private final StateReader stateReader;
    private final Thread stateParserThread;


    public AutodetectCommunicator(ThreadPool threadPool, Job job, AutodetectProcess process, Logger jobLogger,
                                  JobResultsPersister persister, StatusReporter statusReporter,
                                  AutoDetectResultProcessor autoDetectResultProcessor) {
        this.autodetectProcess = process;
        this.jobLogger = jobLogger;
        this.statusReporter = statusReporter;
        this.autoDetectResultProcessor = autoDetectResultProcessor;
        this.stateReader = new StateReader(persister, process.getPersistStream(), this.jobLogger);

        // TODO norelease: prevent that we fail to start any of the required threads for interacting with analytical process:
        // We should before we start the analytical process (and scheduler) verify that have enough threads.
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        boolean usePerPartitionNormalization = analysisConfig.getUsePerPartitionNormalization();
        threadPool.executor(PrelertPlugin.THREAD_POOL_NAME).execute(() -> {
            this.autoDetectResultProcessor.process(jobLogger, process.getProcessOutStream(), usePerPartitionNormalization);
        });
        // NORELEASE - use ES ThreadPool
        stateParserThread = new Thread(stateReader, job.getId() + "-State-Parser");
        stateParserThread.start();

        this.autoDetectWriter = createProcessWriter(job, process, statusReporter);
    }

    private DataToProcessWriter createProcessWriter(Job job, AutodetectProcess process, StatusReporter statusReporter) {
        return DataToProcessWriterFactory.create(true, process, job.getDataDescription(), job.getAnalysisConfig(),
                job.getSchedulerConfig(), new TransformConfigs(job.getTransforms()) , statusReporter, jobLogger);
    }

    public DataCounts writeToJob(InputStream inputStream) throws IOException {
        checkProcessIsAlive();
        CountingInputStream countingStream = new CountingInputStream(inputStream, statusReporter);
        DataCounts results = autoDetectWriter.write(countingStream);
        autoDetectWriter.flush();
        return results;
    }

    @Override
    public void close() throws IOException {
        checkProcessIsAlive();
        autodetectProcess.close();
        autoDetectResultProcessor.awaitCompletion();
    }

    public void writeResetBucketsControlMessage(DataLoadParams params) throws IOException {
        checkProcessIsAlive();
        autodetectProcess.writeResetBucketsControlMessage(params);
    }

    public void writeUpdateConfigMessage(String config) throws IOException {
        checkProcessIsAlive();
        autodetectProcess.writeUpdateConfigMessage(config);
    }

    public void flushJob(InterimResultsParams params) throws IOException {
        flushJob(params, DEFAULT_TRY_COUNT, DEFAULT_TRY_TIMEOUT_SECS);
    }

    void flushJob(InterimResultsParams params, int tryCount, int tryTimeoutSecs) throws IOException {
        String flushId = autodetectProcess.flushJob(params);

        // TODO: norelease: I think waiting once 30 seconds will have the same effect as 5 * 6 seconds.
        // So we may want to remove this retry logic here
        Duration intermittentTimeout = Duration.ofSeconds(tryTimeoutSecs);
        boolean isFlushComplete = false;
        while (isFlushComplete == false && --tryCount >= 0) {
            // Check there wasn't an error in the flush
            if (!autodetectProcess.isProcessAlive()) {

                String msg = Messages.getMessage(Messages.AUTODETECT_FLUSH_UNEXPTECTED_DEATH) + " " + autodetectProcess.readError();
                jobLogger.error(msg);
                throw ExceptionsHelper.serverError(msg);
            }
            isFlushComplete = autoDetectResultProcessor.waitForFlushAcknowledgement(flushId, intermittentTimeout);
            jobLogger.info("isFlushComplete={}", isFlushComplete);
        }

        if (!isFlushComplete) {
            String msg = Messages.getMessage(Messages.AUTODETECT_FLUSH_TIMEOUT) + " " + autodetectProcess.readError();
            jobLogger.error(msg);
            throw ExceptionsHelper.serverError(msg);
        }

        // We also have to wait for the normaliser to become idle so that we block
        // clients from querying results in the middle of normalisation.
        autoDetectResultProcessor.waitUntilRenormaliserIsIdle();
    }

    /**
     * Throws an exception if the process has exited
     */
    private void checkProcessIsAlive() {
        if (!autodetectProcess.isProcessAlive()) {
            String errorMsg = "Unexpected death of autodetect: " + autodetectProcess.readError();
            jobLogger.error(errorMsg);
            throw ExceptionsHelper.serverError(errorMsg);
        }
    }

    public ZonedDateTime getProcessStartTime() {
        return autodetectProcess.getProcessStartTime();
    }

    public Optional<ModelSizeStats> getModelSizeStats() {
        return autoDetectResultProcessor.modelSizeStats();
    }

    public Optional<DataCounts> getDataCounts() {
        return Optional.ofNullable(statusReporter.runningTotalStats());
    }
}
