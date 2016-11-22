/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.prelert.job.process.autodetect.output.StateProcessor;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class AutodetectCommunicator implements Closeable {

    private static final int DEFAULT_TRY_COUNT = 5;
    private static final int DEFAULT_TRY_TIMEOUT_SECS = 6;

    private final String jobId;
    private final Logger jobLogger;
    private final StatusReporter statusReporter;
    private final AutodetectProcess autodetectProcess;
    private final DataToProcessWriter autoDetectWriter;
    private final AutoDetectResultProcessor autoDetectResultProcessor;

    final AtomicBoolean inUse = new AtomicBoolean(false);

    public AutodetectCommunicator(ExecutorService autoDetectExecutor, Job job, AutodetectProcess process, Logger jobLogger,
                                  StatusReporter statusReporter, AutoDetectResultProcessor autoDetectResultProcessor,
                                  StateProcessor stateProcessor) {
        this.jobId = job.getJobId();
        this.autodetectProcess = process;
        this.jobLogger = jobLogger;
        this.statusReporter = statusReporter;
        this.autoDetectResultProcessor = autoDetectResultProcessor;

        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        boolean usePerPartitionNormalization = analysisConfig.getUsePerPartitionNormalization();
        autoDetectExecutor.execute(() ->
            autoDetectResultProcessor.process(jobLogger, process.getProcessOutStream(), usePerPartitionNormalization)
        );
        autoDetectExecutor.execute(() ->
            stateProcessor.process(job.getId(), process.getPersistStream())
        );
        this.autoDetectWriter = createProcessWriter(job, process, statusReporter);
    }

    private DataToProcessWriter createProcessWriter(Job job, AutodetectProcess process, StatusReporter statusReporter) {
        return DataToProcessWriterFactory.create(true, process, job.getDataDescription(), job.getAnalysisConfig(),
                job.getSchedulerConfig(), new TransformConfigs(job.getTransforms()) , statusReporter, jobLogger);
    }

    public DataCounts writeToJob(InputStream inputStream, DataLoadParams params) throws IOException {
        return checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPLOAD, jobId), () -> {
            if (params.isResettingBuckets()) {
                autodetectProcess.writeResetBucketsControlMessage(params);
            }
            CountingInputStream countingStream = new CountingInputStream(inputStream, statusReporter);
            DataCounts results = autoDetectWriter.write(countingStream);
            autoDetectWriter.flush();
            return results;
        });
    }

    @Override
    public void close() throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_CLOSE, jobId), () -> {
            autodetectProcess.close();
            autoDetectResultProcessor.awaitCompletion();
            return null;
        });
    }

    public void writeUpdateConfigMessage(String config) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPDATE, jobId), () -> {
            autodetectProcess.writeUpdateConfigMessage(config);
            return null;
        });
    }

    public void flushJob(InterimResultsParams params) throws IOException {
        flushJob(params, DEFAULT_TRY_COUNT, DEFAULT_TRY_TIMEOUT_SECS);
    }

    void flushJob(InterimResultsParams params, int tryCount, int tryTimeoutSecs) throws IOException {
        checkAndRun(false, () -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_FLUSH, jobId), () -> {
            int tryCountCounter = tryCount;
            String flushId = autodetectProcess.flushJob(params);

            // TODO: norelease: I think waiting once 30 seconds will have the same effect as 5 * 6 seconds.
            // So we may want to remove this retry logic here
            Duration intermittentTimeout = Duration.ofSeconds(tryTimeoutSecs);
            boolean isFlushComplete = false;
            while (isFlushComplete == false && --tryCountCounter >= 0) {
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
            return null;
        });
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

    private <T> T checkAndRun(Supplier<String> errorMessage, Callback<T> callback) throws IOException {
        return checkAndRun(true, errorMessage, callback);
    }

    private <T> T checkAndRun(boolean checkIsAlive, Supplier<String> errorMessage, Callback<T> callback) throws IOException {
        if (inUse.compareAndSet(false, true)) {
            try {
                if (checkIsAlive) {
                    checkProcessIsAlive();
                }
                return callback.run();
            } finally {
                inUse.set(false);
            }
        } else {
            throw new ElasticsearchStatusException(errorMessage.get(), RestStatus.TOO_MANY_REQUESTS);
        }
    }

    private interface Callback<T> {

        T run() throws IOException;

    }

}
