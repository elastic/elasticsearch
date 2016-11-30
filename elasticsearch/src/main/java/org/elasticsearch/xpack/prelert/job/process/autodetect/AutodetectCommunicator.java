/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.logging.Loggers;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class AutodetectCommunicator implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(AutodetectCommunicator.class);
    private static final int DEFAULT_TRY_TIMEOUT_SECS = 30;

    private final String jobId;
    private final StatusReporter statusReporter;
    private final AutodetectProcess autodetectProcess;
    private final DataToProcessWriter autoDetectWriter;
    private final AutoDetectResultProcessor autoDetectResultProcessor;

    final AtomicReference<CountDownLatch> inUse = new AtomicReference<>();

    public AutodetectCommunicator(ExecutorService autoDetectExecutor, Job job, AutodetectProcess process, StatusReporter statusReporter,
                                  AutoDetectResultProcessor autoDetectResultProcessor, StateProcessor stateProcessor) {
        this.jobId = job.getId();
        this.autodetectProcess = process;
        this.statusReporter = statusReporter;
        this.autoDetectResultProcessor = autoDetectResultProcessor;

        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        boolean usePerPartitionNormalization = analysisConfig.getUsePerPartitionNormalization();
        autoDetectExecutor.execute(() ->
            autoDetectResultProcessor.process(jobId, process.getProcessOutStream(), usePerPartitionNormalization)
        );
        autoDetectExecutor.execute(() ->
            stateProcessor.process(job.getId(), process.getPersistStream())
        );
        this.autoDetectWriter = createProcessWriter(job, process, statusReporter);
    }

    private DataToProcessWriter createProcessWriter(Job job, AutodetectProcess process, StatusReporter statusReporter) {
        return DataToProcessWriterFactory.create(true, process, job.getDataDescription(), job.getAnalysisConfig(),
                job.getSchedulerConfig(), new TransformConfigs(job.getTransforms()) , statusReporter, LOGGER);
    }

    public DataCounts writeToJob(InputStream inputStream, DataLoadParams params, Supplier<Boolean> cancelled) throws IOException {
        return checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPLOAD, jobId), () -> {
            if (params.isResettingBuckets()) {
                autodetectProcess.writeResetBucketsControlMessage(params);
            }
            CountingInputStream countingStream = new CountingInputStream(inputStream, statusReporter);
            DataCounts results = autoDetectWriter.write(countingStream, cancelled);
            autoDetectWriter.flush();
            return results;
        }, false);
    }

    @Override
    public void close() throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_CLOSE, jobId), () -> {
            statusReporter.close();
            autodetectProcess.close();
            autoDetectResultProcessor.awaitCompletion();
            return null;
        }, true);
    }

    public void writeUpdateConfigMessage(String config) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPDATE, jobId), () -> {
            autodetectProcess.writeUpdateConfigMessage(config);
            return null;
        }, false);
    }

    public void flushJob(InterimResultsParams params) throws IOException {
        flushJob(params, DEFAULT_TRY_TIMEOUT_SECS);
    }

    void flushJob(InterimResultsParams params, int tryTimeoutSecs) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_FLUSH, jobId), () -> {
            String flushId = autodetectProcess.flushJob(params);

            Duration timeout = Duration.ofSeconds(tryTimeoutSecs);
            LOGGER.info("[{}] waiting for flush", jobId);
            boolean isFlushComplete = autoDetectResultProcessor.waitForFlushAcknowledgement(flushId, timeout);
            LOGGER.info("[{}] isFlushComplete={}", jobId, isFlushComplete);
            if (!isFlushComplete) {
                String msg = Messages.getMessage(Messages.AUTODETECT_FLUSH_TIMEOUT, jobId) + " " + autodetectProcess.readError();
                LOGGER.error(msg);
                throw ExceptionsHelper.serverError(msg);
            }

            // We also have to wait for the normaliser to become idle so that we block
            // clients from querying results in the middle of normalisation.
            autoDetectResultProcessor.waitUntilRenormaliserIsIdle();
            return null;
        }, false);
    }

    /**
     * Throws an exception if the process has exited
     */
    private void checkProcessIsAlive() {
        if (!autodetectProcess.isProcessAlive()) {
            ParameterizedMessage message =
                    new ParameterizedMessage("[{}] Unexpected death of autodetect: {}", jobId, autodetectProcess.readError());
            LOGGER.error(message);
            throw ExceptionsHelper.serverError(message.getFormattedMessage());
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

    private <T> T checkAndRun(Supplier<String> errorMessage, Callback<T> callback, boolean wait) throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        if (inUse.compareAndSet(null, latch)) {
            try {
                checkProcessIsAlive();
                return callback.run();
            } finally {
                latch.countDown();
                inUse.set(null);
            }
        } else {
            if (wait) {
                latch = inUse.get();
                if (latch != null) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ElasticsearchStatusException(errorMessage.get(), RestStatus.TOO_MANY_REQUESTS);
                    }
                }
                checkProcessIsAlive();
                return callback.run();
            } else {
                throw new ElasticsearchStatusException(errorMessage.get(), RestStatus.TOO_MANY_REQUESTS);
            }
        }
    }

    private interface Callback<T> {

        T run() throws IOException;

    }

}
