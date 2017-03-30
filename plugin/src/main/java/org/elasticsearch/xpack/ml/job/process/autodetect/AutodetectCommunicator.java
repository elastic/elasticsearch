/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.process.CountingInputStream;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.DataToProcessWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.DataToProcessWriterFactory;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AutodetectCommunicator implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(AutodetectCommunicator.class);
    private static final Duration FLUSH_PROCESS_CHECK_FREQUENCY = Duration.ofSeconds(1);

    private final Job job;
    private final DataCountsReporter dataCountsReporter;
    private final AutodetectProcess autodetectProcess;
    private final AutoDetectResultProcessor autoDetectResultProcessor;
    private final Consumer<Exception> handler;

    final AtomicReference<CountDownLatch> inUse = new AtomicReference<>();
    private final NamedXContentRegistry xContentRegistry;

    AutodetectCommunicator(Job job, AutodetectProcess process, DataCountsReporter dataCountsReporter,
                           AutoDetectResultProcessor autoDetectResultProcessor, Consumer<Exception> handler,
                           NamedXContentRegistry xContentRegistry) {
        this.job = job;
        this.autodetectProcess = process;
        this.dataCountsReporter = dataCountsReporter;
        this.autoDetectResultProcessor = autoDetectResultProcessor;
        this.handler = handler;
        this.xContentRegistry = xContentRegistry;
    }

    public void writeJobInputHeader() throws IOException {
        createProcessWriter(Optional.empty()).writeHeader();
    }

    private DataToProcessWriter createProcessWriter(Optional<DataDescription> dataDescription) {
        return DataToProcessWriterFactory.create(true, autodetectProcess,
                dataDescription.orElse(job.getDataDescription()), job.getAnalysisConfig(),
                dataCountsReporter, xContentRegistry);
    }

    public DataCounts writeToJob(InputStream inputStream, XContentType xContentType,
            DataLoadParams params) throws IOException {
        return checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPLOAD, job.getId()), () -> {
            if (params.isResettingBuckets()) {
                autodetectProcess.writeResetBucketsControlMessage(params);
            }
            CountingInputStream countingStream = new CountingInputStream(inputStream, dataCountsReporter);

            DataToProcessWriter autoDetectWriter = createProcessWriter(params.getDataDescription());
                    DataCounts results = autoDetectWriter.write(countingStream, xContentType);
            autoDetectWriter.flush();
            return results;
        }, false);
    }

    @Override
    public void close() throws IOException {
        close(false, null);
    }

    /**
     * Closes job this communicator is encapsulating.
     *
     * @param restart   Whether the job should be restarted by persistent tasks
     * @param reason    The reason for closing the job
     */
    public void close(boolean restart, String reason) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_CLOSE, job.getId()), () -> {
            LOGGER.info("[{}] job closing, reason [{}]", job.getId(), reason);
            dataCountsReporter.close();
            autodetectProcess.close();
            autoDetectResultProcessor.awaitCompletion();
            handler.accept(restart ? new ElasticsearchException(reason) : null);
            LOGGER.info("[{}] job closed", job.getId());
            return null;
        }, true);
    }


    public void writeUpdateModelPlotMessage(ModelPlotConfig config) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPDATE, job.getId()), () -> {
            autodetectProcess.writeUpdateModelPlotMessage(config);
            return null;
        }, false);
    }

    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_UPDATE, job.getId()), () -> {
            autodetectProcess.writeUpdateDetectorRulesMessage(detectorIndex, rules);
            return null;
        }, false);
    }

    public void flushJob(InterimResultsParams params) throws IOException {
        checkAndRun(() -> Messages.getMessage(Messages.JOB_DATA_CONCURRENT_USE_FLUSH, job.getId()), () -> {
            String flushId = autodetectProcess.flushJob(params);
            waitFlushToCompletion(flushId);
            return null;
        }, false);
    }

    private void waitFlushToCompletion(String flushId) throws IOException {
        LOGGER.info("[{}] waiting for flush", job.getId());

        try {
            boolean isFlushComplete = autoDetectResultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            while (isFlushComplete == false) {
                checkProcessIsAlive();
                isFlushComplete = autoDetectResultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            }
        } finally {
            autoDetectResultProcessor.clearAwaitingFlush(flushId);
        }

        // We also have to wait for the normalizer to become idle so that we block
        // clients from querying results in the middle of normalization.
        autoDetectResultProcessor.waitUntilRenormalizerIsIdle();

        LOGGER.info("[{}] Flush completed", job.getId());
    }

    /**
     * Throws an exception if the process has exited
     */
    private void checkProcessIsAlive() {
        if (!autodetectProcess.isProcessAlive()) {
            ParameterizedMessage message =
                    new ParameterizedMessage("[{}] Unexpected death of autodetect: {}", job.getId(), autodetectProcess.readError());
            LOGGER.error(message);
            throw ExceptionsHelper.serverError(message.getFormattedMessage());
        }
    }

    public ZonedDateTime getProcessStartTime() {
        return autodetectProcess.getProcessStartTime();
    }

    public ModelSizeStats getModelSizeStats() {
        return autoDetectResultProcessor.modelSizeStats();
    }

    public DataCounts getDataCounts() {
        return dataCountsReporter.runningTotalStats();
    }

    private <T> T checkAndRun(Supplier<String> errorMessage, CheckedSupplier<T, IOException> callback, boolean wait) throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        if (inUse.compareAndSet(null, latch)) {
            try {
                checkProcessIsAlive();
                return callback.get();
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
                return callback.get();
            } else {
                throw new ElasticsearchStatusException(errorMessage.get(), RestStatus.TOO_MANY_REQUESTS);
            }
        }
    }

}
