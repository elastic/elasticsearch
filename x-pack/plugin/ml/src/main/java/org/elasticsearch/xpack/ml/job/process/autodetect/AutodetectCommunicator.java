/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.CountingInputStream;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.DataToProcessWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.JsonDataToProcessWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class AutodetectCommunicator implements Closeable {

    private static final Logger logger = LogManager.getLogger(AutodetectCommunicator.class);
    private static final Duration FLUSH_PROCESS_CHECK_FREQUENCY = Duration.ofSeconds(1);

    private final Job job;
    private final AutodetectProcess autodetectProcess;
    private final StateStreamer stateStreamer;
    private final DataCountsReporter dataCountsReporter;
    private final AutodetectResultProcessor autodetectResultProcessor;
    private final BiConsumer<Exception, Boolean> onFinishHandler;
    private final ExecutorService autodetectWorkerExecutor;
    private final NamedXContentRegistry xContentRegistry;
    private final boolean includeTokensField;
    private volatile CategorizationAnalyzer categorizationAnalyzer;
    private volatile boolean processKilled;

    AutodetectCommunicator(
        Job job,
        AutodetectProcess process,
        StateStreamer stateStreamer,
        DataCountsReporter dataCountsReporter,
        AutodetectResultProcessor autodetectResultProcessor,
        BiConsumer<Exception, Boolean> onFinishHandler,
        NamedXContentRegistry xContentRegistry,
        ExecutorService autodetectWorkerExecutor
    ) {
        this.job = job;
        this.autodetectProcess = process;
        this.stateStreamer = stateStreamer;
        this.dataCountsReporter = dataCountsReporter;
        this.autodetectResultProcessor = autodetectResultProcessor;
        this.onFinishHandler = onFinishHandler;
        this.xContentRegistry = xContentRegistry;
        this.autodetectWorkerExecutor = autodetectWorkerExecutor;
        this.includeTokensField = job.getAnalysisConfig().getCategorizationFieldName() != null;
    }

    public void restoreState(ModelSnapshot modelSnapshot) {
        autodetectProcess.restoreState(stateStreamer, modelSnapshot);
    }

    private DataToProcessWriter createProcessWriter(DataDescription dataDescription) {
        return new JsonDataToProcessWriter(
            true,
            includeTokensField,
            autodetectProcess,
            dataDescription,
            job.getAnalysisConfig(),
            dataCountsReporter,
            xContentRegistry
        );
    }

    /**
     * This must be called once before {@link #writeToJob(InputStream, AnalysisRegistry, XContentType, DataLoadParams, BiConsumer)}
     * can be used
     */
    public void writeHeader() throws IOException {
        createProcessWriter(job.getDataDescription()).writeHeader();
    }

    /**
     * Call {@link #writeHeader()} exactly once before using this method
     */
    public void writeToJob(
        InputStream inputStream,
        AnalysisRegistry analysisRegistry,
        XContentType xContentType,
        DataLoadParams params,
        BiConsumer<DataCounts, Exception> handler
    ) {
        submitOperation(() -> {
            if (params.isResettingBuckets()) {
                autodetectProcess.writeResetBucketsControlMessage(params);
            }

            CountingInputStream countingStream = new CountingInputStream(inputStream, dataCountsReporter);
            DataToProcessWriter autodetectWriter = createProcessWriter(params.getDataDescription().orElse(job.getDataDescription()));

            if (includeTokensField && categorizationAnalyzer == null) {
                createCategorizationAnalyzer(analysisRegistry);
            }

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<DataCounts> dataCountsAtomicReference = new AtomicReference<>();
            AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
            autodetectWriter.write(countingStream, categorizationAnalyzer, xContentType, (dataCounts, e) -> {
                dataCountsAtomicReference.set(dataCounts);
                exceptionAtomicReference.set(e);
                latch.countDown();
            });

            latch.await();
            autodetectWriter.flushStream();

            if (exceptionAtomicReference.get() != null) {
                throw exceptionAtomicReference.get();
            } else {
                return dataCountsAtomicReference.get();
            }
        }, handler);
    }

    /**
     * Closes job this communicator is encapsulating.
     */
    @Override
    public void close() {
        Future<?> future = autodetectWorkerExecutor.submit(() -> {
            checkProcessIsAlive();
            try {
                if (autodetectProcess.isReady()) {
                    autodetectProcess.close();
                } else {
                    killProcess(false, false);
                    stateStreamer.cancel();
                }
                autodetectResultProcessor.awaitCompletion();
            } finally {
                onFinishHandler.accept(null, true);
            }
            logger.info("[{}] autodetect connection for job closed", job.getId());
            return null;
        });
        try {
            future.get();
            autodetectWorkerExecutor.shutdown();
            dataCountsReporter.writeUnreportedCounts();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (processKilled) {
                // In this case the original exception is spurious and highly misleading
                throw ExceptionsHelper.conflictStatusException("Close job interrupted by kill request");
            } else {
                throw FutureUtils.rethrowExecutionException(e);
            }
        } finally {
            destroyCategorizationAnalyzer();
        }
    }

    public void killProcess(boolean awaitCompletion, boolean finish) throws IOException {
        killProcess(awaitCompletion, finish, true);
    }

    public void killProcess(boolean awaitCompletion, boolean finish, boolean finalizeJob) throws IOException {
        try {
            processKilled = true;
            autodetectResultProcessor.setProcessKilled();
            autodetectWorkerExecutor.shutdown();
            autodetectProcess.kill(awaitCompletion);

            if (awaitCompletion) {
                try {
                    autodetectResultProcessor.awaitCompletion();
                } catch (TimeoutException e) {
                    logger.warn(() -> "[" + job.getId() + "] Timed out waiting for killed job", e);
                }
            }
        } finally {
            if (finish) {
                onFinishHandler.accept(null, finalizeJob);
            }
            destroyCategorizationAnalyzer();
        }
    }

    public void writeUpdateProcessMessage(UpdateProcessMessage update, BiConsumer<Void, Exception> handler) {
        submitOperation(() -> {
            if (update.getModelPlotConfig() != null) {
                autodetectProcess.writeUpdateModelPlotMessage(update.getModelPlotConfig());
            }

            if (update.getPerPartitionCategorizationConfig() != null) {
                autodetectProcess.writeUpdatePerPartitionCategorizationMessage(update.getPerPartitionCategorizationConfig());
            }

            // Filters have to be written before detectors
            if (update.getFilters() != null) {
                autodetectProcess.writeUpdateFiltersMessage(update.getFilters());
            }

            // Add detector rules
            if (update.getDetectorUpdates() != null) {
                for (JobUpdate.DetectorUpdate detectorUpdate : update.getDetectorUpdates()) {
                    if (detectorUpdate.getRules() != null) {
                        autodetectProcess.writeUpdateDetectorRulesMessage(detectorUpdate.getDetectorIndex(), detectorUpdate.getRules());
                    }
                }
            }

            // Add scheduled events; null means there's no update but an empty list means we should clear any events in the process
            if (update.getScheduledEvents() != null) {
                autodetectProcess.writeUpdateScheduledEventsMessage(update.getScheduledEvents(), job.getAnalysisConfig().getBucketSpan());
            }

            return null;
        }, handler);
    }

    public void flushJob(FlushJobParams params, BiConsumer<FlushAcknowledgement, Exception> handler) {
        submitOperation(() -> {
            String flushId = autodetectProcess.flushJob(params);
            try {
                dataCountsReporter.writeUnreportedCounts();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return waitFlushToCompletion(flushId, params.isWaitForNormalization());
        }, handler);
    }

    public void forecastJob(ForecastParams params, BiConsumer<Void, Exception> handler) {
        BiConsumer<Void, Exception> forecastConsumer = (aVoid, e) -> {
            if (e == null) {
                // Forecasting does not care about normalization of the local data as it is not being queried
                FlushJobParams flushParams = FlushJobParams.builder().waitForNormalization(false).build();
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
            autodetectProcess.persistState();
            return null;
        }, handler);
    }

    @Nullable
    FlushAcknowledgement waitFlushToCompletion(String flushId, boolean waitForNormalization) throws Exception {
        logger.debug("[{}] waiting for flush", job.getId());

        FlushAcknowledgement flushAcknowledgement;
        try {
            flushAcknowledgement = autodetectResultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            while (flushAcknowledgement == null) {
                checkProcessIsAlive();
                checkResultsProcessorIsAlive();
                flushAcknowledgement = autodetectResultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            }
        } finally {
            autodetectResultProcessor.clearAwaitingFlush(flushId);
        }

        if (processKilled == false) {
            // We also have to wait for the normalizer to become idle so that we block
            // clients from querying results in the middle of normalization.
            if (waitForNormalization) {
                logger.debug("[{}] Initial flush completed, waiting until renormalizer is idle.", job.getId());
                autodetectResultProcessor.waitUntilRenormalizerIsIdle();
            }

            logger.debug("[{}] Flush completed", job.getId());
        }

        return flushAcknowledgement;
    }

    /**
     * Throws an exception if the process has exited
     */
    private void checkProcessIsAlive() {
        if (autodetectProcess.isProcessAlive() == false) {
            // Don't log here - it just causes double logging when the exception gets logged
            throw new ElasticsearchException("[{}] Unexpected death of autodetect: {}", job.getId(), autodetectProcess.readError());
        }
    }

    private void checkResultsProcessorIsAlive() {
        if (autodetectResultProcessor.isFailed()) {
            // Don't log here - it just causes double logging when the exception gets logged
            throw new ElasticsearchException("[{}] Unexpected death of the result processor", job.getId());
        }
    }

    public ZonedDateTime getProcessStartTime() {
        return autodetectProcess.getProcessStartTime();
    }

    public ModelSizeStats getModelSizeStats() {
        return autodetectResultProcessor.modelSizeStats();
    }

    public TimingStats getTimingStats() {
        return autodetectResultProcessor.timingStats();
    }

    public DataCounts getDataCounts() {
        return dataCountsReporter.runningTotalStats();
    }

    /**
     * Care must be taken to ensure this method is not called while data is being posted.
     * The methods in this class that call it wait for all processing to complete first.
     * The expectation is that external calls are only made when cleaning up after a fatal
     * error.
     */
    void destroyCategorizationAnalyzer() {
        if (categorizationAnalyzer != null) {
            categorizationAnalyzer.close();
            categorizationAnalyzer = null;
        }
    }

    private <T> void submitOperation(CheckedSupplier<T, Exception> operation, BiConsumer<T, Exception> handler) {
        autodetectWorkerExecutor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (processKilled) {
                    handler.accept(
                        null,
                        ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed",
                            job.getId()
                        )
                    );
                } else {
                    logger.error(() -> "[" + job.getId() + "] Unexpected exception writing to process", e);
                    handler.accept(null, e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                if (processKilled) {
                    handler.accept(
                        null,
                        ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed",
                            job.getId()
                        )
                    );
                } else {
                    checkProcessIsAlive();
                    handler.accept(operation.get(), null);
                }
            }
        });
    }

    private void createCategorizationAnalyzer(AnalysisRegistry analysisRegistry) throws IOException {
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = analysisConfig.getCategorizationAnalyzerConfig();
        if (categorizationAnalyzerConfig == null) {
            categorizationAnalyzerConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(
                analysisConfig.getCategorizationFilters()
            );
        }
        categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, categorizationAnalyzerConfig);
    }

    public void setVacating(boolean vacating) {
        autodetectResultProcessor.setVacating(vacating);
    }
}
