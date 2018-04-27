/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.writer.DataToProcessWriter;
import org.elasticsearch.xpack.ml.job.process.writer.DataToProcessWriterFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractProcessCommunicator implements Closeable {

    private static final Duration FLUSH_PROCESS_CHECK_FREQUENCY = Duration.ofSeconds(1);

    protected final Logger logger;
    protected final Job job;
    protected final Environment environment;
    protected final MlProcess process;
    protected final StateStreamer stateStreamer;
    private final DataCountsReporter dataCountsReporter;
    private final AutoDetectResultProcessor resultProcessor;
    private final Consumer<Exception> onFinishHandler;
    private final ExecutorService executorService;
    private final NamedXContentRegistry xContentRegistry;
    private final boolean includeTokensField;
    private volatile CategorizationAnalyzer categorizationAnalyzer;
    private volatile boolean processKilled;

    protected AbstractProcessCommunicator(Logger logger, Job job, Environment environment, MlProcess process, StateStreamer stateStreamer,
                                          DataCountsReporter dataCountsReporter, AutoDetectResultProcessor resultProcessor,
                                          Consumer<Exception> onFinishHandler, NamedXContentRegistry xContentRegistry,
                                          ExecutorService executorService) {
        this.logger = logger;
        this.job = job;
        this.environment = environment;
        this.process = process;
        this.stateStreamer = stateStreamer;
        this.dataCountsReporter = dataCountsReporter;
        this.resultProcessor = resultProcessor;
        this.onFinishHandler = onFinishHandler;
        this.xContentRegistry = xContentRegistry;
        this.executorService = executorService;
        this.includeTokensField = MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA
                && job.getAnalysisConfig().getCategorizationFieldName() != null;
    }

    protected DataToProcessWriter createProcessWriter(Optional<DataDescription> dataDescription) {
        return DataToProcessWriterFactory.create(true, includeTokensField, process,
                dataDescription.orElse(job.getDataDescription()), job.getAnalysisConfig(),
                dataCountsReporter, xContentRegistry);
    }

    public void writeToJob(InputStream inputStream, AnalysisRegistry analysisRegistry, XContentType xContentType,
                           DataLoadParams params, BiConsumer<DataCounts, Exception> handler) {
        submitOperation(() -> {
                    extraParamProcessing(params);

                    CountingInputStream countingStream = new CountingInputStream(inputStream, dataCountsReporter);
                    DataToProcessWriter autoDetectWriter = createProcessWriter(params.getDataDescription());

                    if (includeTokensField && categorizationAnalyzer == null) {
                        createCategorizationAnalyzer(analysisRegistry);
                    }

                    CountDownLatch latch = new CountDownLatch(1);
                    AtomicReference<DataCounts> dataCountsAtomicReference = new AtomicReference<>();
                    AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
                    autoDetectWriter.write(countingStream, categorizationAnalyzer, xContentType, (dataCounts, e) -> {
                        dataCountsAtomicReference.set(dataCounts);
                        exceptionAtomicReference.set(e);
                        latch.countDown();
                    });

                    latch.await();
                    autoDetectWriter.flushStream();

                    if (exceptionAtomicReference.get() != null) {
                        throw exceptionAtomicReference.get();
                    } else {
                        return dataCountsAtomicReference.get();
                    }
                },
                handler);
    }

    protected abstract void extraParamProcessing(DataLoadParams params) throws IOException;

    @Override
    public void close() {
        close(false, null);
    }

    /**
     * Closes job this communicator is encapsulating.
     *
     * @param restart   Whether the job should be restarted by persistent tasks
     * @param reason    The reason for closing the job
     */
    public void close(boolean restart, String reason) {
        Future<?> future = executorService.submit(() -> {
            checkProcessIsAlive();
            try {
                if (process.isReady()) {
                    process.close();
                } else {
                    killProcess(false, false);
                    stateStreamer.cancel();
                }
                resultProcessor.awaitCompletion();
            } finally {
                onFinishHandler.accept(restart ? new ElasticsearchException(reason) : null);
            }
            logger.info("[{}] job closed", job.getId());
            return null;
        });
        try {
            future.get();
            executorService.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (processKilled) {
                // In this case the original exception is spurious and highly misleading
                throw ExceptionsHelper.conflictStatusException("Close job interrupted by kill request");
            } else {
                throw new ElasticsearchException(e);
            }
        } finally {
            destroyCategorizationAnalyzer();
        }
    }

    public void killProcess(boolean awaitCompletion, boolean finish) throws IOException {
        try {
            processKilled = true;
            resultProcessor.setProcessKilled();
            executorService.shutdown();
            process.kill();

            if (awaitCompletion) {
                try {
                    resultProcessor.awaitCompletion();
                } catch (TimeoutException e) {
                    logger.warn(new ParameterizedMessage("[{}] Timed out waiting for killed job", job.getId()), e);
                }
            }
        } finally {
            if (finish) {
                onFinishHandler.accept(null);
            }
            destroyCategorizationAnalyzer();
        }
    }

    public void flushJob(FlushJobParams params, BiConsumer<FlushAcknowledgement, Exception> handler) {
        submitOperation(() -> {
            String flushId = process.flushJob(params);
            return waitFlushToCompletion(flushId);
        }, handler);
    }

    @Nullable
    public FlushAcknowledgement waitFlushToCompletion(String flushId) {
        logger.debug("[{}] waiting for flush", job.getId());

        FlushAcknowledgement flushAcknowledgement;
        try {
            flushAcknowledgement = resultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            while (flushAcknowledgement == null) {
                checkProcessIsAlive();
                checkResultsProcessorIsAlive();
                flushAcknowledgement = resultProcessor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
            }
        } finally {
            resultProcessor.clearAwaitingFlush(flushId);
        }

        if (processKilled == false) {
            // We also have to wait for the normalizer to become idle so that we block
            // clients from querying results in the middle of normalization.
            resultProcessor.waitUntilRenormalizerIsIdle();

            logger.debug("[{}] Flush completed", job.getId());
        }

        return flushAcknowledgement;
    }

    /**
     * Throws an exception if the process has exited
     */
    protected void checkProcessIsAlive() {
        if (!process.isProcessAlive()) {
            // Don't log here - it just causes double logging when the exception gets logged
            throw new ElasticsearchException("[{}] Unexpected death of autodetect: {}", job.getId(), process.readError());
        }
    }

    protected void checkResultsProcessorIsAlive() {
        if (resultProcessor.isFailed()) {
            // Don't log here - it just causes double logging when the exception gets logged
            throw new ElasticsearchException("[{}] Unexpected death of the result processor", job.getId());
        }
    }

    public ZonedDateTime getProcessStartTime() {
        return process.getProcessStartTime();
    }

    public ModelSizeStats getModelSizeStats() {
        return resultProcessor.modelSizeStats();
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
    public void destroyCategorizationAnalyzer() {
        if (categorizationAnalyzer != null) {
            categorizationAnalyzer.close();
            categorizationAnalyzer = null;
        }
    }

    protected <T> void submitOperation(CheckedSupplier<T, Exception> operation, BiConsumer<T, Exception> handler) {
        executorService.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (processKilled) {
                    handler.accept(null, ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed", job.getId()));
                } else {
                    logger.error(new ParameterizedMessage("[{}] Unexpected exception writing to process", job.getId()), e);
                    handler.accept(null, e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                if (processKilled) {
                    handler.accept(null, ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed", job.getId()));
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
            categorizationAnalyzerConfig =
                    CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(analysisConfig.getCategorizationFilters());
        }
        categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, environment, categorizationAnalyzerConfig);
    }
}
