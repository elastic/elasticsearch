/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.JobSnapshotUpgraderResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.snapshot.upgrader.SnapshotUpgradeTask;
import org.elasticsearch.xpack.ml.process.NativeStorageProvider;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;

public final class JobModelSnapshotUpgrader {
    private static final Duration FLUSH_PROCESS_CHECK_FREQUENCY = Duration.ofSeconds(1);
    private static final Logger logger = LogManager.getLogger(JobModelSnapshotUpgrader.class);

    private final SnapshotUpgradeTask task;
    private final Job job;
    private final String jobId;
    private final String snapshotId;
    private final AutodetectParams params;
    private final Client client;
    private final Consumer<Exception> onFinish;
    private final Supplier<Boolean> continueRunning;
    private final ThreadPool threadPool;
    private final AutodetectProcessFactory autodetectProcessFactory;
    private final JobResultsPersister jobResultsPersister;
    private final NativeStorageProvider nativeStorageProvider;

    JobModelSnapshotUpgrader(SnapshotUpgradeTask task,
                             Job job,
                             AutodetectParams params,
                             ThreadPool threadPool,
                             AutodetectProcessFactory autodetectProcessFactory,
                             JobResultsPersister jobResultsPersister,
                             Client client,
                             NativeStorageProvider nativeStorageProvider,
                             Consumer<Exception> onFinish,
                             Supplier<Boolean> continueRunning) {
        this.task = Objects.requireNonNull(task);
        this.job = Objects.requireNonNull(job);
        this.params = Objects.requireNonNull(params);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.autodetectProcessFactory = Objects.requireNonNull(autodetectProcessFactory);
        this.jobResultsPersister = Objects.requireNonNull(jobResultsPersister);
        this.nativeStorageProvider = Objects.requireNonNull(nativeStorageProvider);
        this.client = Objects.requireNonNull(client);
        this.onFinish = Objects.requireNonNull(onFinish);
        this.continueRunning = Objects.requireNonNull(continueRunning);
        this.jobId = task.getJobId();
        this.snapshotId = task.getSnapshotId();
    }

    void start() {
        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService autodetectExecutorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);

        AutodetectProcess process = autodetectProcessFactory.createAutodetectProcess(jobId + "-" + snapshotId,
            job,
            params,
            autodetectExecutorService,
            (reason) -> {
                setTaskToFailed(reason, ActionListener.wrap(t -> {
                }, f -> {
                }));
                try {
                    nativeStorageProvider.cleanupLocalTmpStorage(task.getDescription());
                } catch (IOException e) {
                    logger.error(
                        new ParameterizedMessage("[{}] [{}] failed to delete temporary files snapshot upgrade", jobId, snapshotId),
                        e);
                }
            });
        JobSnapshotUpgraderResultProcessor processor = new JobSnapshotUpgraderResultProcessor(
            jobId,
            snapshotId,
            jobResultsPersister,
            process);
        AutodetectWorkerExecutorService autodetectWorkerExecutor;
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            autodetectWorkerExecutor = new AutodetectWorkerExecutorService(threadPool.getThreadContext());
            autodetectExecutorService.submit(autodetectWorkerExecutor::start);
            autodetectExecutorService.submit(processor::process);
        } catch (EsRejectedExecutionException e) {
            // If submitting the operation to read the results from the process fails we need to close
            // the process too, so that other submitted operations to threadpool are stopped.
            try {
                IOUtils.close(process);
            } catch (IOException ioe) {
                logger.error("Can't close autodetect", ioe);
            }
            onFinish.accept(e);
            return;
        }

        StateStreamer stateStreamer = new StateStreamer(client);
        Executor executor = new Executor(stateStreamer, processor, autodetectWorkerExecutor, process);
        if (continueRunning.get() == false) {
            onFinish.accept(null);
            return;
        }
        executor.execute();
    }

    void setTaskToFailed(String reason, ActionListener<PersistentTask<?>> listener) {
        SnapshotUpgradeTaskState taskState = new SnapshotUpgradeTaskState(
            SnapshotUpgradeState.FAILED,
            task.getAllocationId(),
            reason);
        task.updatePersistentTaskState(taskState, ActionListener.wrap(
            listener::onResponse,
            f -> {
                logger.warn(
                    () -> new ParameterizedMessage("[{}] [{}] failed to set task to failed", task.getJobId(), task.getSnapshotId()),
                    f);
                listener.onFailure(f);
            }
        ));
    }

    private class Executor {

        private final StateStreamer stateStreamer;
        private final JobSnapshotUpgraderResultProcessor processor;
        private final ExecutorService autodetectWorkerExecutor;
        private final AutodetectProcess process;

        Executor(StateStreamer stateStreamer,
                 JobSnapshotUpgraderResultProcessor processor,
                 ExecutorService autodetectWorkerExecutor,
                 AutodetectProcess process) {
            this.stateStreamer = stateStreamer;
            this.processor = processor;
            this.autodetectWorkerExecutor = autodetectWorkerExecutor;
            this.process = process;
        }

        void execute() {
            this.restoreState();
        }

        protected final Map<String, Integer> outputFieldIndexes() {
            Map<String, Integer> fieldIndexes = new HashMap<>();
            // time field
            fieldIndexes.put(job.getDataDescription().getTimeField(), 0);
            int index = 1;
            for (String field : job.getAnalysisConfig().analysisFields()) {
                if (AnalysisConfig.ML_CATEGORY_FIELD.equals(field) == false) {
                    fieldIndexes.put(field, index++);
                }
            }
            // field for categorization tokens
            if (MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA && job.getAnalysisConfig().getCategorizationFieldName() != null) {
                fieldIndexes.put(LengthEncodedWriter.PRETOKENISED_TOKEN_FIELD, index++);
            }

            // control field
            fieldIndexes.put(LengthEncodedWriter.CONTROL_FIELD_NAME, index++);
            return fieldIndexes;
        }

        void writeHeader() throws IOException {
            Map<String, Integer> outFieldIndexes = outputFieldIndexes();
            // header is all the analysis input fields + the time field + control field
            int numFields = outFieldIndexes.size();
            String[] record = new String[numFields];
            for (Map.Entry<String, Integer> entry : outFieldIndexes.entrySet()) {
                record[entry.getValue()] = entry.getKey();
            }
            // Write the header
            process.writeRecord(record);
        }

        FlushAcknowledgement waitFlushToCompletion(String flushId) throws Exception {
            logger.debug(() -> new ParameterizedMessage("[{}] [{}] waiting for flush [{}]", jobId, snapshotId, flushId));

            FlushAcknowledgement flushAcknowledgement;
            try {
                flushAcknowledgement = processor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
                while (flushAcknowledgement == null) {
                    checkProcessIsAlive();
                    checkResultsProcessorIsAlive();
                    flushAcknowledgement = processor.waitForFlushAcknowledgement(flushId, FLUSH_PROCESS_CHECK_FREQUENCY);
                }
            } finally {
                processor.clearAwaitingFlush(flushId);
            }
            logger.debug(() -> new ParameterizedMessage("[{}] [{}] flush completed [{}]", jobId, snapshotId, flushId));
            return flushAcknowledgement;
        }

        void restoreState() {
            try {
                process.restoreState(stateStreamer, params.modelSnapshot());
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("[{}] [{}] failed to write old state", jobId, snapshotId), e);
                setTaskToFailed("Failed to write old state due to: " + e.getMessage(),
                    ActionListener.wrap(t -> shutdown(e), f -> shutdown(e)));
                return;
            }
            submitOperation(() -> {
                writeHeader();
                String flushId = process.flushJob(FlushJobParams.builder().waitForNormalization(false).build());
                return waitFlushToCompletion(flushId);
            }, (flushAcknowledgement, e) -> {
                Runnable nextStep;
                if (e != null) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "[{}] [{}] failed to flush after writing old state",
                            jobId,
                            snapshotId
                        ),
                        e);
                    nextStep = () -> setTaskToFailed(
                        "Failed to flush after writing old state due to: " + e.getMessage(),
                        ActionListener.wrap(t -> shutdown(e), f -> shutdown(e))
                    );
                } else {
                    logger.debug(() -> new ParameterizedMessage(
                        "[{}] [{}] flush [{}] acknowledged requesting state write",
                        jobId,
                        snapshotId,
                        flushAcknowledgement.getId()
                    ));
                    nextStep = this::requestStateWrite;
                }
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(nextStep);
            });
        }

        private void requestStateWrite() {
            task.updatePersistentTaskState(
                new SnapshotUpgradeTaskState(SnapshotUpgradeState.SAVING_NEW_STATE, task.getAllocationId(), ""),
                ActionListener.wrap(
                    readingNewState -> {
                        if (continueRunning.get() == false) {
                            shutdown(null);
                            return;
                        }
                        submitOperation(
                            () -> {
                                process.persistState(
                                    params.modelSnapshot().getTimestamp().getTime(),
                                    params.modelSnapshot().getSnapshotId(),
                                    params.modelSnapshot().getDescription());
                                return null;
                            },
                            // Execute callback in the UTILITY thread pool, as the current thread in the callback will be one in the
                            // autodetectWorkerExecutor. Trying to run the callback in that executor will cause a dead lock as that
                            // executor has a single processing queue.
                            (aVoid, e) -> threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> shutdown(e)));
                        logger.info("asked for state to be persisted");
                    },
                    f -> {
                        logger.error(
                            () -> new ParameterizedMessage(
                                "[{}] [{}] failed to update snapshot upgrader task to started",
                                jobId,
                                snapshotId),
                            f);
                        shutdown(new ElasticsearchStatusException(
                            "Failed to start snapshot upgrade [{}] for job [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            f,
                            snapshotId,
                            jobId));
                    }
                ));
        }

        private <T> void submitOperation(CheckedSupplier<T, Exception> operation, BiConsumer<T, Exception> handler) {
            autodetectWorkerExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (continueRunning.get() == false) {
                        handler.accept(null, ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed", job.getId()));
                    } else {
                        logger.error(new ParameterizedMessage("[{}] Unexpected exception writing to process", job.getId()), e);
                        handler.accept(null, e);
                    }
                }

                @Override
                protected void doRun() throws Exception {
                    if (continueRunning.get() == false) {
                        handler.accept(null, ExceptionsHelper.conflictStatusException(
                            "[{}] Could not submit operation to process as it has been killed", job.getId()));
                    } else {
                        checkProcessIsAlive();
                        handler.accept(operation.get(), null);
                    }
                }
            });
        }

        private void checkProcessIsAlive() {
            if (process.isProcessAlive() == false) {
                // Don't log here - it just causes double logging when the exception gets logged
                throw new ElasticsearchException("[{}] Unexpected death of autodetect: {}", job.getId(), process.readError());
            }
        }

        private void checkResultsProcessorIsAlive() {
            if (processor.isFailed()) {
                // Don't log here - it just causes double logging when the exception gets logged
                throw new ElasticsearchException("[{}] Unexpected death of the result processor", job.getId());
            }
        }

        void shutdown(Exception e) {
            // No point in sending an action to the executor if the process has died
            if (process.isProcessAlive() == false) {
                onFinish.accept(e);
                autodetectWorkerExecutor.shutdown();
                stateStreamer.cancel();
                return;
            }
            autodetectWorkerExecutor.execute(() -> {
                try {
                    if (process.isReady()) {
                        process.close();
                    } else {
                        processor.setProcessKilled();
                        process.kill(true);
                        processor.awaitCompletion();
                    }
                } catch (IOException | TimeoutException exc) {
                    logger.warn(() -> new ParameterizedMessage("[{}] [{}] failed to shutdown process", jobId, snapshotId), exc);
                } finally {
                    onFinish.accept(e);
                }
            });
            autodetectWorkerExecutor.shutdown();
            stateStreamer.cancel();
        }
    }
}
