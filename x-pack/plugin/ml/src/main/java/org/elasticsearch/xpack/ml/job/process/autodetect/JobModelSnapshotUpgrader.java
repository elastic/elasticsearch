/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.ProcessWorkerExecutorService;
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

import static org.elasticsearch.core.Strings.format;
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
    // Not volatile as only used in synchronized methods
    private AutodetectProcess process;
    private JobSnapshotUpgraderResultProcessor processor;

    JobModelSnapshotUpgrader(
        SnapshotUpgradeTask task,
        Job job,
        AutodetectParams params,
        ThreadPool threadPool,
        AutodetectProcessFactory autodetectProcessFactory,
        JobResultsPersister jobResultsPersister,
        Client client,
        NativeStorageProvider nativeStorageProvider,
        Consumer<Exception> onFinish,
        Supplier<Boolean> continueRunning
    ) {
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

    synchronized void start() {
        if (task.setJobModelSnapshotUpgrader(this) == false) {
            this.killProcess(task.getReasonCancelled());
            return;
        }

        // A TP with no queue, so that we fail immediately if there are no threads available
        ExecutorService autodetectExecutorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);

        process = autodetectProcessFactory.createAutodetectProcess(
            jobId + "-" + snapshotId,
            job,
            params,
            autodetectExecutorService,
            (reason) -> {
                setTaskToFailed(reason, ActionListener.wrap(t -> {}, task::markAsFailed));
                try {
                    nativeStorageProvider.cleanupLocalTmpStorage(task.getDescription());
                } catch (IOException e) {
                    logger.error(() -> format("[%s] [%s] failed to delete temporary files snapshot upgrade", jobId, snapshotId), e);
                }
            }
        );
        processor = new JobSnapshotUpgraderResultProcessor(jobId, snapshotId, jobResultsPersister, process);
        ProcessWorkerExecutorService autodetectWorkerExecutor;
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            autodetectWorkerExecutor = new AutodetectWorkerExecutorService(threadPool.getThreadContext());
            autodetectExecutorService.submit(autodetectWorkerExecutor::start);
            autodetectExecutorService.submit(processor::process);
        } catch (EsRejectedExecutionException e) {
            // If submitting the operation to read the results from the process fails we need to close
            // the process too, so that other submitted operations to threadpool are stopped.
            try {
                IOUtils.close(process);
                process = null;
                processor = null;
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

    private void removeDuplicateModelSnapshotDoc(Consumer<Exception> runAfter) {
        String snapshotDocId = jobId + "_model_snapshot_" + snapshotId;
        client.prepareSearch(AnomalyDetectorsIndex.jobResultsIndexPattern())
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(snapshotDocId)))
            .setSize(2)
            .addSort(ModelSnapshot.MIN_VERSION.getPreferredName(), org.elasticsearch.search.sort.SortOrder.ASC)
            .execute(ActionListener.wrap(searchResponse -> {
                if (searchResponse.getHits().getTotalHits().value() > 1) {
                    deleteOlderSnapshotDoc(searchResponse, runAfter);
                } else {
                    onFinish.accept(null);
                }
            }, e -> {
                logger.warn(() -> format("[%s] [%s] error during search for model snapshot documents", jobId, snapshotId), e);
                onFinish.accept(null);
            }));
    }

    private void deleteOlderSnapshotDoc(SearchResponse searchResponse, Consumer<Exception> runAfter) {
        SearchHit firstHit = searchResponse.getHits().getAt(0);
        logger.debug(() -> format("[%s] deleting duplicate model snapshot doc [%s]", jobId, firstHit.getId()));
        client.prepareDelete()
            .setIndex(firstHit.getIndex())
            .setId(firstHit.getId())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(ActionListener.runAfter(ActionListener.wrap(deleteResponse -> {
                if ((deleteResponse.getResult() == DocWriteResponse.Result.DELETED) == false) {
                    logger.warn(
                        () -> format(
                            "[%s] [%s] failed to delete old snapshot [%s] result document, document not found",
                            jobId,
                            snapshotId,
                            ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName()
                        )
                    );
                }
            }, e -> {
                logger.warn(
                    () -> format(
                        "[%s] [%s] failed to delete old snapshot [%s] result document",
                        jobId,
                        snapshotId,
                        ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName()
                    ),
                    e
                );
            }), () -> runAfter.accept(null)));
    }

    void setTaskToFailed(String reason, ActionListener<PersistentTask<?>> listener) {
        SnapshotUpgradeTaskState taskState = new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, task.getAllocationId(), reason);
        task.updatePersistentTaskState(taskState, ActionListener.wrap(listener::onResponse, f -> {
            logger.warn(() -> format("[%s] [%s] failed to set task to failed", task.getJobId(), task.getSnapshotId()), f);
            listener.onFailure(f);
        }));
    }

    public synchronized void killProcess(String reason) {
        if (process != null) {
            try {
                logger.debug("[{}] killing upgrade process for model snapshot [{}]: reason [{}]", jobId, snapshotId, reason);
                if (processor != null) {
                    processor.setProcessKilled();
                }
                process.kill(true);
                process = null;
                processor = null;
            } catch (IOException e) {
                logger.error(() -> format("[%s] failed to kill upgrade process for model snapshot [%s]", jobId, snapshotId), e);
            }
        } else {
            logger.warn("[{}] attempt to kill upgrade process for model snapshot [{}] when no such process exists", jobId, snapshotId);
        }
    }

    private class Executor {

        private final StateStreamer stateStreamer;
        private final JobSnapshotUpgraderResultProcessor processor;
        private final ExecutorService autodetectWorkerExecutor;
        private final AutodetectProcess process;

        Executor(
            StateStreamer stateStreamer,
            JobSnapshotUpgraderResultProcessor processor,
            ExecutorService autodetectWorkerExecutor,
            AutodetectProcess process
        ) {
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
            if (job.getAnalysisConfig().getCategorizationFieldName() != null) {
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
            logger.debug(() -> format("[%s] [%s] waiting for flush [%s]", jobId, snapshotId, flushId));

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
            logger.debug(() -> format("[%s] [%s] flush completed [%s]", jobId, snapshotId, flushId));
            return flushAcknowledgement;
        }

        void restoreState() {
            try {
                process.restoreState(stateStreamer, params.modelSnapshot());
            } catch (Exception e) {
                logger.error(() -> format("[%s] [%s] failed to write old state", jobId, snapshotId), e);
                setTaskToFailed(
                    "Failed to write old state due to: " + e.getMessage(),
                    ActionListener.running(() -> shutdownWithFailure(e))
                );
                return;
            }
            submitOperation(() -> {
                writeHeader();
                String flushId = process.flushJob(FlushJobParams.builder().waitForNormalization(false).build());
                return waitFlushToCompletion(flushId);
            }, (flushAcknowledgement, e) -> {
                Runnable nextStep;
                if (e != null) {
                    logger.error(() -> format("[%s] [%s] failed to flush after writing old state", jobId, snapshotId), e);
                    nextStep = () -> setTaskToFailed(
                        "Failed to flush after writing old state due to: " + e.getMessage(),
                        ActionListener.running(() -> shutdownWithFailure(e))
                    );
                } else {
                    logger.debug(
                        () -> format(
                            "[%s] [%s] flush [%s] acknowledged requesting state write",
                            jobId,
                            snapshotId,
                            flushAcknowledgement.getId()
                        )
                    );
                    nextStep = this::requestStateWrite;
                }
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(nextStep);
            });
        }

        private void requestStateWrite() {
            task.updatePersistentTaskState(
                new SnapshotUpgradeTaskState(SnapshotUpgradeState.SAVING_NEW_STATE, task.getAllocationId(), ""),
                ActionListener.wrap(readingNewState -> {
                    if (continueRunning.get() == false) {
                        shutdownWithFailure(null);
                        return;
                    }
                    submitOperation(() -> {
                        process.persistState(
                            params.modelSnapshot().getTimestamp().getTime(),
                            params.modelSnapshot().getSnapshotId(),
                            params.modelSnapshot().getDescription()
                        );
                        logger.debug("[{}] [{}] state persist call made", jobId, snapshotId);
                        return Void.TYPE;
                    },
                        // Execute callback in the UTILITY thread pool, as the current thread in the callback will be one in the
                        // autodetectWorkerExecutor. Trying to run the callback in that executor will cause a dead lock as that
                        // executor has a single processing queue.
                        (aVoid, e) -> threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> handlePersistingState(e))
                    );
                    logger.debug("[{}] [{}] asked for state to be persisted", jobId, snapshotId);
                }, f -> {
                    logger.error(() -> format("[%s] [%s] failed to update snapshot upgrader task to started", jobId, snapshotId), f);
                    shutdownWithFailure(
                        new ElasticsearchStatusException(
                            "Failed to start snapshot upgrade [{}] for job [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            f,
                            snapshotId,
                            jobId
                        )
                    );
                })
            );
        }

        private <T> void submitOperation(CheckedSupplier<T, Exception> operation, BiConsumer<T, Exception> handler) {
            autodetectWorkerExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (continueRunning.get() == false) {
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
                    if (continueRunning.get() == false) {
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

        private void handlePersistingState(@Nullable Exception exception) {
            assert Thread.currentThread().getName().contains(UTILITY_THREAD_POOL_NAME);

            if (exception != null) {
                shutdownWithFailure(exception);
            } else {
                stopProcess((aVoid, e) -> {
                    threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> {
                        autodetectWorkerExecutor.shutdownNow();
                        // If there are two snapshot documents in the results indices with the same snapshot id,
                        // remove the old one. This can happen when the result index has been rolled over and
                        // the write alias is pointing to the new index.
                        removeDuplicateModelSnapshotDoc(onFinish);
                    });

                });
            }
        }

        void shutdownWithFailure(Exception e) {
            stopProcess((aVoid, ignored) -> {
                threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(() -> {
                    onFinish.accept(e);
                    autodetectWorkerExecutor.shutdownNow();
                });
            });
        }

        private void stopProcess(BiConsumer<Class<Void>, Exception> runNext) {
            logger.debug("[{}] [{}] shutdown initiated", jobId, snapshotId);
            // No point in sending an action to the executor if the process has died
            if (process.isProcessAlive() == false) {
                logger.debug("[{}] [{}] process is dead, no need to shutdown", jobId, snapshotId);
                stateStreamer.cancel();
                runNext.accept(null, null);
                return;
            }

            submitOperation(() -> {
                try {
                    logger.debug("[{}] [{}] shutdown is now occurring", jobId, snapshotId);
                    if (process.isReady()) {
                        process.close();
                    } else {
                        processor.setProcessKilled();
                        process.kill(true);
                        stateStreamer.cancel();
                    }
                    processor.awaitCompletion();
                } catch (IOException | TimeoutException exc) {
                    logger.warn(() -> format("[%s] [%s] failed to shutdown process", jobId, snapshotId), exc);
                }
                logger.debug("[{}] [{}] connection for upgrade has been closed, process is shutdown", jobId, snapshotId);
                return Void.TYPE;
            }, runNext);
        }
    }
}
