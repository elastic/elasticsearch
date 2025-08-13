/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.TimingStatsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.JOB_FORECAST_NATIVE_PROCESS_KILLED;

/**
 * A runnable class that reads the autodetect process output in the
 * {@link #process()} method and persists parsed
 * results via the {@linkplain JobResultsPersister} passed in the constructor.
 * <p>
 * Has methods to register and remove alert observers.
 * Also has a method to wait for a flush to be complete.
 * <p>
 * Buckets are the written last after records, influencers etc
 * when the end of bucket is reached. Therefore results aren't persisted
 * until the bucket is read, this means that interim results for all
 * result types can be safely deleted when the bucket is read and before
 * the new results are updated. This is specifically for the case where
 * a flush command is issued repeatedly in the same bucket to generate
 * interim results and the old interim results have to be cleared out
 * before the new ones are written.
 */
public class AutodetectResultProcessor {

    private static final Logger logger = LogManager.getLogger(AutodetectResultProcessor.class);

    private final Client client;
    private final AnomalyDetectionAuditor auditor;
    private final String jobId;
    private final Renormalizer renormalizer;
    private final JobResultsPersister persister;
    private final AutodetectProcess process;
    private final TimingStatsReporter timingStatsReporter;
    private final Clock clock;

    final CountDownLatch completionLatch = new CountDownLatch(1);
    final Semaphore updateModelSnapshotSemaphore = new Semaphore(1);
    private final FlushListener flushListener;
    private volatile boolean processKilled;
    private volatile boolean vacating;
    private volatile boolean failed;
    private final Map<String, ForecastRequestStats> runningForecasts;
    private final long priorRunsBucketCount;
    private long currentRunBucketCount; // only used from the process() thread, so doesn't need to be volatile
    private final JobResultsPersister.Builder bulkResultsPersister;
    private final AnnotationPersister.Builder bulkAnnotationsPersister;
    private boolean deleteInterimRequired;

    /**
     * New model size stats are read as the process is running
     */
    private volatile ModelSizeStats latestModelSizeStats;

    public AutodetectResultProcessor(
        Client client,
        AnomalyDetectionAuditor auditor,
        String jobId,
        Renormalizer renormalizer,
        JobResultsPersister persister,
        AnnotationPersister annotationPersister,
        AutodetectProcess process,
        ModelSizeStats latestModelSizeStats,
        TimingStats timingStats
    ) {
        this(
            client,
            auditor,
            jobId,
            renormalizer,
            persister,
            annotationPersister,
            process,
            latestModelSizeStats,
            timingStats,
            Clock.systemUTC(),
            new FlushListener()
        );
    }

    // Visible for testing
    AutodetectResultProcessor(
        Client client,
        AnomalyDetectionAuditor auditor,
        String jobId,
        Renormalizer renormalizer,
        JobResultsPersister persister,
        AnnotationPersister annotationPersister,
        AutodetectProcess autodetectProcess,
        ModelSizeStats latestModelSizeStats,
        TimingStats timingStats,
        Clock clock,
        FlushListener flushListener
    ) {
        this.client = Objects.requireNonNull(client);
        this.auditor = Objects.requireNonNull(auditor);
        this.jobId = Objects.requireNonNull(jobId);
        this.renormalizer = Objects.requireNonNull(renormalizer);
        this.persister = Objects.requireNonNull(persister);
        this.process = Objects.requireNonNull(autodetectProcess);
        this.flushListener = Objects.requireNonNull(flushListener);
        this.latestModelSizeStats = Objects.requireNonNull(latestModelSizeStats);
        this.bulkResultsPersister = persister.bulkPersisterBuilder(jobId, this::isAlive);
        this.bulkAnnotationsPersister = annotationPersister.bulkPersisterBuilder(jobId, this::isAlive);
        this.timingStatsReporter = new TimingStatsReporter(timingStats, bulkResultsPersister);
        this.clock = Objects.requireNonNull(clock);
        this.deleteInterimRequired = true;
        this.priorRunsBucketCount = timingStats.getBucketCount();
        this.runningForecasts = new ConcurrentHashMap<>();
    }

    public void process() {

        // If a function call in this throws for some reason we don't want it
        // to kill the results reader thread as autodetect will be blocked
        // trying to write its output.
        try {
            readResults();

            try {
                if (processKilled == false) {
                    timingStatsReporter.finishReporting();
                    bulkResultsPersister.executeRequest();
                    bulkAnnotationsPersister.executeRequest();
                }
            } catch (Exception e) {
                logger.warn(() -> "[" + jobId + "] Error persisting autodetect results", e);
            }
            logger.info("[{}] {} buckets parsed from autodetect output", jobId, currentRunBucketCount);

        } catch (Exception e) {
            failed = true;

            if (processKilled) {
                // Don't log the stack trace in this case. Log just enough to hint
                // that it would have been better to close jobs before shutting down,
                // but we now fully expect jobs to move between nodes without doing
                // all their graceful close activities.
                logger.warn("[{}] some results not processed due to the process being killed", jobId);
            } else if (process.isProcessAliveAfterWaiting() == false) {
                // Don't log the stack trace to not shadow the root cause.
                logger.warn("[{}] some results not processed due to the termination of autodetect", jobId);
            } else {
                // We should only get here if the iterator throws in which
                // case parsing the autodetect output has failed.
                logger.error(() -> "[" + jobId + "] error parsing autodetect output", e);
            }
        } finally {
            flushListener.clear();
            handleOpenForecasts();
            completionLatch.countDown();
        }
    }

    private void readResults() {
        currentRunBucketCount = 0;
        try {
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            while (iterator.hasNext()) {
                try {
                    AutodetectResult result = iterator.next();
                    processResult(result);
                    if (result.getBucket() != null) {
                        logger.trace("[{}] Bucket number {} parsed from output", jobId, currentRunBucketCount);
                    }
                } catch (Exception e) {
                    if (isAlive() == false) {
                        throw e;
                    }
                    logger.warn(() -> "[" + jobId + "] Error processing autodetect result", e);
                }
            }
        } finally {
            process.consumeAndCloseOutputStream();
        }
    }

    public void setProcessKilled() {
        processKilled = true;
        vacating = false;
        try {
            renormalizer.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void setVacating(boolean vacating) {
        this.vacating = vacating;
    }

    void handleOpenForecasts() {
        try {
            if (runningForecasts.isEmpty() == false) {
                logger.warn("[{}] still had forecasts {} executing. Attempting to set them to failed.", jobId, runningForecasts.keySet());
                // There may be many docs in the results persistence queue. But we only want to bother updating the running forecasts
                bulkResultsPersister.clear();
                for (ForecastRequestStats forecastRequestStats : runningForecasts.values()) {
                    ForecastRequestStats failedStats = new ForecastRequestStats(forecastRequestStats);
                    failedStats.setStatus(ForecastRequestStats.ForecastRequestStatus.FAILED);
                    failedStats.setMessages(List.of(JOB_FORECAST_NATIVE_PROCESS_KILLED));
                    bulkResultsPersister.persistForecastRequestStats(failedStats);
                }
                bulkResultsPersister.executeRequest();
            }
        } catch (Exception ex) {
            logger.warn(() -> "[" + jobId + "] failure setting running forecasts to failed.", ex);
        }
    }

    void processResult(AutodetectResult result) {
        if (processKilled) {
            return;
        }

        Bucket bucket = result.getBucket();
        if (bucket != null) {
            if (deleteInterimRequired) {
                // Delete any existing interim results generated by a Flush command
                // which have not been replaced or superseded by new results.
                logger.trace("[{}] Deleting interim results", jobId);
                persister.deleteInterimResults(jobId);
            }

            if (bucket.isInterim() == false) {
                timingStatsReporter.reportBucket(bucket);
                ++currentRunBucketCount;
            }
            bulkResultsPersister.persistBucket(bucket);
            if (deleteInterimRequired || bucket.isInterim()) {
                // Execute the bulk request after deleting interim results in case the new
                // results are also interim. Also execute the bulk request after creating new
                // interim results, so that they exist before any subsequent deletion.
                bulkResultsPersister.executeRequest();
                bulkAnnotationsPersister.executeRequest();
                deleteInterimRequired = false;
            }
        }
        List<AnomalyRecord> records = result.getRecords();
        if (records != null && records.isEmpty() == false) {
            bulkResultsPersister.persistRecords(records);
        }
        List<Influencer> influencers = result.getInfluencers();
        if (influencers != null && influencers.isEmpty() == false) {
            bulkResultsPersister.persistInfluencers(influencers);
        }
        CategoryDefinition categoryDefinition = result.getCategoryDefinition();
        if (categoryDefinition != null) {
            bulkResultsPersister.persistCategoryDefinition(categoryDefinition);
        }
        CategorizerStats categorizerStats = result.getCategorizerStats();
        if (categorizerStats != null) {
            bulkResultsPersister.persistCategorizerStats(categorizerStats);
        }
        ModelPlot modelPlot = result.getModelPlot();
        if (modelPlot != null) {
            bulkResultsPersister.persistModelPlot(modelPlot);
        }
        Annotation annotation = result.getAnnotation();
        if (annotation != null) {
            bulkAnnotationsPersister.persistAnnotation(annotation);
            notifyCategorizationStatusChange(annotation);
        }
        Forecast forecast = result.getForecast();
        if (forecast != null) {
            bulkResultsPersister.persistForecast(forecast);
        }
        ForecastRequestStats forecastRequestStats = result.getForecastRequestStats();
        if (forecastRequestStats != null) {
            logger.trace("Received Forecast Stats [{}]", forecastRequestStats.getId());
            bulkResultsPersister.persistForecastRequestStats(forecastRequestStats);

            if (forecastRequestStats.getStatus()
                .isAnyOf(ForecastRequestStats.ForecastRequestStatus.FAILED, ForecastRequestStats.ForecastRequestStatus.FINISHED)) {
                runningForecasts.remove(forecastRequestStats.getForecastId());
            } else {
                runningForecasts.put(forecastRequestStats.getForecastId(), forecastRequestStats);
            }
            // execute the bulk request only in some cases or in doubt
            // otherwise rely on the count-based trigger
            switch (forecastRequestStats.getStatus()) {
                case OK:
                case STARTED:
                    break;
                case FAILED:
                case SCHEDULED:
                case FINISHED:
                default:
                    bulkResultsPersister.executeRequest();

            }
        }
        ModelSizeStats modelSizeStats = result.getModelSizeStats();
        if (modelSizeStats != null) {
            processModelSizeStats(modelSizeStats);
        }
        ModelSnapshot modelSnapshot = result.getModelSnapshot();
        if (modelSnapshot != null) {
            // We need to refresh in order for the snapshot to be available when we try to update the job with it
            BulkResponse bulkResponse = persister.persistModelSnapshot(modelSnapshot, WriteRequest.RefreshPolicy.IMMEDIATE, this::isAlive);
            assert bulkResponse.getItems().length == 1;
            IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                updateModelSnapshotOnJob(modelSnapshot);
            }
            bulkAnnotationsPersister.persistAnnotation(
                ModelSnapshot.annotationDocumentId(modelSnapshot),
                createModelSnapshotAnnotation(modelSnapshot)
            );
        }
        Quantiles quantiles = result.getQuantiles();
        if (quantiles != null) {
            logger.debug("[{}] Parsed Quantiles with timestamp {}", jobId, quantiles.getTimestamp());
            persister.persistQuantiles(quantiles, this::isAlive);

            // If a node is trying to shut down then don't trigger any further normalizations on the node
            if (vacating == false && processKilled == false && renormalizer.isEnabled()) {
                logger.debug("[{}] Quantiles queued for renormalization", jobId);
                renormalizer.renormalize(quantiles, () -> {
                    // We need to make all results written up to these quantiles available for renormalization.
                    // However, this should be done as close to the point of normalization as possible, as many
                    // quantiles are superseded before they're used.
                    bulkResultsPersister.executeRequest();
                    persister.commitWrites(jobId, JobResultsPersister.CommitType.RESULTS);
                });
            }
        }
        FlushAcknowledgement flushAcknowledgement = result.getFlushAcknowledgement();
        if (flushAcknowledgement != null) {
            logger.debug("[{}] Flush acknowledgement parsed from output for ID {}", jobId, flushAcknowledgement.getId());
            // Commit previous writes here, effectively continuing
            // the flush from the C++ autodetect process right
            // through to the data store
            Exception exception = null;
            try {
                bulkResultsPersister.executeRequest();
                bulkAnnotationsPersister.executeRequest();
                if (flushAcknowledgement.getRefreshRequired()) {
                    persister.commitWrites(
                        jobId,
                        EnumSet.of(JobResultsPersister.CommitType.RESULTS, JobResultsPersister.CommitType.ANNOTATIONS)
                    );
                }
            } catch (Exception e) {
                logger.error(
                    "["
                        + jobId
                        + "] failed to bulk persist results and commit writes during flush acknowledgement for ID "
                        + flushAcknowledgement.getId(),
                    e
                );
                exception = e;
                throw e;
            } finally {
                flushListener.acknowledgeFlush(flushAcknowledgement, exception);
            }
            // Interim results may have been produced by the flush,
            // which need to be
            // deleted when the next finalized results come through
            deleteInterimRequired = true;
        }
    }

    private Annotation createModelSnapshotAnnotation(ModelSnapshot modelSnapshot) {
        assert modelSnapshot != null;
        Date currentTime = new Date(clock.millis());
        return new Annotation.Builder().setAnnotation(
            Messages.getMessage(Messages.JOB_AUDIT_SNAPSHOT_STORED, modelSnapshot.getSnapshotId())
        )
            .setCreateTime(currentTime)
            .setCreateUsername(InternalUsers.XPACK_USER.principal())
            .setTimestamp(modelSnapshot.getLatestResultTimeStamp())
            .setEndTimestamp(modelSnapshot.getLatestResultTimeStamp())
            .setJobId(jobId)
            .setModifiedTime(currentTime)
            .setModifiedUsername(InternalUsers.XPACK_USER.principal())
            .setType(Annotation.Type.ANNOTATION)
            .setEvent(Annotation.Event.MODEL_SNAPSHOT_STORED)
            .build();
    }

    private void processModelSizeStats(ModelSizeStats modelSizeStats) {
        logger.trace(
            "[{}] Parsed ModelSizeStats: {} / {} / {} / {} / {} / {}",
            jobId,
            modelSizeStats.getModelBytes(),
            modelSizeStats.getTotalByFieldCount(),
            modelSizeStats.getTotalOverFieldCount(),
            modelSizeStats.getTotalPartitionFieldCount(),
            modelSizeStats.getBucketAllocationFailuresCount(),
            modelSizeStats.getMemoryStatus()
        );

        bulkResultsPersister.persistModelSizeStats(modelSizeStats);
        notifyModelMemoryStatusChange(modelSizeStats);

        latestModelSizeStats = modelSizeStats;
    }

    private void notifyModelMemoryStatusChange(ModelSizeStats modelSizeStats) {
        ModelSizeStats.MemoryStatus memoryStatus = modelSizeStats.getMemoryStatus();
        if (memoryStatus != latestModelSizeStats.getMemoryStatus()) {
            if (memoryStatus == ModelSizeStats.MemoryStatus.SOFT_LIMIT) {
                auditor.warning(jobId, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_SOFT_LIMIT));
            } else if (memoryStatus == ModelSizeStats.MemoryStatus.HARD_LIMIT) {
                if (modelSizeStats.getModelBytesMemoryLimit() == null || modelSizeStats.getModelBytesExceeded() == null) {
                    auditor.error(
                        jobId,
                        Messages.getMessage(
                            Messages.JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT_PRE_7_2,
                            ByteSizeValue.ofBytes(modelSizeStats.getModelBytes()).toString()
                        )
                    );
                } else {
                    auditor.error(
                        jobId,
                        Messages.getMessage(
                            Messages.JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT,
                            ByteSizeValue.ofBytes(modelSizeStats.getModelBytesMemoryLimit()).toString(),
                            ByteSizeValue.ofBytes(modelSizeStats.getModelBytesExceeded()).toString()
                        )
                    );
                }
            }
        }
    }

    private void notifyCategorizationStatusChange(Annotation annotation) {
        if (annotation.getEvent() == Annotation.Event.CATEGORIZATION_STATUS_CHANGE) {
            long bucketCount = priorRunsBucketCount + currentRunBucketCount;
            auditor.warning(jobId, annotation.getAnnotation() + " after " + bucketCount + ((bucketCount == 1) ? " bucket" : " buckets"));
        }
    }

    protected void updateModelSnapshotOnJob(ModelSnapshot modelSnapshot) {
        JobUpdate update = new JobUpdate.Builder(jobId).setModelSnapshotId(modelSnapshot.getSnapshotId()).build();
        UpdateJobAction.Request updateRequest = UpdateJobAction.Request.internal(jobId, update);

        try {
            // This blocks the main processing thread in the unlikely event
            // there are 2 model snapshots queued up. But it also has the
            // advantage of ensuring order
            updateModelSnapshotSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("[{}] Interrupted acquiring update model snapshot semaphore", jobId);
            return;
        }

        RetryableUpdateModelSnapshotAction updateModelSnapshotAction = new RetryableUpdateModelSnapshotAction(
            client,
            updateRequest,
            new ActionListener<>() {
                @Override
                public void onResponse(PutJobAction.Response response) {
                    updateModelSnapshotSemaphore.release();
                    logger.debug("[{}] Updated job with model snapshot id [{}]", jobId, modelSnapshot.getSnapshotId());
                }

                @Override
                public void onFailure(Exception e) {
                    updateModelSnapshotSemaphore.release();
                    logger.error(
                        "[" + jobId + "] Failed to update job with new model snapshot id [" + modelSnapshot.getSnapshotId() + "]",
                        e
                    );
                }
            }
        );
        updateModelSnapshotAction.run();
    }

    public void awaitCompletion() throws TimeoutException {
        try {
            // Although the results won't take 30 minutes to finish, the pipe won't be closed
            // until the state is persisted, and that can take a while
            if (completionLatch.await(MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT.getMinutes(), TimeUnit.MINUTES) == false) {
                throw new TimeoutException("Timed out waiting for results processor to complete for job " + jobId);
            }

            // Input stream has been completely processed at this point.
            // Wait for any updateModelSnapshotOnJob calls to complete.
            updateModelSnapshotSemaphore.acquire();
            updateModelSnapshotSemaphore.release();

            // These lines ensure that the "completion" we're awaiting includes making the results searchable
            waitUntilRenormalizerIsIdle();
            persister.commitWrites(jobId, EnumSet.allOf(JobResultsPersister.CommitType.class));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("[{}] Interrupted waiting for results processor to complete", jobId);
        }
    }

    /**
     * Blocks until a flush is acknowledged or the timeout expires, whichever happens first.
     *
     * @param flushId the id of the flush request to wait for
     * @param timeout the timeout
     * @return The {@link FlushAcknowledgement} if the flush has completed or the parsing finished; {@code null} if the timeout expired
     */
    @Nullable
    public FlushAcknowledgement waitForFlushAcknowledgement(String flushId, Duration timeout) throws Exception {
        return failed ? null : flushListener.waitForFlush(flushId, timeout);
    }

    public void clearAwaitingFlush(String flushId) {
        flushListener.clear(flushId);
    }

    public void waitUntilRenormalizerIsIdle() throws InterruptedException {
        renormalizer.waitUntilIdle();
    }

    /**
     * If failed then there was an error parsing the results that cannot be recovered from
     * @return true if failed
     */
    public boolean isFailed() {
        return failed;
    }

    public ModelSizeStats modelSizeStats() {
        return latestModelSizeStats;
    }

    public TimingStats timingStats() {
        return timingStatsReporter.getCurrentTimingStats();
    }

    boolean isDeleteInterimRequired() {
        return deleteInterimRequired;
    }

    private boolean isAlive() {
        if (processKilled) {
            return false;
        }
        return process.isProcessAliveAfterWaiting();
    }

    void setDeleteInterimRequired(boolean deleteInterimRequired) {
        this.deleteInterimRequired = deleteInterimRequired;
    }

    // For testing only.
    // Reading currentRunBucketCount is not thread safe
    long getCurrentRunBucketCount() {
        return currentRunBucketCount;
    }
}
