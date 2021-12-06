/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A runnable class that reads the autodetect process output in the
 * {@link #process()} method and persists parsed
 * results via the {@linkplain JobResultsPersister} passed in the constructor.
 * <p>
 * This is a single purpose result processor and only handles snapshot writes
 */
public class JobSnapshotUpgraderResultProcessor {

    private static final Logger LOGGER = LogManager.getLogger(JobSnapshotUpgraderResultProcessor.class);
    final CountDownLatch completionLatch = new CountDownLatch(1);
    private final String jobId;
    private final String snapshotId;
    private final JobResultsPersister persister;
    private final AutodetectProcess process;
    private final JobResultsPersister.Builder bulkResultsPersister;
    private final FlushListener flushListener;
    private volatile boolean processKilled;
    private volatile boolean failed;

    public JobSnapshotUpgraderResultProcessor(
        String jobId,
        String snapshotId,
        JobResultsPersister persister,
        AutodetectProcess autodetectProcess
    ) {
        this.jobId = Objects.requireNonNull(jobId);
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.persister = Objects.requireNonNull(persister);
        this.process = Objects.requireNonNull(autodetectProcess);
        this.bulkResultsPersister = persister.bulkPersisterBuilder(jobId).shouldRetry(this::isAlive);
        this.flushListener = new FlushListener();
    }

    public void process() {

        // If a function call in this throws for some reason we don't want it
        // to kill the results reader thread as autodetect will be blocked
        // trying to write its output.
        try {
            readResults();
            try {
                if (processKilled == false) {
                    bulkResultsPersister.executeRequest();
                }
            } catch (Exception e) {
                LOGGER.warn(new ParameterizedMessage("[{}] [{}] Error persisting model snapshot upgrade results", jobId, snapshotId), e);
            }
        } catch (Exception e) {
            failed = true;

            if (processKilled) {
                // Don't log the stack trace in this case. Log just enough to hint
                // that it would have been better to close jobs before shutting down,
                // but we now fully expect jobs to move between nodes without doing
                // all their graceful close activities.
                LOGGER.warn(
                    "[{}] [{}] some model snapshot upgrade results not processed due to the process being killed",
                    jobId,
                    snapshotId
                );
            } else if (process.isProcessAliveAfterWaiting() == false) {
                // Don't log the stack trace to not shadow the root cause.
                LOGGER.warn(
                    "[{}] [{}] some model snapshot upgrade results not processed due to the termination of autodetect",
                    jobId,
                    snapshotId
                );
            } else {
                // We should only get here if the iterator throws in which
                // case parsing the autodetect output has failed.
                LOGGER.error(new ParameterizedMessage("[{}] [{}] error parsing model snapshot upgrade output", jobId, snapshotId), e);
            }
        } finally {
            completionLatch.countDown();
        }
    }

    private void readResults() {
        try {
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            while (iterator.hasNext()) {
                try {
                    AutodetectResult result = iterator.next();
                    processResult(result);
                } catch (Exception e) {
                    if (isAlive() == false) {
                        throw e;
                    }
                    LOGGER.warn(new ParameterizedMessage("[{}] [{}] Error processing model snapshot upgrade result", jobId, snapshotId), e);
                }
            }
        } finally {
            process.consumeAndCloseOutputStream();
        }
    }

    public void setProcessKilled() {
        processKilled = true;
    }

    public boolean isProcessKilled() {
        return processKilled;
    }

    private void logUnexpectedResult(String resultType) {
        String msg = "[" + jobId + "] [" + snapshotId + "] unexpected result read [" + resultType + "]";
        // This should never happen, but we definitely want to fail if -ea is provided (e.g. during tests)
        assert true : msg;
        LOGGER.info(msg);
    }

    void processResult(AutodetectResult result) {
        if (processKilled) {
            return;
        }

        Bucket bucket = result.getBucket();
        if (bucket != null) {
            logUnexpectedResult(Bucket.RESULT_TYPE_VALUE);
        }
        List<AnomalyRecord> records = result.getRecords();
        if (records != null && records.isEmpty() == false) {
            logUnexpectedResult(AnomalyRecord.RESULT_TYPE_VALUE);
        }
        List<Influencer> influencers = result.getInfluencers();
        if (influencers != null && influencers.isEmpty() == false) {
            logUnexpectedResult(Influencer.RESULT_TYPE_VALUE);
        }
        CategoryDefinition categoryDefinition = result.getCategoryDefinition();
        if (categoryDefinition != null) {
            logUnexpectedResult(CategoryDefinition.TYPE.getPreferredName());
        }
        CategorizerStats categorizerStats = result.getCategorizerStats();
        if (categorizerStats != null) {
            logUnexpectedResult(CategorizerStats.RESULT_TYPE_VALUE);
        }
        ModelPlot modelPlot = result.getModelPlot();
        if (modelPlot != null) {
            logUnexpectedResult(ModelSnapshot.TYPE.getPreferredName());
        }
        Annotation annotation = result.getAnnotation();
        if (annotation != null) {
            logUnexpectedResult(Annotation.TYPE.getPreferredName());
        }
        Forecast forecast = result.getForecast();
        if (forecast != null) {
            logUnexpectedResult(Forecast.RESULT_TYPE_VALUE);
        }
        ForecastRequestStats forecastRequestStats = result.getForecastRequestStats();
        if (forecastRequestStats != null) {
            logUnexpectedResult(ForecastRequestStats.RESULT_TYPE_VALUE);
        }
        ModelSizeStats modelSizeStats = result.getModelSizeStats();
        if (modelSizeStats != null) {
            logUnexpectedResult(ModelSizeStats.RESULT_TYPE_VALUE);
        }
        ModelSnapshot modelSnapshot = result.getModelSnapshot();
        if (modelSnapshot != null) {
            BulkResponse bulkResponse = persister.persistModelSnapshot(modelSnapshot, WriteRequest.RefreshPolicy.IMMEDIATE, this::isAlive);
            assert bulkResponse.getItems().length == 1;
        }
        Quantiles quantiles = result.getQuantiles();
        if (quantiles != null) {
            logUnexpectedResult(Quantiles.TYPE.getPreferredName());
        }
        FlushAcknowledgement flushAcknowledgement = result.getFlushAcknowledgement();
        if (flushAcknowledgement != null) {
            LOGGER.debug(
                () -> new ParameterizedMessage(
                    "[{}] [{}] Flush acknowledgement parsed from output for ID {}",
                    jobId,
                    snapshotId,
                    flushAcknowledgement.getId()
                )
            );
            flushListener.acknowledgeFlush(flushAcknowledgement, null);
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

    public void awaitCompletion() throws TimeoutException {
        try {
            // Although the results won't take 30 minutes to finish, the pipe won't be closed
            // until the state is persisted, and that can take a while
            if (completionLatch.await(MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT.getMinutes(), TimeUnit.MINUTES) == false) {
                throw new TimeoutException(
                    "Timed out waiting for model snapshot upgrader results processor to complete for job "
                        + jobId
                        + " and snapshot "
                        + snapshotId
                );
            }

            // These lines ensure that the "completion" we're awaiting includes making the results searchable
            persister.commitStateWrites(jobId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("[{}] [{}] Interrupted waiting for model snapshot upgrade results processor to complete", jobId, snapshotId);
        }
    }

    /**
     * If failed then there was an error parsing the results that cannot be recovered from
     *
     * @return true if failed
     */
    public boolean isFailed() {
        return failed;
    }

    private boolean isAlive() {
        if (processKilled) {
            return false;
        }
        return process.isProcessAliveAfterWaiting();
    }

}
