/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.ModelPlot;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A runnable class that reads the autodetect process output in the
 * {@link #process(AutodetectProcess)} method and persists parsed
 * results via the {@linkplain JobResultsPersister} passed in the constructor.
 * <p>
 * Has methods to register and remove alert observers.
 * Also has a method to wait for a flush to be complete.
 *
 * Buckets are the written last after records, influencers etc
 * when the end of bucket is reached. Therefore results aren't persisted
 * until the bucket is read, this means that interim results for all
 * result types can be safely deleted when the bucket is read and before
 * the new results are updated. This is specifically for the case where
 * a flush command is issued repeatedly in the same bucket to generate
 * interim results and the old interim results have to be cleared out
 * before the new ones are written.
 */
public class AutoDetectResultProcessor {

    private static final Logger LOGGER = Loggers.getLogger(AutoDetectResultProcessor.class);

    private final Client client;
    private final String jobId;
    private final Renormalizer renormalizer;
    private final JobResultsPersister persister;

    final CountDownLatch completionLatch = new CountDownLatch(1);
    final Semaphore updateModelSnapshotIdSemaphore = new Semaphore(1);
    private final FlushListener flushListener;
    private volatile boolean processKilled;
    private volatile boolean failed;

    /**
     * New model size stats are read as the process is running
     */
    private volatile ModelSizeStats latestModelSizeStats;

    public AutoDetectResultProcessor(Client client, String jobId, Renormalizer renormalizer, JobResultsPersister persister,
                                     ModelSizeStats latestModelSizeStats) {
        this(client, jobId, renormalizer, persister, latestModelSizeStats, new FlushListener());
    }

    AutoDetectResultProcessor(Client client, String jobId, Renormalizer renormalizer, JobResultsPersister persister,
                              ModelSizeStats latestModelSizeStats, FlushListener flushListener) {
        this.client = Objects.requireNonNull(client);
        this.jobId = Objects.requireNonNull(jobId);
        this.renormalizer = Objects.requireNonNull(renormalizer);
        this.persister = Objects.requireNonNull(persister);
        this.flushListener = Objects.requireNonNull(flushListener);
        this.latestModelSizeStats = Objects.requireNonNull(latestModelSizeStats);
    }

    public void process(AutodetectProcess process) {
        Context context = new Context(jobId, persister.bulkPersisterBuilder(jobId));

        // If a function call in this throws for some reason we don't want it
        // to kill the results reader thread as autodetect will be blocked
        // trying to write its output.
        try {
            int bucketCount = 0;
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            while (iterator.hasNext()) {
                try {
                    AutodetectResult result = iterator.next();
                    processResult(context, result);
                    if (result.getBucket() != null) {
                        bucketCount++;
                        LOGGER.trace("[{}] Bucket number {} parsed from output", jobId, bucketCount);
                    }
                } catch (Exception e) {
                    if (processKilled) {
                        throw e;
                    }
                    LOGGER.warn(new ParameterizedMessage("[{}] Error processing autodetect result", jobId), e);
                }
            }

            try {
                if (processKilled == false) {
                    context.bulkResultsPersister.executeRequest();
                }
            } catch (Exception e) {
                LOGGER.warn(new ParameterizedMessage("[{}] Error persisting autodetect results", jobId), e);
            }

            LOGGER.info("[{}] {} buckets parsed from autodetect output", jobId, bucketCount);
        } catch (Exception e) {
            failed = true;

            if (processKilled) {
                // Don't log the stack trace in this case.  Log just enough to hint
                // that it would have been better to close jobs before shutting down,
                // but we now fully expect jobs to move between nodes without doing
                // all their graceful close activities.
                LOGGER.warn("[{}] some results not processed due to the process being killed", jobId);
            } else {
                // We should only get here if the iterator throws in which
                // case parsing the autodetect output has failed.
                LOGGER.error(new ParameterizedMessage("[{}] error parsing autodetect output", jobId), e);
            }
        } finally {
            flushListener.clear();
            completionLatch.countDown();
        }
    }

    public void setProcessKilled() {
        processKilled = true;
        renormalizer.shutdown();
    }

    void processResult(Context context, AutodetectResult result) {
        if (processKilled) {
            return;
        }

        Bucket bucket = result.getBucket();
        if (bucket != null) {
            if (context.deleteInterimRequired) {
                // Delete any existing interim results generated by a Flush command
                // which have not been replaced or superseded by new results.
                LOGGER.trace("[{}] Deleting interim results", context.jobId);
                persister.deleteInterimResults(context.jobId);
                context.deleteInterimRequired = false;
            }

            // persist after deleting interim results in case the new
            // results are also interim
            context.bulkResultsPersister.persistBucket(bucket).executeRequest();
        }
        List<AnomalyRecord> records = result.getRecords();
        if (records != null && !records.isEmpty()) {
            context.bulkResultsPersister.persistRecords(records);
        }
        List<Influencer> influencers = result.getInfluencers();
        if (influencers != null && !influencers.isEmpty()) {
            context.bulkResultsPersister.persistInfluencers(influencers);
        }
        CategoryDefinition categoryDefinition = result.getCategoryDefinition();
        if (categoryDefinition != null) {
            persister.persistCategoryDefinition(categoryDefinition);
        }
        ModelPlot modelPlot = result.getModelPlot();
        if (modelPlot != null) {
            context.bulkResultsPersister.persistModelPlot(modelPlot);
        }
        ModelSizeStats modelSizeStats = result.getModelSizeStats();
        if (modelSizeStats != null) {
            LOGGER.trace("[{}] Parsed ModelSizeStats: {} / {} / {} / {} / {} / {}",
                    context.jobId, modelSizeStats.getModelBytes(), modelSizeStats.getTotalByFieldCount(),
                    modelSizeStats.getTotalOverFieldCount(), modelSizeStats.getTotalPartitionFieldCount(),
                    modelSizeStats.getBucketAllocationFailuresCount(), modelSizeStats.getMemoryStatus());

            latestModelSizeStats = modelSizeStats;
            persister.persistModelSizeStats(modelSizeStats);
        }
        ModelSnapshot modelSnapshot = result.getModelSnapshot();
        if (modelSnapshot != null) {
            persister.persistModelSnapshot(modelSnapshot);
            // We need to refresh the index in order for the snapshot to be available when we'll try to
            // update the job with it
            persister.commitResultWrites(jobId);
            updateModelSnapshotIdOnJob(modelSnapshot);
        }
        Quantiles quantiles = result.getQuantiles();
        if (quantiles != null) {
            persister.persistQuantiles(quantiles);
            context.bulkResultsPersister.executeRequest();

            if (processKilled == false) {
                // We need to make all results written up to these quantiles available for renormalization
                persister.commitResultWrites(context.jobId);
                LOGGER.debug("[{}] Quantiles parsed from output - will trigger renormalization of scores", context.jobId);
                renormalizer.renormalize(quantiles);
            }
        }
        FlushAcknowledgement flushAcknowledgement = result.getFlushAcknowledgement();
        if (flushAcknowledgement != null) {
            LOGGER.debug("[{}] Flush acknowledgement parsed from output for ID {}", context.jobId, flushAcknowledgement.getId());
            // Commit previous writes here, effectively continuing
            // the flush from the C++ autodetect process right
            // through to the data store
            context.bulkResultsPersister.executeRequest();
            persister.commitResultWrites(context.jobId);
            flushListener.acknowledgeFlush(flushAcknowledgement);
            // Interim results may have been produced by the flush,
            // which need to be
            // deleted when the next finalized results come through
            context.deleteInterimRequired = true;
        }
    }

    protected void updateModelSnapshotIdOnJob(ModelSnapshot modelSnapshot) {
        JobUpdate update = new JobUpdate.Builder(jobId).setModelSnapshotId(modelSnapshot.getSnapshotId()).build();
        UpdateJobAction.Request updateRequest = new UpdateJobAction.Request(jobId, update);

        try {
            // This blocks the main processing thread in the unlikely event
            // there are 2 model snapshots queued up. But it also has the
            // advantage of ensuring order
            updateModelSnapshotIdSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("[{}] Interrupted acquiring update model snapshot semaphore", jobId);
            return;
        }

        client.execute(UpdateJobAction.INSTANCE, updateRequest, new ActionListener<PutJobAction.Response>() {
            @Override
            public void onResponse(PutJobAction.Response response) {
                updateModelSnapshotIdSemaphore.release();
                LOGGER.debug("[{}] Updated job with model snapshot id [{}]", jobId, modelSnapshot.getSnapshotId());
            }

            @Override
            public void onFailure(Exception e) {
                updateModelSnapshotIdSemaphore.release();
                LOGGER.error("[" + jobId + "] Failed to update job with new model snapshot id [" + modelSnapshot.getSnapshotId() + "]", e);
            }
        });
    }

    public void awaitCompletion() throws TimeoutException {
        try {
            // Although the results won't take 30 minutes to finish, the pipe won't be closed
            // until the state is persisted, and that can take a while
            if (completionLatch.await(MachineLearning.STATE_PERSIST_RESTORE_TIMEOUT.getMinutes(), TimeUnit.MINUTES) == false) {
                throw new TimeoutException("Timed out waiting for results processor to complete for job " + jobId);
            }
            // Input stream has been completely processed at this point.
            // Wait for any updateModelSnapshotIdOnJob calls to complete.
            updateModelSnapshotIdSemaphore.acquire();
            updateModelSnapshotIdSemaphore.release();

            // These lines ensure that the "completion" we're awaiting includes making the results searchable
            waitUntilRenormalizerIsIdle();
            persister.commitResultWrites(jobId);
            persister.commitStateWrites(jobId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("[{}] Interrupted waiting for results processor to complete", jobId);
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
    public FlushAcknowledgement waitForFlushAcknowledgement(String flushId, Duration timeout) {
        return failed ? null : flushListener.waitForFlush(flushId, timeout);
    }

    public void clearAwaitingFlush(String flushId) {
        flushListener.clear(flushId);
    }

    public void waitUntilRenormalizerIsIdle() {
        renormalizer.waitUntilIdle();
    }

    /**
     * If failed then there was an error parsing the results that cannot be recovered from
     * @return true if failed
     */
    public boolean isFailed() {
        return failed;
    }

    static class Context {

        private final String jobId;
        private JobResultsPersister.Builder bulkResultsPersister;

        boolean deleteInterimRequired;

        Context(String jobId, JobResultsPersister.Builder bulkResultsPersister) {
            this.jobId = jobId;
            this.deleteInterimRequired = true;
            this.bulkResultsPersister = bulkResultsPersister;
        }
    }

    public ModelSizeStats modelSizeStats() {
        return latestModelSizeStats;
    }

}

