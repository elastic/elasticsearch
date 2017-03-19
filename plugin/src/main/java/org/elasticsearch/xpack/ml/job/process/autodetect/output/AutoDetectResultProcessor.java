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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
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
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A runnable class that reads the autodetect process output in the
 * {@link #process(AutodetectProcess, boolean)} method and persists parsed
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
    private final FlushListener flushListener;

    private volatile ModelSizeStats latestModelSizeStats;

    public AutoDetectResultProcessor(Client client, String jobId, Renormalizer renormalizer, JobResultsPersister persister) {
        this(client, jobId, renormalizer, persister, new FlushListener());
    }

    AutoDetectResultProcessor(Client client,String jobId, Renormalizer renormalizer, JobResultsPersister persister,
                              FlushListener flushListener) {
        this.client = client;
        this.jobId = jobId;
        this.renormalizer = renormalizer;
        this.persister = persister;
        this.flushListener = flushListener;

        ModelSizeStats.Builder builder = new ModelSizeStats.Builder(jobId);
        latestModelSizeStats = builder.build();
    }

    public void process(AutodetectProcess process, boolean isPerPartitionNormalization) {
        Context context = new Context(jobId, isPerPartitionNormalization, persister.bulkPersisterBuilder(jobId));
        try {
            int bucketCount = 0;
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            while (iterator.hasNext()) {
                AutodetectResult result = iterator.next();
                processResult(context, result);
                if (result.getBucket() != null) {
                    bucketCount++;
                    LOGGER.trace("[{}] Bucket number {} parsed from output", jobId, bucketCount);
                }
            }
            context.bulkResultsPersister.executeRequest();
            LOGGER.info("[{}] {} buckets parsed from autodetect output", jobId, bucketCount);
            LOGGER.info("[{}] Parse results Complete", jobId);
        } catch (Exception e) {
            LOGGER.error(new ParameterizedMessage("[{}] error parsing autodetect output", new Object[] {jobId}), e);
        } finally {
            try {
                waitUntilRenormalizerIsIdle();
                persister.commitResultWrites(jobId);
                persister.commitStateWrites(jobId);
            } catch (IndexNotFoundException e) {
                LOGGER.error("[{}] Error while closing: no such index [{}]", jobId, e.getIndex().getName());
            } finally {
                flushListener.clear();
                completionLatch.countDown();
            }
        }
    }

    void processResult(Context context, AutodetectResult result) {
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
            context.bulkResultsPersister = persister.bulkPersisterBuilder(context.jobId);
        }
        List<AnomalyRecord> records = result.getRecords();
        if (records != null && !records.isEmpty()) {
            context.bulkResultsPersister.persistRecords(records);
            if (context.isPerPartitionNormalization) {
                context.bulkResultsPersister.persistPerPartitionMaxProbabilities(new PerPartitionMaxProbabilities(records));
            }
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
            persister.persistModelPlot(modelPlot);
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
            updateModelSnapshotIdOnJob(modelSnapshot);
        }
        Quantiles quantiles = result.getQuantiles();
        if (quantiles != null) {
            persister.persistQuantiles(quantiles);
            // We need to make all results written up to these quantiles available for renormalization
            context.bulkResultsPersister.executeRequest();
            persister.commitResultWrites(context.jobId);

            LOGGER.debug("[{}] Quantiles parsed from output - will trigger renormalization of scores", context.jobId);
            renormalizer.renormalize(quantiles);
        }
        FlushAcknowledgement flushAcknowledgement = result.getFlushAcknowledgement();
        if (flushAcknowledgement != null) {
            LOGGER.debug("[{}] Flush acknowledgement parsed from output for ID {}", context.jobId, flushAcknowledgement.getId());
            // Commit previous writes here, effectively continuing
            // the flush from the C++ autodetect process right
            // through to the data store
            context.bulkResultsPersister.executeRequest();
            persister.commitResultWrites(context.jobId);
            flushListener.acknowledgeFlush(flushAcknowledgement.getId());
            // Interim results may have been produced by the flush,
            // which need to be
            // deleted when the next finalized results come through
            context.deleteInterimRequired = true;
        }
    }

    protected void updateModelSnapshotIdOnJob(ModelSnapshot modelSnapshot) {
        JobUpdate update = new JobUpdate.Builder().setModelSnapshotId(modelSnapshot.getSnapshotId()).build();
        UpdateJobAction.Request updateRequest = new UpdateJobAction.Request(jobId, update);
        client.execute(UpdateJobAction.INSTANCE, updateRequest, new ActionListener<PutJobAction.Response>() {
            @Override
            public void onResponse(PutJobAction.Response response) {
                LOGGER.debug("[{}] Updated job with model snapshot id [{}]", jobId, modelSnapshot.getSnapshotId());
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.error("[" + jobId + "] Failed to update job with new model snapshot id [" + modelSnapshot.getSnapshotId() + "]", e);
            }
        });
    }

    public void awaitCompletion() {
        try {
            completionLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Blocks until a flush is acknowledged or the timeout expires, whichever happens first.
     *
     * @param flushId the id of the flush request to wait for
     * @param timeout the timeout
     * @return {@code true} if the flush has completed or the parsing finished; {@code false} if the timeout expired
     */
    public boolean waitForFlushAcknowledgement(String flushId, Duration timeout) {
        return flushListener.waitForFlush(flushId, timeout);
    }

    public void clearAwaitingFlush(String flushId) {
        flushListener.clear(flushId);
    }

    public void waitUntilRenormalizerIsIdle() {
        renormalizer.waitUntilIdle();
    }

    static class Context {

        private final String jobId;
        private final boolean isPerPartitionNormalization;
        private JobResultsPersister.Builder bulkResultsPersister;

        boolean deleteInterimRequired;

        Context(String jobId, boolean isPerPartitionNormalization, JobResultsPersister.Builder bulkResultsPersister) {
            this.jobId = jobId;
            this.isPerPartitionNormalization = isPerPartitionNormalization;
            this.deleteInterimRequired = false;
            this.bulkResultsPersister = bulkResultsPersister;
        }
    }

    public ModelSizeStats modelSizeStats() {
        return latestModelSizeStats;
    }

}

