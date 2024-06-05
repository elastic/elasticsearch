/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.utils.persistence.BatchedDocumentsIterator;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Thread safe class that updates the scores of all existing results
 * with the renormalized scores
 */
public class ScoresUpdater {
    private static final Logger LOGGER = LogManager.getLogger(ScoresUpdater.class);

    /**
     * Target number of buckets to renormalize at a time
     */
    private static final int TARGET_BUCKETS_TO_RENORMALIZE = 100000;

    // 30 days
    private static final long DEFAULT_RENORMALIZATION_WINDOW_MS = 2592000000L;

    private static final int DEFAULT_BUCKETS_IN_RENORMALIZATION_WINDOW = 100;

    private static final long SECONDS_IN_DAY = 86400;
    private static final long MILLISECONDS_IN_SECOND = 1000;

    private final String jobId;
    private final JobResultsProvider jobResultsProvider;
    private final JobRenormalizedResultsPersister updatesPersister;
    private final NormalizerFactory normalizerFactory;
    private int bucketSpan;
    private long normalizationWindow;
    private volatile boolean shutdown;

    public ScoresUpdater(
        Job job,
        JobResultsProvider jobResultsProvider,
        JobRenormalizedResultsPersister jobRenormalizedResultsPersister,
        NormalizerFactory normalizerFactory
    ) {
        jobId = job.getId();
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        updatesPersister = Objects.requireNonNull(jobRenormalizedResultsPersister);
        this.normalizerFactory = Objects.requireNonNull(normalizerFactory);
        bucketSpan = ((Long) job.getAnalysisConfig().getBucketSpan().seconds()).intValue();
        normalizationWindow = getNormalizationWindowOrDefault(job);
    }

    /**
     * Tell the scores updater to shut down ASAP.
     */
    public void shutdown() {
        shutdown = true;
    }

    private long getNormalizationWindowOrDefault(Job job) {
        if (job.getRenormalizationWindowDays() != null) {
            return job.getRenormalizationWindowDays() * SECONDS_IN_DAY * MILLISECONDS_IN_SECOND;
        }
        return Math.max(DEFAULT_RENORMALIZATION_WINDOW_MS, DEFAULT_BUCKETS_IN_RENORMALIZATION_WINDOW * bucketSpan * MILLISECONDS_IN_SECOND);
    }

    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records
     */
    public void update(String quantilesState, long endBucketEpochMs, long windowExtensionMs) {
        Normalizer normalizer = normalizerFactory.create(jobId);
        int[] counts = { 0, 0 };
        updateBuckets(normalizer, quantilesState, endBucketEpochMs, windowExtensionMs, counts);
        updateRecords(normalizer, quantilesState, endBucketEpochMs, windowExtensionMs, counts);
        updateInfluencers(normalizer, quantilesState, endBucketEpochMs, windowExtensionMs, counts);

        // The updates will have been persisted in batches throughout the renormalization
        // process - this call just catches any leftovers
        updatesPersister.executeRequest();

        LOGGER.debug("[{}] Normalization resulted in: {} updates, {} no-ops", jobId, counts[0], counts[1]);
    }

    private void updateBuckets(Normalizer normalizer, String quantilesState, long endBucketEpochMs, long windowExtensionMs, int[] counts) {
        BatchedDocumentsIterator<Result<Bucket>> bucketsIterator = jobResultsProvider.newBatchedBucketsIterator(jobId)
            .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs)
            .includeInterim(false);

        List<BucketNormalizable> bucketsToRenormalize = new ArrayList<>();

        while (bucketsIterator.hasNext() && shutdown == false) {
            Deque<Result<Bucket>> buckets = bucketsIterator.next();
            if (buckets.isEmpty()) {
                LOGGER.debug("[{}] No buckets to renormalize for job", jobId);
                break;
            }

            while (buckets.isEmpty() == false && shutdown == false) {
                Result<Bucket> current = buckets.removeFirst();
                if (current.result.isNormalizable()) {
                    bucketsToRenormalize.add(new BucketNormalizable(current.result, current.index));
                    if (bucketsToRenormalize.size() >= TARGET_BUCKETS_TO_RENORMALIZE) {
                        normalizeBuckets(normalizer, bucketsToRenormalize, quantilesState, counts);
                        bucketsToRenormalize.clear();
                    }
                }
            }
        }
        if (bucketsToRenormalize.isEmpty() == false) {
            normalizeBuckets(normalizer, bucketsToRenormalize, quantilesState, counts);
        }
    }

    private long calcNormalizationWindowStart(long endEpochMs, long windowExtensionMs) {
        return Math.max(0, endEpochMs - normalizationWindow - windowExtensionMs);
    }

    private void normalizeBuckets(
        Normalizer normalizer,
        List<BucketNormalizable> normalizableBuckets,
        String quantilesState,
        int[] counts
    ) {
        normalizer.normalize(bucketSpan, normalizableBuckets, quantilesState);

        for (BucketNormalizable bucketNormalizable : normalizableBuckets) {
            if (bucketNormalizable.hadBigNormalizedUpdate()) {
                updatesPersister.updateBucket(bucketNormalizable);
                ++counts[0];
            } else {
                ++counts[1];
            }
        }
    }

    private void updateRecords(Normalizer normalizer, String quantilesState, long endBucketEpochMs, long windowExtensionMs, int[] counts) {
        BatchedDocumentsIterator<Result<AnomalyRecord>> recordsIterator = jobResultsProvider.newBatchedRecordsIterator(jobId)
            .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs)
            .includeInterim(false);

        while (recordsIterator.hasNext() && shutdown == false) {
            Deque<Result<AnomalyRecord>> records = recordsIterator.next();
            if (records.isEmpty()) {
                LOGGER.debug("[{}] No records to renormalize for job", jobId);
                break;
            }

            LOGGER.debug("[{}] Will renormalize a batch of {} records", jobId, records.size());
            List<Normalizable> asNormalizables = records.stream()
                .map(recordResultIndex -> new RecordNormalizable(recordResultIndex.result, recordResultIndex.index))
                .collect(Collectors.toList());
            normalizer.normalize(bucketSpan, asNormalizables, quantilesState);

            persistChanged(counts, asNormalizables);
        }
    }

    private void updateInfluencers(
        Normalizer normalizer,
        String quantilesState,
        long endBucketEpochMs,
        long windowExtensionMs,
        int[] counts
    ) {
        BatchedDocumentsIterator<Result<Influencer>> influencersIterator = jobResultsProvider.newBatchedInfluencersIterator(jobId)
            .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs)
            .includeInterim(false);

        while (influencersIterator.hasNext() && shutdown == false) {
            Deque<Result<Influencer>> influencers = influencersIterator.next();
            if (influencers.isEmpty()) {
                LOGGER.debug("[{}] No influencers to renormalize for job", jobId);
                break;
            }

            LOGGER.debug("[{}] Will renormalize a batch of {} influencers", jobId, influencers.size());
            List<Normalizable> asNormalizables = influencers.stream()
                .map(influencerResultIndex -> new InfluencerNormalizable(influencerResultIndex.result, influencerResultIndex.index))
                .collect(Collectors.toList());
            normalizer.normalize(bucketSpan, asNormalizables, quantilesState);

            persistChanged(counts, asNormalizables);
        }
    }

    private void persistChanged(int[] counts, List<? extends Normalizable> asNormalizables) {
        if (shutdown) {
            return;
        }

        List<Normalizable> toUpdate = asNormalizables.stream().filter(Normalizable::hadBigNormalizedUpdate).collect(Collectors.toList());

        counts[0] += toUpdate.size();
        counts[1] += asNormalizables.size() - toUpdate.size();
        if (toUpdate.isEmpty() == false) {
            updatesPersister.updateResults(toUpdate);
        }
    }

    long getNormalizationWindow() {
        return normalizationWindow;
    }
}
