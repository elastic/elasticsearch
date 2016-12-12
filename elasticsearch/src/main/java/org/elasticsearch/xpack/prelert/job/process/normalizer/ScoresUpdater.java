/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.normalizer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.prelert.job.AnalysisConfig;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.persistence.BatchedDocumentsIterator;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobRenormalizer;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.PerPartitionMaxProbabilities;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Thread safe class that updates the scores of all existing results
 * with the renormalized scores
 */
class ScoresUpdater {
    private static final Logger LOGGER = Loggers.getLogger(ScoresUpdater.class);

    /**
     * Target number of records to renormalize at a time
     */
    private static final int TARGET_RECORDS_TO_RENORMALIZE = 100000;

    // 30 days
    private static final long DEFAULT_RENORMALIZATION_WINDOW_MS = 2592000000L;

    private static final int DEFAULT_BUCKETS_IN_RENORMALIZATION_WINDOW = 100;

    private static final long SECONDS_IN_DAY = 86400;
    private static final long MILLISECONDS_IN_SECOND = 1000;

    private final Job job;
    private final JobProvider jobProvider;
    private final JobRenormalizer updatesPersister;
    private final NormalizerFactory normalizerFactory;
    private int bucketSpan;
    private long normalizationWindow;
    private boolean perPartitionNormalization;

    public ScoresUpdater(Job job, JobProvider jobProvider, JobRenormalizer jobRenormalizer, NormalizerFactory normalizerFactory) {
        this.job = job;
        this.jobProvider = Objects.requireNonNull(jobProvider);
        updatesPersister = Objects.requireNonNull(jobRenormalizer);
        this.normalizerFactory = Objects.requireNonNull(normalizerFactory);
        bucketSpan = getBucketSpanOrDefault(job.getAnalysisConfig());
        normalizationWindow = getNormalizationWindowOrDefault(job);
        perPartitionNormalization = getPerPartitionNormalizationOrDefault(job.getAnalysisConfig());
    }

    private static int getBucketSpanOrDefault(AnalysisConfig analysisConfig) {
        if (analysisConfig != null && analysisConfig.getBucketSpan() != null) {
            return analysisConfig.getBucketSpan().intValue();
        }
        // A bucketSpan value of 0 will result to the default
        // bucketSpan value being used in the back-end.
        return 0;
    }

    private long getNormalizationWindowOrDefault(Job job) {
        if (job.getRenormalizationWindowDays() != null) {
            return job.getRenormalizationWindowDays() * SECONDS_IN_DAY * MILLISECONDS_IN_SECOND;
        }
        return Math.max(DEFAULT_RENORMALIZATION_WINDOW_MS,
                DEFAULT_BUCKETS_IN_RENORMALIZATION_WINDOW * bucketSpan * MILLISECONDS_IN_SECOND);
    }

    private static boolean getPerPartitionNormalizationOrDefault(AnalysisConfig analysisConfig) {
        return (analysisConfig != null) && analysisConfig.getUsePerPartitionNormalization();
    }

    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records
     */
    public void update(String quantilesState, long endBucketEpochMs, long windowExtensionMs, boolean perPartitionNormalization) {
        Normalizer normalizer = normalizerFactory.create(job.getId());
        int[] counts = {0, 0};
        updateBuckets(normalizer, quantilesState, endBucketEpochMs,
                windowExtensionMs, counts, perPartitionNormalization);
        updateInfluencers(normalizer, quantilesState, endBucketEpochMs,
                windowExtensionMs, counts);

        LOGGER.info("[{}] Normalization resulted in: {} updates, {} no-ops", job.getId(), counts[0], counts[1]);
    }

    private void updateBuckets(Normalizer normalizer, String quantilesState, long endBucketEpochMs,
                               long windowExtensionMs, int[] counts, boolean perPartitionNormalization) {
        BatchedDocumentsIterator<Bucket> bucketsIterator =
                jobProvider.newBatchedBucketsIterator(job.getId())
                        .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs);

        // Make a list of buckets with their records to be renormalized.
        // This may be shorter than the original list of buckets for two
        // reasons:
        // 1) We don't bother with buckets that have raw score 0 and no
        //    records
        // 2) We limit the total number of records to be not much more
        //    than 100000
        List<Bucket> bucketsToRenormalize = new ArrayList<>();
        int batchRecordCount = 0;
        int skipped = 0;

        while (bucketsIterator.hasNext()) {
            // Get a batch of buckets without their records to calculate
            // how many buckets can be sensibly retrieved
            Deque<Bucket> buckets = bucketsIterator.next();
            if (buckets.isEmpty()) {
                LOGGER.debug("[{}] No buckets to renormalize for job", job.getId());
                break;
            }

            while (!buckets.isEmpty()) {
                Bucket currentBucket = buckets.removeFirst();
                if (currentBucket.isNormalizable()) {
                    bucketsToRenormalize.add(currentBucket);
                    batchRecordCount += jobProvider.expandBucket(job.getId(), false, currentBucket);
                } else {
                    ++skipped;
                }

                if (batchRecordCount >= TARGET_RECORDS_TO_RENORMALIZE) {
                    normalizeBuckets(normalizer, bucketsToRenormalize, quantilesState,
                            batchRecordCount, skipped, counts, perPartitionNormalization);

                    bucketsToRenormalize = new ArrayList<>();
                    batchRecordCount = 0;
                    skipped = 0;
                }
            }
        }
        if (!bucketsToRenormalize.isEmpty()) {
            normalizeBuckets(normalizer, bucketsToRenormalize, quantilesState,
                    batchRecordCount, skipped, counts, perPartitionNormalization);
        }
    }

    private long calcNormalizationWindowStart(long endEpochMs, long windowExtensionMs) {
        return Math.max(0, endEpochMs - normalizationWindow - windowExtensionMs);
    }

    private void normalizeBuckets(Normalizer normalizer, List<Bucket> buckets, String quantilesState,
                                  int recordCount, int skipped, int[] counts, boolean perPartitionNormalization) {
        LOGGER.debug("[{}] Will renormalize a batch of {} buckets with {} records ({} empty buckets skipped)",
                job.getId(), buckets.size(), recordCount, skipped);

        List<Normalizable> asNormalizables = buckets.stream()
                .map(bucket -> new BucketNormalizable(bucket)).collect(Collectors.toList());
        normalizer.normalize(bucketSpan, perPartitionNormalization, asNormalizables, quantilesState);

        for (Bucket bucket : buckets) {
            updateSingleBucket(bucket, counts, perPartitionNormalization);
        }
    }

    /**
     * Update the anomaly score and unsual score fields on the bucket provided
     * and all contained records
     *
     * @param counts Element 0 will be incremented if we update a document and
     *               element 1 if we don't
     */
    private void updateSingleBucket(Bucket bucket, int[] counts, boolean perPartitionNormalization) {
        updateBucketIfItHasBigChange(bucket, counts, perPartitionNormalization);
        updateRecordsThatHaveBigChange(bucket, counts);
    }

    private void updateBucketIfItHasBigChange(Bucket bucket, int[] counts, boolean perPartitionNormalization) {
        if (bucket.hadBigNormalizedUpdate()) {
            if (perPartitionNormalization) {
                updatesPersister.updatePerPartitionMaxProbabilities(bucket.getJobId(), bucket.getRecords());
            }
            updatesPersister.updateBucket(bucket);

            ++counts[0];
        } else {
            ++counts[1];
        }
    }

    private void updateRecordsThatHaveBigChange(Bucket bucket, int[] counts) {
        List<AnomalyRecord> toUpdate = new ArrayList<>();

        for (AnomalyRecord record : bucket.getRecords()) {
            if (record.hadBigNormalizedUpdate()) {
                toUpdate.add(record);
                ++counts[0];
            } else {
                ++counts[1];
            }
        }

        if (!toUpdate.isEmpty()) {
            updatesPersister.updateRecords(bucket.getId(), toUpdate);
        }
    }

    private void updateInfluencers(Normalizer normalizer, String quantilesState, long endBucketEpochMs,
                                   long windowExtensionMs, int[] counts) {
        BatchedDocumentsIterator<Influencer> influencersIterator = jobProvider
                .newBatchedInfluencersIterator(job.getId())
                .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs);
        while (influencersIterator.hasNext()) {
            Deque<Influencer> influencers = influencersIterator.next();
            if (influencers.isEmpty()) {
                LOGGER.debug("[{}] No influencers to renormalize for job", job.getId());
                break;
            }

            LOGGER.debug("[{}] Will renormalize a batch of {} influencers", job.getId(), influencers.size());
            List<Normalizable> asNormalizables = influencers.stream()
                    .map(bucket -> new InfluencerNormalizable(bucket)).collect(Collectors.toList());
            normalizer.normalize(bucketSpan, perPartitionNormalization, asNormalizables, quantilesState);

            List<Influencer> toUpdate = new ArrayList<>();
            for (Influencer influencer : influencers) {
                if (influencer.hadBigNormalizedUpdate()) {
                    toUpdate.add(influencer);
                    ++counts[0];
                } else {
                    ++counts[1];
                }
            }
            if (!toUpdate.isEmpty()) {
                updatesPersister.updateInfluencer(job.getId(), toUpdate);
            }
        }
    }
}
