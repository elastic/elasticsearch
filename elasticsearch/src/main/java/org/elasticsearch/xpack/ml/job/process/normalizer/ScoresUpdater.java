/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.persistence.BatchedDocumentsIterator;
import org.elasticsearch.xpack.ml.job.persistence.ElasticsearchBatchedResultsIterator;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Thread safe class that updates the scores of all existing results
 * with the renormalized scores
 */
public class ScoresUpdater {
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
    private final JobRenormalizedResultsPersister updatesPersister;
    private final NormalizerFactory normalizerFactory;
    private int bucketSpan;
    private long normalizationWindow;
    private boolean perPartitionNormalization;

    public ScoresUpdater(Job job, JobProvider jobProvider, JobRenormalizedResultsPersister jobRenormalizedResultsPersister,
                         NormalizerFactory normalizerFactory) {
        this.job = job;
        this.jobProvider = Objects.requireNonNull(jobProvider);
        updatesPersister = Objects.requireNonNull(jobRenormalizedResultsPersister);
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
        updateBuckets(normalizer, quantilesState, endBucketEpochMs, windowExtensionMs, counts,
                perPartitionNormalization);
        updateInfluencers(normalizer, quantilesState, endBucketEpochMs, windowExtensionMs, counts);

        LOGGER.info("[{}] Normalization resulted in: {} updates, {} no-ops", job.getId(), counts[0], counts[1]);
    }

    private void updateBuckets(Normalizer normalizer, String quantilesState, long endBucketEpochMs,
                               long windowExtensionMs, int[] counts, boolean perPartitionNormalization) {
        BatchedDocumentsIterator<ElasticsearchBatchedResultsIterator.ResultWithIndex<Bucket>> bucketsIterator =
                jobProvider.newBatchedBucketsIterator(job.getId())
                        .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs);

        // Make a list of buckets to be renormalized.
        // This may be shorter than the original list of buckets for two
        // reasons:
        // 1) We don't bother with buckets that have raw score 0 and no
        //    records
        // 2) We limit the total number of records to be not much more
        //    than 100000
        List<BucketNormalizable> bucketsToRenormalize = new ArrayList<>();
        int batchRecordCount = 0;
        int skipped = 0;

        while (bucketsIterator.hasNext()) {
            // Get a batch of buckets without their records to calculate
            // how many buckets can be sensibly retrieved
            Deque<ElasticsearchBatchedResultsIterator.ResultWithIndex<Bucket>> buckets = bucketsIterator.next();
            if (buckets.isEmpty()) {
                break;
            }

            while (!buckets.isEmpty()) {
                ElasticsearchBatchedResultsIterator.ResultWithIndex<Bucket> current = buckets.removeFirst();
                Bucket currentBucket = current.result;
                if (currentBucket.isNormalizable()) {
                    BucketNormalizable bucketNormalizable = new BucketNormalizable(current.result, current.indexName);
                    List<RecordNormalizable> recordNormalizables =
                            bucketRecordsAsNormalizables(currentBucket.getTimestamp().getTime());
                    batchRecordCount += recordNormalizables.size();
                    bucketNormalizable.setRecords(recordNormalizables);
                    bucketsToRenormalize.add(bucketNormalizable);

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
            normalizeBuckets(normalizer, bucketsToRenormalize, quantilesState, batchRecordCount, skipped, counts,
                    perPartitionNormalization);
        }
    }

    private List<RecordNormalizable> bucketRecordsAsNormalizables(long bucketTimeStamp) {
        BatchedDocumentsIterator<ElasticsearchBatchedResultsIterator.ResultWithIndex<AnomalyRecord>>
                recordsIterator = jobProvider.newBatchedRecordsIterator(job.getId())
                .timeRange(bucketTimeStamp, bucketTimeStamp + 1);

        List<RecordNormalizable> recordNormalizables = new ArrayList<>();
        while (recordsIterator.hasNext()) {
            for (ElasticsearchBatchedResultsIterator.ResultWithIndex<AnomalyRecord> record : recordsIterator.next() ) {
                recordNormalizables.add(new RecordNormalizable(record.result, record.indexName));
            }
        }

        return recordNormalizables;
    }

    private long calcNormalizationWindowStart(long endEpochMs, long windowExtensionMs) {
        return Math.max(0, endEpochMs - normalizationWindow - windowExtensionMs);
    }

    private void normalizeBuckets(Normalizer normalizer, List<BucketNormalizable> normalizableBuckets,
                                  String quantilesState, int recordCount, int skipped, int[] counts,
                                  boolean perPartitionNormalization) {
        LOGGER.debug("[{}] Will renormalize a batch of {} buckets with {} records ({} empty buckets skipped)",
                job.getId(), normalizableBuckets.size(), recordCount, skipped);

        List<Normalizable> asNormalizables = normalizableBuckets.stream()
                .map(Function.identity()).collect(Collectors.toList());
        normalizer.normalize(bucketSpan, perPartitionNormalization, asNormalizables, quantilesState);

        for (BucketNormalizable bn : normalizableBuckets) {
            updateSingleBucket(counts, bn);
        }

        updatesPersister.executeRequest(job.getId());
    }

    private void updateSingleBucket(int[] counts, BucketNormalizable bucketNormalizable) {
        if (bucketNormalizable.hadBigNormalizedUpdate()) {
            if (perPartitionNormalization) {
                List<AnomalyRecord> anomalyRecords = bucketNormalizable.getRecords().stream()
                        .map(RecordNormalizable::getRecord).collect(Collectors.toList());
                PerPartitionMaxProbabilities ppProbs = new PerPartitionMaxProbabilities(anomalyRecords);
                updatesPersister.updateResult(ppProbs.getId(), bucketNormalizable.getOriginatingIndex(), ppProbs);
            }
            updatesPersister.updateBucket(bucketNormalizable);

            ++counts[0];
        } else {
            ++counts[1];
        }

        persistChanged(counts, bucketNormalizable.getRecords());
    }

    private void updateInfluencers(Normalizer normalizer, String quantilesState, long endBucketEpochMs,
                                   long windowExtensionMs, int[] counts) {
        BatchedDocumentsIterator<ElasticsearchBatchedResultsIterator.ResultWithIndex<Influencer>> influencersIterator =
                jobProvider.newBatchedInfluencersIterator(job.getId())
                .timeRange(calcNormalizationWindowStart(endBucketEpochMs, windowExtensionMs), endBucketEpochMs);

        while (influencersIterator.hasNext()) {
            Deque<ElasticsearchBatchedResultsIterator.ResultWithIndex<Influencer>> influencers = influencersIterator.next();
            if (influencers.isEmpty()) {
                LOGGER.debug("[{}] No influencers to renormalize for job", job.getId());
                break;
            }

            LOGGER.debug("[{}] Will renormalize a batch of {} influencers", job.getId(), influencers.size());
            List<Normalizable> asNormalizables = influencers.stream()
                    .map(influencerResultIndex ->
                            new InfluencerNormalizable(influencerResultIndex.result, influencerResultIndex.indexName))
                    .collect(Collectors.toList());
            normalizer.normalize(bucketSpan, perPartitionNormalization, asNormalizables, quantilesState);

            persistChanged(counts, asNormalizables);
        }

        updatesPersister.executeRequest(job.getId());
    }

    private void persistChanged(int[] counts, List<? extends Normalizable> asNormalizables) {
        List<Normalizable> toUpdate = asNormalizables.stream().filter(n -> n.hadBigNormalizedUpdate()).collect(Collectors.toList());

        counts[0] += toUpdate.size();
        counts[1] += asNormalizables.size() - toUpdate.size();
        if (!toUpdate.isEmpty()) {
            updatesPersister.updateResults(toUpdate);
        }
    }
}
