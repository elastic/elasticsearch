/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.persistence.BatchedDocumentsIterator;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobRenormalizedResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.MockBatchedDocumentsIterator;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.junit.Before;
import org.mockito.MockitoAnnotations;

public class ScoresUpdaterTests extends ESTestCase {
    private static final String JOB_ID = "foo";
    private static final String QUANTILES_STATE = "someState";
    private static final long DEFAULT_BUCKET_SPAN = 3600;
    private static final long DEFAULT_START_TIME = 0;
    private static final long DEFAULT_END_TIME = 3600;

    private JobProvider jobProvider = mock(JobProvider.class);
    private JobRenormalizedResultsPersister jobRenormalizedResultsPersister = mock(JobRenormalizedResultsPersister.class);
    private Normalizer normalizer = mock(Normalizer.class);
    private NormalizerFactory normalizerFactory = mock(NormalizerFactory.class);

    private Job job;
    private ScoresUpdater scoresUpdater;

    private Bucket generateBucket(Date timestamp) throws IOException {
        return new Bucket(JOB_ID, timestamp, DEFAULT_BUCKET_SPAN);
    }

    @Before
    public void setUpMocks() throws IOException {
        MockitoAnnotations.initMocks(this);

        Job.Builder jobBuilder = new Job.Builder(JOB_ID);
        jobBuilder.setRenormalizationWindowDays(1L);
        List<Detector> detectors = new ArrayList<>();
        detectors.add(mock(Detector.class));
        AnalysisConfig.Builder configBuilder = new AnalysisConfig.Builder(detectors);
        configBuilder.setBucketSpan(DEFAULT_BUCKET_SPAN);
        jobBuilder.setAnalysisConfig(configBuilder);

        job = jobBuilder.build();

        scoresUpdater = new ScoresUpdater(job, jobProvider, jobRenormalizedResultsPersister, normalizerFactory);

        givenProviderReturnsNoBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME);
        givenProviderReturnsNoInfluencers(DEFAULT_START_TIME, DEFAULT_END_TIME);
        givenNormalizerFactoryReturnsMock();
    }

    public void testUpdate_GivenBucketWithZeroScoreAndNoRecords() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(0.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.7, 0.0));
        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(0);
        verifyBucketWasNotUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testUpdate_GivenBucketWithNonZeroScoreButNoBucketInfluencers() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(0.0);
        bucket.setBucketInfluencers(new ArrayList<>());
        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(0);
        verifyBucketWasNotUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testUpdate_GivenSingleBucketWithoutBigChangeAndNoRecords() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(30.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 30.0));
        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasNotUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testUpdate_GivenSingleBucketWithoutBigChangeAndRecordsWithoutBigChange() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(30.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 30.0));
        List<AnomalyRecord> records = new ArrayList<>();
        AnomalyRecord record1 = createRecordWithoutBigChange();
        AnomalyRecord record2 = createRecordWithoutBigChange();
        records.add(record1);
        records.add(record2);
        bucket.setRecords(records);
        bucket.setRecordCount(2);

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasNotUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testUpdate_GivenSingleBucketWithBigChangeAndNoRecords() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);
        bucket.raiseBigNormalizedUpdateFlag();

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testUpdate_GivenSingleBucketWithoutBigChangeAndSomeRecordsWithBigChange() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);

        List<AnomalyRecord> records = new ArrayList<>();
        AnomalyRecord record1 = createRecordWithBigChange();
        AnomalyRecord record2 = createRecordWithoutBigChange();
        AnomalyRecord record3 = createRecordWithBigChange();
        records.add(record1);
        records.add(record2);
        records.add(record3);
        bucket.setRecords(records);

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasNotUpdated(bucket);
        verifyRecordsWereUpdated(bucket.getJobId(), Arrays.asList(record1, record3));
    }

    public void testUpdate_GivenSingleBucketWithBigChangeAndSomeRecordsWithBigChange() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);
        bucket.raiseBigNormalizedUpdateFlag();
        List<AnomalyRecord> records = new ArrayList<>();
        AnomalyRecord record1 = createRecordWithBigChange();
        AnomalyRecord record2 = createRecordWithoutBigChange();
        AnomalyRecord record3 = createRecordWithBigChange();
        records.add(record1);
        records.add(record2);
        records.add(record3);
        bucket.setRecords(records);

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, buckets);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasUpdated(bucket);
        verifyRecordsWereUpdated(bucket.getJobId(), Arrays.asList(record1, record3));
    }

    public void testUpdate_GivenEnoughBucketsForTwoBatchesButOneNormalization() throws IOException {
        Deque<Bucket> batch1 = new ArrayDeque<>();
        for (int i = 0; i < 10000; ++i) {
            Bucket bucket = generateBucket(new Date(i * 1000));
            bucket.setAnomalyScore(42.0);
            bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
            bucket.setMaxNormalizedProbability(50.0);
            bucket.raiseBigNormalizedUpdateFlag();
            batch1.add(bucket);
        }

        Bucket secondBatchBucket = generateBucket(new Date(10000 * 1000));
        secondBatchBucket.addBucketInfluencer(createTimeBucketInfluencer(secondBatchBucket.getTimestamp(), 0.04, 42.0));
        secondBatchBucket.setAnomalyScore(42.0);
        secondBatchBucket.setMaxNormalizedProbability(50.0);
        secondBatchBucket.raiseBigNormalizedUpdateFlag();
        Deque<Bucket> batch2 = new ArrayDeque<>();
        batch2.add(secondBatchBucket);

        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, batch1, batch2);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);

        // Batch 1 - Just verify first and last were updated as Mockito
        // is forbiddingly slow when tring to verify all 10000
        verifyBucketWasUpdated(batch1.getFirst());
        verifyBucketRecordsWereNotUpdated(batch1.getFirst().getId());
        verifyBucketWasUpdated(batch1.getLast());
        verifyBucketRecordsWereNotUpdated(batch1.getLast().getId());

        verifyBucketWasUpdated(secondBatchBucket);
        verifyBucketRecordsWereNotUpdated(secondBatchBucket.getId());
    }

    public void testUpdate_GivenTwoBucketsWithFirstHavingEnoughRecordsToForceSecondNormalization() throws IOException {
        Bucket bucket1 = generateBucket(new Date(0));
        bucket1.setAnomalyScore(42.0);
        bucket1.addBucketInfluencer(createTimeBucketInfluencer(bucket1.getTimestamp(), 0.04, 42.0));
        bucket1.setMaxNormalizedProbability(50.0);
        bucket1.raiseBigNormalizedUpdateFlag();
        when(jobProvider.expandBucket(JOB_ID, false, bucket1)).thenReturn(100000);

        Bucket bucket2 = generateBucket(new Date(10000 * 1000));
        bucket2.addBucketInfluencer(createTimeBucketInfluencer(bucket2.getTimestamp(), 0.04, 42.0));
        bucket2.setAnomalyScore(42.0);
        bucket2.setMaxNormalizedProbability(50.0);
        bucket2.raiseBigNormalizedUpdateFlag();

        Deque<Bucket> batch = new ArrayDeque<>();
        batch.add(bucket1);
        batch.add(bucket2);
        givenProviderReturnsBuckets(DEFAULT_START_TIME, DEFAULT_END_TIME, batch);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(2);

        verifyBucketWasUpdated(bucket1);
        verifyBucketRecordsWereNotUpdated(bucket1.getId());
        verifyBucketWasUpdated(bucket2);
        verifyBucketRecordsWereNotUpdated(bucket2.getId());
    }

    public void testUpdate_GivenInfluencerWithBigChange() throws IOException {
        Influencer influencer = new Influencer(JOB_ID, "n", "v", new Date(DEFAULT_START_TIME), 600, 1);
        influencer.raiseBigNormalizedUpdateFlag();

        Deque<Influencer> influencers = new ArrayDeque<>();
        influencers.add(influencer);
        givenProviderReturnsInfluencers(DEFAULT_START_TIME, DEFAULT_END_TIME, influencers);

        scoresUpdater.update(QUANTILES_STATE, 3600, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyInfluencerWasUpdated(influencer);
    }

    public void testDefaultRenormalizationWindowBasedOnTime() throws IOException {
        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);
        bucket.raiseBigNormalizedUpdateFlag();

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(2509200000L, 2595600000L, buckets);
        givenProviderReturnsNoInfluencers(2509200000L, 2595600000L);

        scoresUpdater.update(QUANTILES_STATE, 2595600000L, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testManualRenormalizationWindow() throws IOException {

        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);
        bucket.raiseBigNormalizedUpdateFlag();

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(3600000, 90000000L, buckets);
        givenProviderReturnsNoInfluencers(3600000, 90000000L);

        scoresUpdater.update(QUANTILES_STATE, 90000000L, 0, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    public void testManualRenormalizationWindow_GivenExtension() throws IOException {

        Bucket bucket = generateBucket(new Date(0));
        bucket.setAnomalyScore(42.0);
        bucket.addBucketInfluencer(createTimeBucketInfluencer(bucket.getTimestamp(), 0.04, 42.0));
        bucket.setMaxNormalizedProbability(50.0);
        bucket.raiseBigNormalizedUpdateFlag();

        Deque<Bucket> buckets = new ArrayDeque<>();
        buckets.add(bucket);
        givenProviderReturnsBuckets(2700000, 90000000L, buckets);
        givenProviderReturnsNoInfluencers(2700000, 90000000L);

        scoresUpdater.update(QUANTILES_STATE, 90000000L, 900000, false);

        verifyNormalizerWasInvoked(1);
        verifyBucketWasUpdated(bucket);
        verifyBucketRecordsWereNotUpdated(bucket.getId());
    }

    private void verifyNormalizerWasInvoked(int times) throws IOException {
        int bucketSpan = job.getAnalysisConfig() == null ? 0
                : job.getAnalysisConfig().getBucketSpan().intValue();
        verify(normalizer, times(times)).normalize(
                eq(bucketSpan), eq(false), anyListOf(Normalizable.class),
                eq(QUANTILES_STATE));
    }

    private BucketInfluencer createTimeBucketInfluencer(Date timestamp, double probability, double anomalyScore) {
        BucketInfluencer influencer = new BucketInfluencer(JOB_ID, timestamp, DEFAULT_BUCKET_SPAN, 1);
        influencer.setProbability(probability);
        influencer.setAnomalyScore(anomalyScore);
        influencer.setInfluencerFieldName(BucketInfluencer.BUCKET_TIME);
        return influencer;
    }

    private void givenNormalizerFactoryReturnsMock() {
        when(normalizerFactory.create(JOB_ID)).thenReturn(normalizer);
    }
    private void givenProviderReturnsNoBuckets(long startTime, long endTime) {
        givenBuckets(startTime, endTime, Collections.emptyList());
    }

    private void givenProviderReturnsBuckets(long startTime, long endTime, Deque<Bucket> batch1, Deque<Bucket> batch2) {
        List<Deque<Bucket>> batches = new ArrayList<>();
        batches.add(new ArrayDeque<>(batch1));
        batches.add(new ArrayDeque<>(batch2));
        givenBuckets(startTime, endTime, batches);
    }

    private void givenProviderReturnsBuckets(long startTime, long endTime, Deque<Bucket> buckets) {
        List<Deque<Bucket>> batches = new ArrayList<>();
        batches.add(new ArrayDeque<>(buckets));
        givenBuckets(startTime, endTime, batches);
    }

    private void givenBuckets(long startTime, long endTime, List<Deque<Bucket>> batches) {
        BatchedDocumentsIterator<Bucket> iterator = new MockBatchedDocumentsIterator<>(startTime,
                endTime, batches);
        when(jobProvider.newBatchedBucketsIterator(JOB_ID)).thenReturn(iterator);
    }

    private void givenProviderReturnsNoInfluencers(long startTime, long endTime) {
        givenProviderReturnsInfluencers(startTime, endTime, new ArrayDeque<>());
    }

    private void givenProviderReturnsInfluencers(long startTime, long endTime,
                                                 Deque<Influencer> influencers) {
        List<Deque<Influencer>> batches = new ArrayList<>();
        batches.add(new ArrayDeque<>(influencers));
        BatchedDocumentsIterator<Influencer> iterator = new MockBatchedDocumentsIterator<>(
                startTime, endTime, batches);
        when(jobProvider.newBatchedInfluencersIterator(JOB_ID)).thenReturn(iterator);
    }

    private void verifyBucketWasUpdated(Bucket bucket) {
        verify(jobRenormalizedResultsPersister).updateBucket(bucket);
    }

    private void verifyRecordsWereUpdated(String bucketId, List<AnomalyRecord> records) {
        verify(jobRenormalizedResultsPersister).updateRecords(bucketId, records);
    }

    private void verifyBucketWasNotUpdated(Bucket bucket) {
        verify(jobRenormalizedResultsPersister, never()).updateBucket(bucket);
    }

    private void verifyBucketRecordsWereNotUpdated(String bucketId) {
        verify(jobRenormalizedResultsPersister, never()).updateRecords(eq(bucketId),
                anyListOf(AnomalyRecord.class));
    }

    private static AnomalyRecord createRecordWithoutBigChange() {
        return createRecord(false);
    }

    private static AnomalyRecord createRecordWithBigChange() {
        return createRecord(true);
    }

    private static AnomalyRecord createRecord(boolean hadBigChange) {
        AnomalyRecord anomalyRecord = mock(AnomalyRecord.class);
        when(anomalyRecord.hadBigNormalizedUpdate()).thenReturn(hadBigChange);
        when(anomalyRecord.getId()).thenReturn("someId");
        return anomalyRecord;
    }

    private void verifyInfluencerWasUpdated(Influencer influencer) {
        List<Influencer> list = new ArrayList<>();
        list.add(influencer);
        verify(jobRenormalizedResultsPersister).updateInfluencer(eq(JOB_ID), eq(list));
    }
}
