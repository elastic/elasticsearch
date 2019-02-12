/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;

public class EstablishedMemUsageIT extends BaseMlIntegTestCase {

    private long bucketSpan = AnalysisConfig.Builder.DEFAULT_BUCKET_SPAN.getMillis();

    private JobResultsProvider jobResultsProvider;
    private JobResultsPersister jobResultsPersister;

    @Before
    public void createComponents() {
        Settings settings = nodeSettings(0);
        jobResultsProvider = new JobResultsProvider(client(), settings);
        jobResultsPersister = new JobResultsPersister(client());
    }

    public void testEstablishedMem_givenNoResults() throws Exception {
        String jobId = "no-results-established-mem-job";

        initClusterAndJob(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
    }

    public void testEstablishedMem_givenNoStatsLongHistory() throws Exception {
        String jobId = "no-stats-long-history-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 25);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
    }

    public void testEstablishedMem_givenNoStatsShortHistory() throws Exception {
        String jobId = "no-stats-short-history-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 5);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
    }

    public void testEstablishedMem_givenHistoryTooShort() throws Exception {
        String jobId = "too-short-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 19);
        createModelSizeStats(jobId, 1, 19000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
        assertThat(queryEstablishedMemoryUsage(jobId, 19, latestModelSizeStats), equalTo(0L));
    }

    public void testEstablishedMem_givenHistoryJustEnoughLowVariation() throws Exception {
        String jobId = "just-enough-low-cv-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 20);
        createModelSizeStats(jobId, 1, 19000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(20000L));
        assertThat(queryEstablishedMemoryUsage(jobId, 20, latestModelSizeStats), equalTo(20000L));
    }

    public void testEstablishedMem_givenHistoryJustEnoughAndUninitialized() throws Exception {
        String jobId = "just-enough-low-cv-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 20);
        createModelSizeStats(jobId, 1, 0L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 0L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
        assertThat(queryEstablishedMemoryUsage(jobId, 20, latestModelSizeStats), equalTo(0L));
    }

    public void testEstablishedMem_givenHistoryJustEnoughHighVariation() throws Exception {
        String jobId = "just-enough-high-cv-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 20);
        createModelSizeStats(jobId, 1, 1000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
        assertThat(queryEstablishedMemoryUsage(jobId, 20, latestModelSizeStats), equalTo(0L));
    }

    public void testEstablishedMem_givenLongEstablished() throws Exception {
        String jobId = "long-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 25);
        createModelSizeStats(jobId, 1, 10000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 2, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(20000L));
        assertThat(queryEstablishedMemoryUsage(jobId, 25, latestModelSizeStats), equalTo(20000L));
    }

    public void testEstablishedMem_givenOneRecentChange() throws Exception {
        String jobId = "one-recent-change-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 25);
        createModelSizeStats(jobId, 1, 10000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(20000L));
        assertThat(queryEstablishedMemoryUsage(jobId, 25, latestModelSizeStats), equalTo(20000L));
    }

    public void testEstablishedMem_givenOneRecentChangeOnlyAndUninitialized() throws Exception {
        String jobId = "one-recent-change-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 25);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 0L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
        assertThat(queryEstablishedMemoryUsage(jobId, 25, latestModelSizeStats), equalTo(0L));
    }

    public void testEstablishedMem_givenOneRecentChangeOnly() throws Exception {
        String jobId = "one-recent-change-only-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 25);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 10, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(20000L));
        assertThat(queryEstablishedMemoryUsage(jobId, 25, latestModelSizeStats), equalTo(20000L));
    }

    public void testEstablishedMem_givenHistoricHighVariationRecentLowVariation() throws Exception {
        String jobId = "historic-high-cv-recent-low-cv-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 40);
        createModelSizeStats(jobId, 1, 1000L);
        createModelSizeStats(jobId, 3, 2000L);
        createModelSizeStats(jobId, 10, 6000L);
        createModelSizeStats(jobId, 19, 9000L);
        createModelSizeStats(jobId, 30, 19000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 35, 20000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(20000L));
        assertThat(queryEstablishedMemoryUsage(jobId, 40, latestModelSizeStats), equalTo(20000L));
    }

    public void testEstablishedMem_givenHistoricLowVariationRecentHighVariation() throws Exception {
        String jobId = "historic-low-cv-recent-high-cv-established-mem-job";

        initClusterAndJob(jobId);

        createBuckets(jobId, 40);
        createModelSizeStats(jobId, 1, 19000L);
        createModelSizeStats(jobId, 3, 20000L);
        createModelSizeStats(jobId, 25, 21000L);
        createModelSizeStats(jobId, 27, 39000L);
        createModelSizeStats(jobId, 30, 67000L);
        ModelSizeStats latestModelSizeStats = createModelSizeStats(jobId, 35, 95000L);
        jobResultsPersister.commitResultWrites(jobId);

        assertThat(queryEstablishedMemoryUsage(jobId), equalTo(0L));
        assertThat(queryEstablishedMemoryUsage(jobId, 40, latestModelSizeStats), equalTo(0L));
    }

    private void initClusterAndJob(String jobId) {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        Job.Builder job = createJob(jobId);
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
    }

    private void createBuckets(String jobId, int count) {
        JobResultsPersister.Builder builder = jobResultsPersister.bulkPersisterBuilder(jobId);
        for (int i = 1; i <= count; ++i) {
            Bucket bucket = new Bucket(jobId, new Date(bucketSpan * i), bucketSpan);
            builder.persistBucket(bucket);
        }
        builder.executeRequest();
    }

    private ModelSizeStats createModelSizeStats(String jobId, int bucketNum, long modelBytes) {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder(jobId)
                .setTimestamp(new Date(bucketSpan * bucketNum))
                .setLogTime(new Date(bucketSpan * bucketNum + randomIntBetween(1, 1000)))
                .setModelBytes(modelBytes).build();
        jobResultsPersister.persistModelSizeStats(modelSizeStats);
        return modelSizeStats;
    }

    private Long queryEstablishedMemoryUsage(String jobId) throws Exception {
        return queryEstablishedMemoryUsage(jobId, null, null);
    }

    private Long queryEstablishedMemoryUsage(String jobId, Integer bucketNum, ModelSizeStats latestModelSizeStats)
            throws Exception {
        AtomicReference<Long> establishedModelMemoryUsage = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        Date latestBucketTimestamp = (bucketNum != null) ? new Date(bucketSpan * bucketNum) : null;
        jobResultsProvider.getEstablishedMemoryUsage(jobId, latestBucketTimestamp, latestModelSizeStats, memUse -> {
                    establishedModelMemoryUsage.set(memUse);
                    latch.countDown();
                }, e -> {
                    exception.set(e);
                    latch.countDown();
                });

        latch.await();

        if (exception.get() != null) {
            throw exception.get();
        }

        return establishedModelMemoryUsage.get();
    }
}
