/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.job.results.BucketTests;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinitionTests;
import org.elasticsearch.xpack.ml.job.results.ModelPlotTests;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutodetectResultProcessorIT extends MlSingleNodeTestCase {
    private static final String JOB_ID = "autodetect-result-processor-it-job";

    private JobResultsProvider jobResultsProvider;
    private List<ModelSnapshot> capturedUpdateModelSnapshotOnJobRequests;
    private AutodetectResultProcessor resultProcessor;
    private Renormalizer renormalizer;
    private AutodetectProcess process;
    private ResultsPersisterService resultsPersisterService;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateMachineLearning.class, ReindexPlugin.class);
    }

    @Before
    public void createComponents() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client(), "test_node");
        jobResultsProvider = new JobResultsProvider(client(), builder.build());
        renormalizer = mock(Renormalizer.class);
        process = mock(AutodetectProcess.class);
        capturedUpdateModelSnapshotOnJobRequests = new ArrayList<>();
        ThreadPool tp = mock(ThreadPool.class);
        Settings settings = Settings.builder().put("node.name", "InferenceProcessorFactoryTests_node").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ClusterService.USER_DEFINED_META_DATA,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(settings, clusterSettings, tp);

        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        resultsPersisterService = new ResultsPersisterService(originSettingClient, clusterService, settings);
        resultProcessor = new AutodetectResultProcessor(
                client(),
                auditor,
                JOB_ID,
                renormalizer,
                new JobResultsPersister(originSettingClient, resultsPersisterService, new AnomalyDetectionAuditor(client(), "test_node")),
                process,
                new ModelSizeStats.Builder(JOB_ID).build(),
                new TimingStats(JOB_ID)) {
            @Override
            protected void updateModelSnapshotOnJob(ModelSnapshot modelSnapshot) {
                capturedUpdateModelSnapshotOnJobRequests.add(modelSnapshot);
            }
        };
        waitForMlTemplates();
        putJob();
    }

    @After
    public void deleteJob() {
        DeleteJobAction.Request request = new DeleteJobAction.Request(JOB_ID);
        AcknowledgedResponse response = client().execute(DeleteJobAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());
    }

    public void testProcessResults() throws Exception {
        ResultsBuilder resultsBuilder = new ResultsBuilder();
        Bucket bucket = createBucket(false);
        resultsBuilder.addBucket(bucket);
        List<AnomalyRecord> records = createRecords(false);
        resultsBuilder.addRecords(records);
        List<Influencer> influencers = createInfluencers(false);
        resultsBuilder.addInfluencers(influencers);
        CategoryDefinition categoryDefinition = createCategoryDefinition();
        resultsBuilder.addCategoryDefinition(categoryDefinition);
        ModelPlot modelPlot = createModelPlot();
        resultsBuilder.addModelPlot(modelPlot);
        ModelSizeStats modelSizeStats = createModelSizeStats();
        resultsBuilder.addModelSizeStats(modelSizeStats);
        ModelSnapshot modelSnapshot = createModelSnapshot();
        resultsBuilder.addModelSnapshot(modelSnapshot);
        Quantiles quantiles = createQuantiles();
        resultsBuilder.addQuantiles(quantiles);
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        BucketsQueryBuilder bucketsQuery = new BucketsQueryBuilder().includeInterim(true);
        QueryPage<Bucket> persistedBucket = getBucketQueryPage(bucketsQuery);
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        bucket.setRecords(Collections.emptyList());
        assertEquals(bucket, persistedBucket.results().get(0));

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder());
        assertResultsAreSame(records, persistedRecords);

        QueryPage<Influencer> persistedInfluencers = getInfluencers();
        assertResultsAreSame(influencers, persistedInfluencers);

        QueryPage<CategoryDefinition> persistedDefinition = getCategoryDefinition(categoryDefinition.getCategoryId());
        assertEquals(1, persistedDefinition.count());
        assertEquals(categoryDefinition, persistedDefinition.results().get(0));

        QueryPage<ModelPlot> persistedModelPlot = jobResultsProvider.modelPlot(JOB_ID, 0, 100);
        assertEquals(1, persistedModelPlot.count());
        assertEquals(modelPlot, persistedModelPlot.results().get(0));

        ModelSizeStats persistedModelSizeStats = getModelSizeStats();
        assertEquals(modelSizeStats, persistedModelSizeStats);

        QueryPage<ModelSnapshot> persistedModelSnapshot = getModelSnapshots();
        assertEquals(1, persistedModelSnapshot.count());
        assertEquals(modelSnapshot, persistedModelSnapshot.results().get(0));
        assertEquals(Collections.singletonList(modelSnapshot), capturedUpdateModelSnapshotOnJobRequests);

        Optional<Quantiles> persistedQuantiles = getQuantiles();
        assertTrue(persistedQuantiles.isPresent());
        assertEquals(quantiles, persistedQuantiles.get());
    }

    public void testProcessResults_TimingStats() throws Exception {
        ResultsBuilder resultsBuilder = new ResultsBuilder()
                .addBucket(createBucket(true, 100))
                .addBucket(createBucket(true, 1000))
                .addBucket(createBucket(true, 100))
                .addBucket(createBucket(true, 1000))
                .addBucket(createBucket(true, 100))
                .addBucket(createBucket(true, 1000))
                .addBucket(createBucket(true, 100))
                .addBucket(createBucket(true, 1000))
                .addBucket(createBucket(true, 100))
                .addBucket(createBucket(true, 1000));
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        TimingStats timingStats = resultProcessor.timingStats();
        assertThat(timingStats.getJobId(), equalTo(JOB_ID));
        assertThat(timingStats.getBucketCount(), equalTo(10L));
        assertThat(timingStats.getMinBucketProcessingTimeMs(), equalTo(100.0));
        assertThat(timingStats.getMaxBucketProcessingTimeMs(), equalTo(1000.0));
        assertThat(timingStats.getAvgBucketProcessingTimeMs(), equalTo(550.0));
        assertThat(timingStats.getExponentialAvgBucketProcessingTimeMs(), closeTo(143.244, 1e-3));
    }

    public void testParseQuantiles_GivenRenormalizationIsEnabled() throws Exception {
        when(renormalizer.isEnabled()).thenReturn(true);

        ResultsBuilder resultsBuilder = new ResultsBuilder();
        Quantiles quantiles = createQuantiles();
        resultsBuilder.addQuantiles(quantiles);
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        Optional<Quantiles> persistedQuantiles = getQuantiles();
        assertTrue(persistedQuantiles.isPresent());
        assertEquals(quantiles, persistedQuantiles.get());
        verify(renormalizer).renormalize(quantiles);
    }

    public void testParseQuantiles_GivenRenormalizationIsDisabled() throws Exception {
        when(renormalizer.isEnabled()).thenReturn(false);

        ResultsBuilder resultsBuilder = new ResultsBuilder();
        Quantiles quantiles = createQuantiles();
        resultsBuilder.addQuantiles(quantiles);
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        Optional<Quantiles> persistedQuantiles = getQuantiles();
        assertTrue(persistedQuantiles.isPresent());
        assertEquals(quantiles, persistedQuantiles.get());
        verify(renormalizer, never()).renormalize(quantiles);
    }

    public void testDeleteInterimResults() throws Exception {
        Bucket nonInterimBucket = createBucket(false);
        Bucket interimBucket = createBucket(true);

        ResultsBuilder resultsBuilder = new ResultsBuilder()
                .addRecords(createRecords(true))
                .addInfluencers(createInfluencers(true))
                .addBucket(interimBucket)  // this will persist the interim results
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addBucket(nonInterimBucket); // and this will delete the interim results
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true));
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        nonInterimBucket.setRecords(Collections.emptyList());
        assertEquals(nonInterimBucket, persistedBucket.results().get(0));

        QueryPage<Influencer> persistedInfluencers = getInfluencers();
        assertEquals(0, persistedInfluencers.count());

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().includeInterim(true));
        assertEquals(0, persistedRecords.count());
    }

    public void testMultipleFlushesBetweenPersisting() throws Exception {
        Bucket finalBucket = createBucket(true);
        List<AnomalyRecord> finalAnomalyRecords = createRecords(true);

        ResultsBuilder resultsBuilder = new ResultsBuilder()
                .addRecords(createRecords(true))
                .addInfluencers(createInfluencers(true))
                .addBucket(createBucket(true))  // this will persist the interim results
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addRecords(createRecords(true))
                .addBucket(createBucket(true)) // and this will delete the interim results and persist the new interim bucket & records
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addRecords(finalAnomalyRecords)
                .addBucket(finalBucket); // this deletes the previous interim and persists final bucket & records
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true));
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        finalBucket.setRecords(Collections.emptyList());
        assertEquals(finalBucket, persistedBucket.results().get(0));

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().includeInterim(true));
        assertResultsAreSame(finalAnomalyRecords, persistedRecords);
    }

    public void testEndOfStreamTriggersPersisting() throws Exception {
        Bucket bucket = createBucket(false);
        List<AnomalyRecord> firstSetOfRecords = createRecords(false);
        List<AnomalyRecord> secondSetOfRecords = createRecords(false);

        ResultsBuilder resultsBuilder = new ResultsBuilder()
                .addRecords(firstSetOfRecords)
                .addBucket(bucket)  // bucket triggers persistence
                .addRecords(secondSetOfRecords);
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true));
        assertEquals(1, persistedBucket.count());

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().size(200).includeInterim(true));
        List<AnomalyRecord> allRecords = new ArrayList<>(firstSetOfRecords);
        allRecords.addAll(secondSetOfRecords);
        assertResultsAreSame(allRecords, persistedRecords);
    }

    private void putJob() {
        Detector detector = new Detector.Builder("dc", "by_instance").build();
        Job.Builder jobBuilder = new Job.Builder(JOB_ID);
        jobBuilder.setDataDescription(new DataDescription.Builder());
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector)));
        PutJobAction.Request request = new PutJobAction.Request(jobBuilder);
        client().execute(PutJobAction.INSTANCE, request).actionGet();
    }

    private static Bucket createBucket(boolean isInterim) {
        Bucket bucket = new BucketTests().createTestInstance(JOB_ID);
        bucket.setInterim(isInterim);
        return bucket;
    }

    private static Bucket createBucket(boolean isInterim, long processingTimeMs) {
        Bucket bucket = createBucket(isInterim);
        bucket.setProcessingTimeMs(processingTimeMs);
        return bucket;
    }

    private static Date randomDate() {
        // between 1970 and 2065
        return new Date(randomLongBetween(0, 3000000000000L));
    }

    private static List<AnomalyRecord> createRecords(boolean isInterim) {
        List<AnomalyRecord> records = new ArrayList<>();

        int count = randomIntBetween(0, 100);
        Date now = randomDate();
        for (int i=0; i<count; i++) {
            AnomalyRecord r = new AnomalyRecord(JOB_ID, now, 3600L);
            r.setByFieldName("by_instance");
            r.setByFieldValue(randomAlphaOfLength(8));
            r.setInterim(isInterim);
            records.add(r);
        }
        return records;
    }

    private static List<Influencer> createInfluencers(boolean isInterim) {
        List<Influencer> influencers = new ArrayList<>();

        int count = randomIntBetween(0, 100);
        Date now = new Date();
        for (int i=0; i<count; i++) {
            Influencer influencer = new Influencer(JOB_ID, "influence_field", randomAlphaOfLength(10), now, 3600L);
            influencer.setInterim(isInterim);
            influencers.add(influencer);
        }
        return influencers;
    }

    private static CategoryDefinition createCategoryDefinition() {
        return new CategoryDefinitionTests().createTestInstance(JOB_ID);
    }

    private static ModelPlot createModelPlot() {
        return new ModelPlotTests().createTestInstance(JOB_ID);
    }

    private static ModelSizeStats createModelSizeStats() {
        ModelSizeStats.Builder builder = new ModelSizeStats.Builder(JOB_ID);
        builder.setTimestamp(randomDate());
        builder.setLogTime(randomDate());
        builder.setBucketAllocationFailuresCount(randomNonNegativeLong());
        builder.setModelBytes(randomNonNegativeLong());
        builder.setTotalByFieldCount(randomNonNegativeLong());
        builder.setTotalOverFieldCount(randomNonNegativeLong());
        builder.setTotalPartitionFieldCount(randomNonNegativeLong());
        builder.setMemoryStatus(randomFrom(EnumSet.allOf(ModelSizeStats.MemoryStatus.class)));
        return builder.build();
    }

    private static ModelSnapshot createModelSnapshot() {
        return new ModelSnapshot.Builder(JOB_ID).setSnapshotId(randomAlphaOfLength(12)).build();
    }

    private static Quantiles createQuantiles() {
        return new Quantiles(JOB_ID, randomDate(), randomAlphaOfLength(100));
    }

    private static FlushAcknowledgement createFlushAcknowledgement() {
        return new FlushAcknowledgement(randomAlphaOfLength(5), randomDate());
    }

    private static class ResultsBuilder {

        private final List<AutodetectResult> results = new ArrayList<>();

        ResultsBuilder addBucket(Bucket bucket) {
            results.add(new AutodetectResult(Objects.requireNonNull(bucket), null, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addRecords(List<AnomalyRecord> records) {
            results.add(new AutodetectResult(null, records, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addInfluencers(List<Influencer> influencers) {
            results.add(new AutodetectResult(null, null, influencers, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addCategoryDefinition(CategoryDefinition categoryDefinition) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, null, categoryDefinition, null));
            return this;
        }

        ResultsBuilder addModelPlot(ModelPlot modelPlot) {
            results.add(new AutodetectResult(null, null, null, null, null, null, modelPlot, null, null, null, null));
            return this;
        }

        ResultsBuilder addModelSizeStats(ModelSizeStats modelSizeStats) {
            results.add(new AutodetectResult(null, null, null, null, null, modelSizeStats, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addModelSnapshot(ModelSnapshot modelSnapshot) {
            results.add(new AutodetectResult(null, null, null, null, modelSnapshot, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addQuantiles(Quantiles quantiles) {
            results.add(new AutodetectResult(null, null, null, quantiles, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addFlushAcknowledgement(FlushAcknowledgement flushAcknowledgement) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, null, null, flushAcknowledgement));
            return this;
        }

        Iterable<AutodetectResult> build() {
            return results;
        }
    }


    private <T extends ToXContent & Writeable> void assertResultsAreSame(List<T> expected, QueryPage<T> actual) {
        assertEquals(expected.size(), actual.count());
        assertEquals(actual.results().size(), actual.count());
        Set<T> expectedSet = new HashSet<>(expected);
        expectedSet.removeAll(actual.results());
        assertEquals(0, expectedSet.size());
    }

    private QueryPage<Bucket> getBucketQueryPage(BucketsQueryBuilder bucketsQuery) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<Bucket>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.buckets(JOB_ID, bucketsQuery, r -> {
            resultHolder.set(r);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }, client());
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private QueryPage<CategoryDefinition> getCategoryDefinition(long categoryId) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<CategoryDefinition>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.categoryDefinitions(JOB_ID, categoryId, false, null, null, r -> {
            resultHolder.set(r);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }, client());
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private ModelSizeStats getModelSizeStats() throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<ModelSizeStats> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.modelSizeStats(JOB_ID, modelSizeStats -> {
            resultHolder.set(modelSizeStats);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        });
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private QueryPage<Influencer> getInfluencers() throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<Influencer>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.influencers(JOB_ID, new InfluencersQueryBuilder().build(), page -> {
            resultHolder.set(page);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }, client());
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private QueryPage<AnomalyRecord> getRecords(RecordsQueryBuilder recordsQuery) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<AnomalyRecord>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.records(JOB_ID, recordsQuery, page -> {
            resultHolder.set(page);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        }, client());
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private QueryPage<ModelSnapshot> getModelSnapshots() throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<ModelSnapshot>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.modelSnapshots(JOB_ID, 0, 100, page -> {
            resultHolder.set(page);
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        });
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }

    private Optional<Quantiles> getQuantiles() throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<Optional<Quantiles>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.getAutodetectParams(JobTests.buildJobBuilder(JOB_ID).build(), params -> {
            resultHolder.set(Optional.ofNullable(params.quantiles()));
            latch.countDown();
        }, e -> {
            errorHolder.set(e);
            latch.countDown();
        });
        latch.await();
        if (errorHolder.get() != null) {
            throw errorHolder.get();
        }
        return resultHolder.get();
    }
}
