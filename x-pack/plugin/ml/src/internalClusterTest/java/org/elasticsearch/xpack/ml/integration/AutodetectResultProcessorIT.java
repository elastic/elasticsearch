/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationTests;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStatsTests;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
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
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
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

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
        return pluginList(
            LocalStateMachineLearning.class,
            DataStreamsPlugin.class,
            IngestCommonPlugin.class,
            ReindexPlugin.class,
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            MapperExtrasPlugin.class,
            Wildcard.class
        );
    }

    @Before
    public void createComponents() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        // We can't change the signature of createComponents to e.g. pass differing values of includeNodeInfo to pass to the
        // AnomalyDetectionAuditor constructor. Instead we generate a random boolean value for that purpose.
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(client(), getInstanceFromNode(ClusterService.class), randomBoolean());
        jobResultsProvider = new JobResultsProvider(client(), builder.build(), TestIndexNameExpressionResolver.newInstance());
        renormalizer = mock(Renormalizer.class);
        process = mock(AutodetectProcess.class);
        capturedUpdateModelSnapshotOnJobRequests = new ArrayList<>();
        ThreadPool tp = mockThreadPool();
        Settings settings = Settings.builder().put("node.name", "InferenceProcessorFactoryTests_node").build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
                )
            )
        );
        ClusterService clusterService = new ClusterService(settings, clusterSettings, tp, null);
        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        resultsPersisterService = new ResultsPersisterService(tp, originSettingClient, clusterService, settings);
        resultProcessor = new AutodetectResultProcessor(
            client(),
            auditor,
            JOB_ID,
            renormalizer,
            new JobResultsPersister(originSettingClient, resultsPersisterService),
            new AnnotationPersister(resultsPersisterService),
            process,
            new ModelSizeStats.Builder(JOB_ID).build(),
            new TimingStats(JOB_ID)
        ) {
            @Override
            protected void updateModelSnapshotOnJob(ModelSnapshot modelSnapshot) {
                capturedUpdateModelSnapshotOnJobRequests.add(modelSnapshot);
            }
        };
        waitForMlTemplates();
        putJob();
        // In production opening a job ensures the state index exists. These tests
        // do not open jobs, but instead feed JSON directly to the results processor.
        // A a result they must create the index as part of the test setup. Do not
        // copy this setup to tests that run jobs in the way they are run in production.
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        createStateIndexAndAliasIfNecessary(
            client(),
            ClusterState.EMPTY_STATE,
            TestIndexNameExpressionResolver.newInstance(),
            MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            future
        );
        future.get();
    }

    @After
    public void deleteJob() throws Exception {
        DeleteJobAction.Request request = new DeleteJobAction.Request(JOB_ID);
        AcknowledgedResponse response = client().execute(DeleteJobAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());
        // Verify that deleting job also deletes associated model snapshots annotations
        assertThat(
            getAnnotations().stream().map(Annotation::getAnnotation).collect(toList()),
            everyItem(not(startsWith("Job model snapshot")))
        );
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
        CategorizerStats categorizerStats = createCategorizerStats();
        resultsBuilder.addCategorizerStats(categorizerStats);
        ModelPlot modelPlot = createModelPlot();
        resultsBuilder.addModelPlot(modelPlot);
        Annotation annotation = createAnnotation();
        resultsBuilder.addAnnotation(annotation);
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

        QueryPage<CategoryDefinition> persistedDefinition = getCategoryDefinition(
            randomBoolean() ? categoryDefinition.getCategoryId() : null,
            randomBoolean() ? categoryDefinition.getPartitionFieldValue() : null
        );
        assertEquals(1, persistedDefinition.count());
        assertEquals(categoryDefinition, persistedDefinition.results().get(0));

        QueryPage<CategorizerStats> persistedCategorizerStats = jobResultsProvider.categorizerStats(JOB_ID, 0, 100);
        assertEquals(1, persistedCategorizerStats.count());
        assertEquals(categorizerStats, persistedCategorizerStats.results().get(0));

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

        // Verify that there are two annotations:
        // 1. one related to creating model snapshot
        // 2. one for {@link Annotation} result
        List<Annotation> annotations = getAnnotations();
        assertThat("Annotations were: " + annotations, annotations, hasSize(2));
        assertThat(
            annotations.stream().map(Annotation::getAnnotation).collect(toList()),
            containsInAnyOrder("Job model snapshot with id [" + modelSnapshot.getSnapshotId() + "] stored", annotation.getAnnotation())
        );
    }

    public void testProcessResults_ModelSnapshot() throws Exception {
        ModelSnapshot modelSnapshot = createModelSnapshot();
        ResultsBuilder resultsBuilder = new ResultsBuilder().addModelSnapshot(modelSnapshot);
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        QueryPage<ModelSnapshot> persistedModelSnapshot = getModelSnapshots();
        assertThat(persistedModelSnapshot.count(), is(equalTo(1L)));
        assertThat(persistedModelSnapshot.results(), contains(modelSnapshot));

        // Verify that creating model snapshot also creates associated annotation
        List<Annotation> annotations = getAnnotations();
        assertThat(annotations, hasSize(1));
        assertThat(
            annotations.get(0).getAnnotation(),
            is(equalTo("Job model snapshot with id [" + modelSnapshot.getSnapshotId() + "] stored"))
        );

        // Verify that deleting model snapshot also deletes associated annotation
        deleteModelSnapshot(JOB_ID, modelSnapshot.getSnapshotId());
        assertThat(getAnnotations(), empty());
    }

    public void testProcessResults_TimingStats() throws Exception {
        ResultsBuilder resultsBuilder = new ResultsBuilder().addBucket(createBucket(false, 100))
            .addBucket(createBucket(false, 1000))
            .addBucket(createBucket(false, 100))
            .addBucket(createBucket(false, 1000))
            .addBucket(createBucket(false, 100))
            .addBucket(createBucket(false, 1000))
            .addBucket(createBucket(false, 100))
            .addBucket(createBucket(false, 1000))
            .addBucket(createBucket(false, 100))
            .addBucket(createBucket(false, 1000));
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

    public void testProcessResults_InterimResultsDoNotChangeTimingStats() throws Exception {
        ResultsBuilder resultsBuilder = new ResultsBuilder().addBucket(createBucket(true, 100))
            .addBucket(createBucket(true, 100))
            .addBucket(createBucket(true, 100))
            .addBucket(createBucket(false, 10000));
        when(process.readAutodetectResults()).thenReturn(resultsBuilder.build().iterator());

        resultProcessor.process();
        resultProcessor.awaitCompletion();

        TimingStats timingStats = resultProcessor.timingStats();
        assertThat(timingStats.getBucketCount(), equalTo(1L));
        assertThat(timingStats.getMinBucketProcessingTimeMs(), equalTo(10000.0));
        assertThat(timingStats.getMaxBucketProcessingTimeMs(), equalTo(10000.0));
        assertThat(timingStats.getAvgBucketProcessingTimeMs(), equalTo(10000.0));
        assertThat(timingStats.getExponentialAvgBucketProcessingTimeMs(), closeTo(10000.0, 1e-3));
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
        verify(renormalizer).renormalize(eq(quantiles), any(Runnable.class));
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
        verify(renormalizer, never()).renormalize(any(), any());
    }

    public void testDeleteInterimResults() throws Exception {
        Bucket nonInterimBucket = createBucket(false);
        Bucket interimBucket = createBucket(true);

        ResultsBuilder resultsBuilder = new ResultsBuilder().addRecords(createRecords(true))
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

        ResultsBuilder resultsBuilder = new ResultsBuilder().addRecords(createRecords(true))
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

        ResultsBuilder resultsBuilder = new ResultsBuilder().addRecords(firstSetOfRecords)
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

    private static Instant randomInstant() {
        // between 1970 and 2065
        return Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
    }

    private static List<AnomalyRecord> createRecords(boolean isInterim) {
        List<AnomalyRecord> records = new ArrayList<>();

        int count = randomIntBetween(0, 100);
        Date now = randomDate();
        for (int i = 0; i < count; i++) {
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
        for (int i = 0; i < count; i++) {
            Influencer influencer = new Influencer(JOB_ID, "influence_field", randomAlphaOfLength(10), now, 3600L);
            influencer.setInterim(isInterim);
            influencers.add(influencer);
        }
        return influencers;
    }

    private static CategoryDefinition createCategoryDefinition() {
        return CategoryDefinitionTests.createTestInstance(JOB_ID);
    }

    private static CategorizerStats createCategorizerStats() {
        return CategorizerStatsTests.createRandomized(JOB_ID);
    }

    private static ModelPlot createModelPlot() {
        return new ModelPlotTests().createTestInstance(JOB_ID);
    }

    private static Annotation createAnnotation() {
        return AnnotationTests.randomAnnotation(JOB_ID);
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
        return new ModelSnapshot.Builder(JOB_ID).setSnapshotId(randomAlphaOfLength(12))
            .setLatestResultTimeStamp(Date.from(Instant.ofEpochMilli(1000_000_000)))
            .setTimestamp(Date.from(Instant.ofEpochMilli(2000_000_000)))
            .build();
    }

    private static Quantiles createQuantiles() {
        return new Quantiles(JOB_ID, randomDate(), randomAlphaOfLength(100));
    }

    private static FlushAcknowledgement createFlushAcknowledgement() {
        return new FlushAcknowledgement(randomAlphaOfLength(5), randomInstant(), true);
    }

    private static class ResultsBuilder {

        private final List<AutodetectResult> results = new ArrayList<>();

        ResultsBuilder addBucket(Bucket bucket) {
            Objects.requireNonNull(bucket);
            results.add(new AutodetectResult(bucket, null, null, null, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addRecords(List<AnomalyRecord> records) {
            results.add(new AutodetectResult(null, records, null, null, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addInfluencers(List<Influencer> influencers) {
            results.add(new AutodetectResult(null, null, influencers, null, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addCategoryDefinition(CategoryDefinition categoryDefinition) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, null, null, categoryDefinition, null, null));
            return this;
        }

        ResultsBuilder addCategorizerStats(CategorizerStats categorizerStats) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, null, null, null, categorizerStats, null));
            return this;
        }

        ResultsBuilder addModelPlot(ModelPlot modelPlot) {
            results.add(new AutodetectResult(null, null, null, null, null, null, modelPlot, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addAnnotation(Annotation annotation) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, annotation, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addModelSizeStats(ModelSizeStats modelSizeStats) {
            results.add(new AutodetectResult(null, null, null, null, null, modelSizeStats, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addModelSnapshot(ModelSnapshot modelSnapshot) {
            results.add(new AutodetectResult(null, null, null, null, modelSnapshot, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addQuantiles(Quantiles quantiles) {
            results.add(new AutodetectResult(null, null, null, quantiles, null, null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addFlushAcknowledgement(FlushAcknowledgement flushAcknowledgement) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, null, null, null, null, flushAcknowledgement));
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

    private QueryPage<CategoryDefinition> getCategoryDefinition(Long categoryId, String partitionFieldValue) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<CategoryDefinition>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.categoryDefinitions(
            JOB_ID,
            categoryId,
            partitionFieldValue,
            false,
            (categoryId == null) ? 0 : null,
            (categoryId == null) ? 100 : null,
            r -> {
                resultHolder.set(r);
                latch.countDown();
            },
            e -> {
                errorHolder.set(e);
                latch.countDown();
            },
            null,
            null,
            client()
        );
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

    private List<Annotation> getAnnotations() throws Exception {
        // Refresh the annotations index so that recently indexed annotation docs are visible.
        indicesAdmin().prepareRefresh(AnnotationIndex.LATEST_INDEX_NAME)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .execute()
            .actionGet();

        SearchRequest searchRequest = new SearchRequest(AnnotationIndex.READ_ALIAS_NAME);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        List<Annotation> annotations = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            annotations.add(parseAnnotation(hit.getSourceRef()));
        }
        return annotations;
    }

    private Annotation parseAnnotation(BytesReference source) throws IOException {
        try (XContentParser parser = createParser(jsonXContent, source)) {
            return Annotation.fromXContent(parser, null);
        }
    }

    private void deleteModelSnapshot(String jobId, String snapshotId) {
        DeleteModelSnapshotAction.Request request = new DeleteModelSnapshotAction.Request(jobId, snapshotId);
        AcknowledgedResponse response = client().execute(DeleteModelSnapshotAction.INSTANCE, request).actionGet();
        assertThat(response.isAcknowledged(), is(true));
    }

    private Optional<Quantiles> getQuantiles() throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<Optional<Quantiles>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobResultsProvider.getAutodetectParams(JobTests.buildJobBuilder(JOB_ID).setModelSnapshotId("test_snapshot").build(), params -> {
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
