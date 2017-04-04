/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearningTemplateRegistry;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.JobTests;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.process.normalizer.noop.NoOpRenormalizer;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.BucketTests;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinitionTests;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.results.ModelPlotTests;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutodetectResultProcessorIT extends ESSingleNodeTestCase {
    private static final String JOB_ID = "foo";

    private Renormalizer renormalizer;
    private JobResultsPersister jobResultsPersister;
    private JobProvider jobProvider;
    private List<ModelSnapshot> capturedUpdateModelSnapshotOnJobRequests;
    private AutoDetectResultProcessor resultProcessor;

    @Before
    public void createComponents() {
        renormalizer = new NoOpRenormalizer();
        jobResultsPersister = new JobResultsPersister(nodeSettings(), client());
        Settings.Builder builder = Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1));
        jobProvider = new JobProvider(client(), builder.build());
        capturedUpdateModelSnapshotOnJobRequests = new ArrayList<>();
        resultProcessor = new AutoDetectResultProcessor(client(), JOB_ID, renormalizer, jobResultsPersister,
                new ModelSizeStats.Builder(JOB_ID).build()) {
            @Override
            protected void updateModelSnapshotIdOnJob(ModelSnapshot modelSnapshot) {
                capturedUpdateModelSnapshotOnJobRequests.add(modelSnapshot);
            }
        };
    }

    public void testProcessResults() throws Exception {
        putIndexTemplates();

        ResultsBuilder builder = new ResultsBuilder();
        Bucket bucket = createBucket(false);
        builder.addBucket(bucket);
        List<AnomalyRecord> records = createRecords(false);
        builder.addRecords(records);
        List<Influencer> influencers = createInfluencers(false);
        builder.addInfluencers(influencers);
        CategoryDefinition categoryDefinition = createCategoryDefinition();
        builder.addCategoryDefinition(categoryDefinition);
        ModelPlot modelPlot = createmodelPlot();
        builder.addmodelPlot(modelPlot);
        ModelSizeStats modelSizeStats = createModelSizeStats();
        builder.addModelSizeStats(modelSizeStats);
        ModelSnapshot modelSnapshot = createModelSnapshot();
        builder.addModelSnapshot(modelSnapshot);
        Quantiles quantiles = createQuantiles();
        builder.addQuantiles(quantiles);

        resultProcessor.process(builder.buildTestProcess(), false);
        jobResultsPersister.commitResultWrites(JOB_ID);

        BucketsQueryBuilder.BucketsQuery bucketsQuery = new BucketsQueryBuilder().includeInterim(true).build();
        QueryPage<Bucket> persistedBucket = getBucketQueryPage(bucketsQuery);
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        bucket.setRecords(Collections.emptyList());
        assertEquals(bucket, persistedBucket.results().get(0));

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().build());
        assertResultsAreSame(records, persistedRecords);

        QueryPage<Influencer> persistedInfluencers = getInfluencers();
        assertResultsAreSame(influencers, persistedInfluencers);

        QueryPage<CategoryDefinition> persistedDefinition =
                getCategoryDefinition(Long.toString(categoryDefinition.getCategoryId()));
        assertEquals(1, persistedDefinition.count());
        assertEquals(categoryDefinition, persistedDefinition.results().get(0));

        QueryPage<ModelPlot> persistedmodelPlot = jobProvider.modelPlot(JOB_ID, 0, 100);
        assertEquals(1, persistedmodelPlot.count());
        assertEquals(modelPlot, persistedmodelPlot.results().get(0));

        ModelSizeStats persistedModelSizeStats = getModelSizeStats();
        assertEquals(modelSizeStats, persistedModelSizeStats);

        QueryPage<ModelSnapshot> persistedModelSnapshot = getModelSnapshots();
        assertEquals(1, persistedModelSnapshot.count());
        assertEquals(modelSnapshot, persistedModelSnapshot.results().get(0));
        assertEquals(Arrays.asList(modelSnapshot), capturedUpdateModelSnapshotOnJobRequests);

        Optional<Quantiles> persistedQuantiles = getQuantiles();
        assertTrue(persistedQuantiles.isPresent());
        assertEquals(quantiles, persistedQuantiles.get());
    }

    public void testDeleteInterimResults() throws Exception {
        putIndexTemplates();
        Bucket nonInterimBucket = createBucket(false);
        Bucket interimBucket = createBucket(true);

        ResultsBuilder resultBuilder = new ResultsBuilder()
                .addRecords(createRecords(true))
                .addInfluencers(createInfluencers(true))
                .addBucket(interimBucket)  // this will persist the interim results
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addBucket(nonInterimBucket); // and this will delete the interim results

        resultProcessor.process(resultBuilder.buildTestProcess(), false);
        jobResultsPersister.commitResultWrites(JOB_ID);

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true).build());
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        nonInterimBucket.setRecords(Collections.emptyList());
        assertEquals(nonInterimBucket, persistedBucket.results().get(0));

        QueryPage<Influencer> persistedInfluencers = getInfluencers();
        assertEquals(0, persistedInfluencers.count());

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().includeInterim(true).build());
        assertEquals(0, persistedRecords.count());
    }

    public void testMultipleFlushesBetweenPersisting() throws Exception {
        putIndexTemplates();
        Bucket finalBucket = createBucket(true);
        List<AnomalyRecord> finalAnomalyRecords = createRecords(true);

        ResultsBuilder resultBuilder = new ResultsBuilder()
                .addRecords(createRecords(true))
                .addInfluencers(createInfluencers(true))
                .addBucket(createBucket(true))  // this will persist the interim results
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addRecords(createRecords(true))
                .addBucket(createBucket(true)) // and this will delete the interim results and persist the new interim bucket & records
                .addFlushAcknowledgement(createFlushAcknowledgement())
                .addRecords(finalAnomalyRecords)
                .addBucket(finalBucket); // this deletes the previous interim and persists final bucket & records

        resultProcessor.process(resultBuilder.buildTestProcess(), false);
        jobResultsPersister.commitResultWrites(JOB_ID);

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true).build());
        assertEquals(1, persistedBucket.count());
        // Records are not persisted to Elasticsearch as an array within the bucket
        // documents, so remove them from the expected bucket before comparing
        finalBucket.setRecords(Collections.emptyList());
        assertEquals(finalBucket, persistedBucket.results().get(0));

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().includeInterim(true).build());
        assertResultsAreSame(finalAnomalyRecords, persistedRecords);
    }

    public void testEndOfStreamTriggersPersisting() throws Exception {
        putIndexTemplates();
        Bucket bucket = createBucket(false);
        List<AnomalyRecord> firstSetOfRecords = createRecords(false);
        List<AnomalyRecord> secondSetOfRecords = createRecords(false);

        ResultsBuilder resultBuilder = new ResultsBuilder()
                .addRecords(firstSetOfRecords)
                .addBucket(bucket)  // bucket triggers persistence
                .addRecords(secondSetOfRecords);

        resultProcessor.process(resultBuilder.buildTestProcess(), false);
        jobResultsPersister.commitResultWrites(JOB_ID);

        QueryPage<Bucket> persistedBucket = getBucketQueryPage(new BucketsQueryBuilder().includeInterim(true).build());
        assertEquals(1, persistedBucket.count());

        QueryPage<AnomalyRecord> persistedRecords = getRecords(new RecordsQueryBuilder().size(200).includeInterim(true).build());
        List<AnomalyRecord> allRecords = new ArrayList<>(firstSetOfRecords);
        allRecords.addAll(secondSetOfRecords);
        assertResultsAreSame(allRecords, persistedRecords);
    }

    private void putIndexTemplates() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        new MachineLearningTemplateRegistry(Settings.EMPTY, mock(ClusterService.class), client(), threadPool)
                .addTemplatesIfMissing(client().admin().cluster().state(new ClusterStateRequest().all()).actionGet().getState());

        // block until the templates are installed
        assertBusy(() -> {
            MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();
            assertTrue("Timed out waiting for the ML templates to be installed",
                    MachineLearningTemplateRegistry.allTemplatesInstalled(metaData));
        });
    }

    private Bucket createBucket(boolean isInterim) {
        Bucket bucket = new BucketTests().createTestInstance(JOB_ID);
        bucket.setInterim(isInterim);
        return bucket;
    }

    private List<AnomalyRecord> createRecords(boolean isInterim) {
        List<AnomalyRecord> records = new ArrayList<>();

        int count = randomIntBetween(0, 100);
        Date now = new Date(randomNonNegativeLong());
        for (int i=0; i<count; i++) {
            AnomalyRecord r = new AnomalyRecord(JOB_ID, now, 3600L, i);
            r.setByFieldName("by_instance");
            r.setByFieldValue(randomAlphaOfLength(8));
            r.setInterim(isInterim);
            records.add(r);
        }
        return records;
    }

    private List<Influencer> createInfluencers(boolean isInterim) {
        List<Influencer> influencers = new ArrayList<>();

        int count = randomIntBetween(0, 100);
        Date now = new Date();
        for (int i=0; i<count; i++) {
            Influencer influencer = new Influencer(JOB_ID, "influence_field", randomAlphaOfLength(10), now, 3600L, i);
            influencer.setInterim(isInterim);
            influencers.add(influencer);
        }
        return influencers;
    }

    private CategoryDefinition createCategoryDefinition() {
        return new CategoryDefinitionTests().createTestInstance(JOB_ID);
    }

    private ModelPlot createmodelPlot() {
        return new ModelPlotTests().createTestInstance(JOB_ID);
    }

    private ModelSizeStats createModelSizeStats() {
        ModelSizeStats.Builder builder = new ModelSizeStats.Builder(JOB_ID);
        builder.setId(randomAlphaOfLength(20));
        builder.setTimestamp(new Date(randomNonNegativeLong()));
        builder.setLogTime(new Date(randomNonNegativeLong()));
        builder.setBucketAllocationFailuresCount(randomNonNegativeLong());
        builder.setModelBytes(randomNonNegativeLong());
        builder.setTotalByFieldCount(randomNonNegativeLong());
        builder.setTotalOverFieldCount(randomNonNegativeLong());
        builder.setTotalPartitionFieldCount(randomNonNegativeLong());
        builder.setMemoryStatus(randomFrom(EnumSet.allOf(ModelSizeStats.MemoryStatus.class)));
        return builder.build();
    }

    private ModelSnapshot createModelSnapshot() {
        return new ModelSnapshot.Builder(JOB_ID).setSnapshotId(randomAlphaOfLength(12)).build();
    }

    private Quantiles createQuantiles() {
        return new Quantiles(JOB_ID, new Date(randomNonNegativeLong()), randomAlphaOfLength(100));
    }

    private FlushAcknowledgement createFlushAcknowledgement() {
        return new FlushAcknowledgement(randomAlphaOfLength(5));
    }

    private class ResultsBuilder {

        private List<AutodetectResult> results = new ArrayList<>();
        FlushAcknowledgement flushAcknowledgement;

        ResultsBuilder addBucket(Bucket bucket) {
            results.add(new AutodetectResult(Objects.requireNonNull(bucket), null, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addRecords(List<AnomalyRecord> records) {
            results.add(new AutodetectResult(null, records, null, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addInfluencers(List<Influencer> influencers) {
            results.add(new AutodetectResult(null, null, influencers, null, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addCategoryDefinition(CategoryDefinition categoryDefinition) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, categoryDefinition, null));
            return this;
        }

        ResultsBuilder addmodelPlot(ModelPlot modelPlot) {
            results.add(new AutodetectResult(null, null, null, null, null, null, modelPlot, null, null));
            return this;
        }

        ResultsBuilder addModelSizeStats(ModelSizeStats modelSizeStats) {
            results.add(new AutodetectResult(null, null, null, null, null, modelSizeStats, null, null, null));
            return this;
        }

        ResultsBuilder addModelSnapshot(ModelSnapshot modelSnapshot) {
            results.add(new AutodetectResult(null, null, null, null, modelSnapshot, null, null, null, null));
            return this;
        }

        ResultsBuilder addQuantiles(Quantiles quantiles) {
            results.add(new AutodetectResult(null, null, null, quantiles, null, null, null, null, null));
            return this;
        }

        ResultsBuilder addFlushAcknowledgement(FlushAcknowledgement flushAcknowledgement) {
            results.add(new AutodetectResult(null, null, null, null, null, null, null, null, flushAcknowledgement));
            return this;
        }


        AutodetectProcess buildTestProcess() {
            AutodetectResult[] results = this.results.toArray(new AutodetectResult[0]);
            AutodetectProcess process = mock(AutodetectProcess.class);
            when(process.readAutodetectResults()).thenReturn(Arrays.asList(results).iterator());
            return process;
        }
    }


    private <T extends ToXContent & Writeable> void assertResultsAreSame(List<T> expected, QueryPage<T> actual) {
        assertEquals(expected.size(), actual.count());
        assertEquals(actual.results().size(), actual.count());
        Set<T> expectedSet = new HashSet<>(expected);
        expectedSet.removeAll(actual.results());
        assertEquals(0, expectedSet.size());
    }

    private QueryPage<Bucket> getBucketQueryPage(BucketsQueryBuilder.BucketsQuery bucketsQuery) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<Bucket>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.buckets(JOB_ID, bucketsQuery, r -> {
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

    private QueryPage<CategoryDefinition> getCategoryDefinition(String categoryId) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<CategoryDefinition>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.categoryDefinitions(JOB_ID, categoryId, null, null, r -> {
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
        jobProvider.modelSizeStats(JOB_ID, modelSizeStats -> {
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
        jobProvider.influencers(JOB_ID, new InfluencersQueryBuilder().build(), page -> {
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

    private QueryPage<AnomalyRecord> getRecords(RecordsQueryBuilder.RecordsQuery recordsQuery) throws Exception {
        AtomicReference<Exception> errorHolder = new AtomicReference<>();
        AtomicReference<QueryPage<AnomalyRecord>> resultHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        jobProvider.records(JOB_ID, recordsQuery, page -> {
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
        jobProvider.modelSnapshots(JOB_ID, 0, 100, page -> {
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
        jobProvider.getAutodetectParams(JobTests.buildJobBuilder(JOB_ID).build(),params -> {
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
