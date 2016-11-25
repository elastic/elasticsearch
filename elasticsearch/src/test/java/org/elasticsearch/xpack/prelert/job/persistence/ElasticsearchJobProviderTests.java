/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.job.AnalysisLimits;
import org.elasticsearch.xpack.prelert.job.CategorizerState;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.ModelState;
import org.elasticsearch.xpack.prelert.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.Result;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchJobProviderTests extends ESTestCase {
    private static final String CLUSTER_NAME = "myCluster";
    private static final String JOB_ID = "foo";
    private static final String INDEX_NAME = "prelertresults-foo";

    @Captor
    private ArgumentCaptor<Map<String, Object>> mapCaptor;

    public void testGetQuantiles_GivenNoIndexForJob() throws InterruptedException, ExecutionException {

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .throwMissingIndexOnPrepareGet(INDEX_NAME, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID);

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        ESTestCase.expectThrows(IndexNotFoundException.class, () -> provider.getQuantiles(JOB_ID));
    }

    public void testGetQuantiles_GivenNoQuantilesForJob() throws Exception {
        GetResponse getResponse = createGetResponse(false, null);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareGet(INDEX_NAME, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID, getResponse);

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        Optional<Quantiles> quantiles = provider.getQuantiles(JOB_ID);

        assertFalse(quantiles.isPresent());
    }

    public void testGetQuantiles_GivenQuantilesHaveNonEmptyState() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(Quantiles.JOB_ID.getPreferredName(), "foo");
        source.put(Quantiles.TIMESTAMP.getPreferredName(), 0L);
        source.put(Quantiles.QUANTILE_STATE.getPreferredName(), "state");
        GetResponse getResponse = createGetResponse(true, source);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareGet(INDEX_NAME, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID, getResponse);

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        Optional<Quantiles> quantiles = provider.getQuantiles(JOB_ID);

        assertTrue(quantiles.isPresent());
        assertEquals("state", quantiles.get().getQuantileState());
    }

    public void testGetQuantiles_GivenQuantilesHaveEmptyState() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(Quantiles.JOB_ID.getPreferredName(), "foo");
        source.put(Quantiles.TIMESTAMP.getPreferredName(), new Date(0L).getTime());
        source.put(Quantiles.QUANTILE_STATE.getPreferredName(), "");
        GetResponse getResponse = createGetResponse(true, source);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareGet(INDEX_NAME, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID, getResponse);

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        Optional<Quantiles> quantiles = provider.getQuantiles(JOB_ID);

        assertTrue(quantiles.isPresent());
        assertEquals("", quantiles.get().getQuantileState());
    }

    public void testCreateUsageMetering() throws InterruptedException, ExecutionException {
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, false)
                .prepareCreate(ElasticsearchJobProvider.PRELERT_USAGE_INDEX)
                .addClusterStatusYellowResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX);
        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        provider.createUsageMeteringIndex((result, error) -> logger.info("result={}", result));
        clientBuilder.verifyIndexCreated(ElasticsearchJobProvider.PRELERT_USAGE_INDEX);
    }

    public void testCreateJob() throws InterruptedException, ExecutionException {
        Job.Builder job = buildJobBuilder("marscapone");
        job.setDescription("This is a very cheesy job");
        AnalysisLimits limits = new AnalysisLimits(9878695309134L, null);
        job.setAnalysisLimits(limits);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).createIndexRequest("prelertresults-" + job.getId());

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        AtomicReference<Boolean> resultHolder = new AtomicReference<>();
        provider.createJobRelatedIndices(job.build(), new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                resultHolder.set(aBoolean);
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
        assertNotNull(resultHolder.get());
        assertTrue(resultHolder.get());
    }

    public void testDeleteJob() throws InterruptedException, ExecutionException, IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteJobAction.Response> actionListener = mock(ActionListener.class);
        String jobId = "ThisIsMyJob";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true);
        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        clientBuilder.resetIndices();
        clientBuilder.addIndicesExistsResponse("prelertresults-" + jobId, true).addIndicesDeleteResponse("prelertresults-" + jobId, true,
                false, actionListener);
        clientBuilder.build();

        provider.deleteJobRelatedIndices(jobId, actionListener);

        ArgumentCaptor<DeleteJobAction.Response> responseCaptor = ArgumentCaptor.forClass(DeleteJobAction.Response.class);
        verify(actionListener).onResponse(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().isAcknowledged());
    }

    public void testDeleteJob_InvalidIndex() throws InterruptedException, ExecutionException, IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteJobAction.Response> actionListener = mock(ActionListener.class);
        String jobId = "ThisIsMyJob";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true);
        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        clientBuilder.resetIndices();
        clientBuilder.addIndicesExistsResponse("prelertresults-" + jobId, true).addIndicesDeleteResponse("prelertresults-" + jobId, true,
                true, actionListener);
        clientBuilder.build();

        provider.deleteJobRelatedIndices(jobId, actionListener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), instanceOf(InterruptedException.class));
    }

    public void testBuckets_OneBucketNoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("timestamp", now.getTime());
        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        int from = 0;
        int size = 10;
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(0.0)
                .normalizedProbabilityThreshold(1.0);

        QueryPage<Bucket> buckets = provider.buckets(jobId, bq.build());
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilder.getValue();
        String queryString = query.toString();
        assertTrue(
                queryString.matches("(?s).*maxNormalizedProbability[^}]*from. : 1\\.0.*must_not[^}]*term[^}]*isInterim.*value. : .true.*"));
    }

    public void testBuckets_OneBucketInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("timestamp", now.getTime());
        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        int from = 99;
        int size = 17;
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(5.1)
                .normalizedProbabilityThreshold(10.9).includeInterim(true);

        QueryPage<Bucket> buckets = provider.buckets(jobId, bq.build());
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilder.getValue();
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*maxNormalizedProbability[^}]*from. : 10\\.9.*"));
        assertTrue(queryString.matches("(?s).*anomalyScore[^}]*from. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*isInterim.*"));
    }

    public void testBuckets_UsingBuilder()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("timestamp", now.getTime());
        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        int from = 99;
        int size = 17;
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.from(from);
        bq.size(size);
        bq.anomalyScoreThreshold(5.1);
        bq.normalizedProbabilityThreshold(10.9);
        bq.includeInterim(true);

        QueryPage<Bucket> buckets = provider.buckets(jobId, bq.build());
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilder.getValue();
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*maxNormalizedProbability[^}]*from. : 10\\.9.*"));
        assertTrue(queryString.matches("(?s).*anomalyScore[^}]*from. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*isInterim.*"));
    }

    public void testBucket_NoBucketNoExpandNoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Long timestamp = 98765432123456789L;
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("timestamp", now.getTime());
        // source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(false, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), 0, 0, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketQueryBuilder bq = new BucketQueryBuilder(Long.toString(timestamp));

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () ->provider.bucket(jobId, bq.build()));
    }

    public void testBucket_OneBucketNoExpandNoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("timestamp", now.getTime());
        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), 0, 0, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketQueryBuilder bq = new BucketQueryBuilder(Long.toString(now.getTime()));

        QueryPage<Bucket> bucketHolder = provider.bucket(jobId, bq.build());
        assertThat(bucketHolder.count(), equalTo(1L));
        Bucket b = bucketHolder.results().get(0);
        assertEquals(now, b.getTimestamp());
    }

    public void testBucket_OneBucketNoExpandInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("timestamp", now.getTime());
        map.put("isInterim", true);
        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), 0, 0, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        BucketQueryBuilder bq = new BucketQueryBuilder(Long.toString(now.getTime()));

        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class,
                () ->provider.bucket(jobId, bq.build()));
    }

    public void testRecords() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucketSpan", 22);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucketSpan", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder().from(from).size(size).epochStart(String.valueOf(now.getTime()))
                .epochEnd(String.valueOf(now.getTime())).includeInterim(true).sortField(sortfield).anomalyScoreThreshold(11.1)
                .normalizedProbability(2.2);

        QueryPage<AnomalyRecord> recordPage = provider.records(jobId, rqb.build());
        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();
        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testRecords_UsingBuilder()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucketSpan", 22);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucketSpan", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder();
        rqb.from(from);
        rqb.size(size);
        rqb.epochStart(String.valueOf(now.getTime()));
        rqb.epochEnd(String.valueOf(now.getTime()));
        rqb.includeInterim(true);
        rqb.sortField(sortfield);
        rqb.anomalyScoreThreshold(11.1);
        rqb.normalizedProbability(2.2);

        QueryPage<AnomalyRecord> recordPage = provider.records(jobId, rqb.build());
        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();
        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testBucketRecords() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        Bucket bucket = mock(Bucket.class);
        when(bucket.getTimestamp()).thenReturn(now);

        List<Map<String, Object>> source = new ArrayList<>();
        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucketSpan", 22);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucketSpan", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        QueryPage<AnomalyRecord> recordPage = provider.bucketRecords(jobId, bucket, from, size, true, sortfield, true, "");

        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();
        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testexpandBucket() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        Bucket bucket = new Bucket("foo");
        bucket.setTimestamp(now);

        List<Map<String, Object>> source = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("jobId", "foo");
            recordMap.put("typical", 22.4 + i);
            recordMap.put("actual", 33.3 + i);
            recordMap.put("timestamp", now.getTime());
            recordMap.put("function", "irritable");
            recordMap.put("bucketSpan", 22);
            source.add(recordMap);
        }

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearchAnySize("prelertresults-" + jobId, Result.TYPE.getPreferredName(), response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        int records = provider.expandBucket(jobId, false, bucket);
        assertEquals(400L, records);
    }

    public void testexpandBucket_WithManyRecords()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        Bucket bucket = new Bucket("foo");
        bucket.setTimestamp(now);

        List<Map<String, Object>> source = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("jobId", "foo");
            recordMap.put("typical", 22.4 + i);
            recordMap.put("actual", 33.3 + i);
            recordMap.put("timestamp", now.getTime());
            recordMap.put("function", "irritable");
            recordMap.put("bucketSpan", 22);
            source.add(recordMap);
        }

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearchAnySize("prelertresults-" + jobId, Result.TYPE.getPreferredName(), response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        int records = provider.expandBucket(jobId, false, bucket);
        // This is not realistic, but is an artifact of the fact that the mock
        // query
        // returns all the records, not a subset
        assertEquals(1200L, records);
    }

    public void testCategoryDefinitions()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("jobId", "foo");
        map.put("categoryId", String.valueOf(map.hashCode()));
        map.put("terms", terms);

        source.add(map);

        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        int from = 0;
        int size = 10;
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, CategoryDefinition.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        QueryPage<CategoryDefinition> categoryDefinitions = provider.categoryDefinitions(jobId, from, size);
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testCategoryDefinition()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";

        Map<String, Object> source = new HashMap<>();
        String categoryId = String.valueOf(source.hashCode());
        source.put("jobId", "foo");
        source.put("categoryId", categoryId);
        source.put("terms", terms);

        GetResponse getResponse = createGetResponse(true, source);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareGet("prelertresults-" + jobId, CategoryDefinition.TYPE.getPreferredName(), categoryId, getResponse);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);
        QueryPage<CategoryDefinition> categoryDefinitions = provider.categoryDefinition(jobId, categoryId);
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testInfluencers_NoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("probability", 0.555);
        recordMap1.put("influencerFieldName", "Builder");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("influencerFieldValue", "Bob");
        recordMap1.put("initialAnomalyScore", 22.2);
        recordMap1.put("anomalyScore", 22.6);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("probability", 0.99);
        recordMap2.put("influencerFieldName", "Builder");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("influencerFieldValue", "James");
        recordMap2.put("initialAnomalyScore", 5.0);
        recordMap2.put("anomalyScore", 5.0);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(),
                        from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        InfluencersQuery query = new InfluencersQueryBuilder().from(from).size(size).includeInterim(false).build();
        QueryPage<Influencer> page = provider.influencers(jobId, query);
        assertEquals(2L, page.count());

        String queryString = queryBuilder.getValue().toString();
        assertTrue(queryString.matches("(?s).*must_not[^}]*term[^}]*isInterim.*value. : .true.*"));

        List<Influencer> records = page.results();
        assertEquals("foo", records.get(0).getJobId());
        assertEquals("Bob", records.get(0).getInfluencerFieldValue());
        assertEquals("Builder", records.get(0).getInfluencerFieldName());
        assertEquals(now, records.get(0).getTimestamp());
        assertEquals(0.555, records.get(0).getProbability(), 0.00001);
        assertEquals(22.6, records.get(0).getAnomalyScore(), 0.00001);
        assertEquals(22.2, records.get(0).getInitialAnomalyScore(), 0.00001);

        assertEquals("James", records.get(1).getInfluencerFieldValue());
        assertEquals("Builder", records.get(1).getInfluencerFieldName());
        assertEquals(now, records.get(1).getTimestamp());
        assertEquals(0.99, records.get(1).getProbability(), 0.00001);
        assertEquals(5.0, records.get(1).getAnomalyScore(), 0.00001);
        assertEquals(5.0, records.get(1).getInitialAnomalyScore(), 0.00001);
    }

    public void testInfluencers_WithInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("probability", 0.555);
        recordMap1.put("influencerFieldName", "Builder");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("influencerFieldValue", "Bob");
        recordMap1.put("initialAnomalyScore", 22.2);
        recordMap1.put("anomalyScore", 22.6);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("probability", 0.99);
        recordMap2.put("influencerFieldName", "Builder");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("influencerFieldValue", "James");
        recordMap2.put("initialAnomalyScore", 5.0);
        recordMap2.put("anomalyScore", 5.0);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, Result.TYPE.getPreferredName(), from, size, response,
                        queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        InfluencersQuery query = new InfluencersQueryBuilder().from(from).size(size).epochStart("0").epochEnd("0").sortField("sort")
                .sortDescending(true).anomalyScoreThreshold(0.0).includeInterim(true).build();
        QueryPage<Influencer> page = provider.influencers(jobId, query);
        assertEquals(2L, page.count());

        String queryString = queryBuilder.getValue().toString();
        assertFalse(queryString.matches("(?s).*isInterim.*"));

        List<Influencer> records = page.results();
        assertEquals("Bob", records.get(0).getInfluencerFieldValue());
        assertEquals("Builder", records.get(0).getInfluencerFieldName());
        assertEquals(now, records.get(0).getTimestamp());
        assertEquals(0.555, records.get(0).getProbability(), 0.00001);
        assertEquals(22.6, records.get(0).getAnomalyScore(), 0.00001);
        assertEquals(22.2, records.get(0).getInitialAnomalyScore(), 0.00001);

        assertEquals("James", records.get(1).getInfluencerFieldValue());
        assertEquals("Builder", records.get(1).getInfluencerFieldName());
        assertEquals(now, records.get(1).getTimestamp());
        assertEquals(0.99, records.get(1).getProbability(), 0.00001);
        assertEquals(5.0, records.get(1).getAnomalyScore(), 0.00001);
        assertEquals(5.0, records.get(1).getInitialAnomalyScore(), 0.00001);
    }

    public void testInfluencer() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        String influencerId = "ThisIsAnInfluencerId";

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        try {
            provider.influencer(jobId, influencerId);
            assertTrue(false);
        } catch (IllegalStateException e) {
        }
    }

    public void testModelSnapshots() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("description", "snapshot1");
        recordMap1.put("restorePriority", 1);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("snapshotDocCount", 5);
        recordMap1.put("latestRecordTimeStamp", now.getTime());
        recordMap1.put("latestResultTimeStamp", now.getTime());
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("description", "snapshot2");
        recordMap2.put("restorePriority", 999);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("snapshotDocCount", 6);
        recordMap2.put("latestRecordTimeStamp", now.getTime());
        recordMap2.put("latestResultTimeStamp", now.getTime());
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, ModelSnapshot.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        QueryPage<ModelSnapshot> page = provider.modelSnapshots(jobId, from, size);
        assertEquals(2L, page.count());
        List<ModelSnapshot> snapshots = page.results();

        assertEquals("foo", snapshots.get(0).getJobId());
        assertEquals(now, snapshots.get(0).getTimestamp());
        assertEquals(now, snapshots.get(0).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(0).getLatestResultTimeStamp());
        assertEquals("snapshot1", snapshots.get(0).getDescription());
        assertEquals(1L, snapshots.get(0).getRestorePriority());
        assertEquals(5, snapshots.get(0).getSnapshotDocCount());

        assertEquals(now, snapshots.get(1).getTimestamp());
        assertEquals(now, snapshots.get(1).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(1).getLatestResultTimeStamp());
        assertEquals("snapshot2", snapshots.get(1).getDescription());
        assertEquals(999L, snapshots.get(1).getRestorePriority());
        assertEquals(6, snapshots.get(1).getSnapshotDocCount());
    }

    public void testModelSnapshots_WithDescription()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("jobId", "foo");
        recordMap1.put("description", "snapshot1");
        recordMap1.put("restorePriority", 1);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("snapshotDocCount", 5);
        recordMap1.put("latestRecordTimeStamp", now.getTime());
        recordMap1.put("latestResultTimeStamp", now.getTime());
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("jobId", "foo");
        recordMap2.put("description", "snapshot2");
        recordMap2.put("restorePriority", 999);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("snapshotDocCount", 6);
        recordMap2.put("latestRecordTimeStamp", now.getTime());
        recordMap2.put("latestResultTimeStamp", now.getTime());
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        ArgumentCaptor<QueryBuilder> queryBuilder = ArgumentCaptor.forClass(QueryBuilder.class);
        SearchResponse response = createSearchResponse(true, source);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareSearch("prelertresults-" + jobId, ModelSnapshot.TYPE.getPreferredName(), from, size, response, queryBuilder);

        Client client = clientBuilder.build();
        ElasticsearchJobProvider provider = createProvider(client);

        QueryPage<ModelSnapshot> page = provider.modelSnapshots(jobId, from, size, null, null, "sortfield", true, "snappyId",
                "description1");
        assertEquals(2L, page.count());
        List<ModelSnapshot> snapshots = page.results();

        assertEquals(now, snapshots.get(0).getTimestamp());
        assertEquals(now, snapshots.get(0).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(0).getLatestResultTimeStamp());
        assertEquals("snapshot1", snapshots.get(0).getDescription());
        assertEquals(1L, snapshots.get(0).getRestorePriority());
        assertEquals(5, snapshots.get(0).getSnapshotDocCount());

        assertEquals(now, snapshots.get(1).getTimestamp());
        assertEquals(now, snapshots.get(1).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(1).getLatestResultTimeStamp());
        assertEquals("snapshot2", snapshots.get(1).getDescription());
        assertEquals(999L, snapshots.get(1).getRestorePriority());
        assertEquals(6, snapshots.get(1).getSnapshotDocCount());

        String queryString = queryBuilder.getValue().toString();
        assertTrue(queryString.matches("(?s).*snapshotId.*value. : .snappyId.*description.*value. : .description1.*"));
    }

    public void testMergePartitionScoresIntoBucket() throws InterruptedException, ExecutionException {
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true).addClusterStatusYellowResponse();

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        List<ElasticsearchJobProvider.ScoreTimestamp> scores = new ArrayList<>();
        scores.add(provider.new ScoreTimestamp(new Date(2), 1.0));
        scores.add(provider.new ScoreTimestamp(new Date(3), 2.0));
        scores.add(provider.new ScoreTimestamp(new Date(5), 3.0));

        List<Bucket> buckets = new ArrayList<>();
        buckets.add(createBucketAtEpochTime(1));
        buckets.add(createBucketAtEpochTime(2));
        buckets.add(createBucketAtEpochTime(3));
        buckets.add(createBucketAtEpochTime(4));
        buckets.add(createBucketAtEpochTime(5));
        buckets.add(createBucketAtEpochTime(6));

        provider.mergePartitionScoresIntoBucket(scores, buckets);
        assertEquals(0.0, buckets.get(0).getMaxNormalizedProbability(), 0.001);
        assertEquals(1.0, buckets.get(1).getMaxNormalizedProbability(), 0.001);
        assertEquals(2.0, buckets.get(2).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(3).getMaxNormalizedProbability(), 0.001);
        assertEquals(3.0, buckets.get(4).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(5).getMaxNormalizedProbability(), 0.001);
    }

    public void testMergePartitionScoresIntoBucket_WithEmptyScoresList() throws InterruptedException, ExecutionException {
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true).addClusterStatusYellowResponse();

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        List<ElasticsearchJobProvider.ScoreTimestamp> scores = new ArrayList<>();

        List<Bucket> buckets = new ArrayList<>();
        buckets.add(createBucketAtEpochTime(1));
        buckets.add(createBucketAtEpochTime(2));
        buckets.add(createBucketAtEpochTime(3));
        buckets.add(createBucketAtEpochTime(4));

        provider.mergePartitionScoresIntoBucket(scores, buckets);
        assertEquals(0.0, buckets.get(0).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(1).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(2).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(3).getMaxNormalizedProbability(), 0.001);
    }

    public void testRestoreStateToStream() throws Exception {
        Map<String, Object> categorizerState = new HashMap<>();
        categorizerState.put("catName", "catVal");
        GetResponse categorizerStateGetResponse1 = createGetResponse(true, categorizerState);
        GetResponse categorizerStateGetResponse2 = createGetResponse(false, null);
        Map<String, Object> modelState = new HashMap<>();
        modelState.put("modName", "modVal1");
        GetResponse modelStateGetResponse1 = createGetResponse(true, modelState);
        modelState.put("modName", "modVal2");
        GetResponse modelStateGetResponse2 = createGetResponse(true, modelState);

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse()
                .addIndicesExistsResponse(ElasticsearchJobProvider.PRELERT_USAGE_INDEX, true)
                .prepareGet(INDEX_NAME, CategorizerState.TYPE, "1", categorizerStateGetResponse1)
                .prepareGet(INDEX_NAME, CategorizerState.TYPE, "2", categorizerStateGetResponse2)
                .prepareGet(INDEX_NAME, ModelState.TYPE, "123_1", modelStateGetResponse1)
                .prepareGet(INDEX_NAME, ModelState.TYPE, "123_2", modelStateGetResponse2);

        ElasticsearchJobProvider provider = createProvider(clientBuilder.build());

        ModelSnapshot modelSnapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotId("123");
        modelSnapshot.setSnapshotDocCount(2);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        provider.restoreStateToStream(JOB_ID, modelSnapshot, stream);

        String[] restoreData = stream.toString(StandardCharsets.UTF_8.name()).split("\0");
        assertEquals(3, restoreData.length);
        assertEquals("{\"catName\":\"catVal\"}", restoreData[0]);
        assertEquals("{\"modName\":\"modVal1\"}", restoreData[1]);
        assertEquals("{\"modName\":\"modVal2\"}", restoreData[2]);
    }

    private Bucket createBucketAtEpochTime(long epoch) {
        Bucket b = new Bucket("foo");
        b.setTimestamp(new Date(epoch));
        b.setMaxNormalizedProbability(10.0);
        return b;
    }

    private ElasticsearchJobProvider createProvider(Client client) {
        return new ElasticsearchJobProvider(client, 0, ParseFieldMatcher.STRICT);
    }

    private static GetResponse createGetResponse(boolean exists, Map<String, Object> source) throws IOException {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(exists);
        when(getResponse.getSourceAsBytesRef()).thenReturn(XContentFactory.jsonBuilder().map(source).bytes());
        return getResponse;
    }

    private static SearchResponse createSearchResponse(boolean exists, List<Map<String, Object>> source) throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits hits = mock(SearchHits.class);
        List<SearchHit> list = new ArrayList<>();

        for (Map<String, Object> map : source) {
            SearchHit hit = mock(SearchHit.class);
            Map<String, Object> _source = new HashMap<>(map);
            when(hit.getSourceRef()).thenReturn(XContentFactory.jsonBuilder().map(_source).bytes());
            when(hit.getId()).thenReturn(String.valueOf(map.hashCode()));
            doAnswer(invocation -> {
                String field = (String) invocation.getArguments()[0];
                SearchHitField shf = mock(SearchHitField.class);
                when(shf.getValue()).thenReturn(map.get(field));
                return shf;
            }).when(hit).field(any(String.class));
            list.add(hit);
        }
        when(response.getHits()).thenReturn(hits);
        when(hits.getHits()).thenReturn(list.toArray(new SearchHit[0]));
        when(hits.getTotalHits()).thenReturn((long) source.size());

        doAnswer(invocation -> {
            Integer idx = (Integer) invocation.getArguments()[0];
            return list.get(idx);
        }).when(hits).getAt(any(Integer.class));

        return response;
    }
}
