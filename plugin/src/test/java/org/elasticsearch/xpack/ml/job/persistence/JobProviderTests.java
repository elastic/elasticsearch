/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobProviderTests extends ESTestCase {
    private static final String CLUSTER_NAME = "myCluster";
    private static final String JOB_ID = "foo";

    public void testGetQuantiles_GivenNoQuantilesForJob() throws Exception {
        GetResponse getResponse = createGetResponse(false, null);

        Client client = getMockedClient(getResponse);
        JobProvider provider = createProvider(client);

        Quantiles[] holder = new Quantiles[1];
        provider.getQuantiles(JOB_ID, quantiles -> holder[0] = quantiles, RuntimeException::new);
        Quantiles quantiles = holder[0];
        assertNull(quantiles);
    }

    public void testGetQuantiles_GivenQuantilesHaveNonEmptyState() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(Job.ID.getPreferredName(), "foo");
        source.put(Quantiles.TIMESTAMP.getPreferredName(), 0L);
        source.put(Quantiles.QUANTILE_STATE.getPreferredName(), "state");
        GetResponse getResponse = createGetResponse(true, source);

        Client client = getMockedClient(getResponse);
        JobProvider provider = createProvider(client);

        Quantiles[] holder = new Quantiles[1];
        provider.getQuantiles(JOB_ID, quantiles -> holder[0] = quantiles, RuntimeException::new);
        Quantiles quantiles = holder[0];
        assertNotNull(quantiles);
        assertEquals("state", quantiles.getQuantileState());
    }

    public void testGetQuantiles_GivenQuantilesHaveEmptyState() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(Job.ID.getPreferredName(), "foo");
        source.put(Quantiles.TIMESTAMP.getPreferredName(), new Date(0L).getTime());
        source.put(Quantiles.QUANTILE_STATE.getPreferredName(), "");
        GetResponse getResponse = createGetResponse(true, source);

        Client client = getMockedClient(getResponse);
        JobProvider provider = createProvider(client);

        Quantiles[] holder = new Quantiles[1];
        provider.getQuantiles(JOB_ID, quantiles -> holder[0] = quantiles, RuntimeException::new);
        Quantiles quantiles = holder[0];
        assertNotNull(quantiles);
        assertEquals("", quantiles.getQuantileState());
    }

    @SuppressWarnings("unchecked")
    public void testCreateJobResultsIndex() {
        String resultsIndexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT;
        QueryBuilder jobFilter = QueryBuilders.termQuery("job_id", "foo");

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME);
        ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        clientBuilder.createIndexRequest(resultsIndexName, captor);
        clientBuilder.prepareAlias(resultsIndexName, AnomalyDetectorsIndex.jobResultsAliasedName("foo"), jobFilter);

        Job.Builder job = buildJobBuilder("foo");
        JobProvider provider = createProvider(clientBuilder.build());
        AtomicReference<Boolean> resultHolder = new AtomicReference<>();

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA).indices(ImmutableOpenMap.of())).build();

        ClusterService clusterService = mock(ClusterService.class);

        doAnswer(invocationOnMock -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocationOnMock.getArguments()[1];
            task.execute(cs);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("put-job-foo"), any(AckedClusterStateUpdateTask.class));

        provider.createJobResultIndex(job.build(), cs, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                CreateIndexRequest request = captor.getValue();
                assertNotNull(request);
                assertEquals(resultsIndexName, request.index());
                clientBuilder.verifyIndexCreated(resultsIndexName);
                resultHolder.set(aBoolean);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        });

        assertNotNull(resultHolder.get());
        assertTrue(resultHolder.get());
    }

    @SuppressWarnings("unchecked")
    public void testCreateJobWithExistingIndex() {
        QueryBuilder jobFilter = QueryBuilders.termQuery("job_id", "foo");
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME);
        clientBuilder.prepareAlias(AnomalyDetectorsIndex.jobResultsAliasedName("foo"),
                AnomalyDetectorsIndex.jobResultsAliasedName("foo123"), jobFilter);
        clientBuilder.preparePutMapping(mock(PutMappingResponse.class), Result.TYPE.getPreferredName());

        GetMappingsResponse getMappingsResponse = mock(GetMappingsResponse.class);
        ImmutableOpenMap<String, MappingMetaData> typeMappings = ImmutableOpenMap.<String, MappingMetaData>of();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
                ImmutableOpenMap.<String, ImmutableOpenMap<String, MappingMetaData>>builder()
                        .fPut(AnomalyDetectorsIndex.jobResultsAliasedName("foo"), typeMappings).build();
        when(getMappingsResponse.mappings()).thenReturn(mappings);
        clientBuilder.prepareGetMapping(getMappingsResponse);

        Job.Builder job = buildJobBuilder("foo123");
        job.setResultsIndexName("foo");
        JobProvider provider = createProvider(clientBuilder.build());

        Index index = mock(Index.class);
        when(index.getName()).thenReturn(AnomalyDetectorsIndex.jobResultsAliasedName("foo"));
        IndexMetaData indexMetaData = mock(IndexMetaData.class);
        when(indexMetaData.getIndex()).thenReturn(index);

        ImmutableOpenMap<String, AliasMetaData> aliases = ImmutableOpenMap.of();
        when(indexMetaData.getAliases()).thenReturn(aliases);

        ImmutableOpenMap<String, IndexMetaData> indexMap = ImmutableOpenMap.<String, IndexMetaData>builder()
                .fPut(AnomalyDetectorsIndex.jobResultsAliasedName("foo"), indexMetaData).build();

        ClusterState cs2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA).indices(indexMap)).build();

        ClusterService clusterService = mock(ClusterService.class);

        doAnswer(invocationOnMock -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocationOnMock.getArguments()[1];
            task.execute(cs2);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("put-job-foo123"), any(AckedClusterStateUpdateTask.class));

        doAnswer(invocationOnMock -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocationOnMock.getArguments()[1];
            task.execute(cs2);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("index-aliases"), any(AckedClusterStateUpdateTask.class));

        provider.createJobResultIndex(job.build(), cs2, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                assertTrue(aBoolean);
                verify(clientBuilder.build().admin().indices(), times(1)).preparePutMapping(any());
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void testCreateJobRelatedIndicies_createsAliasBecauseIndexNameIsSet() {
        String indexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-bar";
        String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName("foo");
        QueryBuilder jobFilter = QueryBuilders.termQuery("job_id", "foo");

        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME);
        ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        clientBuilder.createIndexRequest(indexName, captor);
        clientBuilder.prepareAlias(indexName, aliasName, jobFilter);
        clientBuilder.preparePutMapping(mock(PutMappingResponse.class), Result.TYPE.getPreferredName());

        Job.Builder job = buildJobBuilder("foo");
        job.setResultsIndexName("bar");
        Client client = clientBuilder.build();
        JobProvider provider = createProvider(client);

        ImmutableOpenMap<String, IndexMetaData> indexMap = ImmutableOpenMap.<String, IndexMetaData>builder().build();

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA).indices(indexMap)).build();

        ClusterService clusterService = mock(ClusterService.class);

        doAnswer(invocationOnMock -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocationOnMock.getArguments()[1];
            task.execute(cs);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("put-job-foo"), any(AckedClusterStateUpdateTask.class));

        provider.createJobResultIndex(job.build(), cs, new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                verify(client.admin().indices(), times(1)).prepareAliases();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        });
    }

    public void testDeleteJobRelatedIndices() throws InterruptedException, ExecutionException, IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteJobAction.Response> actionListener = mock(ActionListener.class);
        String jobId = "ThisIsMyJob";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse();
        Client client = clientBuilder.build();
        JobProvider provider = createProvider(client);
        clientBuilder.resetIndices();
        clientBuilder.addIndicesExistsResponse(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), true)
                .addIndicesDeleteResponse(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), true,
                false, actionListener);
        clientBuilder.build();

        provider.deleteJobRelatedIndices(jobId, actionListener);

        ArgumentCaptor<DeleteJobAction.Response> responseCaptor = ArgumentCaptor.forClass(DeleteJobAction.Response.class);
        verify(actionListener).onResponse(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().isAcknowledged());
    }

    public void testDeleteJobRelatedIndices_InvalidIndex() throws InterruptedException, ExecutionException, IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteJobAction.Response> actionListener = mock(ActionListener.class);
        String jobId = "ThisIsMyJob";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME).addClusterStatusYellowResponse();
        Client client = clientBuilder.build();
        JobProvider provider = createProvider(client);
        clientBuilder.resetIndices();
        clientBuilder.addIndicesExistsResponse(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), true)
                .addIndicesDeleteResponse(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), true,
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
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        int from = 0;
        int size = 10;
        Client client = getMockedClient(queryBuilder -> {queryBuilderHolder[0] = queryBuilder;}, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(0.0)
                .normalizedProbabilityThreshold(1.0);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Bucket>[] holder = new QueryPage[1];
        provider.buckets(jobId, bq.build(), r -> holder[0] = r, e -> {throw new RuntimeException(e);});
        QueryPage<Bucket> buckets = holder[0];
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(
                queryString.matches("(?s).*max_normalized_probability[^}]*from. : 1\\.0.*must_not[^}]*term[^}]*is_interim.*value. : .true" +
                        ".*"));
    }

    public void testBuckets_OneBucketInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        int from = 99;
        int size = 17;

        Client client = getMockedClient(queryBuilder -> queryBuilderHolder[0] = queryBuilder, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(5.1)
                .normalizedProbabilityThreshold(10.9).includeInterim(true);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Bucket>[] holder = new QueryPage[1];
        provider.buckets(jobId, bq.build(), r -> holder[0] = r, e -> {throw new RuntimeException(e);});
        QueryPage<Bucket> buckets = holder[0];
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*max_normalized_probability[^}]*from. : 10\\.9.*"));
        assertTrue(queryString.matches("(?s).*anomaly_score[^}]*from. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*is_interim.*"));
    }

    public void testBuckets_UsingBuilder()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        int from = 99;
        int size = 17;

        Client client = getMockedClient(queryBuilder -> queryBuilderHolder[0] = queryBuilder, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.from(from);
        bq.size(size);
        bq.anomalyScoreThreshold(5.1);
        bq.normalizedProbabilityThreshold(10.9);
        bq.includeInterim(true);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Bucket>[] holder = new QueryPage[1];
        provider.buckets(jobId, bq.build(), r -> holder[0] = r, e -> {throw new RuntimeException(e);});
        QueryPage<Bucket> buckets = holder[0];
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*max_normalized_probability[^}]*from. : 10\\.9.*"));
        assertTrue(queryString.matches("(?s).*anomaly_score[^}]*from. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*is_interim.*"));
    }

    public void testBucket_NoBucketNoExpandNoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Long timestamp = 98765432123456789L;
        List<Map<String, Object>> source = new ArrayList<>();

        SearchResponse response = createSearchResponse(false, source);

        Client client = getMockedClient(queryBuilder -> {}, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.timestamp(Long.toString(timestamp));
        Exception[] holder = new Exception[1];
        provider.buckets(jobId, bq.build(), q -> {}, e -> {holder[0] = e;});
        assertEquals(ResourceNotFoundException.class, holder[0].getClass());
    }

    public void testBucket_OneBucketNoExpandNoInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(queryBuilder -> {}, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.timestamp(Long.toString(now.getTime()));

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Bucket>[] bucketHolder = new QueryPage[1];
        provider.buckets(jobId, bq.build(), q -> {bucketHolder[0] = q;}, e -> {});
        assertThat(bucketHolder[0].count(), equalTo(1L));
        Bucket b = bucketHolder[0].results().get(0);
        assertEquals(now, b.getTimestamp());
    }

    public void testBucket_OneBucketNoExpandInterim()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        map.put("is_interim", true);
        source.add(map);

        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(queryBuilder -> {}, response);
        JobProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.timestamp(Long.toString(now.getTime()));

        Exception[] holder = new Exception[1];
        provider.buckets(jobId, bq.build(), q -> {}, e -> {holder[0] = e;});
        assertEquals(ResourceNotFoundException.class, holder[0].getClass());
    }

    public void testRecords() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("job_id", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucket_span", 22);
        recordMap1.put("sequence_num", 1);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        recordMap2.put("sequence_num", 2);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder().from(from).size(size).epochStart(String.valueOf(now.getTime()))
                .epochEnd(String.valueOf(now.getTime())).includeInterim(true).sortField(sortfield).anomalyScoreThreshold(11.1)
                .normalizedProbability(2.2);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<AnomalyRecord>[] holder = new QueryPage[1];
        provider.records(jobId, rqb.build(), page -> holder[0] = page, RuntimeException::new);
        QueryPage<AnomalyRecord> recordPage = holder[0];
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
        recordMap1.put("job_id", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucket_span", 22);
        recordMap1.put("sequence_num", 1);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        recordMap2.put("sequence_num", 2);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(true, source);

        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder();
        rqb.from(from);
        rqb.size(size);
        rqb.epochStart(String.valueOf(now.getTime()));
        rqb.epochEnd(String.valueOf(now.getTime()));
        rqb.includeInterim(true);
        rqb.sortField(sortfield);
        rqb.anomalyScoreThreshold(11.1);
        rqb.normalizedProbability(2.2);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<AnomalyRecord>[] holder = new QueryPage[1];
        provider.records(jobId, rqb.build(), page -> holder[0] = page, RuntimeException::new);
        QueryPage<AnomalyRecord> recordPage = holder[0];
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
        recordMap1.put("job_id", "foo");
        recordMap1.put("typical", 22.4);
        recordMap1.put("actual", 33.3);
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("function", "irritable");
        recordMap1.put("bucket_span", 22);
        recordMap1.put("sequence_num", 1);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        recordMap2.put("sequence_num", 2);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<AnomalyRecord>[] holder = new QueryPage[1];
        provider.bucketRecords(jobId, bucket, from, size, true, sortfield, true, "", page -> holder[0] = page, RuntimeException::new);
        QueryPage<AnomalyRecord> recordPage = holder[0];
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
        Bucket bucket = new Bucket("foo", now, 22);

        List<Map<String, Object>> source = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("job_id", "foo");
            recordMap.put("typical", 22.4 + i);
            recordMap.put("actual", 33.3 + i);
            recordMap.put("timestamp", now.getTime());
            recordMap.put("function", "irritable");
            recordMap.put("bucket_span", 22);
            recordMap.put("sequence_num", i + 1);
            source.add(recordMap);
        }

        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        Integer[] holder = new Integer[1];
        provider.expandBucket(jobId, false, bucket, null, 0, records -> holder[0] = records, RuntimeException::new);
        int records = holder[0];
        assertEquals(400L, records);
    }

    public void testexpandBucket_WithManyRecords()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        Bucket bucket = new Bucket("foo", now, 22);

        List<Map<String, Object>> source = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            Map<String, Object> recordMap = new HashMap<>();
            recordMap.put("job_id", "foo");
            recordMap.put("typical", 22.4 + i);
            recordMap.put("actual", 33.3 + i);
            recordMap.put("timestamp", now.getTime());
            recordMap.put("function", "irritable");
            recordMap.put("bucket_span", 22);
            recordMap.put("sequence_num", i + 1);
            source.add(recordMap);
        }

        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        Integer[] holder = new Integer[1];
        provider.expandBucket(jobId, false, bucket, null, 0, records -> holder[0] = records, RuntimeException::new);
        int records = holder[0];

        // This is not realistic, but is an artifact of the fact that the mock
        // query returns all the records, not a subset
        assertEquals(1200L, records);
    }

    public void testCategoryDefinitions()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("category_id", String.valueOf(map.hashCode()));
        map.put("terms", terms);

        source.add(map);

        SearchResponse response = createSearchResponse(true, source);
        int from = 0;
        int size = 10;
        Client client = getMockedClient(q -> {}, response);

        JobProvider provider = createProvider(client);
        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<CategoryDefinition>[] holder = new QueryPage[1];
        provider.categoryDefinitions(jobId, null, from, size, r -> {holder[0] = r;},
                e -> {throw new RuntimeException(e);});
        QueryPage<CategoryDefinition> categoryDefinitions = holder[0];
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testCategoryDefinition()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";

        Map<String, Object> source = new HashMap<>();
        String categoryId = String.valueOf(source.hashCode());
        source.put("job_id", "foo");
        source.put("category_id", categoryId);
        source.put("terms", terms);

        SearchResponse response = createSearchResponse(true, Collections.singletonList(source));
        Client client = getMockedClient(q -> {}, response);
        JobProvider provider = createProvider(client);
        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<CategoryDefinition>[] holder = new QueryPage[1];
        provider.categoryDefinitions(jobId, categoryId, null, null,
                r -> {holder[0] = r;}, e -> {throw new RuntimeException(e);});
        QueryPage<CategoryDefinition> categoryDefinitions = holder[0];
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testInfluencers_NoInterim() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("job_id", "foo");
        recordMap1.put("probability", 0.555);
        recordMap1.put("influencer_field_name", "Builder");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("influencer_field_value", "Bob");
        recordMap1.put("initial_anomaly_score", 22.2);
        recordMap1.put("anomaly_score", 22.6);
        recordMap1.put("bucket_span", 123);
        recordMap1.put("sequence_num", 1);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("probability", 0.99);
        recordMap2.put("influencer_field_name", "Builder");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("influencer_field_value", "James");
        recordMap2.put("initial_anomaly_score", 5.0);
        recordMap2.put("anomaly_score", 5.0);
        recordMap2.put("bucket_span", 123);
        recordMap2.put("sequence_num", 2);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        QueryBuilder[] qbHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(q -> qbHolder[0] = q, response);
        JobProvider provider = createProvider(client);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Influencer>[] holder = new QueryPage[1];
        InfluencersQuery query = new InfluencersQueryBuilder().from(from).size(size).includeInterim(false).build();
        provider.influencers(jobId, query, page -> holder[0] = page, RuntimeException::new);
        QueryPage<Influencer> page = holder[0];
        assertEquals(2L, page.count());

        String queryString = qbHolder[0].toString();
        assertTrue(queryString.matches("(?s).*must_not[^}]*term[^}]*is_interim.*value. : .true.*"));

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

    public void testInfluencers_WithInterim() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("job_id", "foo");
        recordMap1.put("probability", 0.555);
        recordMap1.put("influencer_field_name", "Builder");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("influencer_field_value", "Bob");
        recordMap1.put("initial_anomaly_score", 22.2);
        recordMap1.put("anomaly_score", 22.6);
        recordMap1.put("bucket_span", 123);
        recordMap1.put("sequence_num", 1);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("probability", 0.99);
        recordMap2.put("influencer_field_name", "Builder");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("influencer_field_value", "James");
        recordMap2.put("initial_anomaly_score", 5.0);
        recordMap2.put("anomaly_score", 5.0);
        recordMap2.put("bucket_span", 123);
        recordMap2.put("sequence_num", 2);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        QueryBuilder[] qbHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(q -> qbHolder[0] = q, response);
        JobProvider provider = createProvider(client);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<Influencer>[] holder = new QueryPage[1];
        InfluencersQuery query = new InfluencersQueryBuilder().from(from).size(size).start("0").end("0").sortField("sort")
                .sortDescending(true).anomalyScoreThreshold(0.0).includeInterim(true).build();
        provider.influencers(jobId, query, page -> holder[0] = page, RuntimeException::new);
        QueryPage<Influencer> page = holder[0];
        assertEquals(2L, page.count());

        String queryString = qbHolder[0].toString();
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

    public void testModelSnapshots() throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("job_id", "foo");
        recordMap1.put("description", "snapshot1");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("snapshot_doc_count", 5);
        recordMap1.put("latest_record_time_stamp", now.getTime());
        recordMap1.put("latest_result_time_stamp", now.getTime());
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("description", "snapshot2");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("snapshot_doc_count", 6);
        recordMap2.put("latest_record_time_stamp", now.getTime());
        recordMap2.put("latest_result_time_stamp", now.getTime());
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> {}, response);
        JobProvider provider = createProvider(client);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<ModelSnapshot>[] holder = new QueryPage[1];
        provider.modelSnapshots(jobId, from, size, r -> holder[0] = r, RuntimeException::new);
        QueryPage<ModelSnapshot> page = holder[0];
        assertEquals(2L, page.count());
        List<ModelSnapshot> snapshots = page.results();

        assertEquals("foo", snapshots.get(0).getJobId());
        assertEquals(now, snapshots.get(0).getTimestamp());
        assertEquals(now, snapshots.get(0).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(0).getLatestResultTimeStamp());
        assertEquals("snapshot1", snapshots.get(0).getDescription());
        assertEquals(5, snapshots.get(0).getSnapshotDocCount());

        assertEquals(now, snapshots.get(1).getTimestamp());
        assertEquals(now, snapshots.get(1).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(1).getLatestResultTimeStamp());
        assertEquals("snapshot2", snapshots.get(1).getDescription());
        assertEquals(6, snapshots.get(1).getSnapshotDocCount());
    }

    public void testModelSnapshots_WithDescription()
            throws InterruptedException, ExecutionException, IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> recordMap1 = new HashMap<>();
        recordMap1.put("job_id", "foo");
        recordMap1.put("description", "snapshot1");
        recordMap1.put("timestamp", now.getTime());
        recordMap1.put("snapshot_doc_count", 5);
        recordMap1.put("latest_record_time_stamp", now.getTime());
        recordMap1.put("latest_result_time_stamp", now.getTime());
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("description", "snapshot2");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("snapshot_doc_count", 6);
        recordMap2.put("latest_record_time_stamp", now.getTime());
        recordMap2.put("latest_result_time_stamp", now.getTime());
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        QueryBuilder[] qbHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(true, source);
        Client client = getMockedClient(qb -> qbHolder[0] = qb, response);
        JobProvider provider = createProvider(client);

        @SuppressWarnings({"unchecked", "rawtypes"})
        QueryPage<ModelSnapshot>[] hodor = new QueryPage[1];
        provider.modelSnapshots(jobId, from, size, null, null, "sortfield", true, "snappyId", "description1",
                p -> hodor[0] = p, RuntimeException::new);
        QueryPage<ModelSnapshot> page = hodor[0];
        assertEquals(2L, page.count());
        List<ModelSnapshot> snapshots = page.results();

        assertEquals(now, snapshots.get(0).getTimestamp());
        assertEquals(now, snapshots.get(0).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(0).getLatestResultTimeStamp());
        assertEquals("snapshot1", snapshots.get(0).getDescription());
        assertEquals(5, snapshots.get(0).getSnapshotDocCount());

        assertEquals(now, snapshots.get(1).getTimestamp());
        assertEquals(now, snapshots.get(1).getLatestRecordTimeStamp());
        assertEquals(now, snapshots.get(1).getLatestResultTimeStamp());
        assertEquals("snapshot2", snapshots.get(1).getDescription());
        assertEquals(6, snapshots.get(1).getSnapshotDocCount());

        String queryString = qbHolder[0].toString();
        assertTrue(queryString.matches("(?s).*snapshot_id.*value. : .snappyId.*description.*value. : .description1.*"));
    }

    public void testMergePartitionScoresIntoBucket() throws InterruptedException, ExecutionException {
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME);

        JobProvider provider = createProvider(clientBuilder.build());

        List<PerPartitionMaxProbabilities> partitionMaxProbs = new ArrayList<>();

        List<AnomalyRecord> records = new ArrayList<>();
        records.add(createAnomalyRecord("partitionValue1", new Date(2), 1.0));
        records.add(createAnomalyRecord("partitionValue2", new Date(2), 4.0));
        partitionMaxProbs.add(new PerPartitionMaxProbabilities(records));

        records.clear();
        records.add(createAnomalyRecord("partitionValue1", new Date(3), 2.0));
        records.add(createAnomalyRecord("partitionValue2", new Date(3), 1.0));
        partitionMaxProbs.add(new PerPartitionMaxProbabilities(records));

        records.clear();
        records.add(createAnomalyRecord("partitionValue1", new Date(5), 3.0));
        records.add(createAnomalyRecord("partitionValue2", new Date(5), 2.0));
        partitionMaxProbs.add(new PerPartitionMaxProbabilities(records));

        List<Bucket> buckets = new ArrayList<>();
        buckets.add(createBucketAtEpochTime(1));
        buckets.add(createBucketAtEpochTime(2));
        buckets.add(createBucketAtEpochTime(3));
        buckets.add(createBucketAtEpochTime(4));
        buckets.add(createBucketAtEpochTime(5));
        buckets.add(createBucketAtEpochTime(6));

        provider.mergePartitionScoresIntoBucket(partitionMaxProbs, buckets, "partitionValue1");
        assertEquals(0.0, buckets.get(0).getMaxNormalizedProbability(), 0.001);
        assertEquals(1.0, buckets.get(1).getMaxNormalizedProbability(), 0.001);
        assertEquals(2.0, buckets.get(2).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(3).getMaxNormalizedProbability(), 0.001);
        assertEquals(3.0, buckets.get(4).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(5).getMaxNormalizedProbability(), 0.001);

        provider.mergePartitionScoresIntoBucket(partitionMaxProbs, buckets, "partitionValue2");
        assertEquals(0.0, buckets.get(0).getMaxNormalizedProbability(), 0.001);
        assertEquals(4.0, buckets.get(1).getMaxNormalizedProbability(), 0.001);
        assertEquals(1.0, buckets.get(2).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(3).getMaxNormalizedProbability(), 0.001);
        assertEquals(2.0, buckets.get(4).getMaxNormalizedProbability(), 0.001);
        assertEquals(0.0, buckets.get(5).getMaxNormalizedProbability(), 0.001);
    }

    private AnomalyRecord createAnomalyRecord(String partitionFieldValue, Date timestamp, double normalizedProbability) {
        AnomalyRecord record = new AnomalyRecord("foo", timestamp, 600, 42);
        record.setPartitionFieldValue(partitionFieldValue);
        record.setNormalizedProbability(normalizedProbability);
        return record;
    }

    public void testMergePartitionScoresIntoBucket_WithEmptyScoresList() throws InterruptedException, ExecutionException {
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME);

        JobProvider provider = createProvider(clientBuilder.build());

        List<PerPartitionMaxProbabilities> scores = new ArrayList<>();

        List<Bucket> buckets = new ArrayList<>();
        buckets.add(createBucketAtEpochTime(1));
        buckets.add(createBucketAtEpochTime(2));
        buckets.add(createBucketAtEpochTime(3));
        buckets.add(createBucketAtEpochTime(4));

        provider.mergePartitionScoresIntoBucket(scores, buckets, "partitionValue");
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
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), CategorizerState.TYPE, JOB_ID + "_1", categorizerStateGetResponse1)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), CategorizerState.TYPE, JOB_ID + "_2", categorizerStateGetResponse2)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ModelState.TYPE.getPreferredName(), JOB_ID + "-123_1",
                        modelStateGetResponse1)
                .prepareGet(AnomalyDetectorsIndex.jobStateIndexName(), ModelState.TYPE.getPreferredName(), JOB_ID + "-123_2",
                        modelStateGetResponse2);

        JobProvider provider = createProvider(clientBuilder.build());

        ModelSnapshot modelSnapshot = new ModelSnapshot(JOB_ID);
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

    public void testViolatedFieldCountLimit() throws Exception {
        Map<String, Object> mapping = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            mapping.put("field" + i, Collections.singletonMap("type", "string"));
        }

        IndexMetaData.Builder indexMetaData1 = new IndexMetaData.Builder("index1")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                .putMapping(new MappingMetaData("type1", Collections.singletonMap("properties", mapping)));
        MetaData metaData = MetaData.builder()
                .put(indexMetaData1)
                .build();
        boolean result = JobProvider.violatedFieldCountLimit("index1", 0, 10,
                ClusterState.builder(new ClusterName("_name")).metaData(metaData).build());
        assertFalse(result);

        result = JobProvider.violatedFieldCountLimit("index1", 1, 10,
                ClusterState.builder(new ClusterName("_name")).metaData(metaData).build());
        assertTrue(result);

        IndexMetaData.Builder indexMetaData2 = new IndexMetaData.Builder("index1")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                .putMapping(new MappingMetaData("type1", Collections.singletonMap("properties", mapping)))
                .putMapping(new MappingMetaData("type2", Collections.singletonMap("properties", mapping)));
        metaData = MetaData.builder()
                .put(indexMetaData2)
                .build();
        result = JobProvider.violatedFieldCountLimit("index1", 0, 19,
                ClusterState.builder(new ClusterName("_name")).metaData(metaData).build());
        assertTrue(result);
    }

    public void testCountFields() {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("field1", Collections.singletonMap("type", "string"));
        mapping.put("field2", Collections.singletonMap("type", "string"));
        mapping.put("field3", Collections.singletonMap("type", "string"));
        assertEquals(3, JobProvider.countFields(Collections.singletonMap("properties", mapping)));

        Map<String, Object> objectProperties = new HashMap<>();
        objectProperties.put("field4", Collections.singletonMap("type", "string"));
        objectProperties.put("field5", Collections.singletonMap("type", "string"));
        objectProperties.put("field6", Collections.singletonMap("type", "string"));
        Map<String, Object> objectField = new HashMap<>();
        objectField.put("type", "object");
        objectField.put("properties", objectProperties);

        mapping.put("field4", objectField);
        assertEquals(7, JobProvider.countFields(Collections.singletonMap("properties", mapping)));
    }

    private Bucket createBucketAtEpochTime(long epoch) {
        Bucket b = new Bucket("foo", new Date(epoch), 123);
        b.setMaxNormalizedProbability(10.0);
        return b;
    }

    private JobProvider createProvider(Client client) {
        return new JobProvider(client, Settings.EMPTY);
    }

    private static GetResponse createGetResponse(boolean exists, Map<String, Object> source) throws IOException {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(exists);
        when(getResponse.getSourceAsBytesRef()).thenReturn(XContentFactory.jsonBuilder().map(source).bytes());
        return getResponse;
    }

    private static SearchResponse createSearchResponse(boolean exists, List<Map<String, Object>> source) throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        List<SearchHit> list = new ArrayList<>();

        for (Map<String, Object> map : source) {
            Map<String, Object> _source = new HashMap<>(map);

            Map<String, SearchHitField> fields = new HashMap<>();
            fields.put("field_1", new SearchHitField("field_1", Arrays.asList("foo")));
            fields.put("field_2", new SearchHitField("field_2", Arrays.asList("foo")));

            SearchHit hit = new SearchHit(123, String.valueOf(map.hashCode()), new Text("foo"), fields)
                    .sourceRef(XContentFactory.jsonBuilder().map(_source).bytes());

            list.add(hit);
        }
        SearchHits hits = new SearchHits(list.toArray(new SearchHit[0]), source.size(), 1);
        when(response.getHits()).thenReturn(hits);

        return response;
    }

    private Client getMockedClient(Consumer<QueryBuilder> queryBuilderConsumer, SearchResponse response) {
        Client client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocationOnMock.getArguments()[0];
            queryBuilderConsumer.accept(multiSearchRequest.requests().get(0).source().query());
            @SuppressWarnings("unchecked")
            ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) invocationOnMock.getArguments()[1];
            MultiSearchResponse mresponse =
                    new MultiSearchResponse(new MultiSearchResponse.Item[]{new MultiSearchResponse.Item(response, null)});
            actionListener.onResponse(mresponse);
            return null;
        }).when(client).multiSearch(any(), any());
        doAnswer(invocationOnMock -> {
            SearchRequest searchRequest = (SearchRequest) invocationOnMock.getArguments()[0];
            queryBuilderConsumer.accept(searchRequest.source().query());
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> actionListener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
            actionListener.onResponse(response);
            return null;
        }).when(client).search(any(), any());
        return client;
    }

    private Client getMockedClient(GetResponse response) {
        Client client = mock(Client.class);
        @SuppressWarnings("unchecked")
        ActionFuture<GetResponse> actionFuture = mock(ActionFuture.class);
        when(client.get(any())).thenReturn(actionFuture);
        when(actionFuture.actionGet()).thenReturn(response);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> actionListener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            actionListener.onResponse(response);
            return null;
        }).when(client).get(any(), any());
        return client;
    }
}
