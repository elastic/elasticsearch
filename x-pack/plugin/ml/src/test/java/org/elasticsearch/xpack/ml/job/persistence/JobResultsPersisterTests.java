/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class JobResultsPersisterTests extends ESTestCase {

    private static final String JOB_ID = "foo";

    private Client client;
    private OriginSettingClient originSettingClient;
    private ArgumentCaptor<BulkRequest> bulkRequestCaptor;
    private JobResultsPersister persister;

    @Before
    public void setUpTests() {
        bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        client = mock(Client.class);
        doAnswer(withResponse(mock(BulkResponse.class))).when(client).execute(eq(BulkAction.INSTANCE), any(), any());
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        persister = new JobResultsPersister(originSettingClient, buildResultsPersisterService(originSettingClient), makeAuditor());
    }

    public void testPersistBucket_OneRecord() {
        Bucket bucket = new Bucket("foo", new Date(), 123456);
        bucket.setAnomalyScore(99.9);
        bucket.setEventCount(57);
        bucket.setInitialAnomalyScore(88.8);
        bucket.setProcessingTimeMs(8888);

        BucketInfluencer bi = new BucketInfluencer(JOB_ID, new Date(), 600);
        bi.setAnomalyScore(14.15);
        bi.setInfluencerFieldName("biOne");
        bi.setInitialAnomalyScore(18.12);
        bi.setProbability(0.0054);
        bi.setRawAnomalyScore(19.19);
        bucket.addBucketInfluencer(bi);

        // We are adding a record but it shouldn't be persisted as part of the bucket
        AnomalyRecord record = new AnomalyRecord(JOB_ID, new Date(), 600);
        bucket.setRecords(Collections.singletonList(record));

        persister.bulkPersisterBuilder(JOB_ID, () -> true).persistBucket(bucket).executeRequest();

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertEquals(2, bulkRequest.numberOfActions());

        String s = ((IndexRequest)bulkRequest.requests().get(0)).source().utf8ToString();
        assertTrue(s.matches(".*anomaly_score.:99\\.9.*"));
        assertTrue(s.matches(".*initial_anomaly_score.:88\\.8.*"));
        assertTrue(s.matches(".*event_count.:57.*"));
        assertTrue(s.matches(".*bucket_span.:123456.*"));
        assertTrue(s.matches(".*processing_time_ms.:8888.*"));
        // There should NOT be any nested records
        assertFalse(s.matches(".*records*"));

        s = ((IndexRequest)bulkRequest.requests().get(1)).source().utf8ToString();
        assertTrue(s.matches(".*probability.:0\\.0054.*"));
        assertTrue(s.matches(".*influencer_field_name.:.biOne.*"));
        assertTrue(s.matches(".*initial_anomaly_score.:18\\.12.*"));
        assertTrue(s.matches(".*anomaly_score.:14\\.15.*"));
        assertTrue(s.matches(".*raw_anomaly_score.:19\\.19.*"));
    }

    public void testPersistRecords() {
        List<AnomalyRecord> records = new ArrayList<>();
        AnomalyRecord r1 = new AnomalyRecord(JOB_ID, new Date(), 42);
        records.add(r1);
        List<Double> actuals = new ArrayList<>();
        actuals.add(5.0);
        actuals.add(5.1);
        r1.setActual(actuals);
        r1.setByFieldName("byName");
        r1.setByFieldValue("byValue");
        r1.setCorrelatedByFieldValue("testCorrelations");
        r1.setDetectorIndex(3);
        r1.setFieldName("testFieldName");
        r1.setFunction("testFunction");
        r1.setFunctionDescription("testDescription");
        r1.setInitialRecordScore(23.4);
        r1.setRecordScore(0.005);
        r1.setOverFieldName("overName");
        r1.setOverFieldValue("overValue");
        r1.setPartitionFieldName("partName");
        r1.setPartitionFieldValue("partValue");
        r1.setProbability(0.1);
        List<Double> typicals = new ArrayList<>();
        typicals.add(0.44);
        typicals.add(998765.3);
        r1.setTypical(typicals);

        persister.bulkPersisterBuilder(JOB_ID, () -> true).persistRecords(records).executeRequest();

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertEquals(1, bulkRequest.numberOfActions());

        String s = ((IndexRequest) bulkRequest.requests().get(0)).source().utf8ToString();
        assertTrue(s.matches(".*detector_index.:3.*"));
        assertTrue(s.matches(".*\"probability\":0\\.1.*"));
        assertTrue(s.matches(".*\"record_score\":0\\.005.*"));
        assertTrue(s.matches(".*initial_record_score.:23.4.*"));
        assertTrue(s.matches(".*bucket_span.:42.*"));
        assertTrue(s.matches(".*by_field_name.:.byName.*"));
        assertTrue(s.matches(".*by_field_value.:.byValue.*"));
        assertTrue(s.matches(".*correlated_by_field_value.:.testCorrelations.*"));
        assertTrue(s.matches(".*typical.:.0\\.44,998765\\.3.*"));
        assertTrue(s.matches(".*actual.:.5\\.0,5\\.1.*"));
        assertTrue(s.matches(".*field_name.:.testFieldName.*"));
        assertTrue(s.matches(".*function.:.testFunction.*"));
        assertTrue(s.matches(".*function_description.:.testDescription.*"));
        assertTrue(s.matches(".*partition_field_name.:.partName.*"));
        assertTrue(s.matches(".*partition_field_value.:.partValue.*"));
        assertTrue(s.matches(".*over_field_name.:.overName.*"));
        assertTrue(s.matches(".*over_field_value.:.overValue.*"));
    }

    public void testPersistInfluencers() {
        List<Influencer> influencers = new ArrayList<>();
        Influencer inf = new Influencer(JOB_ID, "infName1", "infValue1", new Date(), 600);
        inf.setInfluencerScore(16);
        inf.setInitialInfluencerScore(55.5);
        inf.setProbability(0.4);
        influencers.add(inf);

        persister.bulkPersisterBuilder(JOB_ID, () -> true).persistInfluencers(influencers).executeRequest();

        verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());

        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertEquals(1, bulkRequest.numberOfActions());

        String s = ((IndexRequest) bulkRequest.requests().get(0)).source().utf8ToString();
        assertTrue(s.matches(".*probability.:0\\.4.*"));
        assertTrue(s.matches(".*influencer_field_name.:.infName1.*"));
        assertTrue(s.matches(".*influencer_field_value.:.infValue1.*"));
        assertTrue(s.matches(".*initial_influencer_score.:55\\.5.*"));
        assertTrue(s.matches(".*influencer_score.:16\\.0.*"));
    }

    public void testExecuteRequest_ClearsBulkRequest() {
        List<Influencer> influencers = new ArrayList<>();
        Influencer inf = new Influencer(JOB_ID, "infName1", "infValue1", new Date(), 600);
        inf.setInfluencerScore(16);
        inf.setInitialInfluencerScore(55.5);
        inf.setProbability(0.4);
        influencers.add(inf);

        JobResultsPersister.Builder builder = persister.bulkPersisterBuilder(JOB_ID, () -> true);
        builder.persistInfluencers(influencers).executeRequest();
        assertEquals(0, builder.getBulkRequest().numberOfActions());
    }

    public void testBulkRequestExecutesWhenReachMaxDocs() {
        JobResultsPersister.Builder bulkBuilder = persister.bulkPersisterBuilder("foo", () -> true);
        ModelPlot modelPlot = new ModelPlot("foo", new Date(), 123456, 0);
        for (int i=0; i<=JobRenormalizedResultsPersister.BULK_LIMIT; i++) {
            bulkBuilder.persistModelPlot(modelPlot);
        }

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).settings();
        inOrder.verify(client, times(3)).threadPool();
        inOrder.verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);
    }

    public void testPersistTimingStats() {
        TimingStats timingStats =
            new TimingStats(
                "foo", 7, 1.0, 2.0, 1.23, 7.89, new ExponentialAverageCalculationContext(600.0, Instant.ofEpochMilli(123456789), 60.0));
        persister.bulkPersisterBuilder(JOB_ID, () -> true).persistTimingStats(timingStats).executeRequest();

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).settings();
        inOrder.verify(client, times(3)).threadPool();
        inOrder.verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);

        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertThat(bulkRequest.requests().size(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
        assertThat(indexRequest.index(), equalTo(".ml-anomalies-.write-foo"));
        assertThat(indexRequest.id(), equalTo("foo_timing_stats"));
        assertThat(
            indexRequest.sourceAsMap(),
            equalTo(
                Map.of(
                    "result_type", "timing_stats",
                    "job_id", "foo",
                    "bucket_count", 7,
                    "minimum_bucket_processing_time_ms", 1.0,
                    "maximum_bucket_processing_time_ms", 2.0,
                    "average_bucket_processing_time_ms", 1.23,
                    "exponential_average_bucket_processing_time_ms", 7.89,
                    "exponential_average_calculation_context", Map.of(
                        "incremental_metric_value_ms", 600.0,
                        "previous_exponential_average_ms", 60.0,
                        "latest_timestamp", 123456789))));
    }

    @SuppressWarnings("unchecked")
    public void testPersistDatafeedTimingStats() {
        DatafeedTimingStats timingStats =
            new DatafeedTimingStats(
                "foo", 6, 66, 666.0, new ExponentialAverageCalculationContext(600.0, Instant.ofEpochMilli(123456789), 60.0));
        persister.persistDatafeedTimingStats(timingStats, WriteRequest.RefreshPolicy.IMMEDIATE);

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).settings();
        inOrder.verify(client, times(3)).threadPool();
        inOrder.verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);

        // Refresh policy is set on the bulk request, not the individual index requests
        assertThat(bulkRequestCaptor.getValue().getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
        IndexRequest indexRequest = (IndexRequest) bulkRequestCaptor.getValue().requests().get(0);
        assertThat(indexRequest.index(), equalTo(".ml-anomalies-.write-foo"));
        assertThat(indexRequest.id(), equalTo("foo_datafeed_timing_stats"));
        assertThat(
            indexRequest.sourceAsMap(),
            equalTo(
                Map.of(
                    "result_type", "datafeed_timing_stats",
                    "job_id", "foo",
                    "search_count", 6,
                    "bucket_count", 66,
                    "total_search_time_ms", 666.0,
                    "exponential_average_calculation_context", Map.of(
                        "incremental_metric_value_ms", 600.0,
                        "previous_exponential_average_ms", 60.0,
                        "latest_timestamp", 123456789))));
    }

    @SuppressWarnings("unchecked")
    private void testPersistQuantilesSync(SearchHits searchHits, String expectedIndexOrAlias) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(withResponse(searchResponse)).when(client).execute(eq(SearchAction.INSTANCE), any(), any());

        Quantiles quantiles = new Quantiles("foo", new Date(), "bar");
        persister.persistQuantiles(quantiles, () -> false);

        InOrder inOrder = inOrder(client);
        inOrder.verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(BulkAction.INSTANCE), bulkRequestCaptor.capture(), any());
        inOrder.verifyNoMoreInteractions();

        BulkRequest bulkRequest = bulkRequestCaptor.getValue();
        assertThat(bulkRequest.requests().size(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);

        assertThat(indexRequest.index(), equalTo(expectedIndexOrAlias));
        assertThat(indexRequest.id(), equalTo("foo_quantiles"));
    }

    public void testPersistQuantilesSync_QuantilesDocumentCreated() {
        testPersistQuantilesSync(SearchHits.empty(), ".ml-state-write");
    }

    public void testPersistQuantilesSync_QuantilesDocumentUpdated() {
        testPersistQuantilesSync(
            new SearchHits(new SearchHit[]{ SearchHit.createFromMap(Map.of("_index", ".ml-state-dummy")) }, null, 0.0f),
            ".ml-state-dummy");
    }

    @SuppressWarnings("unchecked")
    private void testPersistQuantilesAsync(SearchHits searchHits, String expectedIndexOrAlias) {
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(withResponse(searchResponse)).when(client).execute(eq(SearchAction.INSTANCE), any(), any());

        IndexResponse indexResponse = mock(IndexResponse.class);
        doAnswer(withResponse(indexResponse)).when(client).execute(eq(IndexAction.INSTANCE), any(), any());

        Quantiles quantiles = new Quantiles("foo", new Date(), "bar");
        ActionListener<IndexResponse> indexResponseListener = mock(ActionListener.class);
        persister.persistQuantiles(quantiles, WriteRequest.RefreshPolicy.IMMEDIATE, indexResponseListener);

        InOrder inOrder = inOrder(client, indexResponseListener);
        inOrder.verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(IndexAction.INSTANCE), indexRequestCaptor.capture(), any());
        inOrder.verify(indexResponseListener).onResponse(any());
        inOrder.verifyNoMoreInteractions();

        IndexRequest indexRequest = indexRequestCaptor.getValue();

        assertThat(indexRequest.index(), equalTo(expectedIndexOrAlias));
        assertThat(indexRequest.id(), equalTo("foo_quantiles"));
    }

    public void testPersistQuantilesAsync_QuantilesDocumentCreated() {
        testPersistQuantilesAsync(SearchHits.empty(), ".ml-state-write");
    }

    public void testPersistQuantilesAsync_QuantilesDocumentUpdated() {
        testPersistQuantilesAsync(
            new SearchHits(new SearchHit[]{ SearchHit.createFromMap(Map.of("_index", ".ml-state-dummy")) }, null, 0.0f),
            ".ml-state-dummy");
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private ResultsPersisterService buildResultsPersisterService(OriginSettingClient client) {
        ThreadPool tp = mock(ThreadPool.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
            new HashSet<>(Arrays.asList(InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                ResultsPersisterService.PERSIST_RESULTS_MAX_RETRIES,
                ClusterService.USER_DEFINED_METADATA,
                ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING)));
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, tp);

        return new ResultsPersisterService(client, clusterService, Settings.EMPTY);
    }

    private AnomalyDetectionAuditor makeAuditor() {
        AnomalyDetectionAuditor anomalyDetectionAuditor = mock(AnomalyDetectionAuditor.class);
        doNothing().when(anomalyDetectionAuditor).warning(any(), any());
        doNothing().when(anomalyDetectionAuditor).info(any(), any());
        doNothing().when(anomalyDetectionAuditor).error(any(), any());
        return anomalyDetectionAuditor;
    }
}
