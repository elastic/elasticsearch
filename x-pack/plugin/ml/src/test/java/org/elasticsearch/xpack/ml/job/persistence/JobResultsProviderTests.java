/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder.InfluencersQuery;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JobResultsProviderTests extends ESTestCase {

    public void testBuckets_OneBucketNoInterim() throws IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(source);
        int from = 0;
        int size = 10;
        Client client = getMockedClient(queryBuilder -> queryBuilderHolder[0] = queryBuilder, response);
        JobResultsProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(1.0);

        SetOnce<QueryPage<Bucket>> holder = new SetOnce<>();
        provider.buckets(jobId, bq, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<Bucket> buckets = holder.get();
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*anomaly_score[^}]*gte. : 1\\.0.*must_not[^}]*term[^}]*is_interim.*value. : true" + ".*"));
    }

    public void testBuckets_OneBucketInterim() throws IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(source);
        int from = 99;
        int size = 17;

        Client client = getMockedClient(queryBuilder -> queryBuilderHolder[0] = queryBuilder, response);
        JobResultsProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder().from(from).size(size).anomalyScoreThreshold(5.1).includeInterim(true);

        SetOnce<QueryPage<Bucket>> holder = new SetOnce<>();
        provider.buckets(jobId, bq, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<Bucket> buckets = holder.get();
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*anomaly_score[^}]*gte. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*is_interim.*"));
    }

    public void testBuckets_UsingBuilder() throws IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        QueryBuilder[] queryBuilderHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(source);
        int from = 99;
        int size = 17;

        Client client = getMockedClient(queryBuilder -> queryBuilderHolder[0] = queryBuilder, response);
        JobResultsProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.from(from);
        bq.size(size);
        bq.anomalyScoreThreshold(5.1);
        bq.includeInterim(true);

        SetOnce<QueryPage<Bucket>> holder = new SetOnce<>();
        provider.buckets(jobId, bq, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<Bucket> buckets = holder.get();
        assertEquals(1L, buckets.count());
        QueryBuilder query = queryBuilderHolder[0];
        String queryString = query.toString();
        assertTrue(queryString.matches("(?s).*anomaly_score[^}]*gte. : 5\\.1.*"));
        assertFalse(queryString.matches("(?s).*is_interim.*"));
    }

    public void testBucket_NoBucketNoExpand() throws IOException {
        String jobId = "TestJobIdentification";
        long timestamp = 98765432123456789L;
        List<Map<String, Object>> source = new ArrayList<>();

        SearchResponse response = createSearchResponse(source);

        Client client = getMockedClient(queryBuilder -> {}, response);
        JobResultsProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.timestamp(Long.toString(timestamp));
        Exception[] holder = new Exception[1];
        provider.buckets(jobId, bq, q -> {}, e -> holder[0] = e, client);
        assertEquals(ResourceNotFoundException.class, holder[0].getClass());
    }

    public void testBucket_OneBucketNoExpand() throws IOException {
        String jobId = "TestJobIdentification";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("timestamp", now.getTime());
        map.put("bucket_span", 22);
        source.add(map);

        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(queryBuilder -> {}, response);
        JobResultsProvider provider = createProvider(client);

        BucketsQueryBuilder bq = new BucketsQueryBuilder();
        bq.timestamp(Long.toString(now.getTime()));

        SetOnce<QueryPage<Bucket>> bucketHolder = new SetOnce<>();
        provider.buckets(jobId, bq, bucketHolder::set, e -> {}, client);
        assertThat(bucketHolder.get().count(), equalTo(1L));
        Bucket b = bucketHolder.get().results().get(0);
        assertEquals(now, b.getTimestamp());
    }

    public void testRecords() throws IOException {
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
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(qb -> {}, response);
        JobResultsProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder().from(from)
            .size(size)
            .epochStart(String.valueOf(now.getTime()))
            .epochEnd(String.valueOf(now.getTime()))
            .includeInterim(true)
            .sortField(sortfield)
            .recordScore(2.2);

        SetOnce<QueryPage<AnomalyRecord>> holder = new SetOnce<>();
        provider.records(jobId, rqb, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<AnomalyRecord> recordPage = holder.get();
        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();
        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testRecords_UsingBuilder() throws IOException {
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
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(source);

        Client client = getMockedClient(qb -> {}, response);
        JobResultsProvider provider = createProvider(client);

        RecordsQueryBuilder rqb = new RecordsQueryBuilder();
        rqb.from(from);
        rqb.size(size);
        rqb.epochStart(String.valueOf(now.getTime()));
        rqb.epochEnd(String.valueOf(now.getTime()));
        rqb.includeInterim(true);
        rqb.sortField(sortfield);
        rqb.recordScore(2.2);

        SetOnce<QueryPage<AnomalyRecord>> holder = new SetOnce<>();
        provider.records(jobId, rqb, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<AnomalyRecord> recordPage = holder.get();
        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();
        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testBucketRecords() throws IOException {
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
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("typical", 1122.4);
        recordMap2.put("actual", 933.3);
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("function", "irrascible");
        recordMap2.put("bucket_span", 22);
        source.add(recordMap1);
        source.add(recordMap2);

        int from = 14;
        int size = 2;
        String sortfield = "minefield";
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(qb -> {}, response);
        JobResultsProvider provider = createProvider(client);

        SetOnce<QueryPage<AnomalyRecord>> holder = new SetOnce<>();
        provider.bucketRecords(
            jobId,
            bucket,
            from,
            size,
            true,
            sortfield,
            true,
            holder::set,
            e -> { throw new RuntimeException(e); },
            client
        );
        QueryPage<AnomalyRecord> recordPage = holder.get();
        assertEquals(2L, recordPage.count());
        List<AnomalyRecord> records = recordPage.results();

        assertEquals(22.4, records.get(0).getTypical().get(0), 0.000001);
        assertEquals(33.3, records.get(0).getActual().get(0), 0.000001);
        assertEquals("irritable", records.get(0).getFunction());
        assertEquals(1122.4, records.get(1).getTypical().get(0), 0.000001);
        assertEquals(933.3, records.get(1).getActual().get(0), 0.000001);
        assertEquals("irrascible", records.get(1).getFunction());
    }

    public void testexpandBucket() throws IOException {
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
            source.add(recordMap);
        }

        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(qb -> {}, response);
        JobResultsProvider provider = createProvider(client);

        Integer[] holder = new Integer[1];
        provider.expandBucket(jobId, false, bucket, records -> holder[0] = records, e -> { throw new RuntimeException(e); }, client);
        int records = holder[0];
        assertEquals(400L, records);
    }

    public void testCategoryDefinitions() throws IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> map = new HashMap<>();
        map.put("job_id", "foo");
        map.put("category_id", String.valueOf(map.hashCode()));
        map.put("terms", terms);

        source.add(map);

        SearchResponse response = createSearchResponse(source);
        int from = 0;
        int size = 10;
        Client client = getMockedClient(q -> {}, response);

        JobResultsProvider provider = createProvider(client);
        SetOnce<QueryPage<CategoryDefinition>> holder = new SetOnce<>();
        provider.categoryDefinitions(
            jobId,
            null,
            null,
            false,
            from,
            size,
            holder::set,
            e -> { throw new RuntimeException(e); },
            null,
            null,
            client
        );
        QueryPage<CategoryDefinition> categoryDefinitions = holder.get();
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testCategoryDefinition() throws IOException {
        String jobId = "TestJobIdentification";
        String terms = "the terms and conditions are not valid here";

        Map<String, Object> source = new HashMap<>();
        long categoryId = source.hashCode();
        source.put("job_id", "foo");
        source.put("category_id", categoryId);
        source.put("terms", terms);

        SearchResponse response = createSearchResponse(Collections.singletonList(source));
        Client client = getMockedClient(q -> {}, response);
        JobResultsProvider provider = createProvider(client);
        SetOnce<QueryPage<CategoryDefinition>> holder = new SetOnce<>();
        provider.categoryDefinitions(
            jobId,
            categoryId,
            null,
            false,
            null,
            null,
            holder::set,
            e -> { throw new RuntimeException(e); },
            null,
            null,
            client
        );
        QueryPage<CategoryDefinition> categoryDefinitions = holder.get();
        assertEquals(1L, categoryDefinitions.count());
        assertEquals(terms, categoryDefinitions.results().get(0).getTerms());
    }

    public void testInfluencers_NoInterim() throws IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> influencerMap1 = new HashMap<>();
        influencerMap1.put("job_id", "foo");
        influencerMap1.put("probability", 0.555);
        influencerMap1.put("influencer_field_name", "Builder");
        influencerMap1.put("timestamp", now.getTime());
        influencerMap1.put("influencer_field_value", "Bob");
        influencerMap1.put("initial_influencer_score", 22.2);
        influencerMap1.put("influencer_score", 22.6);
        influencerMap1.put("bucket_span", 123);
        Map<String, Object> recordMap2 = new HashMap<>();
        recordMap2.put("job_id", "foo");
        recordMap2.put("probability", 0.99);
        recordMap2.put("influencer_field_name", "Builder");
        recordMap2.put("timestamp", now.getTime());
        recordMap2.put("influencer_field_value", "James");
        recordMap2.put("initial_influencer_score", 5.0);
        recordMap2.put("influencer_score", 5.0);
        recordMap2.put("bucket_span", 123);
        source.add(influencerMap1);
        source.add(recordMap2);

        int from = 4;
        int size = 3;
        QueryBuilder[] qbHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(q -> qbHolder[0] = q, response);
        JobResultsProvider provider = createProvider(client);

        SetOnce<QueryPage<Influencer>> holder = new SetOnce<>();
        InfluencersQuery query = new InfluencersQueryBuilder().from(from).size(size).includeInterim(false).build();
        provider.influencers(jobId, query, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<Influencer> page = holder.get();
        assertEquals(2L, page.count());

        String queryString = qbHolder[0].toString();
        assertTrue(queryString.matches("(?s).*must_not[^}]*term[^}]*is_interim.*value. : true.*"));

        List<Influencer> records = page.results();
        assertEquals("foo", records.get(0).getJobId());
        assertEquals("Bob", records.get(0).getInfluencerFieldValue());
        assertEquals("Builder", records.get(0).getInfluencerFieldName());
        assertEquals(now, records.get(0).getTimestamp());
        assertEquals(0.555, records.get(0).getProbability(), 0.00001);
        assertEquals(22.6, records.get(0).getInfluencerScore(), 0.00001);
        assertEquals(22.2, records.get(0).getInitialInfluencerScore(), 0.00001);

        assertEquals("James", records.get(1).getInfluencerFieldValue());
        assertEquals("Builder", records.get(1).getInfluencerFieldName());
        assertEquals(now, records.get(1).getTimestamp());
        assertEquals(0.99, records.get(1).getProbability(), 0.00001);
        assertEquals(5.0, records.get(1).getInfluencerScore(), 0.00001);
        assertEquals(5.0, records.get(1).getInitialInfluencerScore(), 0.00001);
    }

    public void testInfluencers_WithInterim() throws IOException {
        String jobId = "TestJobIdentificationForInfluencers";
        Date now = new Date();
        List<Map<String, Object>> source = new ArrayList<>();

        Map<String, Object> influencerMap1 = new HashMap<>();
        influencerMap1.put("job_id", "foo");
        influencerMap1.put("probability", 0.555);
        influencerMap1.put("influencer_field_name", "Builder");
        influencerMap1.put("timestamp", now.getTime());
        influencerMap1.put("influencer_field_value", "Bob");
        influencerMap1.put("initial_influencer_score", 22.2);
        influencerMap1.put("influencer_score", 22.6);
        influencerMap1.put("bucket_span", 123);
        Map<String, Object> influencerMap2 = new HashMap<>();
        influencerMap2.put("job_id", "foo");
        influencerMap2.put("probability", 0.99);
        influencerMap2.put("influencer_field_name", "Builder");
        influencerMap2.put("timestamp", now.getTime());
        influencerMap2.put("influencer_field_value", "James");
        influencerMap2.put("initial_influencer_score", 5.0);
        influencerMap2.put("influencer_score", 5.0);
        influencerMap2.put("bucket_span", 123);
        source.add(influencerMap1);
        source.add(influencerMap2);

        int from = 4;
        int size = 3;
        QueryBuilder[] qbHolder = new QueryBuilder[1];
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(q -> qbHolder[0] = q, response);
        JobResultsProvider provider = createProvider(client);

        SetOnce<QueryPage<Influencer>> holder = new SetOnce<>();
        InfluencersQuery query = new InfluencersQueryBuilder().from(from)
            .size(size)
            .start("0")
            .end("0")
            .sortField("sort")
            .sortDescending(true)
            .influencerScoreThreshold(0.0)
            .includeInterim(true)
            .build();
        provider.influencers(jobId, query, holder::set, e -> { throw new RuntimeException(e); }, client);
        QueryPage<Influencer> page = holder.get();
        assertEquals(2L, page.count());

        String queryString = qbHolder[0].toString();
        assertFalse(queryString.matches("(?s).*isInterim.*"));

        List<Influencer> records = page.results();
        assertEquals("Bob", records.get(0).getInfluencerFieldValue());
        assertEquals("Builder", records.get(0).getInfluencerFieldName());
        assertEquals(now, records.get(0).getTimestamp());
        assertEquals(0.555, records.get(0).getProbability(), 0.00001);
        assertEquals(22.6, records.get(0).getInfluencerScore(), 0.00001);
        assertEquals(22.2, records.get(0).getInitialInfluencerScore(), 0.00001);

        assertEquals("James", records.get(1).getInfluencerFieldValue());
        assertEquals("Builder", records.get(1).getInfluencerFieldName());
        assertEquals(now, records.get(1).getTimestamp());
        assertEquals(0.99, records.get(1).getProbability(), 0.00001);
        assertEquals(5.0, records.get(1).getInfluencerScore(), 0.00001);
        assertEquals(5.0, records.get(1).getInitialInfluencerScore(), 0.00001);
    }

    public void testModelSnapshots() throws IOException {
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
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(qb -> {}, response);
        JobResultsProvider provider = createProvider(client);

        SetOnce<QueryPage<ModelSnapshot>> holder = new SetOnce<>();
        provider.modelSnapshots(jobId, from, size, holder::set, e -> { throw new RuntimeException(e); });
        QueryPage<ModelSnapshot> page = holder.get();
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

    public void testViolatedFieldCountLimit() {
        Map<String, Object> mapping = new HashMap<>();

        int i = 0;
        for (; i < 10; i++) {
            mapping.put("field" + i, Collections.singletonMap("type", "string"));
        }

        IndexMetadata indexMetadata1 = new IndexMetadata.Builder("index1").settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).putMapping(new MappingMetadata("type1", Collections.singletonMap("properties", mapping))).build();
        boolean result = JobResultsProvider.violatedFieldCountLimit(0, 10, indexMetadata1.mapping());
        assertFalse(result);

        result = JobResultsProvider.violatedFieldCountLimit(1, 10, indexMetadata1.mapping());
        assertTrue(result);

        for (; i < 20; i++) {
            mapping.put("field" + i, Collections.singletonMap("type", "string"));
        }

        IndexMetadata indexMetadata2 = new IndexMetadata.Builder("index1").settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).putMapping(new MappingMetadata("type1", Collections.singletonMap("properties", mapping))).build();

        result = JobResultsProvider.violatedFieldCountLimit(0, 19, indexMetadata2.mapping());
        assertTrue(result);
    }

    public void testCountFields() {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("field1", Collections.singletonMap("type", "string"));
        mapping.put("field2", Collections.singletonMap("type", "string"));
        mapping.put("field3", Collections.singletonMap("type", "string"));
        assertEquals(3, JobResultsProvider.countFields(Collections.singletonMap("properties", mapping)));

        Map<String, Object> objectProperties = new HashMap<>();
        objectProperties.put("field4", Collections.singletonMap("type", "string"));
        objectProperties.put("field5", Collections.singletonMap("type", "string"));
        objectProperties.put("field6", Collections.singletonMap("type", "string"));
        Map<String, Object> objectField = new HashMap<>();
        objectField.put("type", "object");
        objectField.put("properties", objectProperties);

        mapping.put("field4", objectField);
        assertEquals(7, JobResultsProvider.countFields(Collections.singletonMap("properties", mapping)));
    }

    public void testDatafeedTimingStats_EmptyJobList() {
        Client client = getBasicMockedClient();

        JobResultsProvider provider = createProvider(client);
        provider.datafeedTimingStats(
            List.of(),
            null,
            ActionTestUtils.assertNoFailureListener(statsByJobId -> assertThat(statsByJobId, anEmptyMap()))
        );

        verifyNoMoreInteractions(client);
    }

    public void testDatafeedTimingStats_MultipleDocumentsAtOnce() throws IOException {
        List<Map<String, Object>> sourceFoo = Collections.singletonList(
            Map.of(
                Job.ID.getPreferredName(),
                "foo",
                DatafeedTimingStats.SEARCH_COUNT.getPreferredName(),
                6,
                DatafeedTimingStats.BUCKET_COUNT.getPreferredName(),
                66,
                DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName(),
                666.0,
                DatafeedTimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(),
                Map.of(
                    ExponentialAverageCalculationContext.INCREMENTAL_METRIC_VALUE_MS.getPreferredName(),
                    600.0,
                    ExponentialAverageCalculationContext.LATEST_TIMESTAMP.getPreferredName(),
                    Instant.ofEpochMilli(100000600),
                    ExponentialAverageCalculationContext.PREVIOUS_EXPONENTIAL_AVERAGE_MS.getPreferredName(),
                    60.0
                )
            )
        );
        List<Map<String, Object>> sourceBar = Collections.singletonList(
            Map.of(
                Job.ID.getPreferredName(),
                "bar",
                DatafeedTimingStats.SEARCH_COUNT.getPreferredName(),
                7,
                DatafeedTimingStats.BUCKET_COUNT.getPreferredName(),
                77,
                DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName(),
                777.0,
                DatafeedTimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(),
                Map.of(
                    ExponentialAverageCalculationContext.INCREMENTAL_METRIC_VALUE_MS.getPreferredName(),
                    700.0,
                    ExponentialAverageCalculationContext.LATEST_TIMESTAMP.getPreferredName(),
                    Instant.ofEpochMilli(100000700),
                    ExponentialAverageCalculationContext.PREVIOUS_EXPONENTIAL_AVERAGE_MS.getPreferredName(),
                    70.0
                )
            )
        );
        SearchResponse responseFoo = createSearchResponse(sourceFoo);
        SearchResponse responseBar = createSearchResponse(sourceBar);
        final MultiSearchResponse multiSearchResponse = new MultiSearchResponse(
            new MultiSearchResponse.Item[] {
                new MultiSearchResponse.Item(responseFoo, null),
                new MultiSearchResponse.Item(responseBar, null) },
            randomNonNegativeLong()
        );

        try {
            Client client = getBasicMockedClient();
            when(client.prepareMultiSearch()).thenReturn(new MultiSearchRequestBuilder(client));
            doAnswer(invocationOnMock -> {
                MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocationOnMock.getArguments()[0];
                assertThat(multiSearchRequest.requests(), hasSize(2));
                assertThat(multiSearchRequest.requests().get(0).source().query().getName(), equalTo("ids"));
                assertThat(multiSearchRequest.requests().get(1).source().query().getName(), equalTo("ids"));
                @SuppressWarnings("unchecked")
                ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) invocationOnMock
                    .getArguments()[1];
                actionListener.onResponse(multiSearchResponse);
                return null;
            }).when(client).multiSearch(any(), any());
            when(client.prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName("foo"))).thenReturn(
                new SearchRequestBuilder(client).setIndices(AnomalyDetectorsIndex.jobResultsAliasedName("foo"))
            );
            when(client.prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName("bar"))).thenReturn(
                new SearchRequestBuilder(client).setIndices(AnomalyDetectorsIndex.jobResultsAliasedName("bar"))
            );

            JobResultsProvider provider = createProvider(client);
            ExponentialAverageCalculationContext contextFoo = new ExponentialAverageCalculationContext(
                600.0,
                Instant.ofEpochMilli(100000600),
                60.0
            );
            ExponentialAverageCalculationContext contextBar = new ExponentialAverageCalculationContext(
                700.0,
                Instant.ofEpochMilli(100000700),
                70.0
            );
            provider.datafeedTimingStats(
                List.of("foo", "bar"),
                null,
                ActionTestUtils.assertNoFailureListener(
                    statsByJobId -> assertThat(
                        statsByJobId,
                        equalTo(
                            Map.of(
                                "foo",
                                new DatafeedTimingStats("foo", 6, 66, 666.0, contextFoo),
                                "bar",
                                new DatafeedTimingStats("bar", 7, 77, 777.0, contextBar)
                            )
                        )
                    )
                )
            );

            verify(client).threadPool();
            verify(client).prepareMultiSearch();
            verify(client).multiSearch(any(MultiSearchRequest.class), any());
            verify(client).prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName("foo"));
            verify(client).prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName("bar"));
            verifyNoMoreInteractions(client);
        } finally {
            multiSearchResponse.decRef();
        }
    }

    public void testDatafeedTimingStats_Ok() throws IOException {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName("foo");
        List<Map<String, Object>> source = Collections.singletonList(
            Map.of(
                Job.ID.getPreferredName(),
                "foo",
                DatafeedTimingStats.SEARCH_COUNT.getPreferredName(),
                6,
                DatafeedTimingStats.BUCKET_COUNT.getPreferredName(),
                66,
                DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName(),
                666.0,
                DatafeedTimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(),
                Map.of(
                    ExponentialAverageCalculationContext.INCREMENTAL_METRIC_VALUE_MS.getPreferredName(),
                    600.0,
                    ExponentialAverageCalculationContext.LATEST_TIMESTAMP.getPreferredName(),
                    Instant.ofEpochMilli(100000600),
                    ExponentialAverageCalculationContext.PREVIOUS_EXPONENTIAL_AVERAGE_MS.getPreferredName(),
                    60.0
                )
            )
        );
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(queryBuilder -> assertThat(queryBuilder.getName(), equalTo("ids")), response);

        when(client.prepareSearch(indexName)).thenReturn(new SearchRequestBuilder(client).setIndices(indexName));
        JobResultsProvider provider = createProvider(client);
        ExponentialAverageCalculationContext contextFoo = new ExponentialAverageCalculationContext(
            600.0,
            Instant.ofEpochMilli(100000600),
            60.0
        );
        provider.datafeedTimingStats(
            "foo",
            stats -> assertThat(stats, equalTo(new DatafeedTimingStats("foo", 6, 66, 666.0, contextFoo))),
            e -> {
                throw new AssertionError("Failure getting datafeed timing stats", e);
            }
        );

        verify(client).prepareSearch(indexName);
        verify(client).threadPool();
        verify(client).search(any(SearchRequest.class), any());
        verifyNoMoreInteractions(client);
    }

    public void testDatafeedTimingStats_NotFound() throws IOException {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName("foo");
        List<Map<String, Object>> source = new ArrayList<>();
        SearchResponse response = createSearchResponse(source);
        Client client = getMockedClient(queryBuilder -> assertThat(queryBuilder.getName(), equalTo("ids")), response);

        when(client.prepareSearch(indexName)).thenReturn(new SearchRequestBuilder(client).setIndices(indexName));
        JobResultsProvider provider = createProvider(client);
        provider.datafeedTimingStats("foo", stats -> assertThat(stats, equalTo(new DatafeedTimingStats("foo"))), e -> {
            throw new AssertionError("Failure getting datafeed timing stats", e);
        });

        verify(client).prepareSearch(indexName);
        verify(client).threadPool();
        verify(client).search(any(SearchRequest.class), any());
        verifyNoMoreInteractions(client);
    }

    @SuppressWarnings("unchecked")
    public void testCreateTermFieldsMapping() throws IOException {

        XContentBuilder termFieldsMapping = JsonXContent.contentBuilder();
        JobResultsProvider.createTermFieldsMapping(
            termFieldsMapping,
            Arrays.asList("apple", "strawberry", AnomalyRecord.BUCKET_SPAN.getPreferredName())
        );

        XContentParser parser = createParser(termFieldsMapping);
        Map<String, Object> properties = (Map<String, Object>) parser.map().get("properties");

        Map<String, Object> instanceMapping = (Map<String, Object>) properties.get("apple");
        assertNotNull(instanceMapping);
        String dataType = (String) instanceMapping.get("type");
        assertEquals("keyword", dataType);

        instanceMapping = (Map<String, Object>) properties.get("strawberry");
        assertNotNull(instanceMapping);
        dataType = (String) instanceMapping.get("type");
        assertEquals("keyword", dataType);

        // check no mapping for the reserved field
        instanceMapping = (Map<String, Object>) properties.get(AnomalyRecord.BUCKET_SPAN.getPreferredName());
        assertNull(instanceMapping);
    }

    private JobResultsProvider createProvider(Client client) {
        return new JobResultsProvider(client, Settings.EMPTY, TestIndexNameExpressionResolver.newInstance());
    }

    private static SearchResponse createSearchResponse(List<Map<String, Object>> source) throws IOException {
        SearchResponse response = mock(SearchResponse.class);
        List<SearchHit> list = new ArrayList<>();

        for (Map<String, Object> map : source) {
            Map<String, Object> _source = new HashMap<>(map);

            Map<String, DocumentField> fields = new HashMap<>();
            fields.put("field_1", new DocumentField("field_1", Collections.singletonList("foo")));
            fields.put("field_2", new DocumentField("field_2", Collections.singletonList("foo")));

            SearchHit hit = new SearchHit(123, String.valueOf(map.hashCode()));
            hit.addDocumentFields(fields, Collections.emptyMap());
            hit.sourceRef(BytesReference.bytes(XContentFactory.jsonBuilder().map(_source)));

            list.add(hit);
        }
        SearchHits hits = new SearchHits(list.toArray(SearchHits.EMPTY), new TotalHits(source.size(), TotalHits.Relation.EQUAL_TO), 1);
        when(response.getHits()).thenReturn(hits.asUnpooled());
        hits.decRef();

        return response;
    }

    private Client getBasicMockedClient() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        return client;
    }

    private Client getMockedClient(Consumer<QueryBuilder> queryBuilderConsumer, SearchResponse response) {
        Client client = getBasicMockedClient();
        doAnswer(invocationOnMock -> {
            MultiSearchRequest multiSearchRequest = (MultiSearchRequest) invocationOnMock.getArguments()[0];
            queryBuilderConsumer.accept(multiSearchRequest.requests().get(0).source().query());
            @SuppressWarnings("unchecked")
            ActionListener<MultiSearchResponse> actionListener = (ActionListener<MultiSearchResponse>) invocationOnMock.getArguments()[1];
            ActionListener.respondAndRelease(
                actionListener,
                new MultiSearchResponse(
                    new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(response, null) },
                    randomNonNegativeLong()
                )
            );
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
}
