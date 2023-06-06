/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.searchafter;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchAfterIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test";
    private static final int NUM_DOCS = 100;

    public void testsShouldFail() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field1", "type=long", "field2", "type=keyword").get());
        ensureGreen();
        indexRandom(true, client().prepareIndex("test").setId("0").setSource("field1", 0, "field2", "toto"));
        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 0 })
                    .setScroll("1m")
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("`search_after` cannot be used in a scroll context."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 0 })
                    .setFrom(10)
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("`from` parameter must be set to 0 when `search_after` is used."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").setQuery(matchAllQuery()).searchAfter(new Object[] { 0.75f }).get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("Sort must contain at least one field."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field2", SortOrder.DESC)
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 1 })
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("search_after has 1 value(s) but sort has 2."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[] { 1, 2 })
                    .get()
            );
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertTrue(e.shardFailures().length > 0);
                assertThat(failure.toString(), containsString("search_after has 2 value(s) but sort has 1."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[] { "toto" })
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("Failed to parse search_after value for field [field1]."));
            }
        }
    }

    public void testWithNullStrings() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field2", "type=keyword").get());
        ensureGreen();
        indexRandom(
            true,
            client().prepareIndex("test").setId("0").setSource("field1", 0),
            client().prepareIndex("test").setId("1").setSource("field1", 100, "field2", "toto")
        );
        SearchResponse searchResponse = client().prepareSearch("test")
            .addSort("field1", SortOrder.ASC)
            .addSort("field2", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 0, null })
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, Matchers.equalTo(1));
        assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("field1"), Matchers.equalTo(100));
        assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"), Matchers.equalTo("toto"));
    }

    public void testWithSimpleTypes() throws Exception {
        int numFields = randomInt(20) + 1;
        int[] types = new int[numFields - 1];
        for (int i = 0; i < numFields - 1; i++) {
            types[i] = randomInt(6);
        }
        List<List<Object>> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            List<Object> values = new ArrayList<>();
            for (int type : types) {
                switch (type) {
                    case 0 -> values.add(randomBoolean());
                    case 1 -> values.add(randomByte());
                    case 2 -> values.add(randomShort());
                    case 3 -> values.add(randomInt());
                    case 4 -> values.add(randomFloat());
                    case 5 -> values.add(randomDouble());
                    case 6 -> values.add(randomAlphaOfLengthBetween(5, 20));
                }
            }
            values.add(UUIDs.randomBase64UUID());
            documents.add(values);
        }
        int reqSize = randomInt(NUM_DOCS - 1);
        if (reqSize == 0) {
            reqSize = 1;
        }
        assertSearchFromWithSortValues(INDEX_NAME, documents, reqSize);
    }

    public void testWithCustomFormatSortValueOfDateField() throws Exception {
        final XContentBuilder mappings = jsonBuilder();
        mappings.startObject().startObject("properties");
        {
            mappings.startObject("start_date");
            mappings.field("type", "date");
            mappings.field("format", "yyyy-MM-dd");
            mappings.endObject();
        }
        {
            mappings.startObject("end_date");
            mappings.field("type", "date");
            mappings.field("format", "yyyy-MM-dd");
            mappings.endObject();
        }
        mappings.endObject().endObject();
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3)))
                .setMapping(mappings)
        );

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest("test").id("1").source("start_date", "2019-03-24", "end_date", "2020-01-21"))
            .add(new IndexRequest("test").id("2").source("start_date", "2018-04-23", "end_date", "2021-02-22"))
            .add(new IndexRequest("test").id("3").source("start_date", "2015-01-22", "end_date", "2022-07-23"))
            .add(new IndexRequest("test").id("4").source("start_date", "2016-02-21", "end_date", "2024-03-24"))
            .add(new IndexRequest("test").id("5").source("start_date", "2017-01-20", "end_date", "2025-05-28"))
            .get();

        SearchResponse resp = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("start_date").setFormat("dd/MM/yyyy"))
            .addSort(SortBuilders.fieldSort("end_date").setFormat("yyyy-MM-dd"))
            .setSize(2)
            .get();
        assertNoFailures(resp);
        assertThat(resp.getHits().getHits()[0].getSortValues(), arrayContaining("22/01/2015", "2022-07-23"));
        assertThat(resp.getHits().getHits()[1].getSortValues(), arrayContaining("21/02/2016", "2024-03-24"));

        resp = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("start_date").setFormat("dd/MM/yyyy"))
            .addSort(SortBuilders.fieldSort("end_date").setFormat("yyyy-MM-dd"))
            .searchAfter(new String[] { "21/02/2016", "2024-03-24" })
            .setSize(2)
            .get();
        assertNoFailures(resp);
        assertThat(resp.getHits().getHits()[0].getSortValues(), arrayContaining("20/01/2017", "2025-05-28"));
        assertThat(resp.getHits().getHits()[1].getSortValues(), arrayContaining("23/04/2018", "2021-02-22"));

        resp = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("start_date").setFormat("dd/MM/yyyy"))
            .addSort(SortBuilders.fieldSort("end_date")) // it's okay because end_date has the format "yyyy-MM-dd"
            .searchAfter(new String[] { "21/02/2016", "2024-03-24" })
            .setSize(2)
            .get();
        assertNoFailures(resp);
        assertThat(resp.getHits().getHits()[0].getSortValues(), arrayContaining("20/01/2017", 1748390400000L));
        assertThat(resp.getHits().getHits()[1].getSortValues(), arrayContaining("23/04/2018", 1613952000000L));

        SearchRequestBuilder searchRequest = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("start_date").setFormat("dd/MM/yyyy"))
            .addSort(SortBuilders.fieldSort("end_date").setFormat("epoch_millis"))
            .searchAfter(new Object[] { "21/02/2016", 1748390400000L })
            .setSize(2);
        assertNoFailures(searchRequest.get());

        searchRequest = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("start_date").setFormat("dd/MM/yyyy"))
            .addSort(SortBuilders.fieldSort("end_date").setFormat("epoch_millis")) // wrong format
            .searchAfter(new Object[] { "21/02/2016", "23/04/2018" })
            .setSize(2);
        assertFailures(
            searchRequest,
            RestStatus.BAD_REQUEST,
            containsString("failed to parse date field [23/04/2018] with format [epoch_millis]")
        );
    }

    private static class ListComparator implements Comparator<List<?>> {
        @Override
        public int compare(List<?> o1, List<?> o2) {
            if (o1.size() > o2.size()) {
                return 1;
            }

            if (o2.size() > o1.size()) {
                return -1;
            }

            for (int i = 0; i < o1.size(); i++) {
                if ((o1.get(i) instanceof Comparable) == false) {
                    throw new RuntimeException(o1.get(i).getClass() + " is not comparable");
                }
                Object cmp1 = o1.get(i);
                Object cmp2 = o2.get(i);
                @SuppressWarnings({ "unchecked", "rawtypes" })
                int cmp = ((Comparable) cmp1).compareTo(cmp2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }

    private ListComparator LST_COMPARATOR = new ListComparator();

    private void assertSearchFromWithSortValues(String indexName, List<List<Object>> documents, int reqSize) throws Exception {
        int numFields = documents.get(0).size();
        {
            createIndexMappingsFromObjectType(indexName, documents.get(0));
            List<IndexRequestBuilder> requests = new ArrayList<>();
            for (int i = 0; i < documents.size(); i++) {
                XContentBuilder builder = jsonBuilder();
                assertThat(documents.get(i).size(), Matchers.equalTo(numFields));
                builder.startObject();
                for (int j = 0; j < numFields; j++) {
                    builder.field("field" + Integer.toString(j), documents.get(i).get(j));
                }
                builder.endObject();
                requests.add(client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource(builder));
            }
            indexRandom(true, requests);
        }

        Collections.sort(documents, LST_COMPARATOR);
        int offset = 0;
        Object[] sortValues = null;
        while (offset < documents.size()) {
            SearchRequestBuilder req = client().prepareSearch(indexName);
            for (int i = 0; i < documents.get(0).size(); i++) {
                req.addSort("field" + Integer.toString(i), SortOrder.ASC);
            }
            req.setQuery(matchAllQuery()).setSize(reqSize);
            if (sortValues != null) {
                req.searchAfter(sortValues);
            }
            SearchResponse searchResponse = req.get();
            for (SearchHit hit : searchResponse.getHits()) {
                List<Object> toCompare = convertSortValues(documents.get(offset++));
                assertThat(LST_COMPARATOR.compare(toCompare, Arrays.asList(hit.getSortValues())), equalTo(0));
            }
            sortValues = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1].getSortValues();
        }
    }

    private void createIndexMappingsFromObjectType(String indexName, List<Object> types) {
        CreateIndexRequestBuilder indexRequestBuilder = client().admin().indices().prepareCreate(indexName);
        List<String> mappings = new ArrayList<>();
        int numFields = types.size();
        for (int i = 0; i < numFields; i++) {
            Class<?> type = types.get(i).getClass();
            if (type == Integer.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=integer");
            } else if (type == Long.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=long");
            } else if (type == Float.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=float");
            } else if (type == Double.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=double");
            } else if (type == Byte.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=byte");
            } else if (type == Short.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=short");
            } else if (type == Boolean.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=boolean");
            } else if (types.get(i) instanceof String) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=keyword");
            } else {
                fail("Can't match type [" + type + "]");
            }
        }
        indexRequestBuilder.setMapping(mappings.toArray(new String[0])).get();
        ensureGreen();
    }

    // Convert Integer, Short, Byte and Boolean to Long in order to match the conversion done
    // by the internal hits when populating the sort values.
    private List<Object> convertSortValues(List<Object> sortValues) {
        List<Object> converted = new ArrayList<>();
        for (int i = 0; i < sortValues.size(); i++) {
            Object from = sortValues.get(i);
            if (from instanceof Integer integer) {
                converted.add(integer.longValue());
            } else if (from instanceof Short s) {
                converted.add(s.longValue());
            } else if (from instanceof Byte b) {
                converted.add(b.longValue());
            } else if (from instanceof Boolean b) {
                if (b) {
                    converted.add(1L);
                } else {
                    converted.add(0L);
                }
            } else {
                converted.add(from);
            }
        }
        return converted;
    }

    public void testScrollAndSearchAfterWithBigIndex() {
        int numDocs = randomIntBetween(5000, 10000);
        List<Long> timestamps = new ArrayList<>();
        long currentTime = randomLongBetween(0, 1000);
        for (int i = 0; i < numDocs; i++) {
            int copies = randomIntBetween(0, 100) <= 5 ? randomIntBetween(2, 5) : 1;
            for (int j = 0; j < copies; j++) {
                timestamps.add(currentTime);
            }
            currentTime += randomIntBetween(1, 10);
        }
        final Settings.Builder indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5));
        if (randomBoolean()) {
            indexSettings.put("sort.field", "timestamp").put("sort.order", randomFrom("desc", "asc"));
        }
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(indexSettings).setMapping("""
            {"properties":{"timestamp":{"type": "date", "format": "epoch_millis"}}}"""));
        Randomness.shuffle(timestamps);
        final BulkRequestBuilder bulk = client().prepareBulk();
        bulk.setRefreshPolicy(IMMEDIATE);
        for (long timestamp : timestamps) {
            bulk.add(new IndexRequest("test").source("timestamp", timestamp));
        }
        bulk.get();
        Collections.sort(timestamps);
        // scroll with big index
        {
            SearchResponse resp = client().prepareSearch("test")
                .setSize(randomIntBetween(50, 100))
                .setQuery(new MatchAllQueryBuilder())
                .addSort(new FieldSortBuilder("timestamp"))
                .setScroll(TimeValue.timeValueMinutes(5))
                .get();
            try {
                int foundHits = 0;
                do {
                    for (SearchHit hit : resp.getHits().getHits()) {
                        assertNotNull(hit.getSourceAsMap());
                        final Object timestamp = hit.getSourceAsMap().get("timestamp");
                        assertNotNull(timestamp);
                        assertThat(((Number) timestamp).longValue(), equalTo(timestamps.get(foundHits)));
                        foundHits++;
                    }
                    resp = client().prepareSearchScroll(resp.getScrollId()).setScroll(TimeValue.timeValueMinutes(5)).get();
                } while (resp.getHits().getHits().length > 0);
                assertThat(foundHits, equalTo(timestamps.size()));
            } finally {
                client().prepareClearScroll().addScrollId(resp.getScrollId()).get();
            }
        }
        // search_after with sort with point in time
        String pitID;
        {
            OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueMinutes(5));
            pitID = client().execute(OpenPointInTimeAction.INSTANCE, openPITRequest).actionGet().getPointInTimeId();
            SearchRequest searchRequest = new SearchRequest("test").source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitID).setKeepAlive(TimeValue.timeValueMinutes(5)))
                    .sort("timestamp")
            );
            searchRequest.source().size(randomIntBetween(50, 100));
            SearchResponse resp = client().search(searchRequest).actionGet();
            try {
                int foundHits = 0;
                do {
                    Object[] after = null;
                    for (SearchHit hit : resp.getHits().getHits()) {
                        assertNotNull(hit.getSourceAsMap());
                        final Object timestamp = hit.getSourceAsMap().get("timestamp");
                        assertNotNull(timestamp);
                        assertThat(((Number) timestamp).longValue(), equalTo(timestamps.get(foundHits)));
                        after = hit.getSortValues();
                        foundHits++;
                    }
                    searchRequest.source().size(randomIntBetween(50, 100));
                    assertNotNull(after);
                    assertThat("Sorted by timestamp and pit tier breaker", after, arrayWithSize(2));
                    searchRequest.source().searchAfter(after);
                    resp = client().search(searchRequest).actionGet();
                } while (resp.getHits().getHits().length > 0);
                assertThat(foundHits, equalTo(timestamps.size()));
            } finally {
                client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitID)).actionGet();
            }
        }

        // search_after without sort with point in time
        {
            OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueMinutes(5));
            pitID = client().execute(OpenPointInTimeAction.INSTANCE, openPITRequest).actionGet().getPointInTimeId();
            SearchRequest searchRequest = new SearchRequest("test").source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitID).setKeepAlive(TimeValue.timeValueMinutes(5)))
                    .sort(SortBuilders.pitTiebreaker())
            );
            searchRequest.source().size(randomIntBetween(50, 100));
            SearchResponse resp = client().search(searchRequest).actionGet();
            List<Long> foundSeqNos = new ArrayList<>();
            try {
                do {
                    Object[] after = null;
                    for (SearchHit hit : resp.getHits().getHits()) {
                        assertNotNull(hit.getSourceAsMap());
                        final Object timestamp = hit.getSourceAsMap().get("timestamp");
                        assertNotNull(timestamp);
                        foundSeqNos.add(((Number) timestamp).longValue());
                        after = hit.getSortValues();
                    }
                    searchRequest.source().size(randomIntBetween(50, 100));
                    assertNotNull(after);
                    assertThat("sorted by pit tie breaker", after, arrayWithSize(1));
                    searchRequest.source().searchAfter(after);
                    resp = client().search(searchRequest).actionGet();
                } while (resp.getHits().getHits().length > 0);
                Collections.sort(foundSeqNos);
                assertThat(foundSeqNos, equalTo(timestamps));
            } finally {
                client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(pitID)).actionGet();
            }
        }
    }
}
