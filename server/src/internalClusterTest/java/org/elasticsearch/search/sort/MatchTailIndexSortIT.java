/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * A match-all query sorted by {@code @timestamp} ascending reads only the end of each segment of an index sorted by descending
 * {@code @timestamp}. That is only correct if the last documents are the ones with the smallest timestamps. These tests cover
 * cases where they are not.
 * <p>
 * The tail is only used if the data stream timestamp mapper is enabled, because it makes sure that every document has exactly
 * one {@code @timestamp} value. Most tests enable it on a plain index. Tests that need a document with no value or with many
 * values use an index without it, and check that the search does not use the tail.
 */
public class MatchTailIndexSortIT extends ESIntegTestCase {

    private static final String INDEX = "match_tail_index_sort";

    /** Documents without a timestamp sort last, so with more of them than hits the end of the segment holds no top hit. */
    public void testDocumentsWithoutTimestampFillTheEndOfTheSegment() {
        createIndexWithoutDataStreamTimestamp(descendingTimestampSort(), "date");
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long i = 0; i < 50; i++) {
            docs.add(Map.of("@timestamp", 1000 + i));
        }
        for (int i = 0; i < 20; i++) {
            docs.add(Map.of());
        }
        indexBatch(docs);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());

        assertAscSortTimestamps(5, List.of(1000L, 1001L, 1002L, 1003L, 1004L));
    }

    /**
     * The segments of a shard share one queue, so a segment can start with a full queue. If it has fewer documents than the
     * queue holds, its tail is the whole segment.
     */
    public void testSegmentWithFewerDocumentsThanHits() {
        createIndex(descendingTimestampSort());
        List<Map<String, Object>> large = new ArrayList<>();
        for (long i = 0; i < 25; i++) {
            large.add(Map.of("@timestamp", 2000 + i));
        }
        List<Map<String, Object>> small = new ArrayList<>();
        for (long i = 0; i < 5; i++) {
            small.add(Map.of("@timestamp", 1000 + i));
        }
        // A refresh per batch gives a large segment and a small one
        indexBatch(large);
        indexBatch(small);

        assertAscSortTimestamps(10, List.of(1000L, 1001L, 1002L, 1003L, 1004L, 2000L, 2001L, 2002L, 2003L, 2004L));
    }

    /**
     * Only some documents have {@code host.name}, and always the same value. Documents without one are a separate group of the
     * index sort, so the segment is not sorted by timestamp. The timestamps alternate between the two groups, so the smallest
     * ones are in both, whichever group is last.
     * <p>
     * The index sort is set explicitly, because logsdb sorts on {@code host.name} only through a private setting that the logsdb
     * plugin sets.
     */
    public void testSomeDocumentsWithoutHostName() {
        createIndex(
            Settings.builder()
                .put("index.mode", "logsdb")
                .put("index.mapping.use_doc_values_skipper", true)
                .putList("index.sort.field", "host.name", "@timestamp")
                .putList("index.sort.order", "asc", "desc")
                .build()
        );
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long i = 0; i < 14; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("@timestamp", 1000 + i);
            if (i % 2 == 0) {
                doc.put("host.name", "foo");
            }
            docs.add(doc);
        }
        indexBatch(docs);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());

        assertAscSortTimestamps(2, List.of(1000L, 1001L));
    }

    /**
     * The index sort orders a multi-valued document by its largest value, but the ascending sort uses its smallest value. So the
     * first hit can be before the end of the segment.
     */
    public void testMultiValuedTimestamp() {
        createIndexWithoutDataStreamTimestamp(descendingTimestampSort(), "date");
        List<Map<String, Object>> docs = new ArrayList<>();
        docs.add(Map.of("@timestamp", List.of(1000L, 90_000L)));
        for (long i = 0; i < 50; i++) {
            docs.add(Map.of("@timestamp", 2000 + i));
        }
        // These documents have larger values, so they come before the multi-valued document and fill the first hits
        for (long i = 0; i < 20; i++) {
            docs.add(Map.of("@timestamp", 100_000 + i));
        }
        indexBatch(docs);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());

        assertAscSortTimestamps(5, List.of(1000L, 2000L, 2001L, 2002L, 2003L));
    }

    /**
     * The descending index sort gives documents without a timestamp the key {@code Long.MIN_VALUE}. A document with that real
     * value ties with them, so it can be the last document, after the documents without a value.
     */
    public void testMinValueTimestamp() {
        createIndexWithoutDataStreamTimestamp(descendingTimestampSort(), "long");
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long i = 0; i < 30; i++) {
            docs.add(Map.of("@timestamp", 1000 + i));
        }
        for (int i = 0; i < 30; i++) {
            docs.add(Map.of());
        }
        // Indexed last, so it is after the documents without a value
        docs.add(Map.of("@timestamp", Long.MIN_VALUE));
        indexBatch(docs);

        assertAscSortTimestamps(5, List.of(Long.MIN_VALUE, 1000L, 1001L, 1002L, 1003L));
    }

    /** A columnar index can store a {@code long} with binary doc values, which are not {@code SORTED_NUMERIC} doc values. */
    public void testBinaryDocValues() {
        assumeTrue("behind a feature flag", IndexSettings.BINARY_NUMERIC_DOC_VALUES_FEATURE_FLAG.isEnabled());
        createIndexWithoutDataStreamTimestamp(
            Settings.builder()
                .put(descendingTimestampSort())
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(IndexSettings.BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING.getKey(), true)
                .build(),
            "long"
        );
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long i = 0; i < 50; i++) {
            docs.add(Map.of("@timestamp", 1000 + i));
        }
        indexBatch(docs);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());

        assertAscSortTimestamps(5, List.of(1000L, 1001L, 1002L, 1003L, 1004L));
    }

    /** The tail holds the smallest timestamps, but the post filter rejects them. */
    public void testPostFilter() {
        createSegment(3000);
        assertSameAsWithoutMatchTail(
            query -> prepareSearch(INDEX).setQuery(query)
                .setPostFilter(termQuery("keep", true))
                .addSort("@timestamp", SortOrder.ASC)
                .setTrackTotalHits(true)
                .setSize(5)
        );
    }

    /** With a minimum score the collector counts the hits, so it must see every document. */
    public void testMinScore() {
        createSegment(3000);
        assertSameAsWithoutMatchTail(
            query -> prepareSearch(INDEX).setQuery(query)
                .setMinScore(0.5f)
                .addSort("@timestamp", SortOrder.ASC)
                .setTrackTotalHits(true)
                .setSize(5)
        );
    }

    /** Terminate after collects the first documents of the segment, not its tail. */
    public void testTerminateAfter() {
        createSegment(1500);
        assertSameAsWithoutMatchTail(
            query -> prepareSearch(INDEX).setQuery(query)
                .setTerminateAfter(100)
                .addSort("@timestamp", SortOrder.ASC)
                .setTrackTotalHits(true)
                .setSize(5)
        );
    }

    /** A scroll page after the first one only accepts documents after the last one it returned, so not the tail. */
    public void testScroll() {
        createSegment(1500);
        assertThat(scrollTimestamps(matchAllQuery()), equalTo(scrollTimestamps(everyDocument())));
    }

    private List<List<Object>> scrollTimestamps(QueryBuilder query) {
        List<List<Object>> pages = new ArrayList<>();
        SearchResponse response = prepareSearch(INDEX).setQuery(query)
            .addSort("@timestamp", SortOrder.ASC)
            .setSize(5)
            .setScroll(TimeValue.timeValueMinutes(1))
            .get();
        try {
            for (int page = 0; page < 3; page++) {
                List<Object> timestamps = new ArrayList<>();
                response.getHits().forEach(hit -> timestamps.add(hit.getSortValues()[0]));
                pages.add(timestamps);
                String scrollId = response.getScrollId();
                response.decRef();
                response = client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).get();
            }
        } finally {
            clearScroll(response.getScrollId());
            response.decRef();
        }
        return pages;
    }

    /**
     * One segment with {@code numDocs} documents. From 2048 documents on, a match-all query gets a dense scorer. Documents from
     * 1000 on have {@code keep}, so the smallest timestamps, at the end of the segment, do not.
     */
    private void createSegment(int numDocs) {
        createIndex(descendingTimestampSort());
        List<Map<String, Object>> docs = new ArrayList<>();
        for (long i = 0; i < numDocs; i++) {
            docs.add(Map.of("@timestamp", 1000 + i, "keep", i >= 1000));
        }
        indexBatch(docs);
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
    }

    /** A range query that matches every document does not use the match tail, but reads the documents in the same order. */
    private static QueryBuilder everyDocument() {
        return rangeQuery("@timestamp").gte(0);
    }

    private void assertSameAsWithoutMatchTail(Function<QueryBuilder, SearchRequestBuilder> search) {
        List<Object> expected = new ArrayList<>();
        long[] expectedTotal = new long[1];
        assertNoFailuresAndResponse(search.apply(everyDocument()), response -> {
            response.getHits().forEach(hit -> expected.add(hit.getSortValues()[0]));
            expectedTotal[0] = response.getHits().getTotalHits().value();
        });
        assertNoFailuresAndResponse(search.apply(matchAllQuery()), response -> {
            List<Object> actual = new ArrayList<>();
            response.getHits().forEach(hit -> actual.add(hit.getSortValues()[0]));
            assertThat(actual, equalTo(expected));
            assertThat(response.getHits().getTotalHits().value(), equalTo(expectedTotal[0]));
        });
    }

    private static Settings descendingTimestampSort() {
        return Settings.builder().put("index.sort.field", "@timestamp").put("index.sort.order", "desc").build();
    }

    /** Creates an index with the data stream timestamp mapper, as a backing index of a data stream has. */
    private void createIndex(Settings sortSettings) {
        createIndex(sortSettings, "date", true);
    }

    /** Creates an index without the data stream timestamp mapper, so a document can have no timestamp or many. */
    private void createIndexWithoutDataStreamTimestamp(Settings sortSettings, String timestampType) {
        createIndex(sortSettings, timestampType, false);
    }

    private void createIndex(Settings sortSettings, String timestampType, boolean dataStreamTimestamp) {
        XContentBuilder mapping;
        try {
            mapping = XContentFactory.jsonBuilder().startObject();
            if (dataStreamTimestamp) {
                mapping.startObject("_data_stream_timestamp").field("enabled", true).endObject();
            }
            mapping.startObject("properties");
            mapping.startObject("@timestamp").field("type", timestampType);
            if (timestampType.equals("date")) {
                mapping.field("format", "epoch_millis");
            }
            mapping.endObject();
            mapping.startObject("host.name").field("type", "keyword").endObject();
            mapping.startObject("keep").field("type", "boolean").endObject();
            mapping.endObject().endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.refresh_interval", "-1")
                        .put(sortSettings)
                )
                .setMapping(mapping)
        );
    }

    /** Indexes the documents and refreshes, which makes a new segment. */
    private void indexBatch(List<Map<String, Object>> docs) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (Map<String, Object> doc : docs) {
            bulk.add(prepareIndex(INDEX).setSource(doc));
        }
        assertNoFailures(bulk.get());
        refresh(INDEX);
    }

    private void assertAscSortTimestamps(int size, List<Long> expected) {
        assertNoFailuresAndResponse(
            prepareSearch(INDEX).setQuery(matchAllQuery()).addSort("@timestamp", SortOrder.ASC).setSize(size),
            response -> {
                List<Object> actual = new ArrayList<>();
                response.getHits().forEach(hit -> actual.add(hit.getSortValues()[0]));
                assertThat(actual, equalTo(new ArrayList<Object>(expected)));
            }
        );
    }
}
