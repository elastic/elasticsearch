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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * A match-all query sorted by {@code @timestamp} ascending reads only the end of each segment of an index sorted by descending
 * {@code @timestamp}. That is only correct if the last documents are the ones with the smallest timestamps. These tests cover
 * cases where they are not. They use indices, not data streams, so a document can lack {@code @timestamp}.
 */
public class MatchTailIndexSortIT extends ESIntegTestCase {

    private static final String INDEX = "match_tail_index_sort";

    /** Documents without a timestamp sort last, so with more of them than hits the end of the segment holds no top hit. */
    public void testDocumentsWithoutTimestampFillTheEndOfTheSegment() {
        createIndex(descendingTimestampSort());
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

    private static Settings descendingTimestampSort() {
        return Settings.builder().put("index.sort.field", "@timestamp").put("index.sort.order", "desc").build();
    }

    private void createIndex(Settings sortSettings) {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.refresh_interval", "-1")
                        .put(sortSettings)
                )
                .setMapping("@timestamp", "type=date,format=epoch_millis", "host.name", "type=keyword")
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
