/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code SORT @timestamp ASC | LIMIT n} on an index that is sorted by descending {@code @timestamp} can read only the end of
 * each segment, because the smallest timestamps are stored last. That is only correct if the documents at the end of the
 * segment are the top n. These tests compare the result with a model computed in Java for the cases where that is easy to get
 * wrong: many documents with the same timestamp, several segments, deleted documents, documents without a timestamp, and
 * a second sort key.
 */
public class EsqlAscTimestampSortOnDescIndexSortIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "asc_sort_on_desc_index";

    /** A document of the test index. {@code timestamp} is {@code null} if the document has no {@code @timestamp}. */
    private record Doc(int id, Long timestamp) {}

    public void testAscSort() throws IOException {
        List<Doc> docs = populate(false);
        int limit = randomIntBetween(1, 20);
        assertThat(timestamps("SORT @timestamp ASC | LIMIT " + limit), equalTo(expectedTimestamps(docs, limit, true)));
    }

    public void testAscSortWithMissingTimestamps() throws IOException {
        List<Doc> docs = populate(true);
        int limit = randomIntBetween(1, 20);
        assertThat(timestamps("SORT @timestamp ASC | LIMIT " + limit), equalTo(expectedTimestamps(docs, limit, true)));
    }

    /**
     * Documents without a timestamp sort last in an index that is sorted by descending {@code @timestamp}, so they are at the end
     * of a segment. When most documents have no timestamp the end of the segment can hold only those, but the top n are the
     * documents that have one.
     */
    public void testAscSortWithManyMissingTimestamps() throws IOException {
        List<Doc> docs = populate(0.5);
        int limit = randomIntBetween(1, 5);
        assertThat(timestamps("SORT @timestamp ASC | LIMIT " + limit), equalTo(expectedTimestamps(docs, limit, true)));
    }

    public void testAscSortNullsFirst() throws IOException {
        List<Doc> docs = populate(true);
        int limit = randomIntBetween(1, 20);
        assertThat(timestamps("SORT @timestamp ASC NULLS FIRST | LIMIT " + limit), equalTo(expectedTimestamps(docs, limit, false)));
    }

    /**
     * With a second key, documents that tie on the timestamp are ordered by it, so the top n can include a document that is
     * not at the end of the segment.
     */
    public void testAscSortWithSecondKey() throws IOException {
        List<Doc> docs = populate(randomBoolean());
        int limit = randomIntBetween(1, 20);
        List<List<Object>> expected = docs.stream()
            .sorted(Comparator.comparing(Doc::timestamp, Comparator.nullsLast(Comparator.<Long>naturalOrder())).thenComparingInt(Doc::id))
            .limit(limit)
            .map(d -> List.<Object>of(d.id()))
            .toList();
        try (EsqlQueryResponse response = run("FROM " + INDEX + " | SORT @timestamp ASC, id ASC | LIMIT " + limit + " | KEEP id")) {
            List<List<Object>> actual = new ArrayList<>();
            getValuesList(response).forEach(row -> actual.add(List.of(((Number) row.get(0)).intValue())));
            assertThat(actual, equalTo(expected));
        }
    }

    private List<Long> timestamps(String sortAndLimit) {
        try (EsqlQueryResponse response = run("FROM " + INDEX + " | " + sortAndLimit + " | EVAL t = TO_LONG(@timestamp) | KEEP t")) {
            List<Long> timestamps = new ArrayList<>();
            getValuesList(response).forEach(row -> timestamps.add((Long) row.get(0)));
            return timestamps;
        }
    }

    private static List<Long> expectedTimestamps(List<Doc> docs, int limit, boolean nullsLast) {
        Comparator<Long> order = nullsLast
            ? Comparator.nullsLast(Comparator.<Long>naturalOrder())
            : Comparator.nullsFirst(Comparator.<Long>naturalOrder());
        return docs.stream().map(Doc::timestamp).sorted(order).limit(limit).toList();
    }

    /**
     * Creates an index sorted by descending {@code @timestamp} with several segments and some deleted documents. Few distinct
     * timestamps make most of the documents tie.
     *
     * @param withMissing whether some documents have no {@code @timestamp}
     * @return the documents that are left after the deletes
     */
    private List<Doc> populate(boolean withMissing) throws IOException {
        return populate(withMissing ? 0.1 : 0.0);
    }

    /**
     * @param missingFraction the fraction of the documents that have no {@code @timestamp}
     */
    private List<Doc> populate(double missingFraction) throws IOException {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", randomIntBetween(1, 3))
                        .put("index.number_of_replicas", 0)
                        .put("index.sort.field", "@timestamp")
                        .put("index.sort.order", "desc")
                )
                .setMapping("@timestamp", "type=date", "id", "type=integer")
        );
        int distinctTimestamps = randomIntBetween(5, 60);
        int docCount = randomIntBetween(200, 600);
        int batches = randomIntBetween(2, 5);
        List<Doc> docs = new ArrayList<>();
        for (int batch = 0; batch < batches; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = batch * docCount / batches; i < (batch + 1) * docCount / batches; i++) {
                Long timestamp = randomDouble() < missingFraction ? null : 1_000_000L + randomInt(distinctTimestamps) * 1000L;
                Map<String, Object> source = new HashMap<>();
                source.put("id", i);
                if (timestamp != null) {
                    source.put("@timestamp", timestamp);
                }
                bulk.add(client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource(source));
                docs.add(new Doc(i, timestamp));
            }
            assertNoFailures(bulk.get());
            // A refresh per batch gives the index several segments
            client().admin().indices().prepareRefresh(INDEX).get();
        }
        BulkRequestBuilder deletes = client().prepareBulk();
        List<Doc> remaining = new ArrayList<>();
        for (Doc doc : docs) {
            if (randomInt(9) == 0) {
                deletes.add(client().prepareDelete(INDEX, Integer.toString(doc.id())));
            } else {
                remaining.add(doc);
            }
        }
        if (deletes.numberOfActions() > 0) {
            assertNoFailures(deletes.get());
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        if (randomBoolean()) {
            assertNoFailures(client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
        }
        return remaining;
    }
}
