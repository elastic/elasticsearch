/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Sorts on {@code @timestamp} over indices covering successive time ranges. The data nodes run their shards one at a time,
 * the most competitive first, and skip the shards whose timestamps can't make the top N.
 */
public class TimestampSortedTopNIT extends AbstractEsqlIntegTestCase {

    private static final int INDICES = 8;
    private static final int DOCS_PER_INDEX = 200;
    private static final int LIMIT = 10;

    private final List<Long> timestamps = new ArrayList<>();

    @Before
    public void populate() {
        long start = 1_700_000_000_000L;
        for (int i = 0; i < INDICES; i++) {
            String index = "events-" + i;
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "message", "type=keyword", "time", "type=alias,path=@timestamp")
                .get();
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int d = 0; d < DOCS_PER_INDEX; d++) {
                // Each index covers its own hour
                long timestamp = start + i * 3_600_000L + randomLongBetween(0, 3_599_999L);
                timestamps.add(timestamp);
                bulk.add(client().prepareIndex(index).setSource("@timestamp", timestamp, "message", index));
            }
            assertFalse(bulk.get().hasFailures());
        }
    }

    public void testDescending() {
        List<Long> expected = timestamps.stream().sorted(Comparator.reverseOrder()).limit(LIMIT).toList();
        assertTopN("FROM events-* | SORT @timestamp DESC | LIMIT " + LIMIT + " | KEEP @timestamp", expected);
    }

    public void testAscending() {
        List<Long> expected = timestamps.stream().sorted().limit(LIMIT).toList();
        assertTopN("FROM events-* | SORT @timestamp ASC | LIMIT " + LIMIT + " | KEEP @timestamp", expected);
    }

    /**
     * A filter that can't run in Lucene keeps the sort out of Lucene too. The top N then filters the sources by its bound.
     */
    public void testSortAfterTheSource() {
        List<Long> expected = timestamps.stream().sorted(Comparator.reverseOrder()).limit(LIMIT).toList();
        assertTopN("FROM events-* | WHERE LENGTH(message) > 0 | SORT @timestamp DESC | LIMIT " + LIMIT + " | KEEP @timestamp", expected);
    }

    /**
     * Documents without {@code @timestamp} sort first when descending, so a shard holding some is never skipped, however old.
     */
    public void testNullsFirst() {
        int nulls = between(1, LIMIT - 1);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < nulls; d++) {
            bulk.add(client().prepareIndex("events-0").setSource("message", "events-0"));
        }
        assertFalse(bulk.get().hasFailures());
        List<Long> expected = new ArrayList<>(Collections.nCopies(nulls, null));
        expected.addAll(timestamps.stream().sorted(Comparator.reverseOrder()).limit(LIMIT - nulls).toList());
        assertTopN("FROM events-* | SORT @timestamp DESC | LIMIT " + LIMIT + " | KEEP @timestamp", expected, false);
    }

    /**
     * The statistics of an alias are those of its target. The order of the shards doesn't follow the alias, so this only
     * checks that no shard holding better rows is skipped.
     */
    public void testAlias() {
        List<Long> expected = timestamps.stream().sorted().limit(LIMIT).toList();
        assertTopN("FROM events-* | SORT time ASC | LIMIT " + LIMIT + " | KEEP @timestamp", expected, false);
    }

    /**
     * The shards of each index miss one of the sort keys, so each batch sorts on its own key after local optimization. The
     * bound of one batch must not skip the shards of the other.
     */
    public void testSortKeyMissingFromSomeShards() {
        String node = internalCluster().getRandomDataNodeName();
        // The index holding the winning rows is the oldest, so it runs last
        createSingleShardIndex("keys-winning", node, "t1", "type=date");
        createSingleShardIndex("keys-other", node, "t2", "type=date");
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < DOCS_PER_INDEX; d++) {
            bulk.add(client().prepareIndex("keys-winning").setSource("t1", Instant.ofEpochMilli(1_000L + d).toString()));
            bulk.add(client().prepareIndex("keys-other").setSource("t2", Instant.ofEpochMilli(1_000_000L + d).toString()));
        }
        assertFalse(bulk.get().hasFailures());
        List<Long> expected = List.of(1_000L + DOCS_PER_INDEX - 1);
        assertTopN("FROM keys-* | SORT t1 DESC NULLS LAST, t2 DESC NULLS LAST | LIMIT 1 | KEEP t1", expected, false);
    }

    private void createSingleShardIndex(String index, String node, String field, String type) {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", node)
            )
            .setMapping(field, type)
            .get();
        ensureGreen(index);
    }

    private void assertTopN(String query, List<Long> expected) {
        assertTopN(query, expected, true);
    }

    private void assertTopN(String query, List<Long> expected, boolean skipsOlderShards) {
        int taskConcurrency = between(1, 2);
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.profile(true);
        // One shard at a time, so the shards that run later can be skipped
        request.pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1)
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), taskConcurrency)
                    .build()
            )
        );
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<Long> actual = new ArrayList<>();
            response.values().forEachRemaining(row -> actual.add(toMillis(row.next())));
            assertThat(actual, equalTo(expected));
            if (skipsOlderShards == false) {
                return;
            }

            int emittingDrivers = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                if (driver.description().equals(ComputeService.DATA_DESCRIPTION)) {
                    for (OperatorStatus operator : driver.operators()) {
                        if (operator.status() instanceof LuceneOperator.Status status && status.rowsEmitted() > 0) {
                            emittingDrivers++;
                        }
                    }
                }
            }
            // Only the drivers of the first shard of each data node read rows, the later shards can't beat their top N
            assertThat(emittingDrivers, lessThanOrEqualTo(internalCluster().numDataNodes() * taskConcurrency));
        }
    }

    private static Long toMillis(Object value) {
        return value == null ? null : Instant.parse((String) value).toEpochMilli();
    }
}
