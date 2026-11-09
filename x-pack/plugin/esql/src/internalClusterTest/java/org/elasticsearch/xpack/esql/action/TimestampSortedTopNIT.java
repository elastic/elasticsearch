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
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ConstantKeywordMapperPlugin.class);
        plugins.add(UnsignedLongMapperPlugin.class);
        return plugins;
    }

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

    public void testSecondarySortAfterTheSource() {
        List<Long> expected = timestamps.stream().sorted(Comparator.reverseOrder()).limit(LIMIT).toList();
        assertTopN(
            "FROM events-* | WHERE LENGTH(message) > 0 | SORT @timestamp DESC, message ASC | LIMIT " + LIMIT + " | KEEP @timestamp",
            expected
        );
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
    public void testSortKeyMissingFromSomeShards() throws Exception {
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

    public void testSecondarySort() {
        List<Long> expected = timestamps.stream().sorted(Comparator.reverseOrder()).limit(LIMIT).toList();
        assertTopN("FROM events-* | SORT @timestamp DESC, message ASC | LIMIT " + LIMIT + " | KEEP @timestamp", expected);
    }

    /**
     * Rows that tie on the first sort key are ordered by the second, so a shard holding such a tie is never skipped.
     */
    public void testTiesOnTheFirstKey() throws Exception {
        assertTiesOnTheFirstKey("FROM ties-* | SORT t1 DESC, message ASC | LIMIT 1 | KEEP message");
    }

    public void testTiesOnTheFirstKeyAfterTheSource() throws Exception {
        assertTiesOnTheFirstKey("FROM ties-* | WHERE LENGTH(message) > 0 | SORT t1 DESC, message ASC | LIMIT 1 | KEEP message");
    }

    private void assertTiesOnTheFirstKey(String query) throws Exception {
        String node = internalCluster().getRandomDataNodeName();
        // The index holding the winning row is the oldest, so it runs last
        createSingleShardIndex("ties-winning", node, "t1", "type=date", "message", "type=keyword");
        createSingleShardIndex("ties-other", node, "t1", "type=date", "message", "type=keyword");
        String timestamp = Instant.ofEpochMilli(1_000_000L).toString();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < DOCS_PER_INDEX; d++) {
            bulk.add(client().prepareIndex("ties-winning").setSource("t1", timestamp, "message", "a"));
            bulk.add(client().prepareIndex("ties-other").setSource("t1", timestamp, "message", "b"));
        }
        assertFalse(bulk.get().hasFailures());
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<Object> rows = new ArrayList<>();
            response.values().forEachRemaining(row -> rows.add(row.next()));
            assertThat(rows, equalTo(List.of("a")));
        }
    }

    /**
     * A {@code constant_keyword} with a different value in each index is dropped from the sort keys of each batch, but it still
     * orders the rows of different batches, so the batches can't share a bound on the remaining keys. Both indices hold the
     * most recent {@code t1}, and only {@code tenant} picks the winner.
     */
    public void testConstantSecondaryKey() throws Exception {
        assertConstantKey("SORT t1 DESC, tenant ASC", 0);
    }

    public void testConstantSecondaryKeyAfterTheSource() throws Exception {
        assertConstantKey("WHERE LENGTH(message) > 0 | SORT t1 DESC, tenant ASC", 0);
    }

    /**
     * The other index holds more recent rows, but {@code tenant} sorts first.
     */
    public void testConstantFirstKey() throws Exception {
        assertConstantKey("SORT tenant ASC, t1 DESC", 1_000);
    }

    public void testConstantFirstKeyAfterTheSource() throws Exception {
        assertConstantKey("WHERE LENGTH(message) > 0 | SORT tenant ASC, t1 DESC", 1_000);
    }

    /**
     * The rows of the other index are {@code newer} millis more recent than those of the winning index.
     */
    private void assertConstantKey(String sort, long newer) throws Exception {
        String node = internalCluster().getRandomDataNodeName();
        // The index holding the winning row is the oldest, so it runs last
        createSingleShardIndex(
            "tenants-winning",
            node,
            "t1",
            "type=date",
            "message",
            "type=keyword",
            "tenant",
            "type=constant_keyword,value=a"
        );
        createSingleShardIndex(
            "tenants-other",
            node,
            "t1",
            "type=date",
            "message",
            "type=keyword",
            "tenant",
            "type=constant_keyword,value=b"
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < DOCS_PER_INDEX; d++) {
            long t1 = 1_000_000L - d;
            bulk.add(client().prepareIndex("tenants-winning").setSource("t1", Instant.ofEpochMilli(t1).toString(), "message", "m"));
            bulk.add(client().prepareIndex("tenants-other").setSource("t1", Instant.ofEpochMilli(t1 + newer).toString(), "message", "m"));
        }
        assertFalse(bulk.get().hasFailures());
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM tenants-* | " + sort + " | LIMIT 1 | KEEP tenant");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<Object> rows = new ArrayList<>();
            response.values().forEachRemaining(row -> rows.add(row.next()));
            assertThat(rows, equalTo(List.of("a")));
        }
    }

    /**
     * A {@code date_nanos} key in nanoseconds, the resolution of the field.
     */
    public void testDateNanosSecondarySortAfterTheSource() throws Exception {
        assertDateNanos("WHERE LENGTH(message) > 0 | SORT ts DESC, message ASC");
    }

    private void assertDateNanos(String sort) throws Exception {
        String node = internalCluster().getRandomDataNodeName();
        createSingleShardIndex("nanos", node, "ts", "type=date_nanos", "message", "type=keyword");
        List<Long> nanos = new ArrayList<>();
        for (int batch = 0; batch < 4; batch++) {
            // several segments, so the bound of the first ones filters the others
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int d = 0; d < DOCS_PER_INDEX; d++) {
                long value = 1_700_000_000_000_000_000L + randomLongBetween(0, 3_600_000_000_000L);
                nanos.add(value);
                Instant instant = Instant.ofEpochSecond(0, value);
                bulk.add(client().prepareIndex("nanos").setSource("ts", instant.toString(), "message", "m" + d));
            }
            assertFalse(bulk.get().hasFailures());
        }
        List<String> expected = nanos.stream()
            .sorted(Comparator.reverseOrder())
            .limit(LIMIT)
            .map(n -> Instant.ofEpochSecond(0, n).toString())
            .toList();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM nanos | " + sort + " | LIMIT " + LIMIT + " | KEEP ts");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<String> actual = new ArrayList<>();
            response.values().forEachRemaining(row -> actual.add(Instant.parse((String) row.next()).toString()));
            assertThat(actual, equalTo(expected));
        }
    }

    public void testUnsignedLongAfterTheSource() throws Exception {
        assertUnsignedLong("WHERE LENGTH(message) > 0 | SORT u ASC");
    }

    public void testUnsignedLongSecondarySortAfterTheSource() throws Exception {
        assertUnsignedLong("WHERE LENGTH(message) > 0 | SORT u ASC, message ASC");
    }

    /**
     * The older index holds the smallest values, so the bound of the newer one, and of the first segments, must not filter them.
     */
    private void assertUnsignedLong(String sort) throws Exception {
        String node = internalCluster().getRandomDataNodeName();
        List<Long> values = new ArrayList<>();
        for (String index : List.of("unsigned-old", "unsigned-new")) {
            createSingleShardIndex(index, node, "u", "type=unsigned_long", "message", "type=keyword");
            for (int batch = 0; batch < 4; batch++) {
                BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int d = 0; d < DOCS_PER_INDEX; d++) {
                    long value = (index.equals("unsigned-new") ? 4_000_000L : 0L) + (3 - batch) * 1_000_000L + randomLongBetween(
                        0,
                        999_999L
                    );
                    values.add(value);
                    bulk.add(client().prepareIndex(index).setSource("u", value, "message", "m" + d));
                }
                assertFalse(bulk.get().hasFailures());
            }
        }
        List<Long> expected = values.stream().sorted().limit(LIMIT).toList();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM unsigned-* | " + sort + " | LIMIT " + LIMIT + " | KEEP u");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<Long> actual = new ArrayList<>();
            response.values().forEachRemaining(row -> actual.add(((Number) row.next()).longValue()));
            assertThat(actual, equalTo(expected));
        }
    }

    /**
     * Creates an index on {@code node}, and waits for the clock to pass its creation date, so that the indices created next
     * are more recent.
     */
    private void createSingleShardIndex(String index, String node, String... mapping) throws Exception {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", node)
            )
            // With @timestamp, the indices run in the order of their creation
            .setMapping(ArrayUtils.concat(new String[] { "@timestamp", "type=date" }, mapping))
            .get();
        ensureGreen(index);
        long created = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(index)
            .getCreationDate();
        assertBusy(() -> assertThat(System.currentTimeMillis(), greaterThan(created)));
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
