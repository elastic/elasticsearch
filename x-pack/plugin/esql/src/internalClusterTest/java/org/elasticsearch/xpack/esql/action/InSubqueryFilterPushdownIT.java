/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@code field IN (subquery)} over an indexed field stays an IN list, which is pushed to Lucene as a {@code terms} query that reads only
 * the matching rows, however many values the subquery returns, up to the most terms the index accepts. Past that, or when the limit is
 * pinned, it becomes a hash join that reads every row. Both shapes return the same rows.
 * <p>
 * The rows the Lucene sources emit tell the two shapes apart: the subquery adds the same rows to both, so the main query reads
 * fewer than the rows of its outer source in total only when the IN list was pushed.
 */
public class InSubqueryFilterPushdownIT extends AbstractEsqlIntegTestCase {

    private static final int DOCS = 1_000;

    /**
     * More values than the default limit of an IN list evaluated per row, which is 100, and fewer than {@link #DOCS}.
     */
    private static final int MANY_VALUES = 300;

    /**
     * The first marker an extra {@code WHERE marker >= ...} before the IN keeps.
     */
    private static final int FIRST_MARKER_KEPT_BY_WHERE = 50;

    /**
     * Every {@code NULL_EVERY}th document has no key, and every {@code MULTI_VALUED_EVERY}th has two. The two never fall on the same
     * document, as no marker is both 7 modulo 50 and 3 modulo 40.
     */
    private static final int NULL_EVERY = 50;
    private static final int NULL_REMAINDER = 7;
    private static final int MULTI_VALUED_EVERY = 40;
    private static final int MULTI_VALUED_REMAINDER = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(VersionFieldPlugin.class);
        return plugins;
    }

    public void testManyValuesOnIndexedFieldArePushedToLucene() {
        createEvents("events", null, "type=keyword");

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat("the IN list was pushed to Lucene, so only the matching rows were read", result.rowsRead(), lessThan((long) DOCS));
    }

    /**
     * A field with doc values and no index is still queried from its doc values, so the IN list is pushed for it too.
     */
    public void testDocValuesOnlyKeywordIsPushedToLucene() {
        createEvents("events", null, "type=keyword,index=false");

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), lessThan((long) DOCS));
    }

    public void testPinnedThresholdUsesHashJoin() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        createEvents("events", null, "type=keyword");

        QueryResult result = query("events", MANY_VALUES, pinnedThreshold(0));

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat("a hash join reads every row", result.rowsRead(), greaterThanOrEqualTo((long) DOCS));
    }

    public void testPinnedThresholdAboveValuesKeepsInList() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        createEvents("events", null, "type=keyword");

        QueryResult result = query("events", MANY_VALUES, pinnedThreshold(MANY_VALUES));

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), lessThan((long) DOCS));
    }

    /**
     * A shard rejects a {@code terms} query longer than its {@code index.max_terms_count}, so a list longer than that must be a hash
     * join rather than a failed query.
     */
    public void testMoreValuesThanMaxTermsCountUseHashJoin() {
        int maxTermsCount = MANY_VALUES / 2;
        createEvents("events", maxTermsCount, "type=keyword");

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), greaterThanOrEqualTo((long) DOCS));
    }

    public void testValuesWithinMaxTermsCountArePushedToLucene() {
        createEvents("events", MANY_VALUES, "type=keyword");

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), lessThan((long) DOCS));
    }

    /**
     * The smallest limit of the indices the query reads is the one that counts, as every shard of every index runs the query.
     */
    public void testSmallestMaxTermsCountOfTheIndicesDecides() {
        createEvents("events-wide", null, "type=keyword");
        createEvents("events-narrow", MANY_VALUES / 2, "type=keyword");

        QueryResult result = query("events-*", MANY_VALUES, getPragmas());

        // Both indices hold the same sessions, so each value matches one document in each of them.
        assertThat(result.matches(), equalTo(2L * MANY_VALUES));
        assertThat(result.rowsRead(), greaterThanOrEqualTo(2L * DOCS));
    }

    /**
     * A date field with a custom format rejects the ISO dates a pushed IN list carries, so a date key must stay on the hash join even
     * with more values than the default limit (https://github.com/elastic/elasticsearch/issues/166324).
     */
    public void testCustomFormatDateKeyStaysOnHashJoin() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("custom_dates")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("d", "type=date,format=yyyy-MM-dd HH:mm:ss", "marker", "type=long")
        );
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
        Instant start = Instant.parse("2015-01-01T00:00:00Z");
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(new IndexRequest("custom_dates").source("d", format.format(start.plusSeconds(i)), "marker", i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        ensureYellow("custom_dates");

        String query = "FROM custom_dates | WHERE d IN (FROM custom_dates | WHERE marker < "
            + MANY_VALUES
            + " | KEEP d) | STATS matches = COUNT(*), total = SUM(marker)";
        QueryResult result = runCounting(query, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), greaterThanOrEqualTo((long) DOCS));
    }

    // -- the key types, NULLs, multi-valued keys and an extra WHERE: the pushed list and the hash join agree --

    public void testKeywordKey() {
        assertPushedMatchesHashJoin(KeyType.KEYWORD);
    }

    public void testIntegerKey() {
        assertPushedMatchesHashJoin(KeyType.INTEGER);
    }

    public void testLongKey() {
        assertPushedMatchesHashJoin(KeyType.LONG);
    }

    public void testIpKey() {
        assertPushedMatchesHashJoin(KeyType.IP);
    }

    public void testDateKey() {
        assertPushedMatchesHashJoin(KeyType.DATE);
    }

    public void testDateNanosKey() {
        assertPushedMatchesHashJoin(KeyType.DATE_NANOS);
    }

    public void testVersionKey() {
        assertPushedMatchesHashJoin(KeyType.VERSION);
    }

    /**
     * The index holds keys of one type, with a document in {@link #NULL_EVERY} that has no key and one in {@link #MULTI_VALUED_EVERY} that
     * has two. The subquery returns {@link #MANY_VALUES} of its rows, so it returns NULLs: for the documents with no key, and for the
     * multi-valued ones, which count as NULL. The outer query then has rows with a NULL key and with a multi-valued key. Neither may
     * match, and the pushed list, which drops the NULLs, must give the rows of the hash join, which keeps them out in its own way. Both
     * are also checked against the rows worked out from the data, and a random {@code WHERE} before the IN is merged into the pushed
     * filter.
     */
    private void assertPushedMatchesHashJoin(KeyType type) {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        String index = "typed_" + type.name().toLowerCase(Locale.ROOT);
        createTyped(index, type);
        boolean extraWhere = randomBoolean();
        List<Long> expected = expectedMarkers(extraWhere);

        String rows = typedQuery(index, extraWhere, "KEEP marker | SORT marker | LIMIT " + DOCS);
        assertThat(markers(rows, getPragmas()), equalTo(expected));
        assertThat(markers(rows, pinnedThreshold(0)), equalTo(expected));

        String count = typedQuery(index, extraWhere, "STATS matches = COUNT(*), total = SUM(marker)");
        QueryResult pushed = runCounting(count, getPragmas());
        QueryResult hashJoin = runCounting(count, pinnedThreshold(0));
        assertThat(pushed.matches(), equalTo((long) expected.size()));
        assertThat(hashJoin.matches(), equalTo((long) expected.size()));
        long outerRows = extraWhere ? DOCS - FIRST_MARKER_KEPT_BY_WHERE : DOCS;
        if (type.pushed) {
            assertThat("the IN list was pushed to Lucene", pushed.rowsRead(), lessThan(outerRows));
        } else {
            assertThat("this key stays on the hash join", pushed.rowsRead(), greaterThanOrEqualTo(outerRows));
        }
        assertThat("a hash join reads every row", hashJoin.rowsRead(), greaterThanOrEqualTo(outerRows));
    }

    /**
     * The markers the subquery selects that have a single key: those below {@link #MANY_VALUES}, without the NULL and the multi-valued
     * documents, and, when the outer query has the extra WHERE, from {@link #FIRST_MARKER_KEPT_BY_WHERE} on.
     */
    private static List<Long> expectedMarkers(boolean extraWhere) {
        List<Long> expected = new ArrayList<>();
        for (int marker = extraWhere ? FIRST_MARKER_KEPT_BY_WHERE : 0; marker < MANY_VALUES; marker++) {
            if (marker % NULL_EVERY != NULL_REMAINDER && marker % MULTI_VALUED_EVERY != MULTI_VALUED_REMAINDER) {
                expected.add((long) marker);
            }
        }
        return expected;
    }

    private static String typedQuery(String index, boolean extraWhere, String tail) {
        return "FROM "
            + index
            + (extraWhere ? " | WHERE marker >= " + FIRST_MARKER_KEPT_BY_WHERE : "")
            + " | WHERE k IN (FROM "
            + index
            + " | WHERE marker < "
            + MANY_VALUES
            + " | KEEP k) | "
            + tail;
    }

    private void createTyped(String index, KeyType type) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3)).put("index.number_of_replicas", 0))
                .setMapping("k", "type=" + type.mapping, "marker", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int marker = 0; marker < DOCS; marker++) {
            Map<String, Object> source = new LinkedHashMap<>();
            source.put("marker", marker);
            if (marker % NULL_EVERY != NULL_REMAINDER) {
                boolean multiValued = marker % MULTI_VALUED_EVERY == MULTI_VALUED_REMAINDER;
                source.put("k", multiValued ? List.of(type.value(marker), type.value(marker + 1)) : type.value(marker));
            }
            bulk.add(new IndexRequest(index).source(source));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        ensureYellow(index);
    }

    private List<Long> markers(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        try (EsqlQueryResponse response = run(request)) {
            List<Long> markers = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                markers.add((Long) row.get(0));
            }
            return markers;
        }
    }

    private enum KeyType {
        KEYWORD("keyword", true) {
            @Override
            Object value(int i) {
                return "s-" + i;
            }
        },
        INTEGER("integer", true) {
            @Override
            Object value(int i) {
                return i * 3;
            }
        },
        LONG("long", true) {
            @Override
            Object value(int i) {
                return 10_000_000_000L + i;
            }
        },
        IP("ip", true) {
            @Override
            Object value(int i) {
                return "10.0." + (i / 256) + "." + (i % 256);
            }
        },
        DATE("date", false) {
            @Override
            Object value(int i) {
                return Instant.parse("2026-01-01T00:00:00Z").plusSeconds(i).toString();
            }
        },
        DATE_NANOS("date_nanos", false) {
            @Override
            Object value(int i) {
                return Instant.parse("2026-01-01T00:00:00Z").plusNanos(i).toString();
            }
        },
        VERSION("version", true) {
            @Override
            Object value(int i) {
                return "1.0." + i;
            }
        };

        private final String mapping;

        /**
         * Whether the IN list is pushed to Lucene. Date keys are not: the shard parses a pushed date with the field's own format,
         * which the coordinator cannot see, and a custom format rejects it (https://github.com/elastic/elasticsearch/issues/166324).
         */
        private final boolean pushed;

        KeyType(String mapping, boolean pushed) {
            this.mapping = mapping;
            this.pushed = pushed;
        }

        /**
         * The key of the {@code i}th document: distinct for every {@code i}.
         */
        abstract Object value(int i);
    }

    // -- one keyword key, the events of a session --

    private void createEvents(String index, Integer maxTermsCount, String sessionMapping) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        if (maxTermsCount != null) {
            settings.put(IndexSettings.MAX_TERMS_COUNT_SETTING.getKey(), maxTermsCount);
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping("session", sessionMapping, "marker", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(new IndexRequest(index).source("session", "s-" + i, "marker", i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        ensureYellow(index);
    }

    /**
     * Counts the events of the sessions of the first {@code values} events, and how many rows the Lucene sources emitted to do so. The
     * aggregation reads a field, so Lucene does not answer it from its own statistics.
     */
    private QueryResult query(String pattern, int values, QueryPragmas pragmas) {
        String query = "FROM "
            + pattern
            + " | WHERE session IN (FROM "
            + pattern
            + " | WHERE marker < "
            + values
            + " | STATS c = COUNT(*) BY session | KEEP session)"
            + " | STATS matches = COUNT(*), total = SUM(marker)";
        return runCounting(query, pragmas);
    }

    /**
     * Runs a query whose first column is a count of matches, and sums the rows the Lucene sources emitted.
     */
    private QueryResult runCounting(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.profile(true);
        request.pragmas(pragmas);
        try (EsqlQueryResponse response = run(request)) {
            long matches = (long) response.values().next().next();
            long rowsRead = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof LuceneSourceOperator.Status status) {
                        rowsRead += status.rowsEmitted();
                    }
                }
            }
            return new QueryResult(matches, rowsRead);
        }
    }

    private static QueryPragmas pinnedThreshold(int threshold) {
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", threshold).build());
    }

    private record QueryResult(long matches, long rowsRead) {}
}
