/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Compares end-to-end latency of simple "search" queries that Lucene can answer directly when issued through the
 * {@code _search} API versus ES|QL. The goal is to measure the dispatch and planning overhead ES|QL adds on top of
 * Lucene for queries that do not need the compute engine, so the cluster is pinned to a single data node, three
 * shards and no replicas, and pragmas are not randomized.
 * <p>
 * The test logs a table with the median and minimum client-side latency per query shape and only asserts that
 * both engines return the same number of hits.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlVsSearchLatencyIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "docs";
    private static final int NUM_DOCS = Integer.parseInt(System.getProperty("tests.latency.docs", "10000"));
    private static final int NUM_SHARDS = Integer.parseInt(System.getProperty("tests.latency.shards", "3"));
    /** Overridable with {@code -Dtests.latency.warmup} and {@code -Dtests.latency.iterations}, e.g. for profiling runs. */
    private static final int WARMUP = Integer.parseInt(System.getProperty("tests.latency.warmup", "50"));
    private static final int ITERATIONS = Integer.parseInt(System.getProperty("tests.latency.iterations", "200"));
    /** Extra unused keyword fields in the mapping, to show how planning scales with mapping width. */
    private static final int EXTRA_FIELDS = Integer.parseInt(System.getProperty("tests.latency.extra_fields", "0"));

    private static final String[] WORDS = {
        "lucene",
        "elasticsearch",
        "search",
        "index",
        "query",
        "shard",
        "segment",
        "vector",
        "token",
        "analyzer",
        "cluster",
        "node",
        "replica",
        "merge",
        "cache",
        "filter" };

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.EMPTY);
    }

    @Before
    public void setupIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", NUM_SHARDS)
                        .put("index.number_of_replicas", 0)
                        .put("index.refresh_interval", -1)
                )
                .setMapping(mapping())
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < NUM_DOCS; i++) {
            StringBuilder body = new StringBuilder();
            for (int w = 0; w < 20; w++) {
                body.append(WORDS[(i * 7 + w * 3) % WORDS.length]).append(' ');
            }
            bulk.add(
                client().prepareIndex(INDEX)
                    .setId(Integer.toString(i))
                    .setSource(
                        "id",
                        i,
                        "category",
                        "cat" + (i % 10),
                        "title",
                        WORDS[i % WORDS.length] + " " + WORDS[(i + 1) % WORDS.length],
                        "body",
                        body.toString(),
                        "status",
                        i % 3 == 0 ? 404 : 200,
                        "@timestamp",
                        "2024-01-01T00:00:00.000Z".replace(
                            "00:00:00",
                            String.format(Locale.ROOT, "%02d:%02d:%02d", (i / 3600) % 24, (i / 60) % 60, i % 60)
                        )
                    )
            );
        }
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        refresh(INDEX);
        assertHitCount(client().prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), NUM_DOCS);
    }

    private static String mapping() {
        StringBuilder sb = new StringBuilder("""
            {
              "properties": {
                "id": { "type": "integer" },
                "status": { "type": "integer" },
                "@timestamp": { "type": "date" },
                "category": { "type": "keyword" },
                "title": { "type": "text" },
                "body": { "type": "text" }""");
        for (int i = 0; i < EXTRA_FIELDS; i++) {
            sb.append(",\n\"f").append(i).append("\": { \"type\": \"keyword\" }");
        }
        return sb.append("\n}\n}").toString();
    }

    public void testLatency() {
        compare(
            "match limit 10",
            () -> client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("body", "lucene")).setSize(10).setFetchSource(false),
            "FROM docs | WHERE match(body, \"lucene\") | KEEP id | LIMIT 10"
        );
        String phrase = WORDS[0] + " " + WORDS[3 % WORDS.length]; // the first two words of every body built from offset 0
        compare(
            "match_phrase limit 10",
            () -> client().prepareSearch(INDEX).setQuery(QueryBuilders.matchPhraseQuery("body", phrase)).setSize(10).setFetchSource(false),
            "FROM docs | WHERE match_phrase(body, \"" + phrase + "\") | KEEP id | LIMIT 10"
        );
        compare(
            "term keep 3 fields",
            () -> client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.termQuery("category", "cat3"))
                .setSize(10)
                .setFetchSource(new String[] { "id", "category", "title" }, null),
            "FROM docs | WHERE category == \"cat3\" | KEEP id, category, title | LIMIT 10"
        );
        compare(
            "match count",
            () -> client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("body", "lucene")).setSize(0).setTrackTotalHits(true),
            "FROM docs | WHERE match(body, \"lucene\") | STATS c = COUNT(*)"
        );
        compare(
            "match_all limit 10",
            () -> client().prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery()).setSize(10).setFetchSource(false),
            "FROM docs | KEEP id | LIMIT 10"
        );
        compare(
            "term sort id top 10",
            () -> client().prepareSearch(INDEX)
                .setQuery(QueryBuilders.termQuery("category", "cat3"))
                .addSort("id", org.elasticsearch.search.sort.SortOrder.DESC)
                .setSize(10)
                .setFetchSource(new String[] { "id" }, null),
            "FROM docs | WHERE category == \"cat3\" | KEEP id | SORT id DESC | LIMIT 10"
        );
        compare(
            "search style: 3 matches + date range, sort desc, top 500 _source",
            () -> client().prepareSearch(INDEX)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("category", "cat3"))
                        .must(QueryBuilders.matchQuery("status", 404))
                        .must(QueryBuilders.matchQuery("body", "lucene"))
                        .must(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01T00:10:05.477Z").lte("2024-01-01T21:10:05.477Z"))
                )
                .addSort("@timestamp", org.elasticsearch.search.sort.SortOrder.DESC)
                .setSize(500)
                .setFetchSource(true),
            "FROM docs METADATA _source | WHERE category: \"cat3\" AND status: 404 AND body: \"lucene\" "
                + "AND @timestamp >= \"2024-01-01T00:10:05.477Z\" AND @timestamp <= \"2024-01-01T21:10:05.477Z\" "
                + "| SORT @timestamp DESC | LIMIT 500 | KEEP _source"
        );
        // The wikipedia rally track shape: scored full text search over two text fields, returning id, score and source.
        compare(
            "wikipedia style: match OR match, sort _score desc, top 20 _id _score _source",
            () -> client().prepareSearch(INDEX)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery("title", "lucene"))
                        .should(QueryBuilders.matchQuery("body", "lucene"))
                )
                .setSize(20)
                .setFetchSource(true),
            "FROM docs METADATA _id, _score, _source | WHERE MATCH(title, \"lucene\") OR MATCH(body, \"lucene\") "
                + "| KEEP _id, _score, _source | SORT _score DESC | LIMIT 20"
        );
        compare(
            "wikipedia style ids-only: match OR match, sort _score desc, top 20 _id _score",
            () -> client().prepareSearch(INDEX)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.matchQuery("title", "lucene"))
                        .should(QueryBuilders.matchQuery("body", "lucene"))
                )
                .setSize(20)
                .setFetchSource(false),
            "FROM docs METADATA _id, _score | WHERE MATCH(title, \"lucene\") OR MATCH(body, \"lucene\") "
                + "| KEEP _id, _score | SORT _score DESC | LIMIT 20"
        );
        // LIKE on a text field without a keyword subfield is evaluated on the data node after loading the field, by both
        // pipelines. "*lucene*" selects the same documents as a match on the token, which is what the DSL side runs.
        compare(
            "log style: term + LIKE on text + date range, sort desc, top 500 _source",
            () -> client().prepareSearch(INDEX)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("category", "cat3"))
                        .must(QueryBuilders.matchQuery("body", "lucene"))
                        .must(QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01T00:10:05.477Z").lte("2024-01-01T21:10:05.477Z"))
                )
                .addSort("@timestamp", org.elasticsearch.search.sort.SortOrder.DESC)
                .setSize(500)
                .setFetchSource(true),
            "FROM docs METADATA _source | WHERE category == \"cat3\" AND body LIKE \"*lucene*\" "
                + "AND @timestamp >= \"2024-01-01T00:10:05.477Z\" AND @timestamp <= \"2024-01-01T21:10:05.477Z\" "
                + "| SORT @timestamp DESC | LIMIT 500 | KEEP _source"
        );
    }

    private void compare(String name, Supplier<SearchRequestBuilder> dsl, String esql) {
        long dslHits = countDslHits(dsl.get());
        long esqlHits = countEsqlHits(esql);
        logger.info("[{}] dsl hits [{}] esql hits [{}]", name, dslHits, esqlHits);
        long[] dslTimes = measure(() -> {
            SearchResponse resp = dsl.get().get();
            resp.decRef();
        });
        long[] esqlTimes = measure(() -> {
            try (EsqlQueryResponse resp = runDirect(syncEsqlQueryRequest(esql).pragmas(getPragmas()))) {
                assertFalse(resp.isPartial());
            }
        });
        long[] dslInterleaved = new long[ITERATIONS];
        long[] esqlInterleaved = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            SearchResponse resp = dsl.get().get();
            dslInterleaved[i] = System.nanoTime() - start;
            resp.decRef();
            start = System.nanoTime();
            try (EsqlQueryResponse eresp = runDirect(syncEsqlQueryRequest(esql).pragmas(getPragmas()))) {
                esqlInterleaved[i] = System.nanoTime() - start;
            }
        }
        logger.info(
            "\n{}",
            String.format(
                Locale.ROOT,
                "LATENCY [%s]%n  %-20s %10s %10s %10s%n  %-20s %10.3f %10.3f %10.3f%n  %-20s %10.3f %10.3f %10.3f%n"
                    + "  %-20s %10.3f %10.3f %10.3f%n  %-20s %10.3f %10.3f %10.3f",
                name,
                "",
                "min ms",
                "median ms",
                "p90 ms",
                "dsl",
                ms(min(dslTimes)),
                ms(percentile(dslTimes, 50)),
                ms(percentile(dslTimes, 90)),
                "esql",
                ms(min(esqlTimes)),
                ms(percentile(esqlTimes, 50)),
                ms(percentile(esqlTimes, 90)),
                "dsl (interleaved)",
                ms(min(dslInterleaved)),
                ms(percentile(dslInterleaved, 50)),
                ms(percentile(dslInterleaved, 90)),
                "esql (interleaved)",
                ms(min(esqlInterleaved)),
                ms(percentile(esqlInterleaved, 50)),
                ms(percentile(esqlInterleaved, 90))
            )
        );
        profileEsql(name, esql);
    }

    /**
     * Runs the ES|QL query with {@code profile=true} and logs the median of the coordinator phase markers and of the
     * per-driver timings, so the overhead can be attributed to planning, dispatch or execution.
     */
    private void profileEsql(String name, String esql) {
        Map<String, List<Long>> samples = new TreeMap<>();
        for (int i = 0; i < WARMUP + ITERATIONS; i++) {
            long start = System.nanoTime();
            try (EsqlQueryResponse resp = runDirect(syncEsqlQueryRequest(esql).pragmas(getPragmas()).profile(true))) {
                long clientNanos = System.nanoTime() - start;
                if (i < WARMUP) {
                    continue;
                }
                if (i == WARMUP) {
                    StringBuilder pb = new StringBuilder("PLANS [").append(name).append("]\n");
                    for (var plan : resp.profile().plans()) {
                        pb.append("--- ").append(plan.description()).append(" logical:\n").append(plan.logicalPlanTree()).append('\n');
                        pb.append("--- ").append(plan.description()).append(" physical:\n").append(plan.planTree()).append('\n');
                    }
                    for (DriverProfile driver : resp.profile().drivers()) {
                        pb.append("--- driver ").append(driver.description()).append(" operators:\n");
                        for (var op : driver.operators()) {
                            pb.append("    ").append(op.operator()).append(" :: ").append(op.status()).append('\n');
                        }
                    }
                    logger.info("\n{}", pb);
                }
                EsqlQueryProfile qp = resp.getExecutionInfo().queryProfile();
                long queryStart = qp.total().timeSpan().startNanos();
                sample(samples, "0.client", clientNanos);
                sample(samples, "1.total", qp.total().timeSpan().durationInNanos());
                sample(samples, "2.planning", qp.planning().timeSpan().durationInNanos());
                sample(samples, "2.planning.parsing", qp.parsing().timeSpan().durationInNanos());
                sample(samples, "2.planning.view_resolution", qp.viewResolution().timeSpan().durationInNanos());
                sample(samples, "2.planning.preanalysis", qp.preAnalysis().timeSpan().durationInNanos());
                sample(samples, "2.planning.indices_resolution", qp.indicesResolutionMarker().timeSpan().durationInNanos());
                sample(samples, "2.planning.enrich_resolution", qp.enrichResolutionMarker().timeSpan().durationInNanos());
                sample(samples, "2.planning.inference_resolution", qp.inferenceResolutionMarker().timeSpan().durationInNanos());
                sample(samples, "2.planning.analysis", qp.analysis().timeSpan().durationInNanos());
                sample(samples, "2.planning.end_offset", qp.planning().timeSpan().stopNanos() - queryStart);
                sample(samples, "3.after_planning", qp.total().timeSpan().stopNanos() - qp.planning().timeSpan().stopNanos());
                for (var plan : resp.profile().plans()) {
                    if (plan.planTimeProfile() != null) {
                        String prefix = "5.plan_time." + plan.description();
                        sample(samples, prefix + ".logical_opt", plan.planTimeProfile().logicalOptimizationNanos());
                        sample(samples, prefix + ".physical_opt", plan.planTimeProfile().physicalOptimizationNanos());
                        sample(samples, prefix + ".reduction", plan.planTimeProfile().reductionPlanNanos());
                    }
                }
                for (DriverProfile driver : resp.profile().drivers()) {
                    String prefix = "4.driver." + driver.description();
                    sample(samples, prefix + ".took", driver.tookNanos());
                    sample(samples, prefix + ".cpu", driver.cpuNanos());
                    sample(samples, prefix + ".iterations", driver.iterations());
                    sample(samples, prefix + ".start_offset_ms", driver.startMillis() - qp.total().timeSpan().startMillis());
                    sample(samples, prefix + ".stop_offset_ms", driver.stopMillis() - qp.total().timeSpan().startMillis());
                    long sleepNanos = 0;
                    for (DriverSleeps.Sleep sleep : driver.sleeps().first()) {
                        if (sleep.wake() > 0) {
                            sleepNanos += (sleep.wake() - sleep.sleep()) * 1_000_000L;
                        }
                    }
                    sample(samples, prefix + ".sleep_ms_first", sleepNanos);
                    for (Map.Entry<String, Long> e : driver.sleeps().counts().entrySet()) {
                        sample(samples, prefix + ".sleeps." + e.getKey(), e.getValue());
                    }
                }
            }
        }
        StringBuilder sb = new StringBuilder("PROFILE [").append(name).append("]\n");
        for (Map.Entry<String, List<Long>> e : samples.entrySet()) {
            long[] values = e.getValue().stream().mapToLong(Long::longValue).toArray();
            boolean nanos = e.getKey().endsWith("iterations") == false
                && e.getKey().contains(".sleeps.") == false
                && e.getKey().endsWith("_ms") == false;
            sb.append(
                String.format(
                    Locale.ROOT,
                    "  %-45s min %10.3f  median %10.3f  p90 %10.3f%n",
                    e.getKey() + (nanos ? " (ms)" : ""),
                    nanos ? ms(min(values)) : (double) min(values),
                    nanos ? ms(percentile(values, 50)) : (double) percentile(values, 50),
                    nanos ? ms(percentile(values, 90)) : (double) percentile(values, 90)
                )
            );
        }
        logger.info("\n{}", sb);
    }

    /**
     * Executes the request directly, bypassing {@link #run(EsqlQueryRequest)} which randomly re-parses the query on the
     * client side with a freshly built function registry and would skew the client-side timings.
     */
    private EsqlQueryResponse runDirect(EsqlQueryRequest request) {
        return client().execute(EsqlQueryAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private static void sample(Map<String, List<Long>> samples, String key, long value) {
        samples.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    private long countDslHits(SearchRequestBuilder builder) {
        SearchResponse resp = builder.setTrackTotalHits(true).get();
        try {
            return resp.getHits().getTotalHits().value();
        } finally {
            resp.decRef();
        }
    }

    private long countEsqlHits(String esql) {
        try (EsqlQueryResponse resp = runDirect(syncEsqlQueryRequest(esql).pragmas(getPragmas()))) {
            List<List<Object>> values = new ArrayList<>();
            resp.values().forEachRemaining(row -> {
                List<Object> list = new ArrayList<>();
                row.forEachRemaining(list::add);
                values.add(list);
            });
            return values.size();
        }
    }

    private static long[] measure(Runnable r) {
        for (int i = 0; i < WARMUP; i++) {
            r.run();
        }
        long[] times = new long[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            r.run();
            times[i] = System.nanoTime() - start;
        }
        return times;
    }

    private static long min(long[] times) {
        return Arrays.stream(times).min().orElseThrow();
    }

    private static long percentile(long[] times, int p) {
        long[] sorted = times.clone();
        Arrays.sort(sorted);
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * p / 100.0) - 1)];
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }
}
