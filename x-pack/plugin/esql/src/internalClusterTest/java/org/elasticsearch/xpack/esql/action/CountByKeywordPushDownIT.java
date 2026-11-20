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
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Counts by a {@code keyword} field. The counts are pushed down to Lucene as a query per value where the terms of the index are
 * the values of the field, and must be the same as when the documents are grouped by the values that are read: a document is
 * counted for each of its values, and the documents with no value are counted together.
 */
public class CountByKeywordPushDownIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "keywords";
    private static final String[] VALUES = { "", "a", "b", "bb", "c d", "E", "é" };

    /** The number of documents of every value of {@code k}, {@code null} for the documents with no value. */
    private final Map<String, Long> expected = new HashMap<>();

    @Before
    public void populate() {
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", between(1, 3))
                    .put("analysis.normalizer.lower.type", "custom")
                    .putList("analysis.normalizer.lower.filter", "lowercase")
            )
            .setMapping(
                "k",
                "type=keyword",
                "normalized",
                "type=keyword,normalizer=lower",
                "ignored_above",
                "type=keyword,ignore_above=1",
                "not_indexed",
                "type=keyword,index=false",
                "n",
                "type=integer"
            )
            .get();
        int docs = between(50, 500);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            List<String> values = randomSubsetOf(between(0, 3), VALUES);
            if (values.isEmpty()) {
                expected.merge(null, 1L, Long::sum);
                bulk.add(client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("n", i));
            } else {
                for (String value : values) {
                    expected.merge(value, 1L, Long::sum);
                }
                Object source = values.size() == 1 ? values.getFirst() : values;
                bulk.add(
                    client().prepareIndex(INDEX)
                        .setId(Integer.toString(i))
                        .setSource("k", source, "normalized", source, "ignored_above", source, "not_indexed", source, "n", i)
                );
            }
        }
        assertFalse(bulk.get().hasFailures());
    }

    public void testCount() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY k", true), equalTo(expected));
    }

    public void testCountWithoutKey() {
        String query = "FROM " + INDEX + " | STATS c = COUNT(*) BY k | KEEP c | STATS total = SUM(c)";
        long total = 0;
        for (long count : expected.values()) {
            total += count;
        }
        try (EsqlQueryResponse response = run(query)) {
            assertThat(response.values().next().next(), equalTo(total));
        }
    }

    /**
     * Deleted documents are not counted. The values that only they hold have no documents, and no group.
     */
    public void testDeletedDocuments() {
        Map<String, Long> all = count("FROM " + INDEX + " | STATS c = COUNT(*) BY k", true);
        assertThat(all, equalTo(expected));
        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(client().prepareIndex(INDEX).setId("only").setSource("k", "only"))
            .add(client().prepareIndex(INDEX).setId("extra").setSource("k", "a"))
            .get();
        expected.merge("a", 1L, Long::sum);
        expected.put("only", 1L);
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY k", true), equalTo(expected));

        client().prepareDelete(INDEX, "only").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        client().prepareDelete(INDEX, "extra").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        expected.merge("a", -1L, Long::sum);
        expected.remove("only");
        if (expected.get("a") == 0) {
            expected.remove("a");
        }
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY k", true), equalTo(expected));
    }

    /**
     * An index that does not map the field has no value for it in any document.
     */
    public void testIndexWithoutTheField() {
        String other = "no_keywords";
        client().admin().indices().prepareCreate(other).setMapping("n", "type=integer").get();
        int docs = between(1, 10);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(client().prepareIndex(other).setSource("n", i));
        }
        assertFalse(bulk.get().hasFailures());
        expected.merge(null, (long) docs, Long::sum);
        assertThat(count("FROM " + INDEX + "," + other + " | STATS c = COUNT(*) BY k", true), equalTo(expected));
    }

    /**
     * An index that maps the field as another type: the values are converted, not the terms of an index.
     */
    public void testIndexWithAnotherType() {
        String other = "ips";
        client().admin().indices().prepareCreate(other).setMapping("k", "type=ip").get();
        client().prepareIndex(other).setSource("k", "127.0.0.1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        expected.put("127.0.0.1", 1L);
        assertThat(count("FROM " + INDEX + "," + other + " | STATS c = COUNT(*) BY k = k::keyword", false), equalTo(expected));
    }

    /**
     * More values than queries to run.
     */
    public void testTooManyValues() {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM " + INDEX + " | STATS c = COUNT(*) BY k");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey(), 3).build()));
        request.acceptedPragmaRisks(true);
        assertThat(count(request, false), equalTo(expected));
    }

    /**
     * The terms of a field with a normalizer are not the values that are read.
     */
    public void testNormalizer() {
        Map<String, Long> counts = count("FROM " + INDEX + " | STATS c = COUNT(*) BY normalized", false);
        Map<String, Long> normalized = new HashMap<>(expected);
        Long upperCase = normalized.remove("E");
        if (upperCase != null) {
            normalized.put("e", upperCase);
        }
        assertThat(counts, equalTo(normalized));
    }

    /**
     * The values that are too long to index may still be read.
     */
    public void testIgnoreAbove() {
        count("FROM " + INDEX + " | STATS c = COUNT(*) BY ignored_above", false);
    }

    public void testNotIndexed() {
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*) BY not_indexed", false), equalTo(expected));
    }

    /**
     * Only the documents that match a filter are counted, by reading their values.
     */
    public void testFilter() {
        Map<String, Long> counts = count("FROM " + INDEX + " | WHERE n >= 0 | STATS c = COUNT(*) BY k", false);
        assertThat(counts, equalTo(expected));
    }

    public void testOtherAggregations() {
        // An aggregation that is not used is not computed, which leaves the count alone
        assertThat(count("FROM " + INDEX + " | STATS c = COUNT(*), m = MAX(n) BY k | KEEP c, k", true), equalTo(expected));
        count("FROM " + INDEX + " | STATS c = COUNT(*), m = MAX(n) BY k | WHERE m >= 0 | KEEP c, k", false);
        count("FROM " + INDEX + " | STATS c = COUNT(n) BY k", false);
        count("FROM " + INDEX + " | STATS c = COUNT(*) WHERE n > 10 BY k", false);
    }

    private Map<String, Long> count(String query, boolean pushedDown) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        return count(request, pushedDown);
    }

    /**
     * Runs a query that returns a count and a key, and checks whether the count is pushed down to Lucene. Then checks that
     * the counts are the same when it is not, and returns them by key.
     */
    private Map<String, Long> count(EsqlQueryRequest request, boolean pushedDown) {
        request.profile(true);
        Map<String, Long> counts = new HashMap<>();
        try (EsqlQueryResponse response = run(request)) {
            response.values().forEachRemaining(row -> {
                Long count = (Long) row.next();
                assertNull(counts.put((String) row.next(), count));
            });
            List<String> operators = new ArrayList<>();
            boolean countsInLucene = false;
            for (DriverProfile driver : response.profile().drivers()) {
                if (driver.description().equals(ComputeService.DATA_DESCRIPTION)) {
                    for (OperatorStatus operator : driver.operators()) {
                        operators.add(operator.operator());
                        countsInLucene |= operator.operator().startsWith("LuceneCountOperator");
                    }
                }
            }
            assertThat(operators.toString(), countsInLucene, equalTo(pushedDown));
        }
        if (pushedDown) {
            EsqlQueryRequest withoutPushDown = new EsqlQueryRequest();
            withoutPushDown.query(request.query());
            withoutPushDown.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey(), 0).build()));
            withoutPushDown.acceptedPragmaRisks(true);
            Map<String, Long> countsWithoutPushDown = new HashMap<>();
            try (EsqlQueryResponse response = run(withoutPushDown)) {
                response.values().forEachRemaining(row -> {
                    Long count = (Long) row.next();
                    countsWithoutPushDown.put((String) row.next(), count);
                });
            }
            assertThat(counts, equalTo(countsWithoutPushDown));
        }
        return counts;
    }
}
