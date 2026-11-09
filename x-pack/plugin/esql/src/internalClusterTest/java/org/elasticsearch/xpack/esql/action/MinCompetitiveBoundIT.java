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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import static org.hamcrest.Matchers.equalTo;

/**
 * A top N that runs after the source filters the segments it reads next with the bound of the rows it already holds. The
 * bound is in the encoding of the sort key in ES|QL, which the range query on the field must decode.
 */
public class MinCompetitiveBoundIT extends AbstractEsqlIntegTestCase {

    private static final int SEGMENTS = 4;
    private static final int DOCS_PER_SEGMENT = 200;
    private static final int LIMIT = 10;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(UnsignedLongMapperPlugin.class);
        return plugins;
    }

    /**
     * The bound of a {@code date_nanos} key is in nanoseconds.
     */
    public void testDateNanos() {
        List<Long> nanos = index(
            "type=date_nanos",
            // the most recent values in the last segment
            segment -> 1_700_000_000_000_000_000L + segment * 3_600_000_000_000L + randomLongBetween(0, 3_599_999_999_999L),
            value -> Instant.ofEpochSecond(0, value).toString()
        );
        List<Instant> expected = nanos.stream()
            .sorted(Comparator.reverseOrder())
            .limit(LIMIT)
            .map(n -> Instant.ofEpochSecond(0, n))
            .toList();
        List<Instant> actual = query("SORT v DESC").stream().map(v -> Instant.parse((String) v)).toList();
        assertThat(actual, equalTo(expected));
    }

    /**
     * ES|QL flips the sign bit of an {@code unsigned_long}.
     */
    public void testUnsignedLong() {
        List<Long> values = index(
            "type=unsigned_long",
            // the smallest values in the last segment
            segment -> (SEGMENTS - 1 - segment) * 1_000_000L + randomLongBetween(0, 999_999L),
            value -> value
        );
        List<Long> expected = values.stream().sorted().limit(LIMIT).toList();
        List<Long> actual = query("SORT v ASC").stream().map(v -> ((Number) v).longValue()).toList();
        assertThat(actual, equalTo(expected));
    }

    /**
     * Indexes a segment at a time, so the bound of the first segments filters the last one.
     */
    private List<Long> index(String mapping, LongUnaryOperator valueOfSegment, Function<Long, Object> source) {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("v", mapping, "message", "type=keyword")
            .get();
        List<Long> values = new ArrayList<>();
        for (int segment = 0; segment < SEGMENTS; segment++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int d = 0; d < DOCS_PER_SEGMENT; d++) {
                long value = valueOfSegment.applyAsLong(segment);
                values.add(value);
                bulk.add(client().prepareIndex("test").setSource("v", source.apply(value), "message", "m" + d));
            }
            assertFalse(bulk.get().hasFailures());
        }
        return values;
    }

    /**
     * A filter that can't run in Lucene keeps the sort after the source.
     */
    private List<Object> query(String sort) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM test | WHERE LENGTH(message) > 0 | " + sort + " | LIMIT " + LIMIT + " | KEEP v");
        // A single driver reads the segments in order
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            List<Object> rows = new ArrayList<>();
            response.values().forEachRemaining(row -> rows.add(row.next()));
            return rows;
        }
    }
}
