/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Sorts indices on keys computed from fields of every type that has doc values, and checks that the index order is the order
 * of the key as ES|QL computes it in a query, including for documents without a value or with several values.
 */
public class IndexSortComputedKeyTypesIT extends AbstractEsqlIntegTestCase {

    private static final String MAPPING = """
        {
          "properties": {
            "id": { "type": "keyword" },
            "l": { "type": "long" },
            "i": { "type": "integer" },
            "s": { "type": "short" },
            "b": { "type": "byte" },
            "d": { "type": "double" },
            "f": { "type": "float" },
            "hf": { "type": "half_float" },
            "sf": { "type": "scaled_float", "scaling_factor": 100 },
            "dt": { "type": "date" },
            "dn": { "type": "date_nanos" },
            "bool": { "type": "boolean" },
            "kw": { "type": "keyword" },
            "ip": { "type": "ip" },
            "v": { "type": "version" },
            "ul": { "type": "unsigned_long" }
          }
        }""";

    private static final List<String> KEYS = List.of(
        "l * 2",
        "i + s + b",
        "d * 1.5",
        "f + hf",
        "sf * 100",
        "FLOOR(d)",
        "DATE_TRUNC(1 day, dt)",
        "DATE_TRUNC(1 hour, dn)",
        "NOT bool",
        "LENGTH(kw)",
        "COALESCE(i, s)",
        "CASE(bool, l, -l)",
        "MV_MAX(l)",
        "ip > \"10.0.128.0\"",
        "v > \"10.100.1000\"",
        "MV_COUNT(ul)"
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MapperExtrasPlugin.class);
        plugins.add(UnsignedLongMapperPlugin.class);
        plugins.add(VersionFieldPlugin.class);
        return plugins;
    }

    public void testIndexOrderIsTheOrderOfTheKey() {
        for (String key : KEYS) {
            boolean desc = randomBoolean();
            String index = "computed_sort_" + KEYS.indexOf(key);
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0).put("index.sort", key + (desc ? " DESC" : "")))
                    .setMapping(MAPPING)
            );
            int numDocs = indexRandomDocs(index);
            assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
            refresh(index);
            assertIndexOrder(index, key, desc, numDocs);
        }
    }

    private int indexRandomDocs(String index) {
        int numDocs = 0;
        int batches = between(2, 3);
        for (int b = 0; b < batches; b++) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int docs = between(20, 100);
            for (int d = 0; d < docs; d++) {
                Map<String, Object> source = new HashMap<>();
                source.put("id", Integer.toString(numDocs++));
                put(source, "l", () -> randomLongBetween(-1000, 1000));
                put(source, "i", () -> randomIntBetween(-1000, 1000));
                put(source, "s", () -> randomIntBetween(-100, 100));
                put(source, "b", () -> randomIntBetween(-100, 100));
                put(source, "d", () -> randomDoubleBetween(-1000, 1000, true));
                put(source, "f", () -> randomFloat() * 100);
                put(source, "hf", () -> (float) randomIntBetween(-100, 100) / 4);
                put(source, "sf", () -> randomIntBetween(-10000, 10000) / 100.0);
                put(source, "dt", () -> randomLongBetween(0, 10L * 86_400_000));
                put(
                    source,
                    "dn",
                    () -> Instant.ofEpochMilli(1_767_225_600_000L + randomLongBetween(0, 10L * 3_600_000))
                        .plusNanos(between(0, 999_999))
                        .toString()
                );
                put(source, "bool", () -> randomBoolean());
                put(source, "kw", () -> randomAlphaOfLengthBetween(1, 10));
                put(source, "ip", () -> "10.0." + between(0, 255) + "." + between(0, 255));
                put(source, "v", () -> between(0, 20) + "." + between(0, 200) + "." + between(0, 2000));
                put(source, "ul", () -> randomLongBetween(0, Long.MAX_VALUE));
                bulk.add(prepareIndex(index).setSource(source));
            }
            assertNoFailures(bulk.get());
        }
        return numDocs;
    }

    /** Leaves a field out of some documents, and gives it several values in others. */
    private static void put(Map<String, Object> source, String field, Supplier<Object> value) {
        switch (between(0, 9)) {
            case 0 -> {
            }
            case 1 -> source.put(field, List.of(value.get(), value.get()));
            default -> source.put(field, value.get());
        }
    }

    /** The keys that ES|QL computes, in the order of the documents in the index, follow the order of the sort. */
    private void assertIndexOrder(String index, String key, boolean desc, int numDocs) {
        List<String> docOrder = new ArrayList<>();
        assertResponse(
            prepareSearch(index).addSort("_doc", SortOrder.ASC).setSize(numDocs).setFetchSource(false).addDocValueField("id"),
            r -> {
                for (SearchHit hit : r.getHits().getHits()) {
                    docOrder.add(hit.field("id").getValue());
                }
            }
        );
        assertThat(docOrder.size(), equalTo(numDocs));

        Map<String, Object> keys = new HashMap<>();
        try (EsqlQueryResponse response = run("FROM " + index + " | EVAL k = " + key + " | KEEP id, k | LIMIT " + numDocs)) {
            Iterator<Iterator<Object>> rows = response.values();
            while (rows.hasNext()) {
                Iterator<Object> row = rows.next();
                String id = (String) row.next();
                Object k = row.next();
                // Several values are no key, as in the index sort
                keys.put(id, k instanceof List<?> ? null : k);
            }
        }

        // Like ES|QL, null sorts as larger than any value
        Comparator<Object> order = Comparator.nullsLast(EsqlSortKeyOrder.INSTANCE);
        if (desc) {
            order = order.reversed();
        }
        Object previous = keys.get(docOrder.getFirst());
        for (String id : docOrder) {
            Object current = keys.get(id);
            assertThat(
                String.format(Locale.ROOT, "[%s] key [%s] of doc [%s] after [%s]", key, current, id, previous),
                order.compare(previous, current) <= 0,
                equalTo(true)
            );
            previous = current;
        }
    }

    /** Compares the values of a key as ES|QL returns them: numbers, booleans and dates formatted as strings. */
    private enum EsqlSortKeyOrder implements Comparator<Object> {
        INSTANCE;

        @Override
        public int compare(Object a, Object b) {
            if (a instanceof Number x && b instanceof Number y) {
                return Double.compare(x.doubleValue(), y.doubleValue());
            }
            if (a instanceof Boolean x && b instanceof Boolean y) {
                return Boolean.compare(x, y);
            }
            return ((String) a).compareTo((String) b);
        }
    }
}
