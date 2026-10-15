/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Checks that {@code FILLNULL <value> ON *} stays correct and releases every block when it expands into a thousand {@code COALESCE}s,
 * and that one wider than {@code esql.query.max_fillnull_fields} is rejected instead of slowing down quadratically
 * (see {@link HeapAttackFillNullSweepIT}).
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackFillNullIT extends HeapAttackFillNullTestCase {

    /**
     * Index: 1000 {@code long} columns, mapped but never written.
     * Query: {@code FILLNULL 0 ON *} over every row.
     * Expected: every column filled with 0; each all-null block takes {@code COALESCE}'s constant path.
     */
    public void testFillNullManyAllNullColumns() throws IOException {
        int fields = 1000;
        int docs = 100;
        initWideIndex(fields, docs, Shape.ALL_NULL);
        allowFillNullOnAll(fields);
        assertFilled(fields, docs, Shape.ALL_NULL);
    }

    /**
     * Index: 500 {@code long} columns, each null in half of the documents.
     * Query: {@code FILLNULL 0 ON *} over every row.
     * Expected: nulls become 0 and values are kept; each mixed block is rebuilt position by position.
     */
    public void testFillNullManyMixedNullColumns() throws IOException {
        int fields = 500;
        int docs = 100;
        initWideIndex(fields, docs, Shape.MIXED_NULL);
        allowFillNullOnAll(fields);
        assertFilled(fields, docs, Shape.MIXED_NULL);
    }

    @SuppressWarnings("unchecked")
    public void testFillNullAtDefaultFieldLimit() throws IOException {
        int limit = EsqlFlags.ESQL_MAX_FILLNULL_FIELDS.getDefault(Settings.EMPTY);
        int docs = 100;
        initWideIndex(limit, docs, Shape.ALL_NULL);

        ResponseException e = expectThrows(ResponseException.class, () -> query(wideQuery(true, docs, false), "error"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        Map<String, Object> error = (Map<String, Object>) responseAsMap(e.getResponse()).get("error");
        assertThat(error.get("type"), equalTo("verification_exception"));
        assertThat(
            (String) error.get("reason"),
            allOf(
                containsString("FILLNULL would fill " + (limit + 1) + " columns"),
                containsString("exceeding the limit of " + limit + " set by the [esql.query.max_fillnull_fields] cluster setting")
            )
        );

        String fillAllButSortKey = sortedFillNullQuery("f*", fieldName(0), docs, false);
        Map<String, Object> response = responseAsMap(query(fillAllButSortKey, "columns,values"));
        List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        assertThat(columns, hasSize(limit + 1));
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat(values, hasSize(docs));
        for (List<Object> row : values) {
            for (int c = 0; c < columns.size(); c++) {
                if ("sort_key".equals(columns.get(c).get("name")) == false) {
                    assertThat(((Number) row.get(c)).longValue(), equalTo(0L));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertFilled(int fields, int docs, Shape shape) throws IOException {
        Map<String, Object> response = responseAsMap(query(wideQuery(true, docs, false), "columns,values"));
        List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        assertThat(columns, hasSize(fields + 1));
        int sortKey = -1;
        for (int c = 0; c < columns.size(); c++) {
            assertThat(columns.get(c).get("type"), equalTo("long"));
            if ("sort_key".equals(columns.get(c).get("name"))) {
                sortKey = c;
            }
        }
        assertThat("no sort_key column", sortKey, greaterThanOrEqualTo(0));

        List<List<Object>> values = (List<List<Object>>) response.get("values");
        assertThat(values, hasSize(docs));
        for (List<Object> row : values) {
            long doc = ((Number) row.get(sortKey)).longValue();
            for (int c = 0; c < columns.size(); c++) {
                if (c == sortKey) {
                    continue;
                }
                int field = Integer.parseInt(((String) columns.get(c).get("name")).substring(1));
                long expected = shape == Shape.MIXED_NULL && isPopulated(field, (int) doc) ? doc : 0;
                String description = "doc " + doc + " column " + fieldName(field);
                assertThat(description, row.get(c), instanceOf(Number.class));
                assertThat(description, ((Number) row.get(c)).longValue(), equalTo(expected));
            }
        }
    }
}
