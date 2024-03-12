/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.qa.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;

// A subset of ES|QL test scenarios exercised through the xpack core
// transport request API (rather than through the ES|QL request API).
// Tests here have no static dependencies on types from the ES|QL plugin.
public class CoreEsqlActionIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        try {
            @SuppressWarnings("unchecked")
            var c = (Class<? extends Plugin>) Class.forName("org.elasticsearch.xpack.esql.plugin.EsqlPlugin");
            return List.of(c);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e); // the ES|QL plugin must be present
        }
    }

    public void testRowTypesAndValues() {
        var query = "row a = 1, b = \"x\", c = 1000000000000, d = 1.1";
        var req = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var resp = req.execute().actionGet(30, TimeUnit.SECONDS)) {
            logger.info("response= " + resp);
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a", "b", "c", "d"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "keyword", "long", "double"));
            assertThat(getValuesList(resp.values()), contains(List.of(1, "x", 1000000000000L, 1.1d)));
        }
    }

    public void testRowStatsProjectGroupByInt() {
        var query = "row a = 1, b = 2 | stats count(b) by a | keep a";
        var req = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query);
        try (var resp = req.execute().actionGet(30, TimeUnit.SECONDS)) {
            logger.info("response= " + resp);
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer"));
            assertThat(getValuesList(resp.values()), contains(List.of(1)));
        }
    }

    static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }
}
