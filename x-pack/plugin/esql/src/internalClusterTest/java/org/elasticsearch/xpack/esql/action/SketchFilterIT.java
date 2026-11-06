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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@code STATS c = COUNT(*) BY k | SORT c DESC | LIMIT N} with the {@code sketch_filter} pragma. The result must be the same as
 * without it, and on skewed data the filter must drop most of the rows before they are aggregated.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SketchFilterIT extends AbstractEsqlIntegTestCase {
    private static final Pattern ROWS = Pattern.compile("SketchFilterOperator\\[rowsReceived=(\\d+), rowsEmitted=(\\d+)");
    private static final String QUERY = "FROM %s | STATS c = COUNT(*) BY k | SORT c DESC | LIMIT 10";

    private void createIndex(String index) {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("k", "type=long", "other", "type=long")
            .get();
    }

    /** 10 keys with large distinct counts, and a lot of keys that occur a few times. Some documents have no key. */
    private int populateSkewed(String index, int docsWithoutKey) {
        createIndex(index);
        int docs = 0;
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int heavy = 0; heavy < 10; heavy++) {
            for (int i = 0; i < 1000 + heavy * 100; i++) {
                bulk.add(client().prepareIndex(index).setSource("k", 1_000_000L + heavy));
                docs++;
            }
        }
        for (int i = 0; i < 20_000; i++) {
            bulk.add(client().prepareIndex(index).setSource("k", randomLongBetween(0, 50_000)));
            docs++;
        }
        for (int i = 0; i < docsWithoutKey; i++) {
            bulk.add(client().prepareIndex(index).setSource("other", 1));
            docs++;
        }
        assertFalse(bulk.get().hasFailures());
        return docs;
    }

    private record Result(List<List<Object>> rows, long received, long emitted, int filters) {}

    private Result run(String index, boolean sketchFilter) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(String.format(java.util.Locale.ROOT, QUERY, index));
        request.profile(true);
        request.acceptedPragmaRisks(true);
        request.pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 4)
                    .put(QueryPragmas.DATA_PARTITIONING.getKey(), "doc")
                    .put(QueryPragmas.SKETCH_FILTER.getKey(), sketchFilter)
                    .put(QueryPragmas.SKETCH_FILTER_SLOTS.getKey(), 1024)
                    .build()
            )
        );
        try (EsqlQueryResponse response = run(request)) {
            List<List<Object>> rows = new ArrayList<>();
            response.values().forEachRemaining(row -> {
                List<Object> r = new ArrayList<>();
                row.forEachRemaining(r::add);
                rows.add(r);
            });
            long received = 0;
            long emitted = 0;
            int filters = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                if (driver.description().equals(ComputeService.DATA_DESCRIPTION)) {
                    for (OperatorStatus operator : driver.operators()) {
                        Matcher m = ROWS.matcher(operator.operator());
                        if (m.find()) {
                            filters++;
                            received += Long.parseLong(m.group(1));
                            emitted += Long.parseLong(m.group(2));
                        }
                    }
                }
            }
            return new Result(rows, received, emitted, filters);
        }
    }

    public void testSkewedDataGivesTheSameResultAndDropsRows() {
        int docs = populateSkewed("skewed", 0);
        Result plain = run("skewed", false);
        assertThat(plain.filters, equalTo(0));
        Result filtered = run("skewed", true);
        assertThat("the filter should be planned for every data driver", filtered.filters, greaterThan(0));
        assertThat(filtered.rows, equalTo(plain.rows));
        assertThat(filtered.received, equalTo((long) docs));
        assertThat("most rows should be dropped before the aggregation", filtered.emitted, lessThan(filtered.received / 2));
    }

    public void testDocumentsWithoutAKeyPassThrough() {
        int docs = populateSkewed("with_nulls", 500);
        Result plain = run("with_nulls", false);
        Result filtered = run("with_nulls", true);
        assertThat(filtered.rows, equalTo(plain.rows));
        // A null key makes the operator give up, so every row is passed on.
        assertThat(filtered.filters, greaterThan(0));
        assertThat(filtered.emitted, equalTo((long) docs));
    }

    public void testUniformDataIsNotFiltered() {
        createIndex("uniform");
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5000; i++) {
            bulk.add(client().prepareIndex("uniform").setSource("k", randomLongBetween(0, 4999)));
        }
        assertFalse(bulk.get().hasFailures());
        Result plain = run("uniform", false);
        Result filtered = run("uniform", true);
        // The counts are small and tie, so the keys of the top rows can differ. The counts cannot.
        assertThat(filtered.rows.stream().map(r -> r.get(0)).toList(), equalTo(plain.rows.stream().map(r -> r.get(0)).toList()));
        assertThat(filtered.emitted, equalTo(filtered.received));
    }
}
