/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A partial aggregation with mostly distinct keys switches to a direct-mapped table, which can emit a key more than once.
 * The final aggregation must still return one row per key with the right values.
 */
public class DirectTableAggregationIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "direct-table";
    private static final int DOCS = 60_000;
    private static final int DISTINCT_KEYS = 8_000;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Switch at the first check, with so few slots that most keys evict each other.
            .put(PlannerSettings.AGG_DIRECT_TABLE_SLOTS.getKey(), 2)
            .put(PlannerSettings.AGG_DIRECT_TABLE_SWITCH_MIN_KEYS.getKey(), 0)
            .put(PlannerSettings.AGG_DIRECT_TABLE_SWITCH_UNIQUENESS.getKey(), 0.1)
            .build();
    }

    public void testGroupByLongAndInt() {
        assertAggregation("long_key, int_key");
    }

    public void testGroupByLong() {
        assertAggregation("long_key");
    }

    public void testGroupByKeyword() {
        assertAggregation("keyword_key");
    }

    private void assertAggregation(String by) {
        createIndexWithData();
        var request = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM " + INDEX + " | STATS c = COUNT(*), s = SUM(v) BY " + by + " | LIMIT " + DISTINCT_KEYS * 2
        );
        request.profile(true);
        request.acceptedPragmaRisks(true);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1).build()));
        // Partitioned partial output, which the direct table needs, is only used when the data and final aggregations share a node.
        try (var resp = client(dataNode).execute(EsqlQueryAction.INSTANCE, request).actionGet(TimeValue.THIRTY_SECONDS)) {
            Map<List<Object>, List<Long>> actual = new HashMap<>();
            for (List<Object> row : getValuesList(resp)) {
                List<Object> key = row.subList(2, row.size());
                assertNull(
                    "duplicated key " + key,
                    actual.put(key, List.of(((Number) row.get(0)).longValue(), ((Number) row.get(1)).longValue()))
                );
            }
            Map<List<Object>, List<Long>> expected = expected(by);
            int missing = 0;
            int wrong = 0;
            List<String> examples = new java.util.ArrayList<>();
            for (var e : expected.entrySet()) {
                var a = actual.get(e.getKey());
                if (a == null) {
                    missing++;
                } else if (a.equals(e.getValue()) == false) {
                    wrong++;
                    if (examples.size() < 5) {
                        examples.add(e.getKey() + " expected " + e.getValue() + " actual " + a);
                    }
                }
            }
            assertThat(
                "expected "
                    + expected.size()
                    + " keys, got "
                    + actual.size()
                    + ", missing "
                    + missing
                    + ", wrong "
                    + wrong
                    + " "
                    + examples,
                missing + wrong,
                equalTo(0)
            );
            assertThat(actual.size(), equalTo(expected.size()));

            long partialRows = 0;
            List<String> operators = new java.util.ArrayList<>();
            for (DriverProfile driver : resp.profile().drivers()) {
                if (driver.description().contains("data") == false) {
                    continue;
                }
                for (OperatorStatus operator : driver.operators()) {
                    operators.add(operator.operator());
                    if (operator.status() instanceof HashAggregationOperator.Status status) {
                        partialRows += status.rowsEmitted();
                    }
                }
            }
            // A regular table emits one row per key. The two slots of the direct table can't keep them apart.
            assertThat("direct table was used " + operators, partialRows, greaterThan((long) DISTINCT_KEYS * 2));
        }
    }

    private final long[] longKeys = new long[DOCS];
    private final int[] values = new int[DOCS];

    private String dataNode;

    private void createIndexWithData() {
        dataNode = clusterService().state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(n -> n.getName())
            .sorted()
            .findFirst()
            .orElseThrow();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", dataNode))
                .setMapping("long_key", "type=long", "int_key", "type=integer", "keyword_key", "type=keyword", "v", "type=integer")
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            longKeys[i] = randomIntBetween(0, DISTINCT_KEYS - 1);
            values[i] = randomIntBetween(-1000, 1000);
            bulk.add(
                new IndexRequest(INDEX).id("doc-" + i)
                    .source("long_key", longKeys[i], "int_key", longKeys[i] % 7, "keyword_key", "key-" + longKeys[i], "v", values[i])
            );
            if (bulk.numberOfActions() >= 5000) {
                bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
                bulk = client().prepareBulk();
            }
        }
        if (bulk.numberOfActions() > 0) {
            bulk.get();
        }
        refresh(INDEX);
    }

    private Map<List<Object>, List<Long>> expected(String by) {
        Map<List<Object>, long[]> sums = new HashMap<>();
        for (int i = 0; i < DOCS; i++) {
            List<Object> key = switch (by) {
                case "long_key, int_key" -> List.of(longKeys[i], (int) (longKeys[i] % 7));
                case "long_key" -> List.of(longKeys[i]);
                case "keyword_key" -> List.of("key-" + longKeys[i]);
                default -> throw new IllegalArgumentException(by);
            };
            long[] sum = sums.computeIfAbsent(key, k -> new long[2]);
            sum[0]++;
            sum[1] += values[i];
        }
        Map<List<Object>, List<Long>> expected = new HashMap<>();
        sums.forEach((k, v) -> expected.put(k, List.of(v[0], v[1])));
        return expected;
    }
}
