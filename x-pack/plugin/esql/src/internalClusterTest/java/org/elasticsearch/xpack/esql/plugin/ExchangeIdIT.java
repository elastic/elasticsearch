/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * A plan viewer joins the two sides of every exchange by the {@code exchange_id} the exchange nodes of the plan JSON and the
 * exchange operators of PROFILE share. These tests hold real PROFILE responses to that: every sink and source has an id, every id
 * has both sides, and an operator names the same exchange as the plan node it was built from.
 */
public class ExchangeIdIT extends AbstractEsqlIntegTestCase {

    public void testLinearQuery() {
        createIndex();
        Exchanges exchanges = profile("FROM emps | SORT id | LIMIT 5");
        assertThat("the sinks and sources of a linear query pair up", exchanges.ids(), not(empty()));
    }

    public void testAggregation() {
        createIndex();
        Exchanges exchanges = profile("FROM emps | STATS c = COUNT(*), s = SUM(id) BY lang | SORT lang");
        assertThat(exchanges.ids(), not(empty()));
    }

    /** A FORK over a UNION of subqueries has merges, whose children all feed one source. */
    public void testForkOverSubqueries() {
        createIndex();
        Exchanges exchanges = profile("""
            FROM (FROM emps | WHERE id <= 3), (FROM (FROM emps | WHERE id == 4), (FROM emps | WHERE id == 5))
            | FORK (WHERE id <= 2) (WHERE id > 2 AND id < 5) (WHERE id >= 5)
            | KEEP _fork, id
            | SORT _fork, id
            """);
        // A merge has one source and a sink for each of its children, told apart by their position
        assertThat(exchanges.branches.values().stream().mapToInt(Set::size).max().orElse(0), greaterThan(1));
    }

    private void createIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("emps")
                .setSettings(indexSettings(randomIntBetween(1, 3), 0))
                .setMapping("id", "type=integer", "lang", "type=integer")
        );
        for (int i = 1; i <= 10; i++) {
            prepareIndex("emps").setSource(Map.of("id", i, "lang", i % 3)).get();
        }
        client().admin().indices().prepareRefresh("emps").get();
    }

    private Exchanges profile(String query) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            EsqlQueryResponse.Profile profile = response.profile();
            Exchanges exchanges = new Exchanges();
            for (PlanProfile plan : profile.plans()) {
                exchanges.addPlan(plan);
            }
            assertThat(profile.drivers(), not(empty()));
            for (DriverProfile driver : profile.drivers()) {
                exchanges.checkOperators(driver);
            }
            exchanges.assertPairsUp();
            return exchanges;
        }
    }

    /** The exchange nodes of every compute plan of a profile, and the exchange operators of its drivers. */
    private static class Exchanges {
        final Map<String, Integer> sinks = new HashMap<>();
        final Map<String, Integer> sources = new HashMap<>();
        /** The positions of the sinks of each exchange that has branches. */
        final Map<String, Set<Integer>> branches = new HashMap<>();

        /**
         * The exchange nodes of every plan, by node id. A node id is unique in the JVM that built the plan, so it finds the plan of a
         * driver. Descriptions cannot: a FORK has a {@code data} plan for each branch.
         */
        private final Map<String, Map<String, Object>> exchangeByNodeId = new HashMap<>();
        /** The id of every node of every plan, exchange or not. */
        private final Set<String> nodeIds = new HashSet<>();

        void addPlan(PlanProfile plan) {
            collect(XContentHelper.convertToMap(JsonXContent.jsonXContent, plan.planJson(), false), exchangeByNodeId);
        }

        void checkOperators(DriverProfile driver) {
            for (OperatorStatus operator : driver.operators()) {
                if (operator.planNodeId() != null) {
                    assertTrue(
                        "["
                            + operator.operator()
                            + "] of ["
                            + driver.description()
                            + "] reports plan node "
                            + operator.planNodeId()
                            + ", which no plan JSON has",
                        nodeIds.contains(operator.planNodeId())
                    );
                }
                boolean exchangeOperator = operator.operator().startsWith("ExchangeSinkOperator")
                    || operator.operator().startsWith("ExchangeSourceOperator");
                if (exchangeOperator == false) {
                    assertNull("only exchange operators name an exchange: " + operator.operator(), operator.exchangeId());
                    continue;
                }
                String where = "[" + operator.operator() + "] of [" + driver.description() + "]";
                assertNotNull(where + " names its exchange", operator.exchangeId());
                Map<String, Object> node = exchangeByNodeId.get(operator.planNodeId());
                assertNotNull(where + " is built from an exchange node of the plan JSON", node);
                assertThat(
                    "the operator and its plan node name the same exchange",
                    operator.exchangeId(),
                    equalTo(node.get("exchange_id"))
                );
            }
        }

        private void collect(Map<String, Object> node, Map<String, Map<String, Object>> exchangeByNodeId) {
            nodeIds.add((String) node.get("id"));
            String type = (String) node.get("type");
            boolean sink = "ExchangeSinkExec".equals(type);
            boolean source = "ExchangeSourceExec".equals(type);
            if (sink || source) {
                String id = (String) node.get("exchange_id");
                assertThat("[" + type + "] " + node.get("id") + " has an exchange id", id, not(equalTo(null)));
                exchangeByNodeId.put((String) node.get("id"), node);
                (sink ? sinks : sources).merge(id, 1, Integer::sum);
                if (sink && node.get("child_index") != null) {
                    branches.computeIfAbsent(id, k -> new HashSet<>()).add(((Number) node.get("child_index")).intValue());
                }
            }
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> children = (List<Map<String, Object>>) node.get("children");
            for (Map<String, Object> child : children == null ? List.<Map<String, Object>>of() : children) {
                collect(child, exchangeByNodeId);
            }
        }

        Set<String> ids() {
            Set<String> ids = new TreeSet<>(sinks.keySet());
            ids.addAll(sources.keySet());
            return ids;
        }

        void assertPairsUp() {
            List<String> sinkOnly = new ArrayList<>(sinks.keySet());
            sinkOnly.removeAll(sources.keySet());
            List<String> sourceOnly = new ArrayList<>(sources.keySet());
            sourceOnly.removeAll(sinks.keySet());
            assertThat("exchanges with a sink and no source", sinkOnly, empty());
            assertThat("exchanges with a source and no sink", sourceOnly, empty());
        }
    }
}
