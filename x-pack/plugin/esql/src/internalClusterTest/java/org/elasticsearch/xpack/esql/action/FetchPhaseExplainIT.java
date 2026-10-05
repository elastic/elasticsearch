/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXPLAIN;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * {@code EXPLAIN} runs the whole distributed query over empty sources, so every node plans its part of the fetch phase
 * into operators. Each data node turns {@code _doc} into document references in its node reduce stage, and the
 * coordinator fetches after its cut.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class FetchPhaseExplainIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetch_phase_explain";
    private static final String QUERY = "FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP a, b, ts";
    private static final Pattern FIELD_EXTRACT = Pattern.compile("FieldExtractExec\\[\\[([^\\]]*)]");

    @Before
    public void setupIndex() {
        assumeTrue("EXPLAIN requires the capability to be enabled", EXPLAIN.isEnabled());
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(between(3, 6), 0))
                .setMapping("ts", "type=long", "a", "type=keyword", "b", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 50; i++) {
            bulk.add(prepareIndex(INDEX).setSource(Map.of("ts", i, "a", "a-" + i, "b", i * 10L)));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(INDEX);
    }

    public void testExplainShowsTheFetchPhaseOnEveryNode() {
        List<List<Object>> rows = explain(true);
        assertThat(plans(rows, "coordinator", "fetchPhase"), contains("APPLIED"));
        List<String> coordinatorPlans = plans(rows, "coordinator", "optimizedPhysicalPlan");
        assertThat(coordinatorPlans, contains(containsString("FetchExec")));
        assertThat("the fetch plan loads the deferred columns", extractedFields(coordinatorPlans.getFirst()), equalTo(Set.of("a", "b")));
        assertThat(plans(rows, "final", "physicalPlan"), hasItem(containsString("FetchExec")));
        assertDataNodesLoadOnlyTheSortKey(rows);

        Map<String, List<String>> nodeReducePlans = new HashMap<>();
        for (List<Object> row : rows) {
            if ("node_reduce".equals(row.get(2)) && "physicalPlan".equals(row.get(3))) {
                nodeReducePlans.computeIfAbsent((String) row.get(1), node -> new ArrayList<>()).add((String) row.get(4));
            }
        }
        assertThat(nodeReducePlans.keySet(), equalTo(nodesWithShards()));
        for (List<String> plans : nodeReducePlans.values()) {
            assertThat(plans, everyItem(containsString("DocRefEncodeExec")));
            for (String plan : plans) {
                assertThat("the node reduce stage loads nothing", extractedFields(plan), empty());
            }
        }
    }

    public void testExplainShowsAPragmaThatTurnsTheFetchPhaseOff() {
        List<List<Object>> rows = explain(false);
        assertThat(plans(rows, "coordinator", "fetchPhase"), contains("DISABLED_PRAGMA"));
        assertThat(plans(rows, "coordinator", "optimizedPhysicalPlan"), contains(not(containsString("FetchExec"))));
    }

    /**
     * The cluster setting is off by default, so a query without the pragma leaves the fetch phase alone and its
     * {@code EXPLAIN} output has no fetch phase row.
     */
    public void testExplainIsUnchangedWhenTheQueryLeavesTheFetchPhaseAlone() {
        List<List<Object>> rows = explain(null);
        assertThat(plans(rows, "coordinator", "fetchPhase"), empty());
        assertThat(plans(rows, "coordinator", "optimizedPhysicalPlan"), contains(not(containsString("FetchExec"))));
        assertDataNodesLoadOnlyTheSortKey(rows);
        List<String> nodeReducePlans = plans(rows, "node_reduce", "physicalPlan");
        assertThat(nodeReducePlans, not(empty()));
        for (String plan : nodeReducePlans) {
            assertThat("each node loads the other columns for its own top rows", extractedFields(plan), equalTo(Set.of("a", "b")));
        }
    }

    /**
     * Node-level late materialization already defers {@code a} and {@code b} past the data drivers, with or without
     * the fetch phase.
     */
    private static void assertDataNodesLoadOnlyTheSortKey(List<List<Object>> rows) {
        List<String> dataPlans = plans(rows, "data", "localPhysicalPlan");
        assertThat(dataPlans, not(empty()));
        for (String plan : dataPlans) {
            assertThat(plan, extractedFields(plan), equalTo(Set.of("ts")));
        }
    }

    /**
     * The fields every {@code FieldExtractExec} of a plan loads, by name.
     */
    private static Set<String> extractedFields(String plan) {
        Set<String> fields = new HashSet<>();
        Matcher matcher = FIELD_EXTRACT.matcher(plan);
        while (matcher.find()) {
            for (String attribute : matcher.group(1).split(", ")) {
                // attributes print as name{kind}#id
                fields.add(attribute.substring(0, attribute.indexOf('{')));
            }
        }
        return fields;
    }

    private List<List<Object>> explain(@Nullable Boolean fetchPhase) {
        Settings.Builder pragmas = Settings.builder();
        if (fetchPhase != null) {
            pragmas.put(QueryPragmas.FETCH_PHASE.getKey(), fetchPhase);
        }
        EsqlQueryRequest request = syncEsqlQueryRequest("EXPLAIN (" + QUERY + ")").pragmas(new QueryPragmas(pragmas.build()));
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response);
        }
    }

    private static List<String> plans(List<List<Object>> rows, String role, String type) {
        List<String> plans = new ArrayList<>();
        for (List<Object> row : rows) {
            if (role.equals(row.get(2)) && type.equals(row.get(3))) {
                plans.add((String) row.get(4));
            }
        }
        return plans;
    }

    private Set<String> nodesWithShards() {
        ClusterState state = clusterService().state();
        Set<String> nodes = new HashSet<>();
        for (ShardRouting shard : state.routingTable(ProjectId.DEFAULT).allShards(INDEX)) {
            nodes.add(state.nodes().get(shard.currentNodeId()).getName());
        }
        return nodes;
    }
}
