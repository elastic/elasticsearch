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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Demonstrates that a {@code LOOKUP JOIN} looks up on another node: the lookup index lives only on one data node and the
 * main index only on the other, so every lookup request has to cross nodes. Not meant to be kept.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class RemoteLookupDemoIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testLookupCrossesNodes() {
        String[] dataNodes = Arrays.stream(internalCluster().getNodeNames()).sorted().toArray(String[]::new);
        assertThat(dataNodes.length, equalTo(2));
        String lookupNode = dataNodes[0];
        String mainNode = dataNodes[1];

        assertAcked(
            prepareCreate("languages_lookup").setSettings(
                Settings.builder()
                    .put("index.mode", "lookup")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", lookupNode)
            ).setMapping("language_code", "type=integer", "language_name", "type=keyword")
        );
        assertAcked(
            prepareCreate("employees").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", mainNode)
            ).setMapping("emp_no", "type=integer", "language_code", "type=integer")
        );
        ensureGreen("languages_lookup", "employees");

        BulkRequestBuilder languages = client().prepareBulk("languages_lookup").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        languages.add(client().prepareIndex().setSource("language_code", 1, "language_name", "English"));
        languages.add(client().prepareIndex().setSource("language_code", 2, "language_name", "French"));
        languages.add(client().prepareIndex().setSource("language_code", 3, "language_name", "Spanish"));
        assertFalse(languages.get().hasFailures());
        BulkRequestBuilder employees = client().prepareBulk("employees").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int[][] rows = { { 10001, 1 }, { 10002, 2 }, { 10003, 3 }, { 10004, 1 }, { 10005, 9 } };
        for (int[] row : rows) {
            employees.add(client().prepareIndex().setSource("emp_no", row[0], "language_code", row[1]));
        }
        assertFalse(employees.get().hasFailures());

        // Where the shards are: proof that no node holds both indices.
        ClusterState state = clusterService().state();
        Set<String> lookupShardNodes = nodesOf(state, "languages_lookup");
        Set<String> mainShardNodes = nodesOf(state, "employees");
        logger.info("--> [languages_lookup] on {}, [employees] on {}", lookupShardNodes, mainShardNodes);
        assertThat(lookupShardNodes, equalTo(Set.of(lookupNode)));
        assertThat(mainShardNodes, equalTo(Set.of(mainNode)));

        // Count the lookup requests each node receives, and from which node they came.
        Map<String, AtomicInteger> received = new ConcurrentHashMap<>();
        List<String> senders = java.util.Collections.synchronizedList(new ArrayList<>());
        for (String node : dataNodes) {
            var transportService = internalCluster().getInstance(TransportService.class, node);
            as(transportService, MockTransportService.class).addRequestHandlingBehavior(
                LookupFromIndexService.LOOKUP_ACTION_NAME,
                (handler, request, channel, task) -> {
                    received.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();
                    senders.add(task.getParentTaskId().getNodeId());
                    handler.messageReceived(request, channel, task);
                }
            );
        }

        try (var resp = run("""
            FROM employees
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP emp_no, language_code, language_name
            | SORT emp_no
            """)) {
            List<List<Object>> values = getValuesList(resp);
            logger.info("--> results {}", values);
            assertThat(
                values,
                equalTo(
                    List.of(
                        Arrays.asList(10001, 1, "English"),
                        Arrays.asList(10002, 2, "French"),
                        Arrays.asList(10003, 3, "Spanish"),
                        Arrays.asList(10004, 1, "English"),
                        Arrays.asList(10005, 9, null)
                    )
                )
            );
        } finally {
            for (String node : dataNodes) {
                as(internalCluster().getInstance(TransportService.class, node), MockTransportService.class).clearAllRules();
            }
        }

        String mainNodeId = internalCluster().getInstance(TransportService.class, mainNode).getLocalNode().getId();
        logger.info(
            "--> lookup requests received per node {}, sent by node ids {} ([{}] is [{}])",
            received,
            senders,
            mainNode,
            mainNodeId
        );
        assertThat(received.get(lookupNode).get(), greaterThan(0));
        assertNull("the node without the lookup shard must not serve lookups", received.get(mainNode));
        assertThat(Set.copyOf(senders), equalTo(Set.of(mainNodeId)));
    }

    private static Set<String> nodesOf(ClusterState state, String index) {
        var table = state.routingTable(ProjectId.DEFAULT).index(index);
        Set<String> ids = new java.util.TreeSet<>();
        for (int shard = 0; shard < table.size(); shard++) {
            table.shard(shard).allShards().map(ShardRouting::currentNodeId).forEach(ids::add);
        }
        return ids.stream().map(id -> state.nodes().get(id).getName()).collect(Collectors.toSet());
    }
}
