/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.SnapshotRestoreAllocationSettings;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/** Verifies that publishing storage capacity can resume a pending repository restore. */
public class StatelessRestoreStorageIT extends AbstractStatelessPluginIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockInternalClusterInfoService.TestPlugin.class)).toList();
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(SnapshotRestoreAllocationSettings.SNAPSHOT_RESTORE_ALLOCATION_ENABLED.getKey(), true);
    }

    public void testRestoreResumesAfterDiskCapacityAppears() throws Exception {
        var master = startMasterOnlyNode();
        var smallNode = startIndexNode();
        startSearchNode();
        createIndex("source", indexSettings(1, 1).build());
        ensureGreen("source");
        indexDocs("source", 20);
        flush("source");
        createRepository(logger, "repo", "fs");
        createSnapshot("repo", "snapshot", List.of("source"), List.of("none"));
        assertAcked(indicesAdmin().prepareDelete("source"));

        var infoService = (MockInternalClusterInfoService) internalCluster().getInstance(ClusterInfoService.class, master);
        infoService.setDiskUsageFunctionAndRefresh((node, path) -> {
            long total = node.getName().equals(smallNode) ? ByteSizeValue.ofKb(1).getBytes() : ByteSizeValue.ofGb(100).getBytes();
            return new FsInfo.Path(path.getPath(), path.getMount(), total, total, total);
        });
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snapshot").setWaitForCompletion(false).get();

        assertBusy(() -> {
            var explanation = client().execute(
                TransportClusterAllocationExplainAction.TYPE,
                new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex("source").setShard(0).setPrimary(true)
            ).actionGet();
            assertEquals(ShardRoutingState.UNASSIGNED, explanation.getExplanation().getShardState());
            var decisions = explanation.getExplanation().getShardAllocationDecision().getAllocateDecision().getNodeDecisions();
            assertNotNull(decisions);
            assertTrue(
                decisions.stream()
                    .anyMatch(
                        node -> node.getCanAllocateDecision()
                            .getDecisions()
                            .stream()
                            .anyMatch(
                                decision -> decision.type() == Decision.Type.THROTTLE
                                    && decision.label().equals("stateless_snapshot_restore_storage")
                            )
                    )
            );
        });

        startIndexNode();
        // Node-join ClusterInfo refresh is async; publish disk for the new node before waiting on assignment.
        ClusterInfoServiceUtils.refresh(infoService);
        internalCluster().stopNode(smallNode);

        awaitClusterState(state -> state.routingTable().index("source").shard(0).primaryShard().started());
        ensureGreen("source");
        assertResponse(prepareSearch("source"), response -> {
            assertNoFailures(response);
            assertEquals(20L, response.getHits().getTotalHits().value());
        });
    }
}
