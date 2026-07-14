/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.NotMultiProjectCapable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

public class WatcherLifeCycleService implements ClusterStateListener {

    private volatile boolean shutDown = false; // indicates that the node has been shutdown and we should never start watcher after this.
    private final WatcherService watcherService;

    WatcherLifeCycleService(ClusterService clusterService, WatcherService watcherService) {
        this.watcherService = watcherService;
        clusterService.addListener(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an watch is scheduled.
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                shutDown();
            }
        });
    }

    void shutDown() {
        shutDown = true;
        watcherService.setDesiredShutdown();
    }

    /**
     * @param event The event of containing the new cluster state
     *
     * stop certain parts of watcher, when there are no watcher indices on this node by checking the shardrouting
     * note that this is not easily possible, because of the execute watch api, that needs to be able to execute anywhere!
     * this means, only certain components can be stopped
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || shutDown) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .watches and
            // a .triggered_watches index, but they may not have been restored from the cluster state on disk
            return;
        }

        if (Strings.isNullOrEmpty(event.state().nodes().getMasterNodeId())) {
            pauseExecution("no master node");
            return;
        }

        if (event.state().getBlocks().hasGlobalBlockWithLevel(ClusterBlockLevel.WRITE)) {
            pauseExecution("write level cluster block");
            return;
        }

        boolean isWatcherStoppedManually = isWatcherStoppedManually(event.state());
        // Non-data nodes have no shard routing fingerprint, but may still need Watcher running for manual execution.
        if (event.state().nodes().getLocalNode().canContainData() == false && isWatcherStoppedManually == false) {
            watcherService.setDesiredRunning(event.state(), List.of(), "starting");
            return;
        }

        if (isWatcherStoppedManually) {
            watcherService.setDesiredStopped("watcher manually marked to shutdown by cluster state update");
            return;
        }

        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        RoutingNode routingNode = event.state().getRoutingNodes().node(localNode.getId());
        if (routingNode == null) {
            pauseExecution("routing node in cluster state undefined. network issue?");
            return;
        }

        IndexMetadata watcherIndexMetadata = WatchStoreUtils.getConcreteIndex(Watch.INDEX, event.state().metadata());
        if (watcherIndexMetadata == null) {
            pauseExecution("no watcher index found");
            return;
        }

        String watchIndex = watcherIndexMetadata.getIndex().getName();
        List<ShardRouting> localShards = routingNode.shardsWithState(watchIndex, RELOCATING, STARTED).toList();
        // no local shards, empty out watcher and don't waste resources!
        if (localShards.isEmpty()) {
            pauseExecution("no local watcher shards found");
            return;
        }

        // also check if non-local shards have changed, as losing a shard on a
        // remote node or adding a replica on a remote node needs to trigger a reload too
        Set<ShardId> localShardIds = localShards.stream().map(ShardRouting::shardId).collect(Collectors.toSet());

        @NotMultiProjectCapable(description = "Watcher is not available in serverless")
        IndexRoutingTable routingTable = event.state().routingTable(ProjectId.DEFAULT).index(watchIndex);
        List<ShardRouting> allShards = routingTable.shardsWithState(STARTED);
        allShards.addAll(routingTable.shardsWithState(RELOCATING));
        List<ShardRouting> localAffectedShardRoutings = allShards.stream()
            .filter(shardRouting -> localShardIds.contains(shardRouting.shardId()))
            // shardrouting is not comparable, so we need some order mechanism
            .sorted(Comparator.comparing(ShardRouting::hashCode))
            .toList();

        if (watcherService.validate(event.state())) {
            watcherService.setDesiredRunning(event.state(), localAffectedShardRoutings, "watcher shard allocation changed");
        } else {
            watcherService.setDesiredStopped("watcher failed validation");
        }
    }

    private void pauseExecution(String reason) {
        watcherService.setDesiredPaused(reason);
    }

    /**
     * check if watcher has been stopped manually via the stop API
     */
    @NotMultiProjectCapable(description = "Watcher is not available in serverless")
    private static boolean isWatcherStoppedManually(ClusterState state) {
        WatcherMetadata watcherMetadata = state.getMetadata().getProject(ProjectId.DEFAULT).custom(WatcherMetadata.TYPE);
        return watcherMetadata != null && watcherMetadata.manuallyStopped();
    }
}
