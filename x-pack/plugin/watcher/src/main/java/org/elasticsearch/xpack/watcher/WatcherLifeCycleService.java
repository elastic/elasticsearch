/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    // this option configures watcher not to start, unless the cluster state contains information to start watcher
    // if you start with an empty cluster, you can delay starting watcher until you call the API manually
    // if you start with a cluster containing data, this setting might have no effect, once you called the API yourself
    // this is merely for testing, to make sure that watcher only starts when manually called
    public static final Setting<Boolean> SETTING_REQUIRE_MANUAL_START =
            Setting.boolSetting("xpack.watcher.require_manual_start", false, Property.NodeScope);

    private final AtomicReference<WatcherState> state = new AtomicReference<>(WatcherState.STARTED);
    private final AtomicReference<List<ShardRouting>> previousShardRoutings = new AtomicReference<>(Collections.emptyList());
    private final boolean requireManualStart;
    private volatile boolean shutDown = false; // indicates that the node has been shutdown and we should never start watcher after this.
    private volatile WatcherService watcherService;

    WatcherLifeCycleService(Settings settings, ClusterService clusterService, WatcherService watcherService) {
        super(settings);
        this.watcherService = watcherService;
        this.requireManualStart = SETTING_REQUIRE_MANUAL_START.get(settings);
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

    synchronized void shutDown() {
        this.state.set(WatcherState.STOPPING);
        shutDown = true;
        clearAllocationIds();
        watcherService.shutDown();
        this.state.set(WatcherState.STOPPED);
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
            clearAllocationIds();
            // wait until the gateway has recovered from disk, otherwise we think may not have .watches and
            // a .triggered_watches index, but they may not have been restored from the cluster state on disk
            return;
        }

        // if watcher should not be started immediately unless it is has been manually configured to do so
        WatcherMetaData watcherMetaData = event.state().getMetaData().custom(WatcherMetaData.TYPE);
        if (watcherMetaData == null && requireManualStart) {
            clearAllocationIds();
            return;
        }

        if (Strings.isNullOrEmpty(event.state().nodes().getMasterNodeId())) {
            pauseExecution("no master node");
            return;
        }

        if (event.state().getBlocks().hasGlobalBlock(ClusterBlockLevel.WRITE)) {
            pauseExecution("write level cluster block");
            return;
        }

        boolean isWatcherStoppedManually = isWatcherStoppedManually(event.state());
        // if this is not a data node, we need to start it ourselves possibly
        if (event.state().nodes().getLocalNode().isDataNode() == false &&
            isWatcherStoppedManually == false && this.state.get() == WatcherState.STOPPED) {
            this.state.set(WatcherState.STARTING);
            watcherService.start(event.state(), () -> this.state.set(WatcherState.STARTED));
            return;
        }

        if (isWatcherStoppedManually) {
            if (this.state.get() == WatcherState.STARTED) {
                clearAllocationIds();
                watcherService.stop("watcher manually marked to shutdown by cluster state update");
                this.state.set(WatcherState.STOPPED);
            }
            return;
        }

        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        RoutingNode routingNode = event.state().getRoutingNodes().node(localNode.getId());
        if (routingNode == null) {
            pauseExecution("routing node in cluster state undefined. network issue?");
            return;
        }

        IndexMetaData watcherIndexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, event.state().metaData());
        if (watcherIndexMetaData == null) {
            pauseExecution("no watcher index found");
            return;
        }

        String watchIndex = watcherIndexMetaData.getIndex().getName();
        List<ShardRouting> localShards = routingNode.shardsWithState(watchIndex, RELOCATING, STARTED);
        // no local shards, empty out watcher and dont waste resources!
        if (localShards.isEmpty()) {
            pauseExecution("no local watcher shards found");
            return;
        }

        // also check if non local shards have changed, as loosing a shard on a
        // remote node or adding a replica on a remote node needs to trigger a reload too
        Set<ShardId> localShardIds = localShards.stream().map(ShardRouting::shardId).collect(Collectors.toSet());
        List<ShardRouting> allShards = event.state().routingTable().index(watchIndex).shardsWithState(STARTED);
        allShards.addAll(event.state().routingTable().index(watchIndex).shardsWithState(RELOCATING));
        List<ShardRouting> localAffectedShardRoutings = allShards.stream()
            .filter(shardRouting -> localShardIds.contains(shardRouting.shardId()))
            // shardrouting is not comparable, so we need some order mechanism
            .sorted(Comparator.comparing(ShardRouting::hashCode))
            .collect(Collectors.toList());

        if (previousShardRoutings.get().equals(localAffectedShardRoutings) == false) {
            if (watcherService.validate(event.state())) {
                previousShardRoutings.set(localAffectedShardRoutings);
                if (state.get() == WatcherState.STARTED) {
                    watcherService.reload(event.state(), "new local watcher shard allocation ids");
                } else if (state.get() == WatcherState.STOPPED) {
                    this.state.set(WatcherState.STARTING);
                    watcherService.start(event.state(), () -> this.state.set(WatcherState.STARTED));
                }
            } else {
                clearAllocationIds();
                this.state.set(WatcherState.STOPPED);
            }
        }
    }

    private void pauseExecution(String reason) {
        if (clearAllocationIds()) {
            watcherService.pauseExecution(reason);
        }
        this.state.set(WatcherState.STARTED);
    }

    /**
     * check if watcher has been stopped manually via the stop API
     */
    private boolean isWatcherStoppedManually(ClusterState state) {
        WatcherMetaData watcherMetaData = state.getMetaData().custom(WatcherMetaData.TYPE);
        return watcherMetaData != null && watcherMetaData.manuallyStopped();
    }

    /**
    /**
     * clear out current allocation ids if not already happened
     * @return true, if existing allocation ids were cleaned out, false otherwise
     */
    private boolean clearAllocationIds() {
        List<ShardRouting> previousIds = previousShardRoutings.getAndSet(Collections.emptyList());
        return previousIds.isEmpty() == false;
    }

    // for testing purposes only
    List<ShardRouting> shardRoutings() {
        return previousShardRoutings.get();
    }

    public WatcherState getState() {
        return state.get();
    }
}
