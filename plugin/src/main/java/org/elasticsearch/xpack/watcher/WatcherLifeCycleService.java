/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    private final WatcherService watcherService;
    private final ClusterService clusterService;
    private final ExecutorService executor;
    private AtomicReference<List<String>> previousAllocationIds = new AtomicReference<>(Collections.emptyList());
    private volatile WatcherMetaData watcherMetaData;

    public WatcherLifeCycleService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   WatcherService watcherService) {
        super(settings);
        this.executor = threadPool.executor(ThreadPool.Names.GENERIC);
        this.watcherService = watcherService;
        this.clusterService = clusterService;
        clusterService.addListener(this);
        // Close if the indices service is being stopped, so we don't run into search failures (locally) that will
        // happen because we're shutting down and an watch is scheduled.
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop("stopping before shutting down");
            }
        });
        watcherMetaData = new WatcherMetaData(!settings.getAsBoolean("xpack.watcher.start_immediately", true));
    }

    public void start() {
        start(clusterService.state(), true);
    }

    public void stop(String reason) {
        watcherService.stop(reason);
    }

    private synchronized void start(ClusterState state, boolean manual) {
        WatcherState watcherState = watcherService.state();
        if (watcherState != WatcherState.STOPPED) {
            logger.debug("not starting watcher. watcher can only start if its current state is [{}], but its current state now is [{}]",
                    WatcherState.STOPPED, watcherState);
            return;
        }

        // If we start from a cluster state update we need to check if previously we stopped manually
        // otherwise Watcher would start upon the next cluster state update while the user instructed Watcher to not run
        if (!manual && watcherMetaData != null && watcherMetaData.manuallyStopped()) {
            logger.debug("not starting watcher. watcher was stopped manually and therefore cannot be auto-started");
            return;
        }

        if (watcherService.validate(state)) {
            logger.trace("starting... (based on cluster state version [{}]) (manual [{}])", state.getVersion(), manual);
            try {
                watcherService.start(state);
            } catch (Exception e) {
                logger.warn("failed to start watcher. please wait for the cluster to become ready or try to start Watcher manually", e);
            }
        } else {
            logger.debug("not starting watcher. because the cluster isn't ready yet to run watcher");
        }
    }

    /**
     *
     * @param event The event of containing the new cluster state
     *
     * stop certain parts of watcher, when there are no watcher indices on this node by checking the shardrouting
     * note that this is not easily possible, because of the execute watch api, that needs to be able to execute anywhere!
     * this means, only certain components can be stopped
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .watches and
            // a .triggered_watches index, but they may not have been restored from the cluster state on disk
            return;
        }

        // find out if watcher was stopped or started manually due to this cluster state change
        WatcherMetaData watcherMetaData = event.state().getMetaData().custom(WatcherMetaData.TYPE);

        if (watcherMetaData != null) {
            this.watcherMetaData = watcherMetaData;
        }

        boolean currentWatcherStopped = watcherMetaData != null && watcherMetaData.manuallyStopped() == true;
        if (currentWatcherStopped) {
            executor.execute(() -> this.stop("watcher manually marked to shutdown in cluster state update, shutting down"));
        } else {
            if (watcherService.state() == WatcherState.STARTED && event.state().nodes().getLocalNode().isDataNode()) {
                DiscoveryNode localNode = event.state().nodes().getLocalNode();
                RoutingNode routingNode = event.state().getRoutingNodes().node(localNode.getId());
                IndexMetaData watcherIndexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, event.state().metaData());

                // no watcher index, time to pause, if we currently have shards here
                if (watcherIndexMetaData == null) {
                    if (previousAllocationIds.get().isEmpty() == false) {
                        previousAllocationIds.set(Collections.emptyList());
                        executor.execute(() -> watcherService.pauseExecution("no watcher index found"));
                    }
                    return;
                }

                String watchIndex = watcherIndexMetaData.getIndex().getName();
                List<ShardRouting> localShards = routingNode.shardsWithState(watchIndex, RELOCATING, STARTED);

                // no local shards, empty out watcher and not waste resources!
                if (localShards.isEmpty()) {
                    executor.execute(() -> watcherService.pauseExecution("no local watcher shards"));
                    previousAllocationIds.set(Collections.emptyList());
                    return;
                }

                List<String> currentAllocationIds = localShards.stream()
                        .map(ShardRouting::allocationId)
                        .map(AllocationId::getId)
                        .collect(Collectors.toList());
                Collections.sort(currentAllocationIds);

                if (previousAllocationIds.get().equals(currentAllocationIds) == false) {
                    previousAllocationIds.set(currentAllocationIds);
                    executor.execute(() -> watcherService.reload(event.state(), "different shard allocation ids"));
                }
            } else if (watcherService.state() != WatcherState.STARTED && watcherService.state() != WatcherState.STARTING) {
                if (isIndexInternalFormat(event.state().metaData(), Watch.INDEX) &&
                        isIndexInternalFormat(event.state().metaData(), TriggeredWatchStore.INDEX_NAME)) {
                    executor.execute(() -> start(event.state(), false));
                } else {
                    logger.error("Not starting watcher, the indices have not been upgraded yet. Please run the Upgrade API");
                }
            }
        }
    }

    /**
     * If the supplied index exists, ensure that the 'index.internal.format' setting is configured appropriately.
     * This ensures that watcher can use this index as needed.
     *
     * @param metaData  The cluster state meta data
     * @param index     The index name to check for. Aliases will be resolved properly.
     * @return          true if the index does not exist or the internal format is set properly
     */
    boolean isIndexInternalFormat(MetaData metaData, String index) {
        IndexMetaData watcherIndexMetaData = WatchStoreUtils.getConcreteIndex(index, metaData);
        if (watcherIndexMetaData == null) {
            return true;
        } else {
            return XPackPlugin.INDEX_INTERNAL_FORMAT_SETTING.get(watcherIndexMetaData.getSettings()) == XPackPlugin.INTERNAL_INDEX_FORMAT;
        }
    }

    public WatcherMetaData watcherMetaData() {
        return watcherMetaData;
    }
}
