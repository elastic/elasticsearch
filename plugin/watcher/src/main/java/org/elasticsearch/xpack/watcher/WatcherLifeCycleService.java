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
import org.elasticsearch.cluster.routing.AllocationId;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.upgrade.UpgradeField;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
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

    private final WatcherService watcherService;
    private final ExecutorService executor;
    private AtomicReference<List<String>> previousAllocationIds = new AtomicReference<>(Collections.emptyList());
    private volatile boolean shutDown = false; // indicates that the node has been shutdown and we should never start watcher after this.
    private final boolean requireManualStart;

    WatcherLifeCycleService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                            WatcherService watcherService) {
        super(settings);
        this.executor = threadPool.executor(ThreadPool.Names.GENERIC);
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

    public synchronized void stop(String reason) {
        watcherService.stop(reason);
    }

    synchronized void shutDown() {
        shutDown = true;
        stop("shutdown initiated");
    }

    private synchronized void start(ClusterState state) {
        if (shutDown) {
            return;
        }
        WatcherState watcherState = watcherService.state();
        if (watcherState != WatcherState.STOPPED) {
            logger.debug("not starting watcher. watcher can only start if its current state is [{}], but its current state now is [{}]",
                    WatcherState.STOPPED, watcherState);
            return;
        }

        // If we start from a cluster state update we need to check if previously we stopped manually
        // otherwise Watcher would start upon the next cluster state update while the user instructed Watcher to not run
        WatcherMetaData watcherMetaData = state.getMetaData().custom(WatcherMetaData.TYPE);
        if (watcherMetaData != null && watcherMetaData.manuallyStopped()) {
            logger.debug("not starting watcher. watcher was stopped manually and therefore cannot be auto-started");
            return;
        }

        // ensure that templates are existing before starting watcher
        // the watcher index template registry is independent from watcher being started or stopped
        if (WatcherIndexTemplateRegistry.validate(state) == false) {
            logger.debug("not starting watcher, watcher templates are missing in the cluster state");
            return;
        }

        if (watcherService.validate(state)) {
            logger.trace("starting... (based on cluster state version [{}])", state.getVersion());
            try {
                // we need to populate the allocation ids before the next cluster state listener comes in
                checkAndSetAllocationIds(state, false);
                watcherService.start(state);
            } catch (Exception e) {
                logger.warn("failed to start watcher. please wait for the cluster to become ready or try to start Watcher manually", e);
            }
        } else {
            logger.debug("not starting watcher. because the cluster isn't ready yet to run watcher");
        }
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
            clearAllocationIds();
            executor.execute(() -> this.stop("no master node"));
            return;
        }

        if (event.state().getBlocks().hasGlobalBlock(ClusterBlockLevel.WRITE)) {
            clearAllocationIds();
            executor.execute(() -> this.stop("write level cluster block"));
            return;
        }

        if (isWatcherStoppedManually(event.state())) {
            clearAllocationIds();
            executor.execute(() -> this.stop("watcher manually marked to shutdown by cluster state update"));
        } else {
            if (watcherService.state() == WatcherState.STARTED && event.state().nodes().getLocalNode().isDataNode()) {
                checkAndSetAllocationIds(event.state(), true);
            } else if (watcherService.state() != WatcherState.STARTED && watcherService.state() != WatcherState.STARTING) {
                IndexMetaData watcherIndexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, event.state().metaData());
                IndexMetaData triggeredWatchesIndexMetaData = WatchStoreUtils.getConcreteIndex(TriggeredWatchStoreField.INDEX_NAME,
                        event.state().metaData());
                boolean isIndexInternalFormatWatchIndex = watcherIndexMetaData == null ||
                        UpgradeField.checkInternalIndexFormat(watcherIndexMetaData);
                boolean isIndexInternalFormatTriggeredWatchIndex = triggeredWatchesIndexMetaData == null ||
                        UpgradeField.checkInternalIndexFormat(triggeredWatchesIndexMetaData);
                if (isIndexInternalFormatTriggeredWatchIndex && isIndexInternalFormatWatchIndex) {
                    checkAndSetAllocationIds(event.state(), false);
                    executor.execute(() -> start(event.state()));
                } else {
                    logger.warn("not starting watcher, upgrade API run required: .watches[{}], .triggered_watches[{}]",
                            isIndexInternalFormatWatchIndex, isIndexInternalFormatTriggeredWatchIndex);
                }
            }
        }
    }

    /**
     * check if watcher has been stopped manually via the stop API
     */
    private boolean isWatcherStoppedManually(ClusterState state) {
        WatcherMetaData watcherMetaData = state.getMetaData().custom(WatcherMetaData.TYPE);
        return watcherMetaData != null && watcherMetaData.manuallyStopped();
    }

    /**
     * check and optionally set the current allocation ids
     *
     * @param state               the current cluster state
     * @param callWatcherService  should the watcher service be called for starting/stopping/reloading or should this be treated as a
     *                            dryrun so that the caller is responsible for this
     */
    private void checkAndSetAllocationIds(ClusterState state, boolean callWatcherService) {
        IndexMetaData watcherIndexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metaData());
        if (watcherIndexMetaData == null) {
            if (clearAllocationIds() && callWatcherService) {
                executor.execute(() -> watcherService.pauseExecution("no watcher index found"));
            }
            return;
        }

        DiscoveryNode localNode = state.nodes().getLocalNode();
        RoutingNode routingNode = state.getRoutingNodes().node(localNode.getId());
        // this can happen if the node does not hold any data
        if (routingNode == null) {
            if (clearAllocationIds() && callWatcherService) {
                executor.execute(() -> watcherService.pauseExecution("no routing node for local node found, network issue?"));
            }
            return;
        }

        String watchIndex = watcherIndexMetaData.getIndex().getName();
        List<ShardRouting> localShards = routingNode.shardsWithState(watchIndex, RELOCATING, STARTED);
        // no local shards, empty out watcher and dont waste resources!
        if (localShards.isEmpty()) {
            if (clearAllocationIds() && callWatcherService) {
                executor.execute(() -> watcherService.pauseExecution("no local watcher shards found"));
            }
            return;
        }

        List<String> currentAllocationIds = localShards.stream()
                .map(ShardRouting::allocationId)
                .map(AllocationId::getId)
                .collect(Collectors.toList());
        Collections.sort(currentAllocationIds);

        if (previousAllocationIds.get().equals(currentAllocationIds) == false) {
            previousAllocationIds.set(Collections.unmodifiableList(currentAllocationIds));
            if (callWatcherService) {
                executor.execute(() -> watcherService.reload(state, "new local watcher shard allocation ids"));
            }
        }
    }

    /**
     * clear out current allocation ids if not already happened
     * @return true, if existing allocation ids were cleaned out, false otherwise
     */
    private boolean clearAllocationIds() {
        List<String> previousIds = previousAllocationIds.getAndSet(Collections.emptyList());
        return previousIds.equals(Collections.emptyList()) == false;
    }

    // for testing purposes only
    List<String> allocationIds() {
        return previousAllocationIds.get();
    }
}
