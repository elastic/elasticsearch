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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class WatcherLifeCycleService extends AbstractComponent implements ClusterStateListener {

    // this option configures watcher not to start, unless the cluster state contains information to start watcher
    // if you start with an empty cluster, you can delay starting watcher until you call the API manually
    // if you start with a cluster containing data, this setting might have no effect, once you called the API yourself
    // this is merely for testing, to make sure that watcher only starts when manually called
    public static final Setting<Boolean> SETTING_REQUIRE_MANUAL_START =
            Setting.boolSetting("xpack.watcher.require_manual_start", false, Property.NodeScope);

    private static final String LIFECYCLE_THREADPOOL_NAME = "watcher-lifecycle";

    private final WatcherService watcherService;
    private final ExecutorService executor;
    private AtomicReference<List<String>> previousAllocationIds = new AtomicReference<>(Collections.emptyList());
    private volatile boolean shutDown = false; // indicates that the node has been shutdown and we should never start watcher after this.
    private final boolean requireManualStart;

    WatcherLifeCycleService(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                            WatcherService watcherService) {
        // use a single thread executor so that lifecycle changes are handled in the order they
        // are submitted in
        this(settings, clusterService, watcherService, EsExecutors.newFixed(
                LIFECYCLE_THREADPOOL_NAME,
                1,
                1000,
                daemonThreadFactory(settings, LIFECYCLE_THREADPOOL_NAME),
                threadPool.getThreadContext()));
    }

    WatcherLifeCycleService(Settings settings, ClusterService clusterService,
                            WatcherService watcherService, ExecutorService executorService) {
        super(settings);
        this.executor = executorService;
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
        stopExecutor();
    }

    void stopExecutor() {
        ThreadPool.terminate(executor, 10L, TimeUnit.SECONDS);
    }

    private synchronized void start(ClusterState state) {
        if (shutDown) {
            return;
        }
        final WatcherState watcherState = watcherService.state();
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
            final WatcherState watcherState = watcherService.state();
            if (watcherState == WatcherState.STARTED && event.state().nodes().getLocalNode().isDataNode()) {
                checkAndSetAllocationIds(event.state(), true);
            } else if (watcherState != WatcherState.STARTED && watcherState != WatcherState.STARTING) {
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
                executor.execute(wrapWatcherService(() -> watcherService.pauseExecution("no watcher index found"),
                        e -> logger.error("error pausing watch execution", e)));
            }
            return;
        }

        DiscoveryNode localNode = state.nodes().getLocalNode();
        RoutingNode routingNode = state.getRoutingNodes().node(localNode.getId());
        // this can happen if the node does not hold any data
        if (routingNode == null) {
            if (clearAllocationIds() && callWatcherService) {
                executor.execute(wrapWatcherService(
                        () -> watcherService.pauseExecution("no routing node for local node found, network issue?"),
                        e -> logger.error("error pausing watch execution", e)));
            }
            return;
        }

        String watchIndex = watcherIndexMetaData.getIndex().getName();
        List<ShardRouting> localShards = routingNode.shardsWithState(watchIndex, RELOCATING, STARTED);
        // no local shards, empty out watcher and dont waste resources!
        if (localShards.isEmpty()) {
            if (clearAllocationIds() && callWatcherService) {
                executor.execute(wrapWatcherService(() -> watcherService.pauseExecution("no local watcher shards found"),
                        e -> logger.error("error pausing watch execution", e)));
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
                executor.execute(wrapWatcherService(() -> watcherService.reload(state, "new local watcher shard allocation ids"),
                        e -> logger.error("error reloading watcher", e)));
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

    /**
     * Wraps an abstract runnable to easier supply onFailure and doRun methods via lambdas
     * This ensures that the uncaught exception handler in the executing threadpool does not get called
     *
     * @param run                 The code to be executed in the runnable
     * @param exceptionConsumer   The exception handling code to be executed, if the runnable fails
     * @return                    The AbstractRunnable instance to pass to the executor
     */
    private static AbstractRunnable wrapWatcherService(Runnable run, Consumer<Exception> exceptionConsumer) {

        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                exceptionConsumer.accept(e);
            }

            @Override
            protected void doRun() throws Exception {
                run.run();
            }
        };
    }
}
