/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.upgrade.UpgradeField;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;
import static org.elasticsearch.xpack.core.watcher.watch.Watch.INDEX;

public class WatcherService {

    private static final String LIFECYCLE_THREADPOOL_NAME = "watcher-lifecycle";
    private static final Logger logger = LogManager.getLogger(WatcherService.class);

    private final TriggerService triggerService;
    private final TriggeredWatchStore triggeredWatchStore;
    private final ExecutionService executionService;
    private final TimeValue scrollTimeout;
    private final int scrollSize;
    private final WatchParser parser;
    private final Client client;
    private final TimeValue defaultSearchTimeout;
    private final AtomicLong processedClusterStateVersion = new AtomicLong(0);
    private final ExecutorService executor;

    WatcherService(Settings settings, TriggerService triggerService, TriggeredWatchStore triggeredWatchStore,
                   ExecutionService executionService, WatchParser parser, Client client, ExecutorService executor) {
        this.triggerService = triggerService;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executionService = executionService;
        this.scrollTimeout = settings.getAsTime("xpack.watcher.watch.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("xpack.watcher.watch.scroll.size", 100);
        this.defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        this.parser = parser;
        this.client = ClientHelper.clientWithOrigin(client, WATCHER_ORIGIN);
        this.executor = executor;
    }

    WatcherService(Settings settings, TriggerService triggerService, TriggeredWatchStore triggeredWatchStore,
                   ExecutionService executionService, WatchParser parser, Client client) {
        this(settings, triggerService, triggeredWatchStore, executionService, parser, client,
            EsExecutors.newFixed(LIFECYCLE_THREADPOOL_NAME, 1, 1000, daemonThreadFactory(settings, LIFECYCLE_THREADPOOL_NAME),
                client.threadPool().getThreadContext(), false));
    }

    /**
     * Ensure that watcher can be reloaded, by checking if all indices are marked as up and ready in the cluster state
     * @param state The current cluster state
     * @return true if everything is good to go, so that the service can be started
     */
    public boolean validate(ClusterState state) {
        // template check makes only sense for non existing indices, we could refine this
        boolean hasValidWatcherTemplates = WatcherIndexTemplateRegistry.validate(state);
        if (hasValidWatcherTemplates == false) {
            logger.debug("missing watcher index templates, not starting watcher service");
            return false;
        }

        IndexMetadata watcherIndexMetadata = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metadata());
        IndexMetadata triggeredWatchesIndexMetadata = WatchStoreUtils.getConcreteIndex(TriggeredWatchStoreField.INDEX_NAME,
            state.metadata());
        boolean isIndexInternalFormatWatchIndex = watcherIndexMetadata == null ||
            UpgradeField.checkInternalIndexFormat(watcherIndexMetadata);
        boolean isIndexInternalFormatTriggeredWatchIndex = triggeredWatchesIndexMetadata == null ||
            UpgradeField.checkInternalIndexFormat(triggeredWatchesIndexMetadata);
        if (isIndexInternalFormatTriggeredWatchIndex == false || isIndexInternalFormatWatchIndex == false) {
            logger.warn("not starting watcher, upgrade API run required: .watches[{}], .triggered_watches[{}]",
                isIndexInternalFormatWatchIndex, isIndexInternalFormatTriggeredWatchIndex);
            return false;
        }

        try {
            boolean storesValid = TriggeredWatchStore.validate(state) && HistoryStore.validate(state);
            if (storesValid == false) {
                return false;
            }

            return watcherIndexMetadata == null || (watcherIndexMetadata.getState() == IndexMetadata.State.OPEN &&
                state.routingTable().index(watcherIndexMetadata.getIndex()).allPrimaryShardsActive());
        } catch (IllegalStateException e) {
            logger.debug("error validating to start watcher", e);
            return false;
        }
    }

    /**
     * Stops the watcher service and marks its services as paused. Callers should set the Watcher state to {@link WatcherState#STOPPING}
     * prior to calling this method.
     *
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may not be {@code null}
     */
    public void stop(String reason, Runnable stoppedListener) {
        assert stoppedListener != null;
        logger.info("stopping watch service, reason [{}]", reason);
        executionService.pause(stoppedListener);
        triggerService.pauseExecution();
    }

    /**
     * shuts down the trigger service as well to make sure there are no lingering threads
     *
     * @param stoppedListener The listener that will set Watcher state to: {@link WatcherState#STOPPED}, may not be {@code null}
     */
    void shutDown(Runnable stoppedListener) {
        assert stoppedListener != null;
        logger.info("stopping watch service, reason [shutdown initiated]");
        executionService.pause(stoppedListener);
        triggerService.stop();
        stopExecutor();
    }

    void stopExecutor() {
        ThreadPool.terminate(executor, 10L, TimeUnit.SECONDS);
    }

    /**
     * Reload the watcher service, does not switch the state from stopped to started, just keep going
     * @param state cluster state, which is needed to find out about local shards
     */
    void reload(ClusterState state, String reason) {
        // this method contains the only async code block, being called by the cluster state listener
        // the reason for this is, that loading he watches is done in a sync manner and thus cannot be done on the cluster state listener
        // thread
        //
        // this method itself is called by the cluster state listener, so will never be called in parallel
        // setting the cluster state version allows us to know if the async method has been overtaken by another async method
        // this is unlikely, but can happen, if the thread pool schedules two of those runnables at the same time
        // by checking the cluster state version before and after loading the watches we can potentially just exit without applying the
        // changes
        processedClusterStateVersion.set(state.getVersion());

        triggerService.pauseExecution();
        int cancelledTaskCount = executionService.clearExecutionsAndQueue(() -> {});
        logger.info("reloading watcher, reason [{}], cancelled [{}] queued tasks", reason, cancelledTaskCount);

        executor.execute(wrapWatcherService(() -> reloadInner(state, reason, false),
            e -> logger.error("error reloading watcher", e)));
    }

    /**
     * start the watcher service, load watches in the background
     *
     * @param state                     the current cluster state
     * @param postWatchesLoadedCallback the callback to be triggered, when watches where loaded successfully
     */
    public void start(ClusterState state, Runnable postWatchesLoadedCallback) {
        executionService.unPause();
        processedClusterStateVersion.set(state.getVersion());
        executor.execute(wrapWatcherService(() -> {
                if (reloadInner(state, "starting", true)) {
                    postWatchesLoadedCallback.run();
                }
            },
            e -> logger.error("error starting watcher", e)));
    }

    /**
     * reload watches and start scheduling them
     *
     * @param state                 the current cluster state
     * @param reason                the reason for reloading, will be logged
     * @param loadTriggeredWatches  should triggered watches be loaded in this run, not needed for reloading, only for starting
     * @return                      true if no other loading of a newer cluster state happened in parallel, false otherwise
     */
    private synchronized boolean reloadInner(ClusterState state, String reason, boolean loadTriggeredWatches) {
        // exit early if another thread has come in between
        if (processedClusterStateVersion.get() != state.getVersion()) {
            logger.debug("watch service has not been reloaded for state [{}], another reload for state [{}] in progress",
                state.getVersion(), processedClusterStateVersion.get());
            return false;
        }

        Collection<Watch> watches = loadWatches(state);
        Collection<TriggeredWatch> triggeredWatches = Collections.emptyList();
        if (loadTriggeredWatches) {
            triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches, state);
        }

        // if we had another state coming in the meantime, we will not start the trigger engines with these watches, but wait
        // until the others are loaded
        // also this is the place where we pause the trigger service execution and clear the current execution service, so that we make sure
        // that existing executions finish, but no new ones are executed
        if (processedClusterStateVersion.get() == state.getVersion()) {
            executionService.unPause();
            triggerService.start(watches);
            if (triggeredWatches.isEmpty() == false) {
                executionService.executeTriggeredWatches(triggeredWatches);
            }
            logger.debug("watch service has been reloaded, reason [{}]", reason);
            return true;
        } else {
            logger.debug("watch service has not been reloaded for state [{}], another reload for state [{}] in progress",
                state.getVersion(), processedClusterStateVersion.get());
            return false;
        }
    }

    /**
     * Stop execution of watches on this node, do not try to reload anything, but still allow
     * manual watch execution, i.e. via the execute watch API
     */
    public void pauseExecution(String reason) {
        triggerService.pauseExecution();
        int cancelledTaskCount = executionService.pause(() -> {});
        logger.info("paused watch execution, reason [{}], cancelled [{}] queued tasks", reason, cancelledTaskCount);
    }

    /**
     * This reads all watches from the .watches index/alias and puts them into memory for a short period of time,
     * before they are fed into the trigger service.
     */
    private Collection<Watch> loadWatches(ClusterState clusterState) {
        IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(INDEX, clusterState.metadata());
        // no index exists, all good, we can start
        if (indexMetadata == null) {
            return Collections.emptyList();
        }

        SearchResponse response = null;
        List<Watch> watches = new ArrayList<>();
        try {
            RefreshResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(INDEX))
                .actionGet(TimeValue.timeValueSeconds(5));
            if (refreshResponse.getSuccessfulShards() < indexMetadata.getNumberOfShards()) {
                throw illegalState("not all required shards have been refreshed");
            }

            // find out local shards
            String watchIndexName = indexMetadata.getIndex().getName();
            RoutingNode routingNode = clusterState.getRoutingNodes().node(clusterState.nodes().getLocalNodeId());
            // yes, this can happen, if the state is not recovered
            if (routingNode == null) {
                return Collections.emptyList();
            }
            List<ShardRouting> localShards = routingNode.shardsWithState(watchIndexName, RELOCATING, STARTED);

            // find out all allocation ids
            List<ShardRouting> watchIndexShardRoutings = clusterState.getRoutingTable().allShards(watchIndexName);

            SearchRequest searchRequest = new SearchRequest(INDEX)
                .scroll(scrollTimeout)
                .preference(Preference.ONLY_LOCAL.toString())
                .source(new SearchSourceBuilder()
                    .size(scrollSize)
                    .sort(SortBuilders.fieldSort("_doc"))
                    .seqNoAndPrimaryTerm(true));
            response = client.search(searchRequest).actionGet(defaultSearchTimeout);

            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading watches");
            }

            if (response.getHits().getTotalHits().value == 0) {
                return Collections.emptyList();
            }

            Map<Integer, List<String>> sortedShards = new HashMap<>(localShards.size());
            for (ShardRouting localShardRouting : localShards) {
                List<String> sortedAllocationIds = watchIndexShardRoutings.stream()
                    .filter(sr -> localShardRouting.getId() == sr.getId())
                    .map(ShardRouting::allocationId).filter(Objects::nonNull)
                    .map(AllocationId::getId).filter(Objects::nonNull)
                    .sorted()
                    .collect(Collectors.toList());

                sortedShards.put(localShardRouting.getId(), sortedAllocationIds);
            }

            while (response.getHits().getHits().length != 0) {
                for (SearchHit hit : response.getHits()) {
                    // find out if this hit should be processed locally
                    Optional<ShardRouting> correspondingShardOptional = localShards.stream()
                        .filter(sr -> sr.shardId().equals(hit.getShard().getShardId()))
                        .findFirst();
                    if (correspondingShardOptional.isPresent() == false) {
                        continue;
                    }
                    ShardRouting correspondingShard = correspondingShardOptional.get();
                    List<String> shardAllocationIds = sortedShards.get(hit.getShard().getShardId().id());
                    // based on the shard allocation ids, get the bucket of the shard, this hit was in
                    int bucket = shardAllocationIds.indexOf(correspondingShard.allocationId().getId());
                    String id = hit.getId();

                    if (parseWatchOnThisNode(hit.getId(), shardAllocationIds.size(), bucket) == false) {
                        continue;
                    }

                    try {
                        Watch watch = parser.parse(id, true, hit.getSourceRef(), XContentType.JSON, hit.getSeqNo(), hit.getPrimaryTerm());
                        if (watch.status().state().isActive()) {
                            watches.add(watch);
                        }
                    } catch (Exception e) {
                        logger.error((org.apache.logging.log4j.util.Supplier<?>)
                            () -> new ParameterizedMessage("couldn't load watch [{}], ignoring it...", id), e);
                    }
                }
                SearchScrollRequest request = new SearchScrollRequest(response.getScrollId());
                request.scroll(scrollTimeout);
                response = client.searchScroll(request).actionGet(defaultSearchTimeout);
            }
        } finally {
            if (response != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(response.getScrollId());
                client.clearScroll(clearScrollRequest).actionGet(scrollTimeout);
            }
        }

        logger.debug("Loaded [{}] watches for execution", watches.size());

        return watches;
    }

    /**
     * Find out if the watch with this id, should be parsed and triggered on this node
     *
     * @param id              The id of the watch
     * @param totalShardCount The count of all primary shards of the current watches index
     * @param index           The index of the local shard
     * @return true if the we should parse the watch on this node, false otherwise
     */
    private boolean parseWatchOnThisNode(String id, int totalShardCount, int index) {
        int hash = Murmur3HashFunction.hash(id);
        int shardIndex = Math.floorMod(hash, totalShardCount);
        return shardIndex == index;
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
