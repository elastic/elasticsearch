/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatch;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalState;
import static org.elasticsearch.xpack.watcher.watch.Watch.INDEX;


public class WatcherService extends AbstractComponent {

    private final TriggerService triggerService;
    private final TriggeredWatchStore triggeredWatchStore;
    private final ExecutionService executionService;
    private final TimeValue scrollTimeout;
    private final int scrollSize;
    private final Watch.Parser parser;
    private final Client client;
    // package-private for testing
    final AtomicReference<WatcherState> state = new AtomicReference<>(WatcherState.STOPPED);
    private final TimeValue defaultSearchTimeout;

    public WatcherService(Settings settings, TriggerService triggerService, TriggeredWatchStore triggeredWatchStore,
                          ExecutionService executionService, Watch.Parser parser, InternalClient client) {
        super(settings);
        this.triggerService = triggerService;
        this.triggeredWatchStore = triggeredWatchStore;
        this.executionService = executionService;
        this.scrollTimeout = settings.getAsTime("xpack.watcher.watch.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("xpack.watcher.watch.scroll.size", 100);
        this.defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        this.parser = parser;
        this.client = client;
    }

    /**
     * Ensure that watcher can be started, by checking if all indices are marked as up and ready in the cluster state
     * @param state The current cluster state
     * @return true if everything is good to go, so that the service can be started
     */
    public boolean validate(ClusterState state) {
        boolean executionServiceValid = executionService.validate(state);
        if (executionServiceValid) {
            try {
                IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(Watch.INDEX, state.metaData());
                // no watch index yet means we are good to go
                if (indexMetaData == null) {
                    return true;
                } else {
                    if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                        logger.debug("watch index [{}] is marked as closed, watcher cannot be started", indexMetaData.getIndex().getName());
                        return false;
                    } else {
                        return state.routingTable().index(indexMetaData.getIndex()).allPrimaryShardsActive();
                    }
                }
            } catch (IllegalStateException e) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("error getting index meta data [{}]: ", Watch.INDEX), e);
                return false;
            }
        }

        return false;
    }

    public void start(ClusterState clusterState) throws Exception {
        // starting already triggered, exit early
        WatcherState currentState = state.get();
        if (currentState == WatcherState.STARTING || currentState == WatcherState.STARTED) {
            throw new IllegalStateException("watcher is already in state ["+ currentState +"]");
        }

        if (state.compareAndSet(WatcherState.STOPPED, WatcherState.STARTING)) {
            try {
                logger.debug("starting watch service...");

                executionService.start();
                Collection<Watch> watches = loadWatches(clusterState);
                triggerService.start(watches);

                Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches, clusterState);
                executionService.executeTriggeredWatches(triggeredWatches);

                state.set(WatcherState.STARTED);
                logger.debug("watch service has started");
            } catch (Exception e) {
                state.set(WatcherState.STOPPED);
                throw e;
            }
        }
    }

    /**
     * Stops the watcher service and it's subservices. Should only be called, when watcher is stopped manually
     */
    public void stop(String reason) {
        WatcherState currentState = state.get();
        if (currentState == WatcherState.STOPPING || currentState == WatcherState.STOPPED) {
            logger.trace("watcher is already in state [{}] not stopping", currentState);
        } else {
            try {
                if (state.compareAndSet(WatcherState.STARTED, WatcherState.STOPPING)) {
                    logger.debug("stopping watch service, reason [{}]", reason);
                    triggerService.stop();
                    executionService.stop();
                    state.set(WatcherState.STOPPED);
                    logger.debug("watch service has stopped");
                }
            } catch (Exception e) {
                state.set(WatcherState.STOPPED);
                logger.error("Error stopping watcher", e);
            }
        }
    }

    /**
     * Reload the watcher service, does not switch the state from stopped to started, just keep going
     * @param clusterState cluster state, which is needed to find out about local shards
     */
    public void reload(ClusterState clusterState, String reason) {
        pauseExecution(reason);

        // load watches
        Collection<Watch> watches = loadWatches(clusterState);
        watches.forEach(triggerService::add);

        // then load triggered watches, which might have been in the queue that we just cleared,
        // maybe we dont need to execute those anymore however, i.e. due to shard shuffling
        // then someone else will
        Collection<TriggeredWatch> triggeredWatches = triggeredWatchStore.findTriggeredWatches(watches, clusterState);
        executionService.executeTriggeredWatches(triggeredWatches);
    }

    /**
     * Stop execution of watches on this node, do not try to reload anything, but still allow
     * manual watch execution, i.e. via the execute watch API
     */
    public void pauseExecution(String reason) {
        int cancelledTaskCount = executionService.pauseExecution();
        triggerService.pauseExecution();
        logger.debug("paused execution service, reason [{}], cancelled [{}] queued tasks", reason, cancelledTaskCount);
    }

    /**
     * This reads all watches from the .watches index/alias and puts them into memory for a short period of time,
     * before they are fed into the trigger service.
     */
    private Collection<Watch> loadWatches(ClusterState clusterState) {
        IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(INDEX, clusterState.metaData());
        // no index exists, all good, we can start
        if (indexMetaData == null) {
            return Collections.emptyList();
        }

        RefreshResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(INDEX))
                .actionGet(TimeValue.timeValueSeconds(5));
        if (refreshResponse.getSuccessfulShards() < indexMetaData.getNumberOfShards()) {
            throw illegalState("not all required shards have been refreshed");
        }

        // find out local shards
        String watchIndexName = indexMetaData.getIndex().getName();
        RoutingNode routingNode = clusterState.getRoutingNodes().node(clusterState.nodes().getLocalNodeId());
        // yes, this can happen, if the state is not recovered
        if (routingNode == null) {
            return Collections.emptyList();
        }
        List<ShardRouting> localShards = routingNode.shardsWithState(watchIndexName, RELOCATING, STARTED);

        // find out all allocation ids
        List<ShardRouting> watchIndexShardRoutings = clusterState.getRoutingTable().allShards(watchIndexName);

        List<Watch> watches = new ArrayList<>();

        SearchRequest searchRequest = new SearchRequest(INDEX)
                .scroll(scrollTimeout)
                .preference(Preference.ONLY_LOCAL.toString())
                .source(new SearchSourceBuilder()
                        .size(scrollSize)
                        .sort(SortBuilders.fieldSort("_doc"))
                        .version(true));
        SearchResponse response = client.search(searchRequest).actionGet(defaultSearchTimeout);
        try {
            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading watches");
            }

            if (response.getHits().getTotalHits() == 0) {
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
                        Watch watch = parser.parse(id, true, hit.getSourceRef(), XContentType.JSON);
                        watch.version(hit.getVersion());
                        watches.add(watch);
                    } catch (Exception e) {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("couldn't load watch [{}], ignoring it...", id), e);
                    }
                }
                SearchScrollRequest request = new SearchScrollRequest(response.getScrollId());
                request.scroll(scrollTimeout);
                response = client.searchScroll(request).actionGet(defaultSearchTimeout);
            }
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(response.getScrollId());
            client.clearScroll(clearScrollRequest).actionGet(scrollTimeout);
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

    public WatcherState state() {
        return state.get();
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> innerMap = executionService.usageStats();
        return innerMap;
    }
}
