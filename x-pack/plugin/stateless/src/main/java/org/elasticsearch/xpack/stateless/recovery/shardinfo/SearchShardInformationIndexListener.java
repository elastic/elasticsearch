/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.xpack.stateless.cache.ShardWarmVolumes;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Map;
import java.util.function.LongSupplier;

/**
 * An IndexEventListener to retrieve state from other shard copies
 *
 * When a shard is moved around in the cluster, the search commit prefetcher stops working on the freshly copied shard, because no
 * searcher has been acquired yet - or unless a new search is executed.
 * This is unwanted behavior. A simple solution is to query other shards about their last time when a searcher was acquired and use that
 * time locally as well.
 * This is exactly the idea of this index listener, which takes care of two cases:
 *
 * Relocation of a shard from one node to another: A request is sent to the node the relocation is coming from.
 * Relocation information is not set when adding a replica, so all nodes with shard copies are queried
 */
public class SearchShardInformationIndexListener implements IndexEventListener {

    private static final FeatureFlag FEATURE_FLAG_QUERY_SEARCH_SHARD_INFORMATION = new FeatureFlag("query_search_shard_information");

    public static final Setting<Boolean> QUERY_SEARCH_SHARD_INFORMATION_SETTING = Setting.boolSetting(
        "stateless.search.query_search_shard_information.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(SearchShardInformationIndexListener.class);

    private final Client client;
    private final SearchShardInformationMetricsCollector collector;
    private final LongSupplier nowSupplier;
    private final ClusterService clusterService;
    private final ShardWarmVolumes shardWarmVolumes;
    private volatile boolean active = FEATURE_FLAG_QUERY_SEARCH_SHARD_INFORMATION.isEnabled();

    @SuppressWarnings("this-escape")
    public SearchShardInformationIndexListener(
        Client client,
        SearchShardInformationMetricsCollector collector,
        ClusterSettings clusterSettings,
        LongSupplier nowSupplier,
        ClusterService clusterService,
        ShardWarmVolumes shardWarmVolumes
    ) {
        this.client = client;
        this.collector = collector;
        this.nowSupplier = nowSupplier;
        this.clusterService = clusterService;
        this.shardWarmVolumes = shardWarmVolumes;
        clusterSettings.initializeAndWatch(QUERY_SEARCH_SHARD_INFORMATION_SETTING, active -> this.active = active);
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            if (active == false) {
                return null;
            }

            // if relocation from another node is in the routing entry, this is the best source of information, no need to ask other shards
            String relocatingNodeId = indexShard.routingEntry().relocatingNodeId();

            final var state = clusterService.state();
            final boolean relocatingFromNodeMarkedForRemoval = isRelocatingFromNodeMarkedForRemoval(indexShard.routingEntry(), state);
            final long claimedShutdownSignalTimestamp = relocatingFromNodeMarkedForRemoval
                ? state.metadata().nodeShutdowns().get(relocatingNodeId).getStartedAtMillis()
                : 0L;
            final boolean shouldFetchSourceNodeShardWarmVolumes = relocatingFromNodeMarkedForRemoval
                && shardWarmVolumes.claimFetch(state, relocatingNodeId);

            final long start = nowSupplier.getAsLong();
            TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(
                relocatingNodeId,
                indexShard.shardId(),
                shouldFetchSourceNodeShardWarmVolumes,
                claimedShutdownSignalTimestamp
            );

            ActionListener<TransportFetchSearchShardInformationAction.Response> fetchListener = ActionListener.wrap(response -> {
                if (shouldFetchSourceNodeShardWarmVolumes && response.volumesCollected()) {
                    shardWarmVolumes.completeFetch(
                        clusterService.state(),
                        response.respondingNodeId(),
                        response.shutdownSignalTimestamp(),
                        response.volumes()
                    );
                }

                if (response.noOtherShardsFound()) {
                    return;
                }

                if (response.shardHasMoved()) {
                    collector.shardMoved();
                    logger.trace("shard was moved before searcher could be acquired for shard [{}]", indexShard.shardId());
                    return;
                }

                long lastSearcherAcquiredTime = response.getLastSearcherAcquiredTime();
                var attributes = Map.<String, Object>of("es_search_last_searcher_acquired_greater_zero", lastSearcherAcquiredTime > 0);
                collector.recordSuccess(nowSupplier.getAsLong() - start, attributes);

                if (lastSearcherAcquiredTime <= 0) {
                    return;
                }

                indexShard.waitForEngineOrClosedShard(ActionListener.wrap(r -> {
                    if (indexShard.state() != IndexShardState.CLOSED) {
                        indexShard.tryWithEngineOrNull(engine -> {
                            if (engine instanceof SearchEngine searchEngine) {
                                searchEngine.setLastSearcherAcquiredTime(lastSearcherAcquiredTime);
                            }
                            return null;
                        });
                    }
                }, e -> { logger.warn("could not set last acquired searcher data for shard [" + indexShard.shardId() + "]", e); }));

            }, e -> {
                logger.warn("could not retrieve search shard information data for shard [" + indexShard.shardId() + "]", e);
                collector.recordError();
            });
            ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = shouldFetchSourceNodeShardWarmVolumes
                ? ActionListener.runAfter(
                    fetchListener,
                    () -> shardWarmVolumes.releaseClaim(relocatingNodeId, claimedShutdownSignalTimestamp)
                )
                : fetchListener;
            try {
                client.execute(TransportFetchSearchShardInformationAction.TYPE, request, responseListener);
            } catch (Exception e) {
                responseListener.onFailure(e);
            }

            return null;
        });
    }

    /**
     * True when {@code routing} is a relocation whose source is marked for removal.
     */
    static boolean isRelocatingFromNodeMarkedForRemoval(ShardRouting routing, ClusterState state) {
        String sourceId = routing.relocatingNodeId();
        return sourceId != null && state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceId);
    }
}
