/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.stateless.cache.ShardWarmVolumes;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Map;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND;
import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED;

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
            boolean fetchVolumes = ShardWarmVolumes.shouldFetch(indexShard.routingEntry(), state)
                && shardWarmVolumes.claimFetch(state, relocatingNodeId);

            final long start = nowSupplier.getAsLong();
            TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(
                relocatingNodeId,
                indexShard.shardId()
            );

            ActionListener<TransportFetchSearchShardInformationAction.Response> responseListener = ActionListener.wrap(response -> {
                long lastSearcherAcquiredTime = response.getLastSearcherAcquiredTime();
                if (lastSearcherAcquiredTime == NO_OTHER_SHARDS_FOUND) {
                    return;
                }

                if (lastSearcherAcquiredTime == SHARD_HAS_MOVED) {
                    collector.shardMoved();
                    logger.trace("shard was moved before searcher could be acquired for shard [{}]", indexShard.shardId());
                    return;
                }

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
            if (fetchVolumes) {
                final String sourceNodeId = relocatingNodeId;
                ActionListener<TransportFetchShardWarmVolumesAction.Response> volumeListener = ActionListener.wrap(response -> {
                    long bytes = 0L;
                    for (long volume : response.volumes().values()) {
                        bytes += volume;
                    }
                    logger.info(
                        "fetched warm volumes from [{}] generation [{}] shards [{}] bytes [{}]",
                        response.respondingNodeId(),
                        response.volumesGeneration(),
                        response.volumes().size(),
                        bytes
                    );
                    collector.recordWarmVolumeFetch("received");
                    shardWarmVolumes.completeFetch(
                        clusterService.state(),
                        sourceNodeId,
                        response.respondingNodeId(),
                        response.volumesGeneration(),
                        response.volumes()
                    );
                }, e -> {
                    shardWarmVolumes.releaseClaim(sourceNodeId);
                    if (ExceptionsHelper.unwrapCause(e) instanceof ActionNotFoundTransportException) {
                        logger.info("warm volume fetch skipped, node [{}] does not have the action", sourceNodeId);
                        collector.recordWarmVolumeFetch("not_found");
                    } else {
                        logger.warn("could not retrieve warm volumes from node [" + sourceNodeId + "]", e);
                        collector.recordWarmVolumeFetch("error");
                    }
                });
                try {
                    client.execute(
                        TransportFetchShardWarmVolumesAction.TYPE,
                        new TransportFetchShardWarmVolumesAction.Request(sourceNodeId),
                        volumeListener
                    );
                } catch (Exception e) {
                    volumeListener.onFailure(e);
                }
            }
            try {
                client.execute(TransportFetchSearchShardInformationAction.TYPE, request, responseListener);
            } catch (Exception e) {
                responseListener.onFailure(e);
            }

            return null;
        });
    }
}
