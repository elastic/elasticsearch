/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Map;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE;

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
 *
 * As only a single instance of this class exists, no state should be shared in here
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
    private volatile boolean active = FEATURE_FLAG_QUERY_SEARCH_SHARD_INFORMATION.isEnabled();

    @SuppressWarnings("this-escape")
    public SearchShardInformationIndexListener(
        Client client,
        SearchShardInformationMetricsCollector collector,
        ClusterSettings clusterSettings,
        LongSupplier nowSupplier
    ) {
        this.client = client;
        this.collector = collector;
        this.nowSupplier = nowSupplier;
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

            final long start = nowSupplier.getAsLong();
            TransportFetchSearchShardInformationAction.Request request = new TransportFetchSearchShardInformationAction.Request(
                relocatingNodeId,
                indexShard.shardId()
            );

            client.execute(TransportFetchSearchShardInformationAction.TYPE, request, ActionListener.wrap(response -> {
                if (NO_OTHER_SHARDS_FOUND_RESPONSE.equals(response)) {
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
                }, e -> { logger.error("error trying to set last acquired searcher data for shard [" + indexShard.shardId() + "]", e); }));

            }, e -> {
                if (e instanceof ShardNotFoundException exc) {
                    logger.trace("shard was moved before searcher could be acquired for shard [{}]", exc.getShardId());
                    collector.shardMoved();
                } else {
                    logger.error("error retrieving search shard information data for shard [" + indexShard.shardId() + "]", e);
                    collector.recordError();
                }
            }));

            return null;
        });
    }
}
