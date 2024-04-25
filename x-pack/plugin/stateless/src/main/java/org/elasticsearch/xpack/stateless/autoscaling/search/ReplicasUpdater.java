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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;

class ReplicasUpdater {
    private static final Logger logger = LogManager.getLogger(ReplicasUpdater.class);

    /**
     * Search power setting below which we only have 1 replica for all interactive indices
     */
    public static final int SEARCH_POWER_MIN_NO_REPLICATION = 100;

    /**
     * Search power setting at which we want to replicate all interactive indices with a factor of 2
     */
    public static final int SEARCH_POWER_MIN_FULL_REPLICATION = 250;

    private final SearchMetricsService searchMetricsService;
    private final NodeClient client;

    private volatile int searchPowerMinSetting;
    private volatile int searchPowerMaxSetting;

    ReplicasUpdater(SearchMetricsService searchMetricsService, ClusterSettings clusterSettings, NodeClient client) {
        this.searchMetricsService = searchMetricsService;
        this.client = client;
        this.searchPowerMinSetting = clusterSettings.get(SEARCH_POWER_MIN_SETTING);
        this.searchPowerMaxSetting = clusterSettings.get(SEARCH_POWER_MAX_SETTING);
        clusterSettings.initializeAndWatch(SEARCH_POWER_SETTING, this::updateSearchPower);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MIN_SETTING, this::updateSearchPowerMin);
        clusterSettings.initializeAndWatch(SEARCH_POWER_MAX_SETTING, this::updateSearchPowerMax);
    }

    void updateSearchPower(Integer sp) {
        if (this.searchPowerMinSetting == this.searchPowerMaxSetting) {
            this.searchPowerMinSetting = sp;
            this.searchPowerMaxSetting = sp;
        } else {
            throw new IllegalArgumentException(
                "Updating "
                    + ServerlessSharedSettings.SEARCH_POWER_SETTING.getKey()
                    + " ["
                    + sp
                    + "] while "
                    + ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                    + " ["
                    + this.searchPowerMinSetting
                    + "] and "
                    + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                    + " ["
                    + this.searchPowerMaxSetting
                    + "] are not equal."
            );
        }
    }

    void updateSearchPowerMin(Integer spMin) {
        this.searchPowerMinSetting = spMin;
    }

    void updateSearchPowerMax(Integer spMax) {
        this.searchPowerMaxSetting = spMax;
    }

    Map<Integer, List<String>> getNumberOfReplicaChanges() {
        Map<Integer, List<String>> numReplicaChanges = new HashMap<>(2);
        if (searchPowerMinSetting < SEARCH_POWER_MIN_NO_REPLICATION) {
            Map<Index, SearchMetricsService.IndexProperties> indicesMap = this.searchMetricsService.getIndices();
            for (Map.Entry<Index, SearchMetricsService.IndexProperties> entry : indicesMap.entrySet()) {
                Index index = entry.getKey();
                SearchMetricsService.IndexProperties settings = entry.getValue();
                if (settings.replicas() != 1) {
                    setNumReplicasForIndex(index.getName(), 1, numReplicaChanges);
                }
            }
        } else if (searchPowerMinSetting >= SEARCH_POWER_MIN_FULL_REPLICATION) {
            Map<Index, SearchMetricsService.IndexProperties> indicesMap = this.searchMetricsService.getIndices();
            Map<ShardId, SearchMetricsService.ShardMetrics> shardMetricsMap = this.searchMetricsService.getShardMetrics();
            for (Map.Entry<Index, SearchMetricsService.IndexProperties> entry : indicesMap.entrySet()) {
                Index index = entry.getKey();
                SearchMetricsService.IndexProperties indexProperties = entry.getValue();
                boolean indexInteractive = false;
                for (int i = 0; i < indexProperties.shards(); i++) {
                    SearchMetricsService.ShardMetrics shardMetrics = shardMetricsMap.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // move to the next index if shard metrics for the current index are not found: they could have been removed
                        // because the index has been removed, or because it has now zero replicas.
                        break;
                    }
                    if (shardMetrics.shardSize.interactiveSizeInBytes() > 0) {
                        indexInteractive = true;
                        break;
                    }
                }
                if (indexInteractive) {
                    if (indexProperties.replicas() != 2) {
                        setNumReplicasForIndex(index.getName(), 2, numReplicaChanges);
                    }
                } else {
                    if (indexProperties.replicas() != 1) {
                        setNumReplicasForIndex(index.getName(), 1, numReplicaChanges);
                    }
                }
            }
        } else {
            Map<Index, SearchMetricsService.IndexProperties> indicesMap = this.searchMetricsService.getIndices();
            Map<ShardId, SearchMetricsService.ShardMetrics> shardMetricsMap = this.searchMetricsService.getShardMetrics();
            // search power should be between 100 and 250 here
            long allIndicesInteractiveSize = 0;
            List<IndexReplicationRanker.IndexRankingProperties> rankingProperties = new ArrayList<>();
            for (Map.Entry<Index, SearchMetricsService.IndexProperties> entry : indicesMap.entrySet()) {
                Index index = entry.getKey();
                SearchMetricsService.IndexProperties indexProperties = entry.getValue();
                long totalIndexInteractiveSize = 0;
                boolean indexSizeValid = true;
                for (int i = 0; i < indexProperties.shards(); i++) {
                    SearchMetricsService.ShardMetrics shardMetrics = shardMetricsMap.get(new ShardId(index, i));
                    if (shardMetrics == null) {
                        // move to the next index if shard metrics for the current index are not found: they could have been removed
                        // because the index has been removed, or because it has now zero replicas.
                        indexSizeValid = false;
                        break;
                    }
                    totalIndexInteractiveSize += shardMetrics.shardSize.interactiveSizeInBytes();
                }
                if (indexSizeValid) {
                    rankingProperties.add(new IndexReplicationRanker.IndexRankingProperties(indexProperties, totalIndexInteractiveSize));
                    allIndicesInteractiveSize += totalIndexInteractiveSize;
                } else {
                    // TODO if we detect an index with invalid shard metrics, should we reset its replica setting to 1 or
                    // trust that it will get removed from state and this.indices map in an upcoming cluster state update?
                }
            }
            final long threshold = allIndicesInteractiveSize * (searchPowerMinSetting - SEARCH_POWER_MIN_NO_REPLICATION)
                / (SEARCH_POWER_MIN_FULL_REPLICATION - SEARCH_POWER_MIN_NO_REPLICATION);
            Set<String> twoReplicaEligibleIndices = getRankedIndicesBelowThreshold(rankingProperties, threshold);
            for (var rankedIndex : rankingProperties) {
                String indexName = rankedIndex.indexProperties().name();
                int replicas = rankedIndex.indexProperties().replicas();
                if (rankedIndex.interactiveSize() > 0 && twoReplicaEligibleIndices.contains(indexName)) {
                    if (replicas != 2) {
                        setNumReplicasForIndex(indexName, 2, numReplicaChanges);
                    }
                } else {
                    if (replicas != 1) {
                        setNumReplicasForIndex(indexName, 1, numReplicaChanges);
                    }
                }
            }
        }
        return numReplicaChanges;
    }

    private static void setNumReplicasForIndex(String index, int numReplicas, Map<Integer, List<String>> numReplicaChanges) {
        numReplicaChanges.compute(numReplicas, (integer, strings) -> {
            if (strings == null) {
                strings = new ArrayList<>();
            }
            strings.add(index);
            return strings;
        });
    }

    private void publishUpdate(Map<Integer, List<String>> replicaUpdates) {
        for (Map.Entry<Integer, List<String>> entry : replicaUpdates.entrySet()) {
            int numReplicas = entry.getKey();
            List<String> indices = entry.getValue();
            Settings settings = Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas).build();
            UpdateSettingsRequest request = new UpdateSettingsRequest(settings, indices.toArray(new String[0]));
            client.executeLocally(TransportUpdateSettingsAction.TYPE, request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    logger.info("Updated replicas for " + indices + " to " + numReplicas);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Error updating replicas for " + indices + " to " + numReplicas, e);
                }
            });
        }
    }
}
