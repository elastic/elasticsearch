/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

/**
 * This class contains the logic used to check the cluster-wide shard limit before shards are created and ensuring that the limit is
 * updated correctly on setting updates, etc.
 *
 * NOTE: This is the limit applied at *shard creation time*. If you are looking for the limit applied at *allocation* time, which is
 * controlled by a different setting,
 * see {@link org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider}.
 */
public class ShardLimitValidator {
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE =
        Setting.intSetting("cluster.max_shards_per_node", 1000, 1, Setting.Property.Dynamic, Setting.Property.NodeScope);
    protected final AtomicInteger shardLimitPerNode = new AtomicInteger();

    public ShardLimitValidator(final Settings settings, ClusterService clusterService) {
        this.shardLimitPerNode.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, this::setShardLimitPerNode);
    }

    private void setShardLimitPerNode(int newValue) {
        this.shardLimitPerNode.set(newValue);
    }

    /**
     * Gets the currently configured value of the {@link ShardLimitValidator#SETTING_CLUSTER_MAX_SHARDS_PER_NODE} setting.
     * @return the current value of the setting
     */
    public int getShardLimitPerNode() {
        return shardLimitPerNode.get();
    }

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     *
     * @param settings       the settings of the index to be created
     * @param state          the current cluster state
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     */
    public void validateShardLimit(final Settings settings, final ClusterState state) {
        final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);

        final Optional<String> shardLimit = checkShardLimit(shardsToCreate, state);
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw e;
        }
    }

    /**
     * Validates whether a list of indices can be opened without going over the cluster shard limit.  Only counts indices which are
     * currently closed and will be opened, ignores indices which are already open.
     *
     * @param currentState The current cluster state.
     * @param indicesToOpen The indices which are to be opened.
     * @throws ValidationException If this operation would take the cluster over the limit and enforcement is enabled.
     */
    public void validateShardLimit(ClusterState currentState, Index[] indicesToOpen) {
        int shardsToOpen = Arrays.stream(indicesToOpen)
            .filter(index -> currentState.metadata().index(index).getState().equals(IndexMetadata.State.CLOSE))
            .mapToInt(index -> getTotalShardCount(currentState, index))
            .sum();

        Optional<String> error = checkShardLimit(shardsToOpen, currentState);
        if (error.isPresent()) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(error.get());
            throw ex;
        }
    }

    private static int getTotalShardCount(ClusterState state, Index index) {
        IndexMetadata indexMetadata = state.metadata().index(index);
        return indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit.
     * Returns an error message if appropriate, or an empty {@link Optional} otherwise.
     *
     * @param newShards         The number of shards to be added by this operation
     * @param state             The current cluster state
     * @return If present, an error message to be given as the reason for failing
     * an operation. If empty, a sign that the operation is valid.
     */
    public Optional<String> checkShardLimit(int newShards, ClusterState state) {
        return checkShardLimit(newShards, state, getShardLimitPerNode());
    }

    // package-private for testing
    static Optional<String> checkShardLimit(int newShards, ClusterState state, int maxShardsPerNodeSetting) {
        int nodeCount = state.getNodes().getDataNodes().size();

        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards < 0) {
            return Optional.empty();
        }
        int maxShardsPerNode = maxShardsPerNodeSetting;
        int maxShardsInCluster = maxShardsPerNode * nodeCount;
        int currentOpenShards = state.getMetadata().getTotalOpenIndexShards();

        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            String errorMessage = "this action would add [" + newShards + "] total shards, but this cluster currently has [" +
                currentOpenShards + "]/[" + maxShardsInCluster + "] maximum shards open";
            return Optional.of(errorMessage);
        }
        return Optional.empty();
    }
}
