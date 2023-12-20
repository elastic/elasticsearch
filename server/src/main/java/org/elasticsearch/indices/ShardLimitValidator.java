/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE = Setting.intSetting(
        "cluster.max_shards_per_node",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN = Setting.intSetting(
        "cluster.max_shards_per_node.frozen",
        3000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final String FROZEN_GROUP = "frozen";
    public static final String NORMAL_GROUP = "normal";
    static final Set<String> VALID_GROUPS = Set.of(NORMAL_GROUP, FROZEN_GROUP);
    public static final Setting<String> INDEX_SETTING_SHARD_LIMIT_GROUP = Setting.simpleString(
        "index.shard_limit.group",
        NORMAL_GROUP,
        value -> {
            if (VALID_GROUPS.contains(value) == false) {
                throw new IllegalArgumentException("[" + value + "] is not a valid shard limit group");
            }
        },
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.NotCopyableOnResize
    );
    protected final AtomicInteger shardLimitPerNode = new AtomicInteger();
    protected final AtomicInteger shardLimitPerNodeFrozen = new AtomicInteger();

    public ShardLimitValidator(final Settings settings, ClusterService clusterService) {
        this.shardLimitPerNode.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings));
        this.shardLimitPerNodeFrozen.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, this::setShardLimitPerNode);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN, this::setShardLimitPerNodeFrozen);
    }

    private void setShardLimitPerNode(int newValue) {
        this.shardLimitPerNode.set(newValue);
    }

    private void setShardLimitPerNodeFrozen(int newValue) {
        this.shardLimitPerNodeFrozen.set(newValue);
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
        final boolean frozen = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(settings));

        final var result = checkShardLimitOnBothGroups(frozen == false ? shardsToCreate : 0, frozen ? shardsToCreate : 0, state);
        if (result.canAddShards == false) {
            final ValidationException e = new ValidationException();
            e.addValidationError(errorMessageFrom(result));
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
        int frozen = 0;
        int normal = 0;
        for (Index index : indicesToOpen) {
            IndexMetadata imd = currentState.metadata().index(index);
            if (imd.getState().equals(IndexMetadata.State.CLOSE)) {
                int totalNewShards = imd.getNumberOfShards() * (1 + imd.getNumberOfReplicas());
                if (FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(imd.getSettings()))) {
                    frozen += totalNewShards;
                } else {
                    normal += totalNewShards;
                }
            }
        }

        var result = checkShardLimitOnBothGroups(normal, frozen, currentState);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    public void validateShardLimitOnReplicaUpdate(ClusterState currentState, Index[] indices, int replicas) {
        int frozen = 0;
        int normal = 0;
        for (Index index : indices) {
            IndexMetadata imd = currentState.metadata().index(index);
            int totalNewShards = getTotalNewShards(index, currentState, replicas);
            if (FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(imd.getSettings()))) {
                frozen += totalNewShards;
            } else {
                normal += totalNewShards;
            }
        }

        var result = checkShardLimitOnBothGroups(normal, frozen, currentState);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    private static int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        int shardsInIndex = indexMetadata.getNumberOfShards();
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit. It follows the
     * next rules:
     *   - Check limits for _normal_ nodes
     *   - If there's no room -> return the Result for _normal_ nodes (fail-fast)
     *   - otherwise -> returns the Result of checking the limits for _frozen_ nodes
     *
     * @param newShards       The number of normal shards to be added by this operation
     * @param newFrozenShards The number of frozen shards to be added by this operation
     * @param state           The current cluster state
     */
    private Result checkShardLimitOnBothGroups(int newShards, int newFrozenShards, ClusterState state) {
        // we verify the two limits independently. This also means that if they have mixed frozen and other data-roles nodes, such a mixed
        // node can have both 1000 normal and 3000 frozen shards. This is the trade-off to keep the simplicity of the counts. We advocate
        // against such mixed nodes for production use anyway.
        int frozenNodeCount = nodeCount(state, ShardLimitValidator::hasFrozen);
        int normalNodeCount = nodeCount(state, ShardLimitValidator::hasNonFrozen);

        var result = checkShardLimit(newShards, state, getShardLimitPerNode(), normalNodeCount, NORMAL_GROUP);
        // fail-fast: in case there's no room on the `normal` nodes, just return the result of that check.
        if (result.canAddShards() == false) {
            return result;
        }
        return checkShardLimit(newFrozenShards, state, shardLimitPerNodeFrozen.get(), frozenNodeCount, FROZEN_GROUP);
    }

    /**
     * This method checks whether there is enough room in the cluster to add the given number of shards with the given number of replicas
     * without exceeding the "cluster.max_shards_per_node" setting for _normal_ nodes. This check does not guarantee that the number of
     * shards can be added, just that there is theoretically room to add them without exceeding the shards per node configuration.
     * @param maxConfiguredShardsPerNode The maximum available number of shards to be allocated within a node
     * @param numberOfNewShards          The number of primary shards that we want to be able to add to the cluster
     * @param replicas                   The number of replicas of the primary shards that we want to be able to add to the cluster
     * @param state                      The cluster state, used to get cluster settings and to get the number of open shards already in
     *                                   the cluster
     */
    public static Result checkShardLimitForNormalNodes(
        int maxConfiguredShardsPerNode,
        int numberOfNewShards,
        int replicas,
        ClusterState state
    ) {
        return checkShardLimit(
            numberOfNewShards * (1 + replicas),
            state,
            maxConfiguredShardsPerNode,
            nodeCount(state, ShardLimitValidator::hasNonFrozen),
            NORMAL_GROUP
        );
    }

    /**
     * This method checks whether there is enough room in the cluster to add the given number of shards with the given number of replicas
     * without exceeding the "cluster.max_shards_per_node_frozen" setting for _frozen_ nodes. This check does not guarantee that the number
     * of shards can be added, just that there is theoretically room to add them without exceeding the shards per node configuration.
     * @param maxConfiguredShardsPerNode The maximum available number of shards to be allocated within a node
     * @param numberOfNewShards          The number of primary shards that we want to be able to add to the cluster
     * @param replicas                   The number of replicas of the primary shards that we want to be able to add to the cluster
     * @param state                      The cluster state, used to get cluster settings and to get the number of open shards already in
     *                                   the cluster
     */
    public static Result checkShardLimitForFrozenNodes(
        int maxConfiguredShardsPerNode,
        int numberOfNewShards,
        int replicas,
        ClusterState state
    ) {
        return checkShardLimit(
            numberOfNewShards * (1 + replicas),
            state,
            maxConfiguredShardsPerNode,
            nodeCount(state, ShardLimitValidator::hasFrozen),
            FROZEN_GROUP
        );
    }

    private static Result checkShardLimit(int newShards, ClusterState state, int maxConfiguredShardsPerNode, int nodeCount, String group) {
        int maxShardsInCluster = maxConfiguredShardsPerNode * nodeCount;
        int currentOpenShards = state.getMetadata().getTotalOpenIndexShards();

        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards <= 0) {
            return new Result(true, Optional.empty(), newShards, maxShardsInCluster, group);
        }

        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            Predicate<IndexMetadata> indexMetadataPredicate = imd -> imd.getState().equals(IndexMetadata.State.OPEN)
                && group.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(imd.getSettings()));
            long currentFilteredShards = state.metadata()
                .indices()
                .values()
                .stream()
                .filter(indexMetadataPredicate)
                .mapToInt(IndexMetadata::getTotalNumberOfShards)
                .sum();

            if ((currentFilteredShards + newShards) > maxShardsInCluster) {
                return new Result(false, Optional.of(currentFilteredShards), newShards, maxShardsInCluster, group);
            }
        }
        return new Result(true, Optional.empty(), newShards, maxShardsInCluster, group);
    }

    private static int nodeCount(ClusterState state, Predicate<DiscoveryNode> nodePredicate) {
        return (int) state.getNodes().getDataNodes().values().stream().filter(nodePredicate).count();
    }

    private static boolean hasFrozen(DiscoveryNode node) {
        return node.getRoles().contains(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    private static boolean hasNonFrozen(DiscoveryNode node) {
        return node.getRoles().stream().anyMatch(r -> r.canContainData() && r != DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);
    }

    static String errorMessageFrom(Result result) {
        return "this action would add ["
            + result.totalShardsToAdd
            + "] shards, but this cluster currently has ["
            + result.currentUsedShards.get()
            + "]/["
            + result.maxShardsInCluster
            + "] maximum "
            + result.group
            + " shards open";
    }

    /**
     * A Result object containing enough information to be used by external callers about the state of the cluster from the shard limits
     * perspective.
     */
    public record Result(
        boolean canAddShards,
        Optional<Long> currentUsedShards,
        int totalShardsToAdd,
        int maxShardsInCluster,
        String group
    ) {}
}
