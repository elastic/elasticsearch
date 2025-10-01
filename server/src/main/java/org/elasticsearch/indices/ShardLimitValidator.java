/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
    private final boolean isStateless;

    public ShardLimitValidator(final Settings settings, ClusterService clusterService) {
        this.shardLimitPerNode.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings));
        this.shardLimitPerNodeFrozen.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, this::setShardLimitPerNode);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN, this::setShardLimitPerNodeFrozen);
        this.isStateless = DiscoveryNode.isStateless(settings);
    }

    private void setShardLimitPerNode(int newValue) {
        this.shardLimitPerNode.set(newValue);
    }

    private void setShardLimitPerNodeFrozen(int newValue) {
        this.shardLimitPerNodeFrozen.set(newValue);
    }

    private int getShardLimitPerNode(ResultGroup resultGroup) {
        return switch (resultGroup) {
            case NORMAL, INDEX, SEARCH -> shardLimitPerNode.get();
            case FROZEN -> shardLimitPerNodeFrozen.get();
        };
    }

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     *
     * @param settings       the settings of the index to be created
     * @param discoveryNodes the nodes in the cluster
     * @param metadata       the cluster state metadata
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     */
    public void validateShardLimit(final Settings settings, final DiscoveryNodes discoveryNodes, final Metadata metadata) {
        final var resultGroups = applicableResultGroups(isStateless);
        final var shardsToCreatePerGroup = resultGroups.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), resultGroup -> resultGroup.numberOfNewShards(settings)));

        final var result = checkShardLimitOnGroups(resultGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
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
     * @param discoveryNodes The nodes in the cluster
     * @param metadata       The cluster state metadata
     * @param indicesToOpen  The indices which are to be opened.
     * @throws ValidationException If this operation would take the cluster over the limit and enforcement is enabled.
     */
    public void validateShardLimit(DiscoveryNodes discoveryNodes, Metadata metadata, Index[] indicesToOpen) {
        final var resultGroups = applicableResultGroups(isStateless);
        final Map<ResultGroup, Integer> shardsToCreatePerGroup = new HashMap<>();

        for (Index index : indicesToOpen) {
            IndexMetadata imd = metadata.indexMetadata(index);
            if (imd.getState().equals(IndexMetadata.State.CLOSE)) {
                resultGroups.forEach(
                    resultGroup -> shardsToCreatePerGroup.merge(resultGroup, resultGroup.numberOfNewShards(imd.getSettings()), Integer::sum)
                );
            }
        }

        var result = checkShardLimitOnGroups(resultGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    public void validateShardLimitOnReplicaUpdate(DiscoveryNodes discoveryNodes, Metadata metadata, Index[] indices, int replicas) {
        final var resultGroups = applicableResultGroups(isStateless);
        final Map<ResultGroup, Integer> shardsToCreatePerGroup = new HashMap<>();

        for (Index index : indices) {
            IndexMetadata imd = metadata.indexMetadata(index);
            resultGroups.forEach(
                resultGroup -> shardsToCreatePerGroup.merge(
                    resultGroup,
                    resultGroup.numberOfNewShards(imd.getSettings(), replicas),
                    Integer::sum
                )
            );
        }

        var result = checkShardLimitOnGroups(resultGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    public static List<ResultGroup> applicableResultGroups(boolean isStateless) {
        return isStateless ? List.of(ResultGroup.INDEX, ResultGroup.SEARCH) : List.of(ResultGroup.NORMAL, ResultGroup.FROZEN);
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit. It follows the
     * next rules:
     * - Check limits for _normal_ nodes
     * - If there's no room -> return the Result for _normal_ nodes (fail-fast)
     * - otherwise -> returns the Result of checking the limits for _frozen_ nodes
     *
     * @param resultGroups The applicable result groups to check for shard limits
     * @param shardsToCreatePerGroup The number of new shards to create per result group
     * @param discoveryNodes The nodes in the cluster
     * @param metadata       The cluster state metadata
     */
    private Result checkShardLimitOnGroups(
        List<ResultGroup> resultGroups,
        Map<ResultGroup, Integer> shardsToCreatePerGroup,
        DiscoveryNodes discoveryNodes,
        Metadata metadata
    ) {
        assert shardsToCreatePerGroup.size() == resultGroups.size()
            : "inconsistent result groups " + resultGroups + "and groups for shards to create " + shardsToCreatePerGroup.keySet();
        // we verify the two limits independently. This also means that if they have mixed frozen and other data-roles nodes, such a mixed
        // node can have both 1000 normal and 3000 frozen shards. This is the trade-off to keep the simplicity of the counts. We advocate
        // against such mixed nodes for production use anyway.
        Result result = null;
        for (var resultGroup : resultGroups) {
            result = checkShardLimit(
                resultGroup,
                shardsToCreatePerGroup.get(resultGroup),
                metadata,
                getShardLimitPerNode(resultGroup),
                discoveryNodes
            );
            // fail-fast: in case there's no room on an earlier group, e.g. `normal`, just return the result of that check.
            if (result.canAddShards() == false) {
                return result;
            }
        }
        assert result != null;
        return result;
    }

    /**
     * This method checks whether there is enough room in the cluster to add the given number of shards with the given number of replicas
     * without exceeding the "cluster.max_shards_per_node" setting for _normal_ nodes. This check does not guarantee that the number of
     * shards can be added, just that there is theoretically room to add them without exceeding the shards per node configuration.
     * @param maxConfiguredShardsPerNode The maximum available number of shards to be allocated within a node
     * @param numberOfNewShards          The number of primary shards that we want to be able to add to the cluster
     * @param replicas                   The number of replicas of the primary shards that we want to be able to add to the cluster
     * @param discoveryNodes             The nodes in the cluster, used to get the number of open shard already in the cluster
     * @param metadata                   The cluster state metadata, used to get the cluster settings
     */
    public static Result checkShardLimitForNormalNodes(
        int maxConfiguredShardsPerNode,
        int numberOfNewShards,
        int replicas,
        DiscoveryNodes discoveryNodes,
        Metadata metadata
    ) {
        return checkShardLimit(
            ResultGroup.NORMAL,
            numberOfNewShards * (1 + replicas),
            metadata,
            maxConfiguredShardsPerNode,
            discoveryNodes
        );
    }

    /**
     * This method checks whether there is enough room in the cluster to add the given number of shards with the given number of replicas
     * without exceeding the "cluster.max_shards_per_node_frozen" setting for _frozen_ nodes. This check does not guarantee that the number
     * of shards can be added, just that there is theoretically room to add them without exceeding the shards per node configuration.
     * @param maxConfiguredShardsPerNode The maximum available number of shards to be allocated within a node
     * @param numberOfNewShards          The number of primary shards that we want to be able to add to the cluster
     * @param replicas                   The number of replicas of the primary shards that we want to be able to add to the cluster
     * @param discoveryNodes             The nodes in the cluster, used to get the number of open shard already in the cluster
     * @param metadata                   The cluster state metadata, used to get the cluster settings
     */
    public static Result checkShardLimitForFrozenNodes(
        int maxConfiguredShardsPerNode,
        int numberOfNewShards,
        int replicas,
        DiscoveryNodes discoveryNodes,
        Metadata metadata
    ) {
        return checkShardLimit(
            ResultGroup.FROZEN,
            numberOfNewShards * (1 + replicas),
            metadata,
            maxConfiguredShardsPerNode,
            discoveryNodes
        );
    }

    private static Result checkShardLimit(
        ResultGroup resultGroup,
        int newShards,
        Metadata metadata,
        int maxConfiguredShardsPerNode,
        DiscoveryNodes discoveryNodes
    ) {
        final int nodeCount = resultGroup.numberOfNodes(discoveryNodes);
        int maxShardsInCluster = maxConfiguredShardsPerNode * nodeCount;
        int currentOpenShards = metadata.getTotalOpenIndexShards();

        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards <= 0) {
            return new Result(true, Optional.empty(), newShards, maxShardsInCluster, resultGroup);
        }

        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            long currentFilteredShards = metadata.projects()
                .values()
                .stream()
                .flatMap(projectMetadata -> projectMetadata.indices().values().stream())
                .mapToInt(resultGroup::countShards)
                .filter(value -> value > 0)
                .sum();

            if ((currentFilteredShards + newShards) > maxShardsInCluster) {
                return new Result(false, Optional.of(currentFilteredShards), newShards, maxShardsInCluster, resultGroup);
            }
        }
        return new Result(true, Optional.empty(), newShards, maxShardsInCluster, resultGroup);
    }

    private static int nodeCount(DiscoveryNodes discoveryNodes, Predicate<DiscoveryNode> nodePredicate) {
        return (int) discoveryNodes.getDataNodes().values().stream().filter(nodePredicate).count();
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
            + " shards open; for more information, see "
            + ReferenceDocs.MAX_SHARDS_PER_NODE;
    }

    public enum ResultGroup {
        NORMAL(NORMAL_GROUP),
        FROZEN(FROZEN_GROUP),
        INDEX("index"),
        SEARCH("search");

        private final String group;

        ResultGroup(String group) {
            this.group = group;
        }

        public String group() {
            return group;
        }

        @Override
        public String toString() {
            return group;
        }

        public int numberOfNodes(DiscoveryNodes discoveryNodes) {
            return switch (this) {
                case NORMAL -> nodeCount(discoveryNodes, ShardLimitValidator::hasNonFrozen);
                case FROZEN -> nodeCount(discoveryNodes, ShardLimitValidator::hasFrozen);
                case INDEX -> nodeCount(discoveryNodes, node -> node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()));
                case SEARCH -> nodeCount(discoveryNodes, node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()));
            };
        }

        public int countShards(IndexMetadata indexMetadata) {
            return switch (this) {
                case NORMAL -> isOpenIndex(indexMetadata) && matchesGroup(indexMetadata, ResultGroup.NORMAL.group())
                    ? indexMetadata.getTotalNumberOfShards()
                    : 0;
                case FROZEN -> isOpenIndex(indexMetadata) && matchesGroup(indexMetadata, ResultGroup.FROZEN.group())
                    ? indexMetadata.getTotalNumberOfShards()
                    : 0;
                case INDEX -> isOpenIndex(indexMetadata) ? indexMetadata.getNumberOfShards() : 0;
                case SEARCH -> isOpenIndex(indexMetadata) ? indexMetadata.getNumberOfReplicas() : 0;
            };
        }

        public int numberOfNewShards(Settings indexSettings) {
            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
            final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexSettings);
            final boolean frozen = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexSettings));
            return switch (this) {
                case NORMAL -> frozen ? 0 : numberOfShards * (1 + numberOfReplicas);
                case FROZEN -> frozen ? numberOfShards * (1 + numberOfReplicas) : 0;
                case INDEX -> numberOfShards;
                case SEARCH -> numberOfShards * numberOfReplicas;
            };
        }

        public int numberOfNewShards(Settings indexSettings, int updatedReplicas) {
            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
            final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexSettings);
            final int replicaIncrease = updatedReplicas - numberOfReplicas;
            final boolean frozen = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexSettings));
            return switch (this) {
                case NORMAL -> frozen ? 0 : numberOfShards * replicaIncrease;
                case FROZEN -> frozen ? numberOfShards * replicaIncrease : 0;
                case INDEX -> 0;
                case SEARCH -> numberOfShards * replicaIncrease;
            };
        }
    }

    private static boolean isOpenIndex(IndexMetadata indexMetadata) {
        return indexMetadata.getState().equals(IndexMetadata.State.OPEN);
    }

    private static boolean matchesGroup(IndexMetadata indexMetadata, String group) {
        return group.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexMetadata.getSettings()));
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
        ResultGroup group
    ) {}

}
