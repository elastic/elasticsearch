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
import org.elasticsearch.health.metadata.HealthMetadata;
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
    private static final Set<String> VALID_INDEX_SETTING_GROUPS = Set.of(NORMAL_GROUP, FROZEN_GROUP);
    public static final Setting<String> INDEX_SETTING_SHARD_LIMIT_GROUP = Setting.simpleString(
        "index.shard_limit.group",
        NORMAL_GROUP,
        value -> {
            if (VALID_INDEX_SETTING_GROUPS.contains(value) == false) {
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

    private int getShardLimitPerNode(LimitGroup limitGroup) {
        return switch (limitGroup) {
            case NORMAL, INDEX, SEARCH -> shardLimitPerNode.get();
            case FROZEN -> shardLimitPerNodeFrozen.get();
        };
    }

    public static int getShardLimitPerNode(LimitGroup limitGroup, HealthMetadata.ShardLimits shardLimits) {
        return switch (limitGroup) {
            case NORMAL, INDEX, SEARCH -> shardLimits.maxShardsPerNode();
            case FROZEN -> shardLimits.maxShardsPerNodeFrozen();
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
        final var limitGroups = applicableLimitGroups(isStateless);
        final var shardsToCreatePerGroup = limitGroups.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), limitGroup -> limitGroup.newShardsTotal(settings)));

        final var result = checkShardLimitOnGroups(limitGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
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
        final var limitGroups = applicableLimitGroups(isStateless);
        final Map<LimitGroup, Integer> shardsToCreatePerGroup = new HashMap<>();

        // TODO: we can short circuit when indicesToOpen is empty
        for (Index index : indicesToOpen) {
            IndexMetadata imd = metadata.indexMetadata(index);
            if (imd.getState().equals(IndexMetadata.State.CLOSE)) {
                limitGroups.forEach(
                    limitGroup -> shardsToCreatePerGroup.merge(limitGroup, limitGroup.newShardsTotal(imd.getSettings()), Integer::sum)
                );
            }
        }

        var result = checkShardLimitOnGroups(limitGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    public void validateShardLimitOnReplicaUpdate(DiscoveryNodes discoveryNodes, Metadata metadata, Index[] indices, int replicas) {
        final var limitGroups = applicableLimitGroups(isStateless);
        final Map<LimitGroup, Integer> shardsToCreatePerGroup = new HashMap<>();

        // TODO: we can short circuit when indices is empty
        for (Index index : indices) {
            IndexMetadata imd = metadata.indexMetadata(index);
            limitGroups.forEach(
                limitGroup -> shardsToCreatePerGroup.merge(limitGroup, limitGroup.newShardsTotal(imd.getSettings(), replicas), Integer::sum)
            );
        }

        var result = checkShardLimitOnGroups(limitGroups, shardsToCreatePerGroup, discoveryNodes, metadata);
        if (result.canAddShards == false) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(errorMessageFrom(result));
            throw ex;
        }
    }

    public static List<LimitGroup> applicableLimitGroups(boolean isStateless) {
        return isStateless ? List.of(LimitGroup.INDEX, LimitGroup.SEARCH) : List.of(LimitGroup.NORMAL, LimitGroup.FROZEN);
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit. It follows the
     * next rules:
     * - Check limits for nodes in the first group, e.g. _normal_ nodes
     * - If there's no room -> return the Result for nodes of the first group (fail-fast)
     * - otherwise -> returns the Result of checking the limits for the next group, e.g. _frozen_ nodes
     * - Rinse and repeat if thera re more groups. But so far we only have 2 members in a group.
     *
     * @param limitGroups The applicable limit groups to check for shard limits
     * @param shardsToCreatePerGroup The number of new shards to create per limit group
     * @param discoveryNodes The nodes in the cluster
     * @param metadata       The cluster state metadata
     */
    private Result checkShardLimitOnGroups(
        List<LimitGroup> limitGroups,
        Map<LimitGroup, Integer> shardsToCreatePerGroup,
        DiscoveryNodes discoveryNodes,
        Metadata metadata
    ) {
        assert limitGroups.containsAll(shardsToCreatePerGroup.keySet())
            : "limit groups " + limitGroups + " do not contain groups for shards creation " + shardsToCreatePerGroup.keySet();
        // we verify the two limits independently. This also means that if they have mixed frozen and other data-roles nodes, such a mixed
        // node can have both 1000 normal and 3000 frozen shards. This is the trade-off to keep the simplicity of the counts. We advocate
        // against such mixed nodes for production use anyway.
        Result result = null;
        for (var limitGroup : limitGroups) {
            result = limitGroup.checkShardLimit(
                getShardLimitPerNode(limitGroup),
                shardsToCreatePerGroup.getOrDefault(limitGroup, 0),
                discoveryNodes,
                metadata
            );
            // fail-fast: in case there's no room on an earlier group, e.g. `normal`, just return the result of that check.
            if (result.canAddShards() == false) {
                return result;
            }
        }
        assert result != null;
        return result;
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

    public enum LimitGroup {
        NORMAL(NORMAL_GROUP) {
            @Override
            public int numberOfNodes(DiscoveryNodes discoveryNodes) {
                return nodeCount(discoveryNodes, ShardLimitValidator::hasNonFrozen);
            }

            @Override
            public int countShards(IndexMetadata indexMetadata) {
                return isOpenIndex(indexMetadata) && matchesIndexSettingGroup(indexMetadata, LimitGroup.NORMAL.groupName())
                    ? indexMetadata.getTotalNumberOfShards()
                    : 0;
            }

            @Override
            public int newShardsTotal(int shards, int replicas) {
                return shards * (1 + replicas);
            }

            @Override
            protected int newReplicaShards(boolean isFrozenIndex, int shards, int replicaIncrease) {
                return isFrozenIndex ? 0 : shards * replicaIncrease;
            }
        },
        FROZEN(FROZEN_GROUP) {
            @Override
            public int numberOfNodes(DiscoveryNodes discoveryNodes) {
                return nodeCount(discoveryNodes, ShardLimitValidator::hasFrozen);
            }

            @Override
            public int countShards(IndexMetadata indexMetadata) {
                return isOpenIndex(indexMetadata) && matchesIndexSettingGroup(indexMetadata, LimitGroup.FROZEN.groupName())
                    ? indexMetadata.getTotalNumberOfShards()
                    : 0;
            }

            @Override
            public int newShardsTotal(int shards, int replicas) {
                return shards * (1 + replicas);
            }

            @Override
            protected int newReplicaShards(boolean isFrozenIndex, int shards, int replicaIncrease) {
                return isFrozenIndex ? shards * replicaIncrease : 0;
            }
        },
        INDEX("index") {
            @Override
            public int numberOfNodes(DiscoveryNodes discoveryNodes) {
                return nodeCount(discoveryNodes, node -> node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()));
            }

            @Override
            public int countShards(IndexMetadata indexMetadata) {
                return isOpenIndex(indexMetadata) ? indexMetadata.getNumberOfShards() : 0;
            }

            @Override
            public int newShardsTotal(int shards, int replicas) {
                return shards;
            }

            @Override
            protected int newReplicaShards(boolean isFrozenIndex, int shards, int replicaIncrease) {
                return 0;
            }
        },
        SEARCH("search") {
            @Override
            public int numberOfNodes(DiscoveryNodes discoveryNodes) {
                return nodeCount(discoveryNodes, node -> node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()));
            }

            @Override
            public int countShards(IndexMetadata indexMetadata) {
                return isOpenIndex(indexMetadata) ? indexMetadata.getNumberOfShards() * indexMetadata.getNumberOfReplicas() : 0;
            }

            @Override
            public int newShardsTotal(int shards, int replicas) {
                return shards * replicas;
            }

            @Override
            protected int newReplicaShards(boolean isFrozenIndex, int shards, int replicaIncrease) {
                return shards * replicaIncrease;
            }
        };

        private final String groupName;

        LimitGroup(String groupName) {
            this.groupName = groupName;
        }

        public String groupName() {
            return groupName;
        }

        @Override
        public String toString() {
            return groupName;
        }

        public abstract int numberOfNodes(DiscoveryNodes discoveryNodes);

        public abstract int countShards(IndexMetadata indexMetadata);

        /**
         * Compute the total number of new shards including both primaries and replicas that would be created for the given
         * number of shards and replicas in this group.
         * @param shards Number of primary shards
         * @param replicas Number of replica shards per primary
         * @return Number of total new shards to be created for the group.
         */
        public abstract int newShardsTotal(int shards, int replicas);

        protected abstract int newReplicaShards(boolean isFrozenIndex, int shards, int replicaIncrease);

        /**
         * Compute the total number of new shards including both primaries and replicas that would be created for an index with the
         * given index settings.
         * @param indexSettings The index settings for the index to be created.
         * @return The total number of new shards to be created for this group.
         */
        public int newShardsTotal(Settings indexSettings) {
            final boolean isFrozenIndex = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexSettings));
            final int numberOfShards = (isFrozenIndex == (this == FROZEN)) ? INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings) : 0;
            final int numberOfReplicas = (isFrozenIndex == (this == FROZEN))
                ? IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexSettings)
                : 0;
            return newShardsTotal(numberOfShards, numberOfReplicas);
        }

        /**
         * Compute the total number of new replica shards that would be created by updating the number of replicas to the given number.
         * @param indexSettings The index settings for the index to be updated.
         * @param updatedReplicas The updated number of replicas for the index.
         * @return The number of new replica shards to be created for this group.
         */
        public int newShardsTotal(Settings indexSettings, int updatedReplicas) {
            final boolean isFrozenIndex = FROZEN_GROUP.equals(INDEX_SETTING_SHARD_LIMIT_GROUP.get(indexSettings));
            final int shards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
            final int replicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexSettings);
            final int replicaIncrease = updatedReplicas - replicas;
            return newReplicaShards(isFrozenIndex, shards, replicaIncrease);
        }

        /**
         * This method checks whether there is enough room in the cluster to add the given number of shards with the given number of
         * replicas without exceeding the max_shards_per_node requirement as specified by maxConfiguredShardsPerNode.
         * This check does not guarantee that the number of shards can be added, just that there is theoretically room to add them
         * without exceeding the shards per node configuration.
         * @param maxConfiguredShardsPerNode The maximum available number of shards to be allocated within a node
         * @param numberOfNewShards          The number of primary shards that we want to be able to add to the cluster
         * @param replicas                   The number of replicas of the primary shards that we want to be able to add to the cluster
         * @param discoveryNodes             The nodes in the cluster, used to get the number of open shard already in the cluster
         * @param metadata                   The cluster state metadata, used to get the cluster settings
         */
        public Result checkShardLimit(
            int maxConfiguredShardsPerNode,
            int numberOfNewShards,
            int replicas,
            DiscoveryNodes discoveryNodes,
            Metadata metadata
        ) {
            return checkShardLimit(maxConfiguredShardsPerNode, newShardsTotal(numberOfNewShards, replicas), discoveryNodes, metadata);
        }

        private Result checkShardLimit(int maxConfiguredShardsPerNode, int newShards, DiscoveryNodes discoveryNodes, Metadata metadata) {
            final int nodeCount = numberOfNodes(discoveryNodes);
            int maxShardsInCluster = maxConfiguredShardsPerNode * nodeCount;
            int currentOpenShards = metadata.getTotalOpenIndexShards();

            // Only enforce the shard limit if we have at least one data node, so that we don't block
            // index creation during cluster setup
            if (nodeCount == 0 || newShards <= 0) {
                return new Result(true, Optional.empty(), newShards, maxShardsInCluster, this);
            }

            if ((currentOpenShards + newShards) > maxShardsInCluster) {
                long currentFilteredShards = metadata.projects()
                    .values()
                    .stream()
                    .flatMap(projectMetadata -> projectMetadata.indices().values().stream())
                    .mapToInt(this::countShards)
                    .sum();

                if ((currentFilteredShards + newShards) > maxShardsInCluster) {
                    return new Result(false, Optional.of(currentFilteredShards), newShards, maxShardsInCluster, this);
                }
            }
            return new Result(true, Optional.empty(), newShards, maxShardsInCluster, this);
        }
    }

    private static boolean isOpenIndex(IndexMetadata indexMetadata) {
        return indexMetadata.getState().equals(IndexMetadata.State.OPEN);
    }

    private static boolean matchesIndexSettingGroup(IndexMetadata indexMetadata, String group) {
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
        LimitGroup group
    ) {}

}
