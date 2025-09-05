/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.REPLACE;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * The {@link BalancedShardsAllocator} allocates and balances shards on the cluster nodes using {@link WeightFunction}.
 * The balancing attempts to:
 * <ul>
 *     <li>even shard count across nodes (weighted by cluster.routing.allocation.balance.shard)</li>
 *     <li>spread shards of the same index across different nodes (weighted by cluster.routing.allocation.balance.index)</li>
 *     <li>even write load of the data streams write indices across nodes (weighted by cluster.routing.allocation.balance.write_load)</li>
 *     <li>even disk usage across nodes (weighted by cluster.routing.allocation.balance.disk_usage)</li>
 * </ul>
 * The sensitivity of the algorithm is defined by cluster.routing.allocation.balance.threshold.
 * Allocator takes into account constraints set by {@code AllocationDeciders} when allocating and balancing shards.
 */
public class BalancedShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger(BalancedShardsAllocator.class);

    public static final Setting<Float> SHARD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.shard",
        0.45f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> INDEX_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.index",
        0.55f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> WRITE_LOAD_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.write_load",
        10.0f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> DISK_USAGE_BALANCE_FACTOR_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.disk_usage",
        2e-11f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Float> THRESHOLD_SETTING = Setting.floatSetting(
        "cluster.routing.allocation.balance.threshold",
        1.0f,
        1.0f,
        Property.Dynamic,
        Property.NodeScope
    );

    private final BalancerSettings balancerSettings;
    private final WriteLoadForecaster writeLoadForecaster;
    private final BalancingWeightsFactory balancingWeightsFactory;

    public BalancedShardsAllocator() {
        this(Settings.EMPTY);
    }

    public BalancedShardsAllocator(Settings settings) {
        this(new BalancerSettings(settings), WriteLoadForecaster.DEFAULT);
    }

    public BalancedShardsAllocator(BalancerSettings balancerSettings, WriteLoadForecaster writeLoadForecaster) {
        this(balancerSettings, writeLoadForecaster, new GlobalBalancingWeightsFactory(balancerSettings));
    }

    @Inject
    public BalancedShardsAllocator(
        BalancerSettings balancerSettings,
        WriteLoadForecaster writeLoadForecaster,
        BalancingWeightsFactory balancingWeightsFactory
    ) {
        this.balancerSettings = balancerSettings;
        this.writeLoadForecaster = writeLoadForecaster;
        this.balancingWeightsFactory = balancingWeightsFactory;
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        if (allocation.metadata().hasAnyIndices()) {
            // must not use licensed features when just starting up
            writeLoadForecaster.refreshLicense();
        }

        assert allocation.ignoreDisable() == false;

        if (allocation.routingNodes().size() == 0) {
            failAllocationOfNewPrimaries(allocation);
            return;
        }
        final BalancingWeights balancingWeights = balancingWeightsFactory.create();
        final Balancer balancer = new Balancer(writeLoadForecaster, allocation, balancerSettings.getThreshold(), balancingWeights);
        balancer.allocateUnassigned();
        balancer.moveShards();
        balancer.balance();

        // Node weights are calculated after each internal balancing round and saved to the RoutingNodes copy.
        collectAndRecordNodeWeightStats(balancer, balancingWeights, allocation);
    }

    private void collectAndRecordNodeWeightStats(Balancer balancer, BalancingWeights balancingWeights, RoutingAllocation allocation) {
        Map<DiscoveryNode, DesiredBalanceMetrics.NodeWeightStats> nodeLevelWeights = new HashMap<>();
        for (var entry : balancer.nodes.entrySet()) {
            var node = entry.getValue();
            var weightFunction = balancingWeights.weightFunctionForNode(node.routingNode);
            var nodeWeight = weightFunction.calculateNodeWeight(
                node.numShards(),
                balancer.avgShardsPerNode(),
                node.writeLoad(),
                balancer.avgWriteLoadPerNode(),
                node.diskUsageInBytes(),
                balancer.avgDiskUsageInBytesPerNode()
            );
            nodeLevelWeights.put(
                node.routingNode.node(),
                new DesiredBalanceMetrics.NodeWeightStats(node.numShards(), node.diskUsageInBytes(), node.writeLoad(), nodeWeight)
            );
        }
        allocation.routingNodes().setBalanceWeightStatsPerNode(nodeLevelWeights);
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(final ShardRouting shard, final RoutingAllocation allocation) {
        Balancer balancer = new Balancer(
            writeLoadForecaster,
            allocation,
            balancerSettings.getThreshold(),
            balancingWeightsFactory.create()
        );
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        MoveDecision moveDecision = MoveDecision.NOT_TAKEN;
        final ProjectIndex index = new ProjectIndex(allocation, shard);
        if (shard.unassigned()) {
            allocateUnassignedDecision = balancer.decideAllocateUnassigned(index, shard);
        } else {
            moveDecision = balancer.decideMove(index, shard);
            if (moveDecision.isDecisionTaken() && moveDecision.canRemain()) {
                moveDecision = balancer.decideRebalance(index, shard, moveDecision.getCanRemainDecision());
            }
        }
        return new ShardAllocationDecision(allocateUnassignedDecision, moveDecision);
    }

    private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
        RoutingNodes routingNodes = allocation.routingNodes();
        assert routingNodes.size() == 0 : routingNodes;
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shardRouting = unassignedIterator.next();
            final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (shardRouting.primary() && unassignedInfo.lastAllocationStatus() == AllocationStatus.NO_ATTEMPT) {
                unassignedIterator.updateUnassigned(
                    new UnassignedInfo(
                        unassignedInfo.reason(),
                        unassignedInfo.message(),
                        unassignedInfo.failure(),
                        unassignedInfo.failedAllocations(),
                        unassignedInfo.unassignedTimeNanos(),
                        unassignedInfo.unassignedTimeMillis(),
                        unassignedInfo.delayed(),
                        AllocationStatus.DECIDERS_NO,
                        unassignedInfo.failedNodeIds(),
                        unassignedInfo.lastAllocatedNodeId()
                    ),
                    shardRouting.recoverySource(),
                    allocation.changes()
                );
            }
        }
    }

    /**
     * A {@link Balancer}
     */
    public static class Balancer {
        private final WriteLoadForecaster writeLoadForecaster;
        private final RoutingAllocation allocation;
        private final RoutingNodes routingNodes;
        private final Metadata metadata;

        private final float threshold;
        private final float avgShardsPerNode;
        private final double avgWriteLoadPerNode;
        private final double avgDiskUsageInBytesPerNode;
        private final Map<String, ModelNode> nodes;
        private final BalancingWeights balancingWeights;
        private final NodeSorters nodeSorters;

        private Balancer(
            WriteLoadForecaster writeLoadForecaster,
            RoutingAllocation allocation,
            float threshold,
            BalancingWeights balancingWeights
        ) {
            this.writeLoadForecaster = writeLoadForecaster;
            this.allocation = allocation;
            this.routingNodes = allocation.routingNodes();
            this.metadata = allocation.metadata();
            this.threshold = threshold;
            avgShardsPerNode = WeightFunction.avgShardPerNode(metadata, routingNodes);
            avgWriteLoadPerNode = WeightFunction.avgWriteLoadPerNode(writeLoadForecaster, metadata, routingNodes);
            avgDiskUsageInBytesPerNode = WeightFunction.avgDiskUsageInBytesPerNode(allocation.clusterInfo(), metadata, routingNodes);
            nodes = Collections.unmodifiableMap(buildModelFromAssigned());
            this.nodeSorters = balancingWeights.createNodeSorters(nodesArray(), this);
            this.balancingWeights = balancingWeights;
        }

        private static long getShardDiskUsageInBytes(ShardRouting shardRouting, IndexMetadata indexMetadata, ClusterInfo clusterInfo) {
            if (indexMetadata.ignoreDiskWatermarks()) {
                // disk watermarks are ignored for partial searchable snapshots
                // and is equivalent to indexMetadata.isPartialSearchableSnapshot()
                return 0;
            }
            return Math.max(indexMetadata.getForecastedShardSizeInBytes().orElse(0L), clusterInfo.getShardSize(shardRouting, 0L));
        }

        private float getShardWriteLoad(ProjectIndex index) {
            final ProjectMetadata projectMetadata = metadata.getProject(index.project);
            return (float) writeLoadForecaster.getForecastedWriteLoad(projectMetadata.index(index.indexName)).orElse(0.0);
        }

        private float maxShardSizeBytes(ProjectIndex index) {
            final var indexMetadata = indexMetadata(index);
            if (indexMetadata.ignoreDiskWatermarks()) {
                // disk watermarks are ignored for partial searchable snapshots
                // and is equivalent to indexMetadata.isPartialSearchableSnapshot()
                return 0;
            }
            var maxShardSizeBytes = indexMetadata.getForecastedShardSizeInBytes().orElse(0L);
            for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                final var shardId = new ShardId(indexMetadata.getIndex(), shard);
                maxShardSizeBytes = maxWithNullable(maxShardSizeBytes, allocation.clusterInfo().getShardSize(shardId, true));
                maxShardSizeBytes = maxWithNullable(maxShardSizeBytes, allocation.clusterInfo().getShardSize(shardId, false));
            }
            return (float) maxShardSizeBytes;
        }

        private static long maxWithNullable(long accumulator, Long newValue) {
            return newValue == null ? accumulator : Math.max(accumulator, newValue);
        }

        /**
         * Returns an array view on the nodes in the balancer. Nodes should not be removed from this list.
         */
        private ModelNode[] nodesArray() {
            return nodes.values().toArray(ModelNode[]::new);
        }

        /**
         * Returns the average of shards per node for the given index
         */
        public float avgShardsPerNode(ProjectIndex index) {
            return ((float) indexMetadata(index).getTotalNumberOfShards()) / nodes.size();
        }

        /**
         * Returns the global average of shards per node
         */
        public float avgShardsPerNode() {
            return avgShardsPerNode;
        }

        public double avgWriteLoadPerNode() {
            return avgWriteLoadPerNode;
        }

        public double avgDiskUsageInBytesPerNode() {
            return avgDiskUsageInBytesPerNode;
        }

        /**
         * The absolute value difference between two weights.
         */
        private static float absDelta(float lower, float higher) {
            assert higher >= lower : higher + " lt " + lower + " but was expected to be gte";
            return Math.abs(higher - lower);
        }

        /**
         * Returns {@code true} iff the weight delta between two nodes is under a defined threshold.
         * See {@link #THRESHOLD_SETTING} for defining the threshold.
         */
        private static boolean lessThan(float delta, float threshold) {
            /* deltas close to the threshold are "rounded" to the threshold manually
               to prevent floating point problems if the delta is very close to the
               threshold ie. 1.000000002 which can trigger unnecessary balance actions*/
            return delta <= (threshold + 0.001f);
        }

        private IndexMetadata indexMetadata(ProjectIndex index) {
            return metadata.getProject(index.project).index(index.indexName);
        }

        /**
         * Balances the nodes on the cluster model according to the weight function.
         * The actual balancing is delegated to {@link #balanceByWeights(NodeSorter)}
         */
        private void balance() {
            if (logger.isTraceEnabled()) {
                logger.trace("Start balancing cluster");
            }
            if (allocation.hasPendingAsyncFetch()) {
                /*
                 * see https://github.com/elastic/elasticsearch/issues/14387
                 * if we allow rebalance operations while we are still fetching shard store data
                 * we might end up with unnecessary rebalance operations which can be super confusion/frustrating
                 * since once the fetches come back we might just move all the shards back again.
                 * Therefore we only do a rebalance if we have fetched all information.
                 */
                logger.debug("skipping rebalance due to in-flight shard/store fetches");
                return;
            }
            if (allocation.deciders().canRebalance(allocation).type() != Type.YES) {
                logger.trace("skipping rebalance as it is disabled");
                return;
            }

            // Balance each partition
            for (NodeSorter nodeSorter : nodeSorters) {
                if (nodeSorter.modelNodes.length < 2) { /* skip if we only have one node */
                    logger.trace("skipping rebalance as the partition has single node only");
                    continue;
                }
                balanceByWeights(nodeSorter);
            }
        }

        /**
         * Makes a decision about moving a single shard to a different node to form a more
         * optimally balanced cluster. This method is invoked from the cluster allocation
         * explain API only.
         */
        private MoveDecision decideRebalance(final ProjectIndex index, final ShardRouting shard, Decision canRemain) {
            final NodeSorter sorter = nodeSorters.sorterForShard(shard);
            index.assertMatch(shard);
            if (shard.started() == false) {
                // we can only rebalance started shards
                return MoveDecision.NOT_TAKEN;
            }

            Decision canRebalance = allocation.deciders().canRebalance(shard, allocation);

            sorter.reset(index);
            ModelNode[] modelNodes = sorter.modelNodes;
            final String currentNodeId = shard.currentNodeId();
            // find currently assigned node
            ModelNode currentNode = null;
            for (ModelNode node : modelNodes) {
                if (node.getNodeId().equals(currentNodeId)) {
                    currentNode = node;
                    break;
                }
            }
            assert currentNode != null : "currently assigned node could not be found";

            // balance the shard, if a better node can be found
            final float currentWeight = sorter.getWeightFunction().calculateNodeWeightWithIndex(this, currentNode, index);
            final AllocationDeciders deciders = allocation.deciders();
            Type rebalanceDecisionType = Type.NO;
            ModelNode targetNode = null;
            List<Tuple<ModelNode, Decision>> betterBalanceNodes = new ArrayList<>();
            List<Tuple<ModelNode, Decision>> sameBalanceNodes = new ArrayList<>();
            List<Tuple<ModelNode, Decision>> worseBalanceNodes = new ArrayList<>();
            for (ModelNode node : modelNodes) {
                if (node == currentNode) {
                    continue; // skip over node we're currently allocated to
                }
                final Decision canAllocate = deciders.canAllocate(shard, node.getRoutingNode(), allocation);
                // the current weight of the node in the cluster, as computed by the weight function;
                // this is a comparison of the number of shards on this node to the number of shards
                // that should be on each node on average (both taking the cluster as a whole into account
                // as well as shards per index)
                final float nodeWeight = sorter.getWeightFunction().calculateNodeWeightWithIndex(this, node, index);
                // if the node we are examining has a worse (higher) weight than the node the shard is
                // assigned to, then there is no way moving the shard to the node with the worse weight
                // can make the balance of the cluster better, so we check for that here
                final boolean betterWeightThanCurrent = nodeWeight <= currentWeight;
                boolean rebalanceConditionsMet = false;
                if (betterWeightThanCurrent) {
                    // get the delta between the weights of the node we are checking and the node that holds the shard
                    float currentDelta = absDelta(nodeWeight, currentWeight);
                    // checks if the weight delta is above a certain threshold; if it is not above a certain threshold,
                    // then even though the node we are examining has a better weight and may make the cluster balance
                    // more even, it doesn't make sense to execute the heavyweight operation of relocating a shard unless
                    // the gains make it worth it, as defined by the threshold
                    final float localThreshold = sorter.minWeightDelta() * threshold;
                    boolean deltaAboveThreshold = lessThan(currentDelta, localThreshold) == false;
                    // calculate the delta of the weights of the two nodes if we were to add the shard to the
                    // node in question and move it away from the node that currently holds it.
                    boolean betterWeightWithShardAdded = nodeWeight + localThreshold < currentWeight;
                    rebalanceConditionsMet = deltaAboveThreshold && betterWeightWithShardAdded;
                    // if the simulated weight delta with the shard moved away is better than the weight delta
                    // with the shard remaining on the current node, and we are allowed to allocate to the
                    // node in question, then allow the rebalance
                    if (rebalanceConditionsMet && canAllocate.type().higherThan(rebalanceDecisionType)) {
                        // rebalance to the node, only will get overwritten if the decision here is to
                        // THROTTLE and we get a decision with YES on another node
                        rebalanceDecisionType = canAllocate.type();
                        targetNode = node;
                    }
                }
                Tuple<ModelNode, Decision> nodeResult = Tuple.tuple(node, canAllocate);
                if (rebalanceConditionsMet) {
                    betterBalanceNodes.add(nodeResult);
                } else if (betterWeightThanCurrent) {
                    sameBalanceNodes.add(nodeResult);
                } else {
                    worseBalanceNodes.add(nodeResult);
                }
            }

            int weightRanking = 0;
            List<NodeAllocationResult> nodeDecisions = new ArrayList<>(modelNodes.length - 1);
            for (Tuple<ModelNode, Decision> result : betterBalanceNodes) {
                nodeDecisions.add(
                    new NodeAllocationResult(
                        result.v1().routingNode.node(),
                        AllocationDecision.fromDecisionType(result.v2().type()),
                        result.v2(),
                        ++weightRanking
                    )
                );
            }
            int currentNodeWeightRanking = ++weightRanking;
            for (Tuple<ModelNode, Decision> result : sameBalanceNodes) {
                AllocationDecision nodeDecision = result.v2().type() == Type.NO ? AllocationDecision.NO : AllocationDecision.WORSE_BALANCE;
                nodeDecisions.add(
                    new NodeAllocationResult(result.v1().routingNode.node(), nodeDecision, result.v2(), currentNodeWeightRanking)
                );
            }
            for (Tuple<ModelNode, Decision> result : worseBalanceNodes) {
                AllocationDecision nodeDecision = result.v2().type() == Type.NO ? AllocationDecision.NO : AllocationDecision.WORSE_BALANCE;
                nodeDecisions.add(new NodeAllocationResult(result.v1().routingNode.node(), nodeDecision, result.v2(), ++weightRanking));
            }

            if (canRebalance.type() != Type.YES || allocation.hasPendingAsyncFetch()) {
                // can not rebalance
                return MoveDecision.rebalance(
                    canRemain,
                    canRebalance,
                    allocation.hasPendingAsyncFetch()
                        ? AllocationDecision.AWAITING_INFO
                        : AllocationDecision.fromDecisionType(canRebalance.type()),
                    null,
                    currentNodeWeightRanking,
                    nodeDecisions
                );
            } else {
                return MoveDecision.rebalance(
                    canRemain,
                    canRebalance,
                    AllocationDecision.fromDecisionType(rebalanceDecisionType),
                    targetNode != null ? targetNode.routingNode.node() : null,
                    currentNodeWeightRanking,
                    nodeDecisions
                );
            }
        }

        /**
         * Balances the nodes on the cluster model according to the weight
         * function. The configured threshold is the minimum delta between the
         * weight of the maximum node and the minimum node according to the
         * {@link WeightFunction}. This weight is calculated per index to
         * distribute shards evenly per index. The balancer tries to relocate
         * shards only if the delta exceeds the threshold. In the default case
         * the threshold is set to {@code 1.0} to enforce gaining relocation
         * only, or in other words relocations that move the weight delta closer
         * to {@code 0.0}
         */
        private void balanceByWeights(NodeSorter sorter) {
            final AllocationDeciders deciders = allocation.deciders();
            final ModelNode[] modelNodes = sorter.modelNodes;
            final float[] weights = sorter.weights;
            for (var index : buildWeightOrderedIndices(sorter)) {
                IndexMetadata indexMetadata = indexMetadata(index);

                // find nodes that have a shard of this index or where shards of this index are allowed to be allocated to,
                // move these nodes to the front of modelNodes so that we can only balance based on these nodes
                int relevantNodes = 0;
                for (int i = 0; i < modelNodes.length; i++) {
                    ModelNode modelNode = modelNodes[i];
                    if (modelNode.getIndex(index) != null
                        || deciders.canAllocate(indexMetadata, modelNode.getRoutingNode(), allocation).type() != Type.NO) {
                        // swap nodes at position i and relevantNodes
                        modelNodes[i] = modelNodes[relevantNodes];
                        modelNodes[relevantNodes] = modelNode;
                        relevantNodes++;
                    }
                }

                if (relevantNodes < 2) {
                    continue;
                }

                sorter.reset(index, 0, relevantNodes);
                int lowIdx = 0;
                int highIdx = relevantNodes - 1;
                final float localThreshold = sorter.minWeightDelta() * threshold;
                while (true) {
                    final ModelNode minNode = modelNodes[lowIdx];
                    final ModelNode maxNode = modelNodes[highIdx];
                    advance_range: if (maxNode.numShards(index) > 0) {
                        final float delta = absDelta(weights[lowIdx], weights[highIdx]);
                        if (lessThan(delta, localThreshold)) {
                            if (lowIdx > 0
                                && highIdx - 1 > 0 // is there a chance for a higher delta?
                                && (absDelta(weights[0], weights[highIdx - 1]) > localThreshold) // check if we need to break at all
                            ) {
                                /* This is a special case if allocations from the "heaviest" to the "lighter" nodes is not possible
                                 * due to some allocation decider restrictions like zone awareness. if one zone has for instance
                                 * less nodes than another zone. so one zone is horribly overloaded from a balanced perspective but we
                                 * can't move to the "lighter" shards since otherwise the zone would go over capacity.
                                 *
                                 * This break jumps straight to the condition below were we start moving from the high index towards
                                 * the low index to shrink the window we are considering for balance from the other direction.
                                 * (check shrinking the window from MAX to MIN)
                                 * See #3580
                                 */
                                break advance_range;
                            }
                            if (logger.isTraceEnabled()) {
                                logger.trace(
                                    "Stop balancing index [{}]  min_node [{}] weight: [{}] max_node [{}] weight: [{}] delta: [{}]",
                                    index,
                                    maxNode.getNodeId(),
                                    weights[highIdx],
                                    minNode.getNodeId(),
                                    weights[lowIdx],
                                    delta
                                );
                            }
                            break;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "Balancing from node [{}] weight: [{}] to node [{}] weight: [{}] delta: [{}]",
                                maxNode.getNodeId(),
                                weights[highIdx],
                                minNode.getNodeId(),
                                weights[lowIdx],
                                delta
                            );
                        }
                        if (delta <= localThreshold) {
                            /*
                             * prevent relocations that only swap the weights of the two nodes. a relocation must bring us closer to the
                             * balance if we only achieve the same delta the relocation is useless
                             *
                             * NB this comment above was preserved from an earlier version but doesn't obviously describe the code today. We
                             * already know that lessThan(delta, threshold) == false and threshold defaults to 1.0, so by default we never
                             * hit this case anyway.
                             */
                            logger.trace(
                                "Couldn't find shard to relocate from node [{}] to node [{}]",
                                maxNode.getNodeId(),
                                minNode.getNodeId()
                            );
                        } else if (tryRelocateShard(minNode, maxNode, index)) {
                            /*
                             * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                             * we could just find the place to insert linearly but the win might be minor
                             * compared to the added complexity
                             */
                            weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                            weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                            sorter.sort(0, relevantNodes);
                            lowIdx = 0;
                            highIdx = relevantNodes - 1;
                            continue;
                        }
                    }
                    if (lowIdx < highIdx - 1) {
                        /* Shrinking the window from MIN to MAX
                         * we can't move from any shard from the min node lets move on to the next node
                         * and see if the threshold still holds. We either don't have any shard of this
                         * index on this node of allocation deciders prevent any relocation.*/
                        lowIdx++;
                    } else if (lowIdx > 0) {
                        /* Shrinking the window from MAX to MIN
                         * now we go max to min since obviously we can't move anything to the max node
                         * lets pick the next highest */
                        lowIdx = 0;
                        highIdx--;
                    } else {
                        /* we are done here, we either can't relocate anymore or we are balanced */
                        break;
                    }
                }
            }
        }

        /**
         * This builds a initial index ordering where the indices are returned
         * in most unbalanced first. We need this in order to prevent over
         * allocations on added nodes from one index when the weight parameters
         * for global balance overrule the index balance at an intermediate
         * state. For example this can happen if we have 3 nodes and 3 indices
         * with 3 primary and 1 replica shards. At the first stage all three nodes hold
         * 2 shard for each index. Now we add another node and the first index
         * is balanced moving three shards from two of the nodes over to the new node since it
         * has no shards yet and global balance for the node is way below
         * average. To re-balance we need to move shards back eventually likely
         * to the nodes we relocated them from.
         */
        private ProjectIndex[] buildWeightOrderedIndices(NodeSorter sorter) {
            final ProjectIndex[] indices = allocation.globalRoutingTable()
                .routingTables()
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().indicesRouting().keySet().stream().map(index -> new ProjectIndex(entry.getKey(), index)))
                .toArray(ProjectIndex[]::new);
            final float[] deltas = new float[indices.length];
            for (int i = 0; i < deltas.length; i++) {
                sorter.reset(indices[i]);
                deltas[i] = sorter.delta();
            }
            new IntroSorter() {

                float pivotWeight;

                @Override
                protected void swap(int i, int j) {
                    final var tmpIdx = indices[i];
                    indices[i] = indices[j];
                    indices[j] = tmpIdx;
                    final float tmpDelta = deltas[i];
                    deltas[i] = deltas[j];
                    deltas[j] = tmpDelta;
                }

                @Override
                protected int compare(int i, int j) {
                    return Float.compare(deltas[j], deltas[i]);
                }

                @Override
                protected void setPivot(int i) {
                    pivotWeight = deltas[i];
                }

                @Override
                protected int comparePivot(int j) {
                    return Float.compare(deltas[j], pivotWeight);
                }
            }.sort(0, deltas.length);

            return indices;
        }

        /**
         * Move started shards that can not be allocated to a node anymore
         *
         * For each shard to be moved this function executes a move operation
         * to the minimal eligible node with respect to the
         * weight function. If a shard is moved the shard will be set to
         * {@link ShardRoutingState#RELOCATING} and a shadow instance of this
         * shard is created with an incremented version in the state
         * {@link ShardRoutingState#INITIALIZING}.
         */
        public void moveShards() {
            // Iterate over the started shards interleaving between nodes, and check if they can remain. In the presence of throttling
            // shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
            // offloading the shards.
            for (Iterator<ShardRouting> it = allocation.routingNodes().nodeInterleavedShardIterator(); it.hasNext();) {
                ShardRouting shardRouting = it.next();
                ProjectIndex index = projectIndex(shardRouting);
                final MoveDecision moveDecision = decideMove(index, shardRouting);
                if (moveDecision.isDecisionTaken() && moveDecision.forceMove()) {
                    final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
                    final ModelNode targetNode = nodes.get(moveDecision.getTargetNode().getId());
                    sourceNode.removeShard(index, shardRouting);
                    Tuple<ShardRouting, ShardRouting> relocatingShards = routingNodes.relocateShard(
                        shardRouting,
                        targetNode.getNodeId(),
                        allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                        "move",
                        allocation.changes()
                    );
                    final ShardRouting shard = relocatingShards.v2();
                    targetNode.addShard(projectIndex(shard), shard);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Moved shard [{}] to node [{}]", shardRouting, targetNode.getRoutingNode());
                    }
                } else if (moveDecision.isDecisionTaken() && moveDecision.canRemain() == false) {
                    logger.trace("[{}][{}] can't move", shardRouting.index(), shardRouting.id());
                }
            }
        }

        /**
         * Makes a decision on whether to move a started shard to another node. The following rules apply
         * to the {@link MoveDecision} return object:
         *   1. If the shard is not started, no decision will be taken and {@link MoveDecision#isDecisionTaken()} will return false.
         *   2. If the shard is allowed to remain on its current node, no attempt will be made to move the shard and
         *      {@link MoveDecision#getCanRemainDecision} will have a decision type of YES. All other fields in the object will be null.
         *   3. If the shard is not allowed to remain on its current node, then {@link MoveDecision#getAllocationDecision()} will be
         *      populated with the decision of moving to another node. If {@link MoveDecision#forceMove()} ()} returns {@code true}, then
         *      {@link MoveDecision#getTargetNode} will return a non-null value, otherwise the assignedNodeId will be null.
         *   4. If the method is invoked in explain mode (e.g. from the cluster allocation explain APIs), then
         *      {@link MoveDecision#getNodeDecisions} will have a non-null value.
         */
        public MoveDecision decideMove(final ProjectIndex index, final ShardRouting shardRouting) {
            NodeSorter sorter = nodeSorters.sorterForShard(shardRouting);
            index.assertMatch(shardRouting);

            if (shardRouting.started() == false) {
                // we can only move started shards
                return MoveDecision.NOT_TAKEN;
            }

            final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
            assert sourceNode != null && sourceNode.containsShard(index, shardRouting);
            RoutingNode routingNode = sourceNode.getRoutingNode();
            Decision canRemain = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            if (canRemain.type() != Decision.Type.NO) {
                return MoveDecision.remain(canRemain);
            }

            sorter.reset(index);
            /*
             * the sorter holds the minimum weight node first for the shards index.
             * We now walk through the nodes until we find a node to allocate the shard.
             * This is not guaranteed to be balanced after this operation we still try best effort to
             * allocate on the minimal eligible node.
             */
            MoveDecision moveDecision = decideMove(sorter, shardRouting, sourceNode, canRemain, this::decideCanAllocate);
            if (moveDecision.canRemain() == false && moveDecision.forceMove() == false) {
                final boolean shardsOnReplacedNode = allocation.metadata().nodeShutdowns().contains(shardRouting.currentNodeId(), REPLACE);
                if (shardsOnReplacedNode) {
                    return decideMove(sorter, shardRouting, sourceNode, canRemain, this::decideCanForceAllocateForVacate);
                }
            }
            return moveDecision;
        }

        private MoveDecision decideMove(
            NodeSorter sorter,
            ShardRouting shardRouting,
            ModelNode sourceNode,
            Decision remainDecision,
            BiFunction<ShardRouting, RoutingNode, Decision> decider
        ) {
            final boolean explain = allocation.debugDecision();
            Type bestDecision = Type.NO;
            RoutingNode targetNode = null;
            final List<NodeAllocationResult> nodeResults = explain ? new ArrayList<>() : null;
            int weightRanking = 0;
            for (ModelNode currentNode : sorter.modelNodes) {
                if (currentNode != sourceNode) {
                    RoutingNode target = currentNode.getRoutingNode();
                    Decision allocationDecision = decider.apply(shardRouting, target);
                    if (explain) {
                        nodeResults.add(new NodeAllocationResult(currentNode.getRoutingNode().node(), allocationDecision, ++weightRanking));
                    }
                    // TODO maybe we can respect throttling here too?
                    if (allocationDecision.type().higherThan(bestDecision)) {
                        bestDecision = allocationDecision.type();
                        if (bestDecision == Type.YES) {
                            targetNode = target;
                            if (explain == false) {
                                // we are not in explain mode and already have a YES decision on the best weighted node,
                                // no need to continue iterating
                                break;
                            }
                        }
                    }
                }
            }

            return MoveDecision.move(
                remainDecision,
                AllocationDecision.fromDecisionType(bestDecision),
                targetNode != null ? targetNode.node() : null,
                nodeResults
            );
        }

        private Decision decideCanAllocate(ShardRouting shardRouting, RoutingNode target) {
            // don't use canRebalance as we want hard filtering rules to apply. See #17698
            return allocation.deciders().canAllocate(shardRouting, target, allocation);
        }

        private Decision decideCanForceAllocateForVacate(ShardRouting shardRouting, RoutingNode target) {
            return allocation.deciders().canForceAllocateDuringReplace(shardRouting, target, allocation);
        }

        /**
         * Builds the internal model from all shards in the given
         * {@link Iterable}. All shards in the {@link Iterable} must be assigned
         * to a node. This method will skip shards in the state
         * {@link ShardRoutingState#RELOCATING} since each relocating shard has
         * a shadow shard in the state {@link ShardRoutingState#INITIALIZING}
         * on the target node which we respect during the allocation / balancing
         * process. In short, this method recreates the status-quo in the cluster.
         */
        private Map<String, ModelNode> buildModelFromAssigned() {
            Map<String, ModelNode> nodes = Maps.newMapWithExpectedSize(routingNodes.size());
            for (RoutingNode rn : routingNodes) {
                ModelNode node = new ModelNode(writeLoadForecaster, metadata, allocation.clusterInfo(), rn);
                nodes.put(rn.nodeId(), node);
                for (ShardRouting shard : rn) {
                    assert rn.nodeId().equals(shard.currentNodeId());
                    /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                    if (shard.state() != RELOCATING) {
                        node.addShard(projectIndex(shard), shard);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Assigned shard [{}] to node [{}]", shard, node.getNodeId());
                        }
                    }
                }
            }
            return nodes;
        }

        /**
         * Allocates all given shards on the minimal eligible node for the shards index
         * with respect to the weight function. All given shards must be unassigned.
         */
        private void allocateUnassigned() {
            RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            assert nodes.isEmpty() == false;
            if (logger.isTraceEnabled()) {
                logger.trace("Start allocating unassigned shards");
            }
            if (unassigned.isEmpty()) {
                return;
            }

            /*
             * TODO: We could be smarter here and group the shards by index and then
             * use the sorter to save some iterations.
             */
            final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
            final Comparator<ShardRouting> comparator = (o1, o2) -> {
                if (o1.primary() ^ o2.primary()) {
                    return o1.primary() ? -1 : 1;
                }
                if (o1.getIndexName().compareTo(o2.getIndexName()) == 0) {
                    return o1.getId() - o2.getId();
                }
                // this comparator is more expensive than all the others up there
                // that's why it's added last even though it could be easier to read
                // if we'd apply it earlier. this comparator will only differentiate across
                // indices all shards of the same index is treated equally.
                final int secondary = secondaryComparator.compare(o1, o2);
                assert secondary != 0 : "Index names are equal, should be returned early.";
                return secondary;
            };
            /*
             * we use 2 arrays and move replicas to the second array once we allocated an identical
             * replica in the current iteration to make sure all indices get allocated in the same manner.
             * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with
             * 2 replica and 1 shard would look like:
             * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
             * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
             * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
             */
            ShardRouting[] primary = unassigned.drain();
            ShardRouting[] secondary = new ShardRouting[primary.length];
            int secondaryLength = 0;
            int primaryLength = primary.length;
            ArrayUtil.timSort(primary, comparator);
            do {
                for (int i = 0; i < primaryLength; i++) {
                    ShardRouting shard = primary[i];
                    final ProjectIndex index = projectIndex(shard);
                    final AllocateUnassignedDecision allocationDecision = decideAllocateUnassigned(index, shard);
                    final String assignedNodeId = allocationDecision.getTargetNode() != null
                        ? allocationDecision.getTargetNode().getId()
                        : null;
                    final ModelNode minNode = assignedNodeId != null ? nodes.get(assignedNodeId) : null;

                    if (allocationDecision.getAllocationDecision() == AllocationDecision.YES) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                        }

                        final long shardSize = getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation);
                        shard = routingNodes.initializeShard(shard, minNode.getNodeId(), null, shardSize, allocation.changes());
                        minNode.addShard(index, shard);
                        if (shard.primary() == false) {
                            // copy over the same replica shards to the secondary array so they will get allocated
                            // in a subsequent iteration, allowing replicas of other shards to be allocated first
                            while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                                secondary[secondaryLength++] = primary[++i];
                            }
                        }
                    } else {
                        // did *not* receive a YES decision
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "No eligible node found to assign shard [{}] allocation_status [{}]",
                                shard,
                                allocationDecision.getAllocationStatus()
                            );
                        }

                        if (minNode != null) {
                            // throttle decision scenario
                            assert allocationDecision.getAllocationStatus() == AllocationStatus.DECIDERS_THROTTLED;
                            final long shardSize = getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation);
                            minNode.addShard(projectIndex(shard), shard.initialize(minNode.getNodeId(), null, shardSize));
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("No Node found to assign shard [{}]", shard);
                            }
                        }

                        unassigned.ignoreShard(shard, allocationDecision.getAllocationStatus(), allocation.changes());
                        if (shard.primary() == false) {
                            // we could not allocate it and we are a replica - check if we can ignore the other replicas
                            while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                                unassigned.ignoreShard(primary[++i], allocationDecision.getAllocationStatus(), allocation.changes());
                            }
                        }
                    }
                }
                primaryLength = secondaryLength;
                ShardRouting[] tmp = primary;
                primary = secondary;
                secondary = tmp;
                secondaryLength = 0;
            } while (primaryLength > 0);
            // clear everything we have either added it or moved to ignoreUnassigned
        }

        private ProjectIndex projectIndex(ShardRouting shardRouting) {
            return new ProjectIndex(allocation, shardRouting);
        }

        /**
         * Make a decision for allocating an unassigned shard. This method returns a two values in a tuple: the
         * first value is the {@link Decision} taken to allocate the unassigned shard, the second value is the
         * {@link ModelNode} representing the node that the shard should be assigned to. If the decision returned
         * is of type {@link Type#NO}, then the assigned node will be null.
         */
        private AllocateUnassignedDecision decideAllocateUnassigned(final ProjectIndex index, final ShardRouting shard) {
            WeightFunction weightFunction = balancingWeights.weightFunctionForShard(shard);
            index.assertMatch(shard);
            if (shard.assignedToNode()) {
                // we only make decisions for unassigned shards here
                return AllocateUnassignedDecision.NOT_TAKEN;
            }

            final boolean explain = allocation.debugDecision();
            Decision shardLevelDecision = allocation.deciders().canAllocate(shard, allocation);
            if (shardLevelDecision.type() == Type.NO && explain == false) {
                // NO decision for allocating the shard, irrespective of any particular node, so exit early
                return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, null);
            }

            /* find an node with minimal weight we can allocate on*/
            float minWeight = Float.POSITIVE_INFINITY;
            ModelNode minNode = null;
            Decision decision = null;
            /* Don't iterate over an identity hashset here the
             * iteration order is different for each run and makes testing hard */
            Map<String, NodeAllocationResult> nodeExplanationMap = explain ? new HashMap<>() : null;
            List<Tuple<String, Float>> nodeWeights = explain ? new ArrayList<>() : null;
            for (ModelNode node : nodes.values()) {
                if (node.containsShard(index, shard) && explain == false) {
                    // decision is NO without needing to check anything further, so short circuit
                    continue;
                }

                // weight of this index currently on the node
                float currentWeight = weightFunction.calculateNodeWeightWithIndex(this, node, index);
                // moving the shard would not improve the balance, and we are not in explain mode, so short circuit
                if (currentWeight > minWeight && explain == false) {
                    continue;
                }

                Decision currentDecision = allocation.deciders().canAllocate(shard, node.getRoutingNode(), allocation);
                if (explain) {
                    nodeExplanationMap.put(node.getNodeId(), new NodeAllocationResult(node.getRoutingNode().node(), currentDecision, 0));
                    nodeWeights.add(Tuple.tuple(node.getNodeId(), currentWeight));
                }
                if (currentDecision.type() == Type.YES || currentDecision.type() == Type.THROTTLE) {
                    final boolean updateMinNode;
                    if (currentWeight == minWeight) {
                        /*  we have an equal weight tie breaking:
                         *  1. if one decision is YES prefer it
                         *  2. prefer the node that holds the primary for this index with the next id in the ring ie.
                         *  for the 3 shards 2 replica case we try to build up:
                         *    1 2 0
                         *    2 0 1
                         *    0 1 2
                         *  such that if we need to tie-break we try to prefer the node holding a shard with the minimal id greater
                         *  than the id of the shard we need to assign. This works find when new indices are created since
                         *  primaries are added first and we only add one shard set a time in this algorithm.
                         */
                        if (currentDecision.type() == decision.type()) {
                            final int repId = shard.id();
                            final int nodeHigh = node.highestPrimary(index);
                            final int minNodeHigh = minNode.highestPrimary(index);
                            updateMinNode = ((((nodeHigh > repId && minNodeHigh > repId) || (nodeHigh < repId && minNodeHigh < repId))
                                && (nodeHigh < minNodeHigh)) || (nodeHigh > repId && minNodeHigh < repId));
                        } else {
                            updateMinNode = currentDecision.type() == Type.YES;
                        }
                    } else {
                        updateMinNode = currentWeight < minWeight;
                    }
                    if (updateMinNode) {
                        minNode = node;
                        minWeight = currentWeight;
                        decision = currentDecision;
                    }
                }
            }
            if (decision == null) {
                // decision was not set and a node was not assigned, so treat it as a NO decision
                decision = Decision.NO;
            }
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = new ArrayList<>();
                // fill in the correct weight ranking, once we've been through all nodes
                nodeWeights.sort((nodeWeight1, nodeWeight2) -> Float.compare(nodeWeight1.v2(), nodeWeight2.v2()));
                int weightRanking = 0;
                for (Tuple<String, Float> nodeWeight : nodeWeights) {
                    NodeAllocationResult current = nodeExplanationMap.get(nodeWeight.v1());
                    nodeDecisions.add(new NodeAllocationResult(current.getNode(), current.getCanAllocateDecision(), ++weightRanking));
                }
            }
            return AllocateUnassignedDecision.fromDecision(decision, minNode != null ? minNode.routingNode.node() : null, nodeDecisions);
        }

        private static final Comparator<ShardRouting> BY_DESCENDING_SHARD_ID = (s1, s2) -> Integer.compare(s2.id(), s1.id());

        /**
         * Scratch space for accumulating/sorting the {@link ShardRouting} instances when contemplating moving the shards away from a node
         * in {@link #tryRelocateShard} - re-used to avoid extraneous allocations etc.
         */
        private ShardRouting[] shardRoutingsOnMaxWeightNode;

        /**
         * Tries to find a relocation from the max node to the minimal node for an arbitrary shard of the given index on the
         * balance model. Iff this method returns a <code>true</code> the relocation has already been executed on the
         * simulation model as well as on the cluster.
         */
        private boolean tryRelocateShard(ModelNode minNode, ModelNode maxNode, ProjectIndex idx) {
            final ModelIndex index = maxNode.getIndex(idx);
            if (index != null) {
                logger.trace("Try relocating shard of [{}] from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
                if (shardRoutingsOnMaxWeightNode == null || shardRoutingsOnMaxWeightNode.length < index.numShards()) {
                    shardRoutingsOnMaxWeightNode = new ShardRouting[index.numShards() * 2]; // oversized so reuse is more likely
                }

                int startedShards = 0;
                for (final var shardRouting : index) {
                    if (shardRouting.started()) { // cannot rebalance unassigned, initializing or relocating shards anyway
                        shardRoutingsOnMaxWeightNode[startedShards] = shardRouting;
                        startedShards += 1;
                    }
                }
                // check in descending order of shard id so that the decision is deterministic
                ArrayUtil.timSort(shardRoutingsOnMaxWeightNode, 0, startedShards, BY_DESCENDING_SHARD_ID);

                final AllocationDeciders deciders = allocation.deciders();
                for (int shardIndex = 0; shardIndex < startedShards; shardIndex++) {
                    final ShardRouting shard = shardRoutingsOnMaxWeightNode[shardIndex];

                    final Decision rebalanceDecision = deciders.canRebalance(shard, allocation);
                    if (rebalanceDecision.type() == Type.NO) {
                        continue;
                    }
                    final Decision allocationDecision = deciders.canAllocate(shard, minNode.getRoutingNode(), allocation);
                    if (allocationDecision.type() == Type.NO) {
                        continue;
                    }

                    final Decision.Type canAllocateOrRebalance = Decision.Type.min(allocationDecision.type(), rebalanceDecision.type());

                    maxNode.removeShard(projectIndex(shard), shard);
                    long shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

                    assert canAllocateOrRebalance == Type.YES || canAllocateOrRebalance == Type.THROTTLE : canAllocateOrRebalance;
                    logger.debug(
                        "decision [{}]: relocate [{}] from [{}] to [{}]",
                        canAllocateOrRebalance,
                        shard,
                        maxNode.getNodeId(),
                        minNode.getNodeId()
                    );
                    minNode.addShard(
                        projectIndex(shard),
                        canAllocateOrRebalance == Type.YES
                            /* only allocate on the cluster if we are not throttled */
                            ? routingNodes.relocateShard(shard, minNode.getNodeId(), shardSize, "rebalance", allocation.changes()).v1()
                            : shard.relocate(minNode.getNodeId(), shardSize)
                    );
                    return true;
                }
            }
            logger.trace("No shards of [{}] can relocate from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
            return false;
        }
    }

    public static class ModelNode implements Iterable<ModelIndex> {
        private int numShards = 0;
        private double writeLoad = 0.0;
        private double diskUsageInBytes = 0.0;
        private final WriteLoadForecaster writeLoadForecaster;
        private final Metadata metadata;
        private final ClusterInfo clusterInfo;
        private final RoutingNode routingNode;
        private final Map<ProjectIndex, ModelIndex> indices;

        public ModelNode(WriteLoadForecaster writeLoadForecaster, Metadata metadata, ClusterInfo clusterInfo, RoutingNode routingNode) {
            this.writeLoadForecaster = writeLoadForecaster;
            this.metadata = metadata;
            this.clusterInfo = clusterInfo;
            this.routingNode = routingNode;
            this.indices = Maps.newMapWithExpectedSize(routingNode.size() + 10);// some extra to account for shard movements
        }

        public ModelIndex getIndex(ProjectIndex index) {
            return indices.get(index);
        }

        public String getNodeId() {
            return routingNode.nodeId();
        }

        public RoutingNode getRoutingNode() {
            return routingNode;
        }

        public int numShards() {
            return numShards;
        }

        public int numShards(ProjectIndex idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numShards();
        }

        public double writeLoad() {
            return writeLoad;
        }

        public double diskUsageInBytes() {
            return diskUsageInBytes;
        }

        public int highestPrimary(ProjectIndex index) {
            ModelIndex idx = indices.get(index);
            if (idx != null) {
                return idx.highestPrimary();
            }
            return -1;
        }

        public void addShard(ProjectIndex index, ShardRouting shard) {
            index.assertMatch(shard);
            indices.computeIfAbsent(index, t -> new ModelIndex()).addShard(shard);
            IndexMetadata indexMetadata = metadata.getProject(index.project).index(shard.index());
            writeLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
            diskUsageInBytes += Balancer.getShardDiskUsageInBytes(shard, indexMetadata, clusterInfo);
            numShards++;
        }

        public void removeShard(ProjectIndex projectIndex, ShardRouting shard) {
            ModelIndex index = indices.get(projectIndex);
            if (index != null) {
                index.removeShard(shard);
                if (index.numShards() == 0) {
                    indices.remove(projectIndex);
                }
            }
            IndexMetadata indexMetadata = metadata.getProject(projectIndex.project).index(shard.index());
            writeLoad -= writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
            diskUsageInBytes -= Balancer.getShardDiskUsageInBytes(shard, indexMetadata, clusterInfo);
            numShards--;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Node(").append(routingNode.nodeId()).append(")");
            return sb.toString();
        }

        @Override
        public Iterator<ModelIndex> iterator() {
            return indices.values().iterator();
        }

        public boolean containsShard(ProjectIndex projIndex, ShardRouting shard) {
            projIndex.assertMatch(shard);
            ModelIndex index = getIndex(projIndex);
            return index != null && index.containsShard(shard);
        }
    }

    static final class ModelIndex implements Iterable<ShardRouting> {
        private final Set<ShardRouting> shards = Sets.newHashSetWithExpectedSize(4); // expect few shards of same index to be allocated on
                                                                                     // same node
        private int highestPrimary = -1;

        ModelIndex() {}

        public int highestPrimary() {
            if (highestPrimary == -1) {
                int maxId = -1;
                for (ShardRouting shard : shards) {
                    if (shard.primary()) {
                        maxId = Math.max(maxId, shard.id());
                    }
                }
                return highestPrimary = maxId;
            }
            return highestPrimary;
        }

        public int numShards() {
            return shards.size();
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return shards.iterator();
        }

        public void removeShard(ShardRouting shard) {
            highestPrimary = -1;
            assert shards.contains(shard) : "Shard not allocated on current node: " + shard;
            shards.remove(shard);
        }

        public void addShard(ShardRouting shard) {
            highestPrimary = -1;
            assert shards.contains(shard) == false : "Shard already allocated on current node: " + shard;
            shards.add(shard);
        }

        public boolean containsShard(ShardRouting shard) {
            return shards.contains(shard);
        }
    }

    /**
     * A NodeSorter sorts the set of nodes for a single partition using the {@link WeightFunction}
     * for that partition. In partitioned cluster topologies there will be one for each partition
     * (e.g. search/indexing in stateless). By default, there is a single partition containing
     * a single weight function that applies to all nodes and shards.
     *
     * @see BalancingWeightsFactory
     */
    public static final class NodeSorter extends IntroSorter {

        final ModelNode[] modelNodes;
        /* the nodes weights with respect to the current weight function / index */
        final float[] weights;
        private final WeightFunction function;
        private ProjectIndex index;
        private final Balancer balancer;
        private float pivotWeight;

        public NodeSorter(ModelNode[] modelNodes, WeightFunction function, Balancer balancer) {
            this.function = function;
            this.balancer = balancer;
            this.modelNodes = modelNodes;
            weights = new float[modelNodes.length];
        }

        /**
         * Resets the sorter, recalculates the weights per node and sorts the
         * nodes by weight, with minimal weight first.
         */
        public void reset(ProjectIndex index, int from, int to) {
            this.index = index;
            for (int i = from; i < to; i++) {
                weights[i] = weight(modelNodes[i]);
            }
            sort(from, to);
        }

        public void reset(ProjectIndex index) {
            reset(index, 0, modelNodes.length);
        }

        public float weight(ModelNode node) {
            return function.calculateNodeWeightWithIndex(balancer, node, index);
        }

        public float minWeightDelta() {
            return function.minWeightDelta(balancer.getShardWriteLoad(index), balancer.maxShardSizeBytes(index));
        }

        @Override
        protected void swap(int i, int j) {
            final ModelNode tmpNode = modelNodes[i];
            modelNodes[i] = modelNodes[j];
            modelNodes[j] = tmpNode;
            final float tmpWeight = weights[i];
            weights[i] = weights[j];
            weights[j] = tmpWeight;
        }

        @Override
        protected int compare(int i, int j) {
            return Float.compare(weights[i], weights[j]);
        }

        @Override
        protected void setPivot(int i) {
            pivotWeight = weights[i];
        }

        @Override
        protected int comparePivot(int j) {
            return Float.compare(pivotWeight, weights[j]);
        }

        public float delta() {
            return weights.length == 0 ? 0.0f : weights[weights.length - 1] - weights[0];
        }

        public WeightFunction getWeightFunction() {
            return function;
        }
    }

    record ProjectIndex(ProjectId project, String indexName) {
        ProjectIndex(RoutingAllocation allocation, ShardRouting shard) {
            this(allocation.metadata().projectFor(shard.index()).id(), shard.getIndexName());
        }

        public void assertMatch(ShardRouting shard) {
            assert indexName.equals(shard.getIndexName()) : "Index name mismatch [" + this + "] vs [" + shard + "]";
        }
    }
}
