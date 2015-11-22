/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * The {@link BalancedShardsAllocator} re-balances the nodes allocations
 * within an cluster based on a {@link WeightFunction}. The clusters balance is defined by four parameters which can be set
 * in the cluster update API that allows changes in real-time:
 * <ul><li><code>cluster.routing.allocation.balance.shard</code> - The <b>shard balance</b> defines the weight factor
 * for shards allocated on a {@link RoutingNode}</li>
 * <li><code>cluster.routing.allocation.balance.index</code> - The <b>index balance</b> defines a factor to the number
 * of {@link org.elasticsearch.cluster.routing.ShardRouting}s per index allocated on a specific node</li>
 * <li><code>cluster.routing.allocation.balance.threshold</code> - A <b>threshold</b> to set the minimal optimization
 * value of operations that should be performed</li>
 * </ul>
 * <p>
 * These parameters are combined in a {@link WeightFunction} that allows calculation of node weights which
 * are used to re-balance shards based on global as well as per-index factors.
 */
public class BalancedShardsAllocator extends AbstractComponent implements ShardsAllocator {

    public static final String SETTING_THRESHOLD = "cluster.routing.allocation.balance.threshold";
    public static final String SETTING_INDEX_BALANCE_FACTOR = "cluster.routing.allocation.balance.index";
    public static final String SETTING_SHARD_BALANCE_FACTOR = "cluster.routing.allocation.balance.shard";

    private static final float DEFAULT_INDEX_BALANCE_FACTOR = 0.55f;
    private static final float DEFAULT_SHARD_BALANCE_FACTOR = 0.45f;

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            final float indexBalance = settings.getAsFloat(SETTING_INDEX_BALANCE_FACTOR, weightFunction.indexBalance);
            final float shardBalance = settings.getAsFloat(SETTING_SHARD_BALANCE_FACTOR, weightFunction.shardBalance);
            float threshold = settings.getAsFloat(SETTING_THRESHOLD, BalancedShardsAllocator.this.threshold);
            if (threshold <= 0.0f) {
                throw new IllegalArgumentException("threshold must be greater than 0.0f but was: " + threshold);
            }
            BalancedShardsAllocator.this.threshold = threshold;
            BalancedShardsAllocator.this.weightFunction = new WeightFunction(indexBalance, shardBalance);
        }
    }

    private volatile WeightFunction weightFunction = new WeightFunction(DEFAULT_INDEX_BALANCE_FACTOR, DEFAULT_SHARD_BALANCE_FACTOR);

    private volatile float threshold = 1.0f;


    public BalancedShardsAllocator(Settings settings) {
        this(settings, new NodeSettingsService(settings));
    }

    @Inject
    public BalancedShardsAllocator(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        ApplySettings applySettings = new ApplySettings();
        applySettings.onRefreshSettings(settings);
        nodeSettingsService.addListener(applySettings);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) { /* ONLY FOR GATEWAYS */ }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) { /* ONLY FOR GATEWAYS */ }

    @Override
    public boolean allocateUnassigned(RoutingAllocation allocation) {
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        return balancer.allocateUnassigned();
    }

    @Override
    public boolean rebalance(RoutingAllocation allocation) {
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        return balancer.balance();
    }

    @Override
    public boolean move(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        return balancer.move(shardRouting, node);
    }

    /**
     * Returns the currently configured delta threshold
     */
    public float getThreshold() {
        return threshold;
    }

    /**
     * Returns the index related weight factor.
     */
    public float getIndexBalance() {
        return weightFunction.indexBalance;
    }

    /**
     * Returns the shard related weight factor.
     */
    public float getShardBalance() {
        return weightFunction.shardBalance;
    }


    /**
     * This class is the primary weight function used to create balanced over nodes and shards in the cluster.
     * Currently this function has 3 properties:
     * <ul>
     * <li><code>index balance</code> - balance property over shards per index</li>
     * <li><code>shard balance</code> - balance property over shards per cluster</li>
     * </ul>
     * <p>
     * Each of these properties are expressed as factor such that the properties factor defines the relative importance of the property for the
     * weight function. For example if the weight function should calculate the weights only based on a global (shard) balance the index balance
     * can be set to <tt>0.0</tt> and will in turn have no effect on the distribution.
     * </p>
     * The weight per index is calculated based on the following formula:
     * <ul>
     * <li>
     * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numShards(index) - avgShardsPerNode(index))</code>
     * </li>
     * <li>
     * <code>weight<sub>node</sub>(node, index) = shardBalance * (node.numShards() - avgShardsPerNode)</code>
     * </li>
     * </ul>
     * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index)</code>
     */
    public static class WeightFunction {

        private final float indexBalance;
        private final float shardBalance;
        private final float[] theta;


        public WeightFunction(float indexBalance, float shardBalance) {
            float sum = indexBalance + shardBalance;
            if (sum <= 0.0f) {
                throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
            }
            theta = new float[]{shardBalance / sum, indexBalance / sum};
            this.indexBalance = indexBalance;
            this.shardBalance = shardBalance;
        }

        public float weight(Operation operation, Balancer balancer, ModelNode node, String index) {
            final float weightShard = (node.numShards() - balancer.avgShardsPerNode());
            final float weightIndex = (node.numShards(index) - balancer.avgShardsPerNode(index));
            assert theta != null;
            return theta[0] * weightShard + theta[1] * weightIndex;
        }

    }

    /**
     * An enum that donates the actual operation the {@link WeightFunction} is
     * applied to.
     */
    public static enum Operation {
        /**
         * Provided during balance operations.
         */
        BALANCE,
        /**
         * Provided during initial allocation operation for unassigned shards.
         */
        ALLOCATE,
        /**
         * Provided during move operation.
         */
        MOVE
    }

    /**
     * A {@link Balancer}
     */
    public static class Balancer {
        private final ESLogger logger;
        private final Map<String, ModelNode> nodes = new HashMap<>();
        private final HashSet<String> indices = new HashSet<>();
        private final RoutingAllocation allocation;
        private final RoutingNodes routingNodes;
        private final WeightFunction weight;

        private final float threshold;
        private final MetaData metaData;

        private final Predicate<ShardRouting> assignedFilter = shard -> shard.assignedToNode();

        public Balancer(ESLogger logger, RoutingAllocation allocation, WeightFunction weight, float threshold) {
            this.logger = logger;
            this.allocation = allocation;
            this.weight = weight;
            this.threshold = threshold;
            this.routingNodes = allocation.routingNodes();
            for (RoutingNode node : routingNodes) {
                nodes.put(node.nodeId(), new ModelNode(node.nodeId()));
            }
            metaData = routingNodes.metaData();
        }

        /**
         * Returns an array view on the nodes in the balancer. Nodes should not be removed from this list.
         */
        private ModelNode[] nodesArray() {
            return nodes.values().toArray(new ModelNode[nodes.size()]);
        }

        /**
         * Returns the average of shards per node for the given index
         */
        public float avgShardsPerNode(String index) {
            return ((float) metaData.index(index).getTotalNumberOfShards()) / nodes.size();
        }

        /**
         * Returns the global average of shards per node
         */
        public float avgShardsPerNode() {
            return ((float) metaData.totalNumberOfShards()) / nodes.size();
        }

        /**
         * Returns the global average of primaries per node
         */
        public float avgPrimariesPerNode() {
            return ((float) metaData.numberOfShards()) / nodes.size();
        }


        /**
         * Returns a new {@link NodeSorter} that sorts the nodes based on their
         * current weight with respect to the index passed to the sorter. The
         * returned sorter is not sorted. Use {@link NodeSorter#reset(org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.Operation, String)}
         * to sort based on an index.
         */
        private NodeSorter newNodeSorter() {
            return new NodeSorter(nodesArray(), weight, this);
        }

        private boolean initialize(RoutingNodes routing, RoutingNodes.UnassignedShards unassigned) {
            if (logger.isTraceEnabled()) {
                logger.trace("Start distributing Shards");
            }
            for (ObjectCursor<String> index : allocation.routingTable().indicesRouting().keys()) {
                indices.add(index.value);
            }
            buildModelFromAssigned(routing.shards(assignedFilter));
            return allocateUnassigned(unassigned);
        }

        private static float absDelta(float lower, float higher) {
            assert higher >= lower : higher + " lt " + lower +" but was expected to be gte";
            return Math.abs(higher - lower);
        }

        private static boolean lessThan(float delta, float threshold) {
            /* deltas close to the threshold are "rounded" to the threshold manually
               to prevent floating point problems if the delta is very close to the
               threshold ie. 1.000000002 which can trigger unnecessary balance actions*/
            return delta <= (threshold + 0.001f);
        }

        /**
         * Allocates all possible unassigned shards
         * @return <code>true</code> if the current configuration has been
         *         changed, otherwise <code>false</code>
         */
        final boolean allocateUnassigned() {
            return balance(true);
        }

        /**
         * Balances the nodes on the cluster model according to the weight
         * function. The configured threshold is the minimum delta between the
         * weight of the maximum node and the minimum node according to the
         * {@link WeightFunction}. This weight is calculated per index to
         * distribute shards evenly per index. The balancer tries to relocate
         * shards only if the delta exceeds the threshold. If the default case
         * the threshold is set to <tt>1.0</tt> to enforce gaining relocation
         * only, or in other words relocations that move the weight delta closer
         * to <tt>0.0</tt>
         *
         * @return <code>true</code> if the current configuration has been
         *         changed, otherwise <code>false</code>
         */
        public boolean balance() {
            return balance(false);
        }

        private boolean balance(boolean onlyAssign) {
            if (this.nodes.isEmpty()) {
                /* with no nodes this is pointless */
                return false;
            }
            if (logger.isTraceEnabled()) {
                if (onlyAssign) {
                    logger.trace("Start balancing cluster");
                } else {
                    logger.trace("Start assigning unassigned shards");
                }
            }
            final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            boolean changed = initialize(routingNodes, unassigned);
            if (onlyAssign == false && changed == false && allocation.deciders().canRebalance(allocation).type() == Type.YES) {
                NodeSorter sorter = newNodeSorter();
                if (nodes.size() > 1) { /* skip if we only have one node */
                    for (String index : buildWeightOrderedIndidces(Operation.BALANCE, sorter)) {
                        sorter.reset(Operation.BALANCE, index);
                        final float[] weights = sorter.weights;
                        final ModelNode[] modelNodes = sorter.modelNodes;
                        int lowIdx = 0;
                        int highIdx = weights.length - 1;
                        while (true) {
                            final ModelNode minNode = modelNodes[lowIdx];
                            final ModelNode maxNode = modelNodes[highIdx];
                            advance_range:
                            if (maxNode.numShards(index) > 0) {
                                final float delta = absDelta(weights[lowIdx], weights[highIdx]);
                                if (lessThan(delta, threshold)) {
                                    if (lowIdx > 0 && highIdx-1 > 0 // is there a chance for a higher delta?
                                        && (absDelta(weights[0], weights[highIdx-1]) > threshold) // check if we need to break at all
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
                                        logger.trace("Stop balancing index [{}]  min_node [{}] weight: [{}]  max_node [{}] weight: [{}]  delta: [{}]",
                                                index, maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], delta);
                                    }
                                    break;
                                }
                                if (logger.isTraceEnabled()) {
                                    logger.trace("Balancing from node [{}] weight: [{}] to node [{}] weight: [{}]  delta: [{}]",
                                            maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], delta);
                                }
                                /* pass the delta to the replication function to prevent relocations that only swap the weights of the two nodes.
                                 * a relocation must bring us closer to the balance if we only achieve the same delta the relocation is useless */
                                if (tryRelocateShard(Operation.BALANCE, minNode, maxNode, index, delta)) {
                                    /*
                                     * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                                     * we could just find the place to insert linearly but the win might be minor
                                     * compared to the added complexity
                                     */
                                    weights[lowIdx] = sorter.weight(Operation.BALANCE, modelNodes[lowIdx]);
                                    weights[highIdx] = sorter.weight(Operation.BALANCE, modelNodes[highIdx]);
                                    sorter.sort(0, weights.length);
                                    lowIdx = 0;
                                    highIdx = weights.length - 1;
                                    changed = true;
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
            }
            return changed;
        }

        /**
         * This builds a initial index ordering where the indices are returned
         * in most unbalanced first. We need this in order to prevent over
         * allocations on added nodes from one index when the weight parameters
         * for global balance overrule the index balance at an intermediate
         * state. For example this can happen if we have 3 nodes and 3 indices
         * with 3 shards and 1 shard. At the first stage all three nodes hold
         * 2 shard for each index. now we add another node and the first index
         * is balanced moving 3 two of the nodes over to the new node since it
         * has no shards yet and global balance for the node is way below
         * average. To re-balance we need to move shards back eventually likely
         * to the nodes we relocated them from.
         */
        private String[] buildWeightOrderedIndidces(Operation operation, NodeSorter sorter) {
            final String[] indices = this.indices.toArray(new String[this.indices.size()]);
            final float[] deltas = new float[indices.length];
            for (int i = 0; i < deltas.length; i++) {
                sorter.reset(operation, indices[i]);
                deltas[i] = sorter.delta();
            }
            new IntroSorter() {

                float pivotWeight;

                @Override
                protected void swap(int i, int j) {
                    final String tmpIdx = indices[i];
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
         * This function executes a move operation moving the given shard from
         * the given node to the minimal eligible node with respect to the
         * weight function. Iff the shard is moved the shard will be set to
         * {@link ShardRoutingState#RELOCATING} and a shadow instance of this
         * shard is created with an incremented version in the state
         * {@link ShardRoutingState#INITIALIZING}.
         *
         * @return <code>true</code> iff the shard has successfully been moved.
         */
        public boolean move(ShardRouting shard, RoutingNode node ) {
            if (nodes.isEmpty() || !shard.started()) {
                /* with no nodes or a not started shard this is pointless */
                return false;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Try moving shard [{}] from [{}]", shard, node);
            }
            final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            boolean changed = initialize(routingNodes, unassigned);
            if (!changed) {
                final ModelNode sourceNode = nodes.get(node.nodeId());
                assert sourceNode != null;
                final NodeSorter sorter = newNodeSorter();
                sorter.reset(Operation.MOVE, shard.getIndex());
                final ModelNode[] nodes = sorter.modelNodes;
                assert sourceNode.containsShard(shard);
                /*
                 * the sorter holds the minimum weight node first for the shards index.
                 * We now walk through the nodes until we find a node to allocate the shard.
                 * This is not guaranteed to be balanced after this operation we still try best effort to
                 * allocate on the minimal eligible node.
                 */

                for (ModelNode currentNode : nodes) {
                    if (currentNode.getNodeId().equals(node.nodeId())) {
                        continue;
                    }
                    RoutingNode target = routingNodes.node(currentNode.getNodeId());
                    Decision allocationDecision = allocation.deciders().canAllocate(shard, target, allocation);
                    Decision rebalanceDecision = allocation.deciders().canRebalance(shard, allocation);
                    Decision decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecision);
                    if (decision.type() == Type.YES) { // TODO maybe we can respect throttling here too?
                        sourceNode.removeShard(shard);
                        ShardRouting targetRelocatingShard = routingNodes.relocate(shard, target.nodeId(), allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
                        currentNode.addShard(targetRelocatingShard, decision);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Moved shard [{}] to node [{}]", shard, currentNode.getNodeId());
                        }
                        changed = true;
                        break;
                    }
                }
            }
            return changed;
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
        private void buildModelFromAssigned(Iterable<ShardRouting> shards) {
            for (ShardRouting shard : shards) {
                assert shard.assignedToNode();
                /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                if (shard.state() == RELOCATING) {
                    continue;
                }
                ModelNode node = nodes.get(shard.currentNodeId());
                assert node != null;
                node.addShard(shard, Decision.single(Type.YES, "Already allocated on node", node.getNodeId()));
                if (logger.isTraceEnabled()) {
                    logger.trace("Assigned shard [{}] to node [{}]", shard, node.getNodeId());
                }
            }
        }

        /**
         * Allocates all given shards on the minimal eligable node for the shards index
         * with respect to the weight function. All given shards must be unassigned.
         */
        private boolean allocateUnassigned(RoutingNodes.UnassignedShards unassigned) {
            assert !nodes.isEmpty();
            if (logger.isTraceEnabled()) {
                logger.trace("Start allocating unassigned shards");
            }
            if (unassigned.isEmpty()) {
                return false;
            }
            boolean changed = false;

            /*
             * TODO: We could be smarter here and group the shards by index and then
             * use the sorter to save some iterations.
             */
            final AllocationDeciders deciders = allocation.deciders();
            final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
            final Comparator<ShardRouting> comparator = new Comparator<ShardRouting>() {
                @Override
                public int compare(ShardRouting o1,
                                   ShardRouting o2) {
                    if (o1.primary() ^ o2.primary()) {
                        return o1.primary() ? -1 : o2.primary() ? 1 : 0;
                    }
                    final int indexCmp;
                    if ((indexCmp = o1.index().compareTo(o2.index())) == 0) {
                        return o1.getId() - o2.getId();
                    }
                    // this comparator is more expensive than all the others up there
                    // that's why it's added last even though it could be easier to read
                    // if we'd apply it earlier. this comparator will only differentiate across
                    // indices all shards of the same index is treated equally.
                    final int secondary = secondaryComparator.compare(o1, o2);
                    return secondary == 0 ? indexCmp : secondary;
                }
            };
            /*
             * we use 2 arrays and move replicas to the second array once we allocated an identical
             * replica in the current iteration to make sure all indices get allocated in the same manner.
             * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with 2 replica and 1 shard would look like:
             * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
             * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
             * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ingoreUnassigned.
             */
            ShardRouting[] primary = unassigned.drain();
            ShardRouting[] secondary = new ShardRouting[primary.length];
            int secondaryLength = 0;
            int primaryLength = primary.length;
            ArrayUtil.timSort(primary, comparator);
            final Set<ModelNode> throttledNodes = Collections.newSetFromMap(new IdentityHashMap<ModelNode, Boolean>());
            do {
                for (int i = 0; i < primaryLength; i++) {
                    ShardRouting shard = primary[i];
                    if (!shard.primary()) {
                        boolean drop = deciders.canAllocate(shard, allocation).type() == Type.NO;
                        if (drop) {
                            unassigned.ignoreShard(shard);
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                unassigned.ignoreShard(primary[++i]);
                            }
                            continue;
                        } else {
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                secondary[secondaryLength++] = primary[++i];
                            }
                        }
                    }
                    assert !shard.assignedToNode() : shard;
                    /* find an node with minimal weight we can allocate on*/
                    float minWeight = Float.POSITIVE_INFINITY;
                    ModelNode minNode = null;
                    Decision decision = null;
                    if (throttledNodes.size() < nodes.size()) {
                        /* Don't iterate over an identity hashset here the
                         * iteration order is different for each run and makes testing hard */
                        for (ModelNode node : nodes.values()) {
                            if (throttledNodes.contains(node)) {
                                continue;
                            }
                            /*
                             * The shard we add is removed below to simulate the
                             * addition for weight calculation we use Decision.ALWAYS to
                             * not violate the not null condition.
                             */
                            if (!node.containsShard(shard)) {
                                node.addShard(shard, Decision.ALWAYS);
                                float currentWeight = weight.weight(Operation.ALLOCATE, this, node, shard.index());
                                /*
                                 * Remove the shard from the node again this is only a
                                 * simulation
                                 */
                                Decision removed = node.removeShard(shard);
                                assert removed != null;
                                /*
                                 * Unless the operation is not providing any gains we
                                 * don't check deciders
                                 */
                                if (currentWeight <= minWeight) {
                                    Decision currentDecision = deciders.canAllocate(shard, routingNodes.node(node.getNodeId()), allocation);
                                    NOUPDATE:
                                    if (currentDecision.type() == Type.YES || currentDecision.type() == Type.THROTTLE) {
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
                                                final int nodeHigh = node.highestPrimary(shard.index());
                                                final int minNodeHigh = minNode.highestPrimary(shard.index());
                                                if ((((nodeHigh > repId && minNodeHigh > repId) || (nodeHigh < repId && minNodeHigh < repId)) && (nodeHigh < minNodeHigh))
                                                        || (nodeHigh > minNodeHigh && nodeHigh > repId && minNodeHigh < repId)) {
                                                    minNode = node;
                                                    minWeight = currentWeight;
                                                    decision = currentDecision;
                                                } else {
                                                    break NOUPDATE;
                                                }
                                            } else if (currentDecision.type() != Type.YES) {
                                                break NOUPDATE;
                                            }
                                        }
                                        minNode = node;
                                        minWeight = currentWeight;
                                        decision = currentDecision;
                                    }
                                }
                            }
                        }
                    }
                    assert decision != null && minNode != null || decision == null && minNode == null;
                    if (minNode != null) {
                        minNode.addShard(shard, decision);
                        if (decision.type() == Type.YES) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                            }
                            routingNodes.initialize(shard, routingNodes.node(minNode.getNodeId()).nodeId(), allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
                            changed = true;
                            continue; // don't add to ignoreUnassigned
                        } else {
                            final RoutingNode node = routingNodes.node(minNode.getNodeId());
                            if (deciders.canAllocate(node, allocation).type() != Type.YES) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("Can not allocate on node [{}] remove from round decision [{}]", node, decision.type());
                                }
                                throttledNodes.add(minNode);
                            }
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("No eligable node found to assign shard [{}] decision [{}]", shard, decision.type());
                        }
                    } else if (logger.isTraceEnabled()) {
                        logger.trace("No Node found to assign shard [{}]", shard);
                    }
                    unassigned.ignoreShard(shard);
                    if (!shard.primary()) { // we could not allocate it and we are a replica - check if we can ignore the other replicas
                        while(secondaryLength > 0 && comparator.compare(shard, secondary[secondaryLength-1]) == 0) {
                            unassigned.ignoreShard(secondary[--secondaryLength]);
                        }
                    }
                }
                primaryLength = secondaryLength;
                ShardRouting[] tmp = primary;
                primary = secondary;
                secondary = tmp;
                secondaryLength = 0;
            } while (primaryLength > 0);
            // clear everything we have either added it or moved to ingoreUnassigned
            return changed;
        }

        /**
         * Tries to find a relocation from the max node to the minimal node for an arbitrary shard of the given index on the
         * balance model. Iff this method returns a <code>true</code> the relocation has already been executed on the
         * simulation model as well as on the cluster.
         */
        private boolean tryRelocateShard(Operation operation, ModelNode minNode, ModelNode maxNode, String idx, float minCost) {
            final ModelIndex index = maxNode.getIndex(idx);
            Decision decision = null;
            if (index != null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Try relocating shard for index index [{}] from node [{}] to node [{}]", idx, maxNode.getNodeId(),
                            minNode.getNodeId());
                }
                final RoutingNode node = routingNodes.node(minNode.getNodeId());
                ShardRouting candidate = null;
                final AllocationDeciders deciders = allocation.deciders();
                /* make a copy since we modify this list in the loop */
                final ArrayList<ShardRouting> shards = new ArrayList<>(index.getAllShards());
                for (ShardRouting shard : shards) {
                    if (shard.started()) {
                        // skip initializing, unassigned and relocating shards we can't relocate them anyway
                        Decision allocationDecision = deciders.canAllocate(shard, node, allocation);
                        Decision rebalanceDecision = deciders.canRebalance(shard, allocation);
                        if (((allocationDecision.type() == Type.YES) || (allocationDecision.type() == Type.THROTTLE))
                                && ((rebalanceDecision.type() == Type.YES) || (rebalanceDecision.type() == Type.THROTTLE))) {
                            Decision srcDecision;
                            if ((srcDecision = maxNode.removeShard(shard)) != null) {
                                minNode.addShard(shard, srcDecision);
                                final float delta = weight.weight(operation, this, minNode, idx) - weight.weight(operation, this, maxNode, idx);
                                if (delta < minCost ||
                                        (candidate != null && delta == minCost && candidate.id() > shard.id())) {
                                    /* this last line is a tie-breaker to make the shard allocation alg deterministic
                                     * otherwise we rely on the iteration order of the index.getAllShards() which is a set.*/
                                    minCost = delta;
                                    candidate = shard;
                                    decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecision);
                                }
                                minNode.removeShard(shard);
                                maxNode.addShard(shard, srcDecision);
                            }
                        }
                    }
                }

                if (candidate != null) {

                    /* allocate on the model even if not throttled */
                    maxNode.removeShard(candidate);
                    minNode.addShard(candidate, decision);
                    if (decision.type() == Type.YES) { /* only allocate on the cluster if we are not throttled */
                        if (logger.isTraceEnabled()) {
                            logger.trace("Relocate shard [{}] from node [{}] to node [{}]", candidate, maxNode.getNodeId(),
                                    minNode.getNodeId());
                        }
                        /* now allocate on the cluster - if we are started we need to relocate the shard */
                        if (candidate.started()) {
                            RoutingNode lowRoutingNode = routingNodes.node(minNode.getNodeId());
                            routingNodes.relocate(candidate, lowRoutingNode.nodeId(), allocation.clusterInfo().getShardSize(candidate, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));

                        } else {
                            routingNodes.initialize(candidate, routingNodes.node(minNode.getNodeId()).nodeId(), allocation.clusterInfo().getShardSize(candidate, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE));
                        }
                        return true;

                    }
                }
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Couldn't find shard to relocate from node [{}] to node [{}] allocation decision [{}]", maxNode.getNodeId(),
                        minNode.getNodeId(), decision == null ? "NO" : decision.type().name());
            }
            return false;
        }

    }

    static class ModelNode implements Iterable<ModelIndex> {
        private final String id;
        private final Map<String, ModelIndex> indices = new HashMap<>();
        /* cached stats - invalidated on add/remove and lazily calculated */
        private int numShards = -1;

        public ModelNode(String id) {
            this.id = id;
        }

        public ModelIndex getIndex(String indexId) {
            return indices.get(indexId);
        }

        public String getNodeId() {
            return id;
        }

        public int numShards() {
            if (numShards == -1) {
                int sum = 0;
                for (ModelIndex index : indices.values()) {
                    sum += index.numShards();
                }
                numShards = sum;
            }
            return numShards;
        }

        public int numShards(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numShards();
        }

        public Collection<ShardRouting> shards() {
            Collection<ShardRouting> result = new ArrayList<>();
            for (ModelIndex index : indices.values()) {
                result.addAll(index.getAllShards());
            }
            return result;
        }

        public int highestPrimary(String index) {
            ModelIndex idx = indices.get(index);
            if (idx != null) {
                return idx.highestPrimary();
            }
            return -1;
        }

        public void addShard(ShardRouting shard, Decision decision) {
            numShards = -1;
            ModelIndex index = indices.get(shard.index());
            if (index == null) {
                index = new ModelIndex(shard.index());
                indices.put(index.getIndexId(), index);
            }
            index.addShard(shard, decision);
        }

        public Decision removeShard(ShardRouting shard) {
            numShards = -1;
            ModelIndex index = indices.get(shard.index());
            Decision removed = null;
            if (index != null) {
                removed = index.removeShard(shard);
                if (removed != null && index.numShards() == 0) {
                    indices.remove(shard.index());
                }
            }
            return removed;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Node(").append(id).append(")");
            return sb.toString();
        }

        @Override
        public Iterator<ModelIndex> iterator() {
            return indices.values().iterator();
        }

        public boolean containsShard(ShardRouting shard) {
            ModelIndex index = getIndex(shard.getIndex());
            return index == null ? false : index.containsShard(shard);
        }

    }

    static final class ModelIndex {
        private final String id;
        private final Map<ShardRouting, Decision> shards = new HashMap<>();
        private int numPrimaries = -1;
        private int highestPrimary = -1;

        public ModelIndex(String id) {
            this.id = id;
        }

        public int highestPrimary() {
            if (highestPrimary == -1) {
                int maxId = -1;
                for (ShardRouting shard : shards.keySet()) {
                    if (shard.primary()) {
                        maxId = Math.max(maxId, shard.id());
                    }
                }
                return highestPrimary = maxId;
            }
            return highestPrimary;
        }

        public String getIndexId() {
            return id;
        }

        public Decision getDecicion(ShardRouting shard) {
            return shards.get(shard);
        }

        public int numShards() {
            return shards.size();
        }

        public Collection<ShardRouting> getAllShards() {
            return shards.keySet();
        }

        public int numPrimaries() {
            if (numPrimaries == -1) {
                int num = 0;
                for (ShardRouting shard : shards.keySet()) {
                    if (shard.primary()) {
                        num++;
                    }
                }
                return numPrimaries = num;
            }
            return numPrimaries;
        }

        public Decision removeShard(ShardRouting shard) {
            highestPrimary = numPrimaries = -1;
            return shards.remove(shard);
        }

        public void addShard(ShardRouting shard, Decision decision) {
            highestPrimary = numPrimaries = -1;
            assert decision != null;
            assert !shards.containsKey(shard) : "Shard already allocated on current node: " + shards.get(shard) + " " + shard;
            shards.put(shard, decision);
        }

        public boolean containsShard(ShardRouting shard) {
            return shards.containsKey(shard);
        }
    }

    static final class NodeSorter extends IntroSorter {

        final ModelNode[] modelNodes;
        /* the nodes weights with respect to the current weight function / index */
        final float[] weights;
        private final WeightFunction function;
        private String index;
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
        public void reset(Operation operation, String index) {
            this.index = index;
            for (int i = 0; i < weights.length; i++) {
                weights[i] = weight(operation, modelNodes[i]);
            }
            sort(0, modelNodes.length);
        }

        public float weight(Operation operation, ModelNode node) {
            return function.weight(operation, balancer, node, index);
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
            return weights[weights.length - 1] - weights[0];
        }
    }
}
