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
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The delayed unassigned allocation allows to control delayed allocation of shards due to nodes leaving the
 * cluster with the assumption that they might come back, and the opertunity to reduce cluster allocations if
 * they do.
 */
public class DelayUnassignedAllocation extends AbstractComponent {

    public static final String DELAY_ALLOCATION_DURATION = "cluster.routing.allocation.delay_unassigned_allocation.duration";
    public static final String DELAY_ALLOCATION_NODE_KEY = "cluster.routing.allocation.delay_unassigned_allocation.node_key";

    private final NodeKey nodeKey;
    private volatile TimeValue duration;

    private final Map<Object, DelayedAllocation> delayedAllocations = new HashMap<>();
    private final Cache<String, DiscoveryNode> seenNodes = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    public DelayUnassignedAllocation(Settings settings) {
        super(settings);
        this.duration = settings.getAsTime(DELAY_ALLOCATION_DURATION, TimeValue.timeValueMinutes(5));
        this.nodeKey = NodeKey.valueOf(settings.get(DELAY_ALLOCATION_NODE_KEY, NodeKey.TRANSPORT_ADDRESS.toString()).toUpperCase(Locale.ROOT));
        logger.debug("delayed allocation using duration [{}] and node_key [{}]", duration, nodeKey);
    }

    /**
     * The duration to wait if a node leaves, a value below or equal to 0 means
     * not taking node leaving into account at all (disabling delayed allocation).
     */
    public TimeValue getDuration() {
        return this.duration;
    }

    /**
     * The duration to wait if a node leaves, a value below or equal to 0 means
     * not taking node leaving into account at all (disabling delayed allocation).
     */
    public void setDuration(TimeValue duration) {
        this.duration = duration;
    }

    /**
     * Clears that current delayed allocation.
     */
    public synchronized void clearDelayedAllocations() {
        delayedAllocations.clear();
    }

    /**
     * Returns the current set of delayed nodes.
     */
    public synchronized Set<DiscoveryNode> getDelayedNodes() {
        Set<DiscoveryNode> nodes = new HashSet<>();
        for (DelayedAllocation delayedAllocation : delayedAllocations.values()) {
            nodes.add(delayedAllocation.node);
        }
        return nodes;
    }

    /**
     * The number of delayed shards.
     */
    public synchronized int getNumberOfDelayedShards() {
        int delayedShards = 0;
        for (DelayedAllocation delayedAllocation : delayedAllocations.values()) {
            delayedShards += delayedAllocation.shards.size();
        }
        return delayedShards;
    }

    /**
     * Checks if, due to nodes leaving the cluster, any unassigned shards need to be delayed
     * and not allocation (moved to ignoreUnassigned).
     * <p/>
     * Also times out (duration) when a node has not being around long enough to bring back
     * those shards to the unassigned list to be allocated.
     * <p/>
     * Returns a Result, used to indicated if anything changed, as well as if a reroute is
     * needed to be scheduled to act upon duration expiry.
     */
    public synchronized Result delayUnassignedAllocation(RoutingAllocation allocation) {
        // refresh the seen nodes, we need the seen nodes since when a node leaves, it is no longer
        // part of the DiscoveryNodes, yet we need its DiscoveryNode instance to properly process it
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : allocation.nodes().dataNodes()) {
            seenNodes.put(cursor.value.getId(), cursor.value);
        }

        TimeValue duration = allocation.getDelayedDuration() == null ? this.duration : allocation.getDelayedDuration();

        if (duration.millis() <= 0) {
            delayedAllocations.clear();
            return new Result(false, null);
        }

        long timestampInNanos = System.nanoTime();
        boolean changed = false;
        boolean scheduleReroute = false;

        // go over all the existing failed shards and add them to the delayed allocations
        for (Map.Entry<String, List<ShardRouting>> entry : allocation.getFailedShards().entrySet()) {
            DiscoveryNode node = seenNodes.getIfPresent(entry.getKey());
            // if we haven't seen this node, we can't do anything sadly...
            if (node == null) {
                continue;
            }
            List<ShardRouting> failed = entry.getValue();

            // if the node still exists in the cluster state, nothing really to do
            if (allocation.nodes().nodeExists(node.getId())) {
                continue;
            }

            if (logger.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder("node ").append(node).append(" left the cluster, delaying allocation for [").append(duration).append("], shards: ");
                for (ShardRouting shardRouting : failed) {
                    sb.append(shardRouting.shardId()).append(" ");
                }
                logger.info(sb.toString());
            }

            delayedAllocations.put(nodeKey.getNodeKey(node), new DelayedAllocation(node, new ArrayList<>(failed), timestampInNanos));
            scheduleReroute = true;
            changed = true;
        }

        // remove any delayed allocations that have timed out, or shards that no longer have an index (deleted), or nodes that
        // came back with the same node key
        for (Iterator<DelayedAllocation> delayedAllocIt = delayedAllocations.values().iterator(); delayedAllocIt.hasNext(); ) {
            DelayedAllocation delayedAllocation = delayedAllocIt.next();
            if ((timestampInNanos - delayedAllocation.failedTimestampInNanos) > duration.getNanos()) {
                logger.info("node {} delayed allocation expired after [{}], enabling allocation", delayedAllocation.node, duration);
                delayedAllocIt.remove();
                changed = true;
            } else {
                for (Iterator<ShardRouting> shardIt = delayedAllocation.shards.iterator(); shardIt.hasNext(); ) {
                    ShardRouting shard = shardIt.next();
                    if (allocation.metaData().hasConcreteIndex(shard.getIndex()) == false) {
                        shardIt.remove();
                        changed = true;
                    }
                }
                if (delayedAllocation.shards.isEmpty() == true) {
                    logger.info("node {} removed from delayed allocation, no more shards associated with it", delayedAllocation.node);
                    delayedAllocIt.remove();
                    changed = true;
                } else {
                    Object delayedAllocationNodeKey = nodeKey.getNodeKey(delayedAllocation.node);
                    for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().dataNodes().values()) {
                        if (delayedAllocationNodeKey.equals(nodeKey.getNodeKey(cursor.value))) {
                            // we found a node key that has came back, remove it from the delayed allocations
                            logger.info("node {} rejoined the cluster, removing delayed allocation", cursor.value);
                            delayedAllocIt.remove();
                            break;
                        }
                    }
                }
            }
        }

        if (delayedAllocations.isEmpty() == false) {
            // create a list of shards, sorting them putting all the primaries at the end, so we
            // will only delay those at the end
            MutableShardRouting[] drained = allocation.routingNodes().unassigned().drain();
            ArrayUtil.timSort(drained, new Comparator<MutableShardRouting>() {
                @Override
                public int compare(MutableShardRouting o1, MutableShardRouting o2) {
                    return Boolean.compare(o1.primary(), o2.primary());
                }
            });
            List<MutableShardRouting> unassigned = Lists.newArrayList(drained);
            for (DelayedAllocation delayedAllocation : delayedAllocations.values()) {
                for (ShardRouting shard : delayedAllocation.shards) {
                    // see if we can find an unassigned shard with the same shard id, if so, just remove it
                    // from the unassigned list and add it to ignored list
                    for (Iterator<MutableShardRouting> it = unassigned.iterator(); it.hasNext(); ) {
                        MutableShardRouting unassignedShard = it.next();
                        if (unassignedShard.shardId().equals(shard.shardId())) {
                            it.remove();
                            allocation.routingNodes().ignoredUnassigned().add(unassignedShard);
                            break;
                        }
                    }
                }
            }

            allocation.routingNodes().unassigned().addAll(unassigned);
        }

        return new Result(changed, scheduleReroute ? duration : null);
    }

    /**
     * Result of delayed allocation.
     */
    public static class Result {
        private final boolean changed;
        private final TimeValue rerouteSchedule;

        public Result(boolean changed, TimeValue rerouteSchedule) {
            this.changed = changed;
            this.rerouteSchedule = rerouteSchedule;
        }

        /**
         * Has anything changed in the allocation.
         */
        public boolean isChanged() {
            return this.changed;
        }

        /**
         * Is reroute needed in order to act upon node delayed duration expiry.
         */
        public boolean isRerouteRequired() {
            return rerouteSchedule != null;
        }

        /**
         * The reroute schedule to execute to act upon node delayed duration expiry.
         */
        public TimeValue getRerouteSchedule() {
            return this.rerouteSchedule;
        }
    }

    static class DelayedAllocation {

        public final DiscoveryNode node;
        public final List<ShardRouting> shards;
        public final long failedTimestampInNanos;

        public DelayedAllocation(DiscoveryNode node, List<ShardRouting> shards, long failedTimestampInNanos) {
            this.node = node;
            this.shards = shards;
            this.failedTimestampInNanos = failedTimestampInNanos;
        }
    }

    public enum NodeKey {
        NAME() {
            @Override
            public Object getNodeKey(DiscoveryNode node) {
                return node.getName();
            }
        },
        ID() {
            @Override
            public Object getNodeKey(DiscoveryNode node) {
                return node.getId();
            }
        },
        HOST_ADDRESS() {
            @Override
            public Object getNodeKey(DiscoveryNode node) {
                return node.getHostAddress();
            }
        },
        HOST_NAME() {
            @Override
            public Object getNodeKey(DiscoveryNode node) {
                return node.getHostName();
            }
        },
        TRANSPORT_ADDRESS() {
            @Override
            public Object getNodeKey(DiscoveryNode node) {
                return node.getAddress();
            }
        };

        public abstract Object getNodeKey(DiscoveryNode node);
    }
}
