/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link RoutingNode} represents a cluster node associated with a single {@link DiscoveryNode} including all shards
 * that are hosted on that nodes. Each {@link RoutingNode} has a unique node id that can be used to identify the node.
 */
public class RoutingNode implements Iterable<ShardRouting> {

    private final String nodeId;

    @Nullable
    private final DiscoveryNode node;

    private final LinkedHashMap<ShardId, ShardRouting> shards; // LinkedHashMap to preserve order

    private final LinkedHashSet<ShardRouting> initializingShards;

    private final LinkedHashSet<ShardRouting> relocatingShards;

    private final LinkedHashSet<ShardRouting> startedShards;

    private final Map<Index, Set<ShardRouting>> shardsByIndex;

    /**
     * @param nodeId    node id of this routing node
     * @param node      discovery node for this routing node
     * @param sizeGuess estimate for the number of shards that will be added to this instance to save re-hashing on subsequent calls to
     *                  {@link #add(ShardRouting)}
     */
    RoutingNode(String nodeId, @Nullable DiscoveryNode node, int sizeGuess) {
        this.nodeId = nodeId;
        this.node = node;
        this.shards = Maps.newLinkedHashMapWithExpectedSize(sizeGuess);
        this.relocatingShards = new LinkedHashSet<>();
        this.initializingShards = new LinkedHashSet<>();
        this.startedShards = new LinkedHashSet<>();
        this.shardsByIndex = Maps.newHashMapWithExpectedSize(sizeGuess);
        assert invariant();
    }

    private RoutingNode(RoutingNode original) {
        this.nodeId = original.nodeId;
        this.node = original.node;
        this.shards = new LinkedHashMap<>(original.shards);
        this.relocatingShards = new LinkedHashSet<>(original.relocatingShards);
        this.initializingShards = new LinkedHashSet<>(original.initializingShards);
        this.startedShards = new LinkedHashSet<>(original.startedShards);
        this.shardsByIndex = Maps.copyOf(original.shardsByIndex, HashSet::new);
        assert invariant();
    }

    RoutingNode copy() {
        return new RoutingNode(this);
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return Collections.unmodifiableCollection(shards.values()).iterator();
    }

    /**
     * Returns the nodes {@link DiscoveryNode}.
     *
     * @return discoveryNode of this node
     */
    @Nullable
    public DiscoveryNode node() {
        return this.node;
    }

    @Nullable
    public ShardRouting getByShardId(ShardId id) {
        return shards.get(id);
    }

    public boolean hasIndex(Index index) {
        return shardsByIndex.containsKey(index);
    }

    /**
     * Get the id of this node
     * @return id of the node
     */
    public String nodeId() {
        return this.nodeId;
    }

    public int size() {
        return shards.size();
    }

    /**
     * Add a new shard to this node
     * @param shard Shard to create on this Node
     */
    void add(ShardRouting shard) {
        addInternal(shard, true);
    }

    void addWithoutValidation(ShardRouting shard) {
        addInternal(shard, false);
    }

    private void addInternal(ShardRouting shard, boolean validate) {
        final ShardRouting existing = shards.putIfAbsent(shard.shardId(), shard);
        if (existing != null) {
            final IllegalStateException e = new IllegalStateException(
                "Trying to add a shard "
                    + shard.shardId()
                    + " to a node ["
                    + nodeId
                    + "] where it already exists. current ["
                    + shards.get(shard.shardId())
                    + "]. new ["
                    + shard
                    + "]"
            );
            assert false : e;
            throw e;
        }

        if (shard.initializing()) {
            initializingShards.add(shard);
        } else if (shard.relocating()) {
            relocatingShards.add(shard);
        } else if (shard.started()) {
            startedShards.add(shard);
        }
        shardsByIndex.computeIfAbsent(shard.index(), k -> new HashSet<>()).add(shard);
        assert validate == false || invariant();
    }

    void update(ShardRouting oldShard, ShardRouting newShard) {
        if (shards.containsKey(oldShard.shardId()) == false) {
            // Shard was already removed by routing nodes iterator
            // TODO: change caller logic in RoutingNodes so that this check can go away
            return;
        }
        ShardRouting previousValue = shards.put(newShard.shardId(), newShard);
        assert previousValue == oldShard : "expected shard " + previousValue + " but was " + oldShard;

        if (oldShard.initializing()) {
            boolean exist = initializingShards.remove(oldShard);
            assert exist : "expected shard " + oldShard + " to exist in initializingShards";
        } else if (oldShard.relocating()) {
            boolean exist = relocatingShards.remove(oldShard);
            assert exist : "expected shard " + oldShard + " to exist in relocatingShards";
        } else if (oldShard.started()) {
            boolean exist = startedShards.remove(oldShard);
            assert exist : "expected shard " + oldShard + " to exist in startedShards";
        }
        final Set<ShardRouting> byIndex = shardsByIndex.get(oldShard.index());
        byIndex.remove(oldShard);
        byIndex.add(newShard);
        if (newShard.initializing()) {
            initializingShards.add(newShard);
        } else if (newShard.relocating()) {
            relocatingShards.add(newShard);
        } else if (newShard.started()) {
            startedShards.add(newShard);
        }
        assert invariant();
    }

    void remove(ShardRouting shard) {
        ShardRouting previousValue = shards.remove(shard.shardId());
        assert previousValue == shard : "expected shard " + previousValue + " but was " + shard;
        if (shard.initializing()) {
            boolean exist = initializingShards.remove(shard);
            assert exist : "expected shard " + shard + " to exist in initializingShards";
        } else if (shard.relocating()) {
            boolean exist = relocatingShards.remove(shard);
            assert exist : "expected shard " + shard + " to exist in relocatingShards";
        } else if (shard.started()) {
            boolean exist = startedShards.remove(shard);
            assert exist : "expected shard " + shard + " to exist in startedShards";
        }
        final Set<ShardRouting> byIndex = shardsByIndex.get(shard.index());
        byIndex.remove(shard);
        if (byIndex.isEmpty()) {
            shardsByIndex.remove(shard.index());
        }
        assert invariant();
    }

    private static final ShardRouting[] EMPTY_SHARD_ROUTING_ARRAY = new ShardRouting[0];

    public ShardRouting[] initializing() {
        return initializingShards.toArray(EMPTY_SHARD_ROUTING_ARRAY);
    }

    public ShardRouting[] relocating() {
        return relocatingShards.toArray(EMPTY_SHARD_ROUTING_ARRAY);
    }

    public ShardRouting[] started() {
        return startedShards.toArray(EMPTY_SHARD_ROUTING_ARRAY);
    }

    /**
     * Determine the number of shards with a specific state
     * @param state which should be counted
     * @return number of shards
     */
    public int numberOfShardsWithState(ShardRoutingState state) {
        return internalGetShardsWithState(state).size();
    }

    /**
     * Determine the shards with a specific state
     * @param state state which should be listed
     * @return List of shards
     */
    public Stream<ShardRouting> shardsWithState(ShardRoutingState state) {
        return internalGetShardsWithState(state).stream();
    }

    /**
     * Determine the shards of an index with a specific state
     * @param index id of the index
     * @param states set of states which should be listed
     * @return a list of shards
     */
    public Stream<ShardRouting> shardsWithState(String index, ShardRoutingState... states) {
        return Stream.of(states).flatMap(state -> shardsWithState(index, state));
    }

    public Stream<ShardRouting> shardsWithState(String index, ShardRoutingState state) {
        return shardsWithState(state).filter(shardRouting -> Objects.equals(shardRouting.getIndexName(), index));
    }

    private LinkedHashSet<ShardRouting> internalGetShardsWithState(ShardRoutingState state) {
        return switch (state) {
            case UNASSIGNED -> throw new IllegalArgumentException("Unassigned shards are not linked to a routing node");
            case INITIALIZING -> initializingShards;
            case STARTED -> startedShards;
            case RELOCATING -> relocatingShards;
        };
    }

    /**
     * The number of shards on this node that will not be eventually relocated.
     */
    public int numberOfOwningShards() {
        return shards.size() - relocatingShards.size();
    }

    public int numberOfOwningShardsForIndex(final Index index) {
        final Set<ShardRouting> shardRoutings = shardsByIndex.get(index);
        if (shardRoutings == null) {
            return 0;
        } else {
            return Math.toIntExact(shardRoutings.stream().filter(Predicate.not(ShardRouting::relocating)).count());
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----node_id[").append(nodeId).append("][").append(node == null ? "X" : "V").append("]\n");
        for (ShardRouting entry : shards.values()) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("routingNode ([");
        if (node != null) {
            sb.append(node.getName());
            sb.append("][");
            sb.append(node.getId());
            sb.append("][");
            sb.append(node.getHostName());
            sb.append("][");
            sb.append(node.getHostAddress());
        } else {
            sb.append("null");
        }
        sb.append("], [");
        sb.append(shards.size());
        sb.append(" assigned shards])");
        return sb.toString();
    }

    public ShardRouting[] copyShards() {
        return shards.values().toArray(EMPTY_SHARD_ROUTING_ARRAY);
    }

    public Index[] copyIndices() {
        return shardsByIndex.keySet().toArray(Index.EMPTY_ARRAY);
    }

    public boolean isEmpty() {
        return shards.isEmpty();
    }

    boolean invariant() {
        var shardRoutingsInitializing = new ArrayList<ShardRouting>(shards.size());
        var shardRoutingsRelocating = new ArrayList<ShardRouting>(shards.size());
        var shardRoutingsStarted = new ArrayList<ShardRouting>(shards.size());
        // this guess assumes 1 shard per index, this is not precise, but okay for assertion
        var shardRoutingsByIndex = Maps.<Index, Set<ShardRouting>>newHashMapWithExpectedSize(shards.size());
        for (var shard : shards.values()) {
            switch (shard.state()) {
                case INITIALIZING -> shardRoutingsInitializing.add(shard);
                case RELOCATING -> shardRoutingsRelocating.add(shard);
                case STARTED -> shardRoutingsStarted.add(shard);
            }
            shardRoutingsByIndex.computeIfAbsent(shard.index(), k -> new HashSet<>(10)).add(shard);
        }
        assert initializingShards.size() == shardRoutingsInitializing.size() && initializingShards.containsAll(shardRoutingsInitializing);
        assert relocatingShards.size() == shardRoutingsRelocating.size() && relocatingShards.containsAll(shardRoutingsRelocating);
        assert startedShards.size() == shardRoutingsStarted.size() && startedShards.containsAll(shardRoutingsStarted);
        assert shardRoutingsByIndex.equals(shardsByIndex);

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoutingNode that = (RoutingNode) o;
        return nodeId.equals(that.nodeId) && Objects.equals(node, that.node) && shards.equals(that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, node, shards);
    }
}
