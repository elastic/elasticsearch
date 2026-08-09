/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The in-flight slots a single search claimed for adaptive replica selection (ARS) probes, at the
 * moment it decided where to route each shard.
 * <p>
 * The probe cap bounds how many concurrent requests a coordinator sends to a peer that has no stats
 * yet, or is still warming up. The cap is read while ranking, in
 * {@link org.elasticsearch.cluster.routing.IndexShardRoutingTable}, but the underlying count used to
 * be raised only once the request reached the transport layer. Concurrent searches therefore all
 * observed the same pre-increment value, all passed the cap, and all routed to the same fresh node.
 * Claiming the slot as soon as the decision is made narrows that window to the ranking itself: a
 * search that ranks after another one has decided now sees the decision, but two searches ranking at
 * the same moment can still both pass the cap check before either of them claims.
 * <p>
 * A slot is given back when the shard request is dispatched, from which point the transport layer's
 * own accounting covers it; when the shard reaches a terminal outcome without ever being dispatched;
 * or, as a backstop, when the search finishes. The batched query phase is the reason for the second
 * of those and not an oversight: it sends one request per node for all of that node's shards, without
 * the counting handler the single-shard path uses, so its slots have to be held until the shard
 * results come back rather than given back when the request is sent.
 * <p>
 * Releases are keyed by shard and idempotent, so a shard whose slot was already given back simply
 * does nothing. That is what makes the many abandonment paths safe - can_match skips, retries against
 * another copy, phase failures - without having to enumerate them exhaustively. It matters because a
 * leaked slot is permanent: a node sitting at the cap forever is never probed again, so it never
 * gains stats and stays deprioritized.
 * <p>
 * Slots are only ever claimed for shards of the cluster the coordinator ranked for, so releases
 * naming a different cluster alias are ignored. A search that targets the same cluster both directly
 * and through a remote alias holds two copies of one shard under the same {@link ShardId}, and only
 * the local one has a reservation.
 */
final class ArsReservations implements OperationRouting.ProbeReservations {

    private static final Logger logger = LogManager.getLogger(ArsReservations.class);

    private final PendingSearchRequests pendingSearchRequests;
    @Nullable
    private final String clusterAlias;
    private final Map<ShardId, String> reservedNodeByShard = ConcurrentCollections.newConcurrentMap();
    // shards claimed at least once, so a re-claim can tell a probe target from a node that was never gated
    private final Set<ShardId> claimedShards = ConcurrentCollections.newConcurrentSet();
    private volatile boolean drained;

    /**
     * @param clusterAlias the alias of the cluster whose shards are being ranked, as carried by the
     *                     search request that produced the routing decisions. Note this is the empty
     *                     string, not {@code null}, for the local half of a cross-cluster search
     *                     with {@code ccs_minimize_roundtrips=true}.
     */
    ArsReservations(PendingSearchRequests pendingSearchRequests, @Nullable String clusterAlias) {
        this.pendingSearchRequests = pendingSearchRequests;
        this.clusterAlias = clusterAlias;
    }

    /**
     * Claims a slot on {@code nodeId} for {@code shardId}. Claiming twice for the same shard counts
     * once: the active and the initializing copies of a shard are ranked separately, and only the
     * first of the two decides where the request is sent.
     */
    @Override
    public void reserve(ShardId shardId, String nodeId) {
        // The count has to be published before the key. The other order lets a concurrent
        // releaseAll() remove the key and decrement before this increment lands, stranding it.
        pendingSearchRequests.increment(nodeId);
        if (reservedNodeByShard.putIfAbsent(shardId, nodeId) != null) {
            pendingSearchRequests.decrement(nodeId);
            return;
        }
        claimedShards.add(shardId);
        if (drained) {
            // Lost the race with releaseAll(), which has already walked the map.
            releaseShard(shardId);
        }
    }

    /**
     * Takes the slot back for a shard this search already claimed once, to cover a stretch where the
     * transport layer is not counting it. Does nothing for a shard that was never claimed, so a node
     * that was never a probe target is not counted here by accident.
     */
    static void reclaimFor(@Nullable SearchTask task, ShardId shardId, String nodeId) {
        final ArsReservations reservations = task == null ? null : task.getArsReservations();
        if (reservations != null && reservations.claimedShards.contains(shardId)) {
            reservations.reserve(shardId, nodeId);
        }
    }

    /**
     * Gives back {@code task}'s slot for {@code shardId}, if it claimed one. Searches that never
     * routed shards of their own hold no ledger, and a task is absent altogether when an action is
     * driven directly rather than through the task manager, so both are treated as nothing to do.
     */
    static void releaseFor(@Nullable SearchTask task, @Nullable String clusterAlias, ShardId shardId) {
        final ArsReservations reservations = task == null ? null : task.getArsReservations();
        if (reservations != null) {
            reservations.release(clusterAlias, shardId);
        }
    }

    /**
     * Gives back every slot {@code task} still holds. See {@link #releaseFor} for when there is none.
     */
    static void releaseAllFor(@Nullable SearchTask task) {
        final ArsReservations reservations = task == null ? null : task.getArsReservations();
        if (reservations != null) {
            reservations.releaseAll();
        }
    }

    /**
     * Gives back the slot claimed for {@code shardId}, if this search still holds one. Idempotent,
     * and a no-op for shards belonging to another cluster.
     */
    void release(@Nullable String clusterAlias, ShardId shardId) {
        if (Objects.equals(this.clusterAlias, clusterAlias)) {
            releaseShard(shardId);
        }
    }

    /**
     * Gives back every slot still held. Runs unconditionally when the search completes, so that a
     * path which does not release its shard explicitly costs a transient over-count rather than a
     * permanent one.
     */
    void releaseAll() {
        drained = true;
        if (reservedNodeByShard.isEmpty() == false) {
            // Not necessarily a bug - a phase can fail without reporting an outcome per shard - but
            // a release site that goes missing shows up here first.
            logger.debug(
                "draining [{}] ARS probe reservations for cluster [{}] that were not released explicitly: {}",
                reservedNodeByShard.size(),
                clusterAlias,
                reservedNodeByShard
            );
            for (ShardId shardId : reservedNodeByShard.keySet()) {
                releaseShard(shardId);
            }
        }
    }

    // package private for testing
    boolean hasReservations() {
        return reservedNodeByShard.isEmpty() == false;
    }

    private void releaseShard(ShardId shardId) {
        // remove() hands the entry to exactly one caller, which is what keeps concurrent releases of
        // the same shard from decrementing twice.
        final String nodeId = reservedNodeByShard.remove(shardId);
        if (nodeId != null) {
            pendingSearchRequests.decrement(nodeId);
        }
    }
}
