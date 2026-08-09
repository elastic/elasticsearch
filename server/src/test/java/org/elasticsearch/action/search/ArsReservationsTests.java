/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

/**
 * Covers the accounting {@link ArsReservations} has to get right for the ARS probe cap: a slot is
 * counted from the routing decision until it is given back, exactly once, no matter which of the
 * several paths gives it back.
 */
public class ArsReservationsTests extends ESTestCase {

    private static final String NODE = "node_a";
    private static final Index INDEX = new Index("index", "index_uuid");

    private PendingSearchRequests pendingSearchRequests;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        pendingSearchRequests = new PendingSearchRequests();
    }

    public void testReserveIsVisibleToConcurrentSearchesAndReleaseGivesItBack() {
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final ShardId shardId = new ShardId(INDEX, 0);

        reservations.reserve(shardId, NODE);
        // this is the map a concurrently routing search reads when it checks the probe cap
        assertThat(pendingSearchRequests.liveView(), equalTo(Map.of(NODE, 1L)));

        reservations.release(null, shardId);
        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
    }

    public void testReleaseIsIdempotent() {
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final ShardId shardId = new ShardId(INDEX, 0);
        reservations.reserve(shardId, NODE);

        reservations.release(null, shardId);
        // a shard is dispatched, then reports an outcome, then the search drains: all three release it
        reservations.release(null, shardId);
        reservations.releaseAll();

        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
    }

    public void testReserveTwiceForTheSameShardCountsOnce() {
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final ShardId shardId = new ShardId(INDEX, 0);

        // the active and the initializing copies of a shard are ranked in separate passes
        reservations.reserve(shardId, NODE);
        reservations.reserve(shardId, NODE);

        assertThat(pendingSearchRequests.liveView(), equalTo(Map.of(NODE, 1L)));
        reservations.release(null, shardId);
        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
    }

    public void testReleaseNamingAnotherClusterIsIgnored() {
        // a search that targets this cluster both directly and through a remote alias holds two
        // copies of one shard under the same ShardId, and only the local one has a reservation
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final ShardId shardId = new ShardId(INDEX, 0);
        reservations.reserve(shardId, NODE);

        reservations.release("remote_cluster", shardId);

        assertThat(pendingSearchRequests.liveView(), equalTo(Map.of(NODE, 1L)));
    }

    public void testReleaseMatchesTheLocalAliasOfAMinimizeRoundtripsSearch() {
        // the local half of a CCS with minimize_roundtrips=true carries "", not null
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, "");
        final ShardId shardId = new ShardId(INDEX, 0);
        reservations.reserve(shardId, NODE);

        reservations.release("", shardId);

        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
    }

    public void testReclaimOnlyAppliesToShardsThisSearchAlreadyClaimed() {
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Map.of());
        task.setArsReservations(reservations);
        final ShardId probed = new ShardId(INDEX, 0);
        final ShardId neverProbed = new ShardId(INDEX, 1);
        reservations.reserve(probed, NODE);
        reservations.release(null, probed);

        // DFS gives the slot back when it sends its request and takes it again when the result lands
        ArsReservations.reclaimFor(task, probed, NODE);
        // a warm node was never claimed, so it must not start being counted here
        ArsReservations.reclaimFor(task, neverProbed, "node_warm");

        assertThat(pendingSearchRequests.liveView(), equalTo(Map.of(NODE, 1L)));
    }

    public void testDrainGivesBackEverySlotStillHeld() {
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        final ShardId dispatched = new ShardId(INDEX, 0);
        final ShardId skippedByCanMatch = new ShardId(INDEX, 1);
        final ShardId onAnotherNode = new ShardId(INDEX, 2);
        reservations.reserve(dispatched, NODE);
        reservations.reserve(skippedByCanMatch, NODE);
        reservations.reserve(onAnotherNode, "node_b");
        reservations.release(null, dispatched);

        reservations.releaseAll();

        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
        assertFalse(reservations.hasReservations());
    }

    public void testReserveLosingTheRaceWithDrainDoesNotLeak() {
        // A slot that outlives the search it belongs to is never given back, and a node sitting at
        // the cap is gated out of probe traffic for good. A claim that lands after the drain has
        // walked the map is the one ordering that could strand a count, so it gives itself back.
        final ArsReservations reservations = new ArsReservations(pendingSearchRequests, null);
        reservations.releaseAll();

        reservations.reserve(new ShardId(INDEX, 0), NODE);

        assertThat(pendingSearchRequests.liveView(), anEmptyMap());
    }
}
