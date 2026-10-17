/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.stateless.snapshots.ShardGenerationsRefresher.Trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ShardGenerationsRefresherTests extends ESTestCase {

    private static final IndexId INDEX = new IndexId("index", "index-id");

    private record Request(List<ShardId> shardIds, ActionListener<GetShardGenerationsResponse> listener) {}

    /**
     * A master that does not answer until the test says so
     */
    private final List<Request> requests = new ArrayList<>();
    private final ShardId shard0 = new ShardId(new Index("index", "index-uuid"), 0);
    private final ShardId shard1 = new ShardId(new Index("index", "index-uuid"), 1);
    private final ShardGeneration gen0 = new ShardGeneration("gen0");
    private final ShardGeneration gen1 = new ShardGeneration("gen1");
    private final RepositoryFilesCache cache = new RepositoryFilesCache(
        "repo",
        (indexId, shardId, shardGeneration) -> BlobStoreIndexShardSnapshots.EMPTY,
        command -> {},
        Runnable::run
    );
    private final ShardGenerationsRefresher refresher = new ShardGenerationsRefresher(
        "repo",
        (shardIds, listener) -> requests.add(new Request(shardIds, listener)),
        cache,
        Runnable::run
    );

    private static GetShardGenerationsResponse response(long repositoryGeneration, Map<ShardId, ShardGeneration> generations) {
        final Map<ShardId, RepositoryShardGeneration> shardGenerations = new HashMap<>();
        generations.forEach((shard, generation) -> shardGenerations.put(shard, new RepositoryShardGeneration(INDEX, generation)));
        return new GetShardGenerationsResponse(repositoryGeneration, shardGenerations);
    }

    public void testAskingForAllShardsAndKeepingTheAnswer() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0, shard1));
        assertThat(requests, hasSize(1));
        assertThat(requests.get(0).shardIds(), containsInAnyOrder(shard0, shard1));

        requests.get(0).listener().onResponse(response(3, Map.of(shard0, gen0, shard1, gen1)));
        assertThat(cache.getRepositoryGeneration(), equalTo(3L));
        assertTrue(cache.hasShardGeneration(shard0));
        assertTrue(cache.hasShardGeneration(shard1));

        // nothing changed, so there is nothing to ask
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0, shard1));
        refresher.refresh(Trigger.CLUSTER_STATE, 3, Set.of(shard0, shard1));
        assertThat(requests, hasSize(1));
    }

    public void testNothingIsAskedWhileTheRepositoryGenerationIsUnknownOrThereAreNoShards() {
        refresher.refresh(Trigger.TICK, RepositoryData.UNKNOWN_REPO_GEN, Set.of(shard0));
        refresher.refresh(Trigger.TICK, 3, Set.of());
        assertThat(requests, empty());
    }

    public void testANewerRepositoryGenerationAsksAgain() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        requests.get(0).listener().onResponse(response(3, Map.of(shard0, gen0)));

        refresher.refresh(Trigger.CLUSTER_STATE, 4, Set.of(shard0));
        assertThat(requests, hasSize(2));
    }

    public void testANewShardAsksAgainWithoutANewRepositoryGeneration() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        requests.get(0).listener().onResponse(response(3, Map.of(shard0, gen0)));

        refresher.refresh(Trigger.CLUSTER_STATE, 3, Set.of(shard0, shard1));
        assertThat(requests, hasSize(2));
        // all the shards on the node are asked for again, so that the answer is from a single repository generation
        assertThat(requests.get(1).shardIds(), containsInAnyOrder(shard0, shard1));
    }

    public void testTriggersWhileARequestIsInFlightAreCoalescedIntoOneMoreRequest() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        assertThat(requests, hasSize(1));

        // several triggers, none of which sends a request
        refresher.refresh(Trigger.CLUSTER_STATE, 4, Set.of(shard0));
        refresher.refresh(Trigger.CLUSTER_STATE, 5, Set.of(shard0, shard1));
        refresher.refresh(Trigger.TICK, 5, Set.of(shard0, shard1));
        assertThat(requests, hasSize(1));

        // the answer is accepted, and the one more request is for what the triggers asked
        requests.get(0).listener().onResponse(response(3, Map.of(shard0, gen0)));
        assertThat(requests, hasSize(2));
        assertThat(requests.get(1).shardIds(), containsInAnyOrder(shard0, shard1));
        requests.get(1).listener().onResponse(response(5, Map.of(shard0, gen0, shard1, gen1)));
        assertThat(requests, hasSize(2));
        assertThat(cache.getRepositoryGeneration(), equalTo(5L));
    }

    public void testNoMoreRequestIsSentIfTheAnswerCoversTheTriggersThatCameMeanwhile() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        refresher.refresh(Trigger.CLUSTER_STATE, 4, Set.of(shard0));
        // the master is already ahead of what this node has seen
        requests.get(0).listener().onResponse(response(4, Map.of(shard0, gen0)));
        assertThat(requests, hasSize(1));
    }

    public void testAnAnswerOlderThanTheGenerationThatTriggeredTheRequestIsDroppedAndAskedAgainOnTheNextTick() {
        refresher.refresh(Trigger.TICK, 5, Set.of(shard0));
        // the master has not caught up with the cluster state
        requests.get(0).listener().onResponse(response(4, Map.of(shard0, gen0)));
        assertThat(cache.getRepositoryGeneration(), equalTo(RepositoryData.UNKNOWN_REPO_GEN));
        assertFalse(cache.hasShardGeneration(shard0));

        // cluster state changes do not ask again, the tick does
        refresher.refresh(Trigger.CLUSTER_STATE, 5, Set.of(shard0));
        assertThat(requests, hasSize(1));
        refresher.refresh(Trigger.TICK, 5, Set.of(shard0));
        assertThat(requests, hasSize(2));
        requests.get(1).listener().onResponse(response(5, Map.of(shard0, gen0)));
        assertThat(cache.getRepositoryGeneration(), equalTo(5L));
        assertTrue(cache.hasShardGeneration(shard0));
    }

    public void testShardsStayUnknownWhenTheMasterDoesNotHaveTheActionAndAreKnownOnceItDoes() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        // a master that is not upgraded yet
        requests.get(0)
            .listener()
            .onFailure(
                new RemoteTransportException("master", new ActionNotFoundTransportException(TransportGetShardGenerationsAction.NAME))
            );
        assertFalse(cache.hasShardGeneration(shard0));
        assertThat(cache.getShardFiles(shard0), nullValue()); // unknown, not zero

        refresher.refresh(Trigger.CLUSTER_STATE, 3, Set.of(shard0));
        assertThat(requests, hasSize(1));
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        assertThat(requests, hasSize(2));

        // and now it is upgraded
        requests.get(1).listener().onResponse(response(3, Map.of(shard0, gen0)));
        assertTrue(cache.hasShardGeneration(shard0));
    }

    public void testAnyFailureIsAskedAgainOnTheNextTick() {
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        requests.get(0).listener().onFailure(new IllegalStateException("simulated"));
        refresher.refresh(Trigger.CLUSTER_STATE, 4, Set.of(shard0));
        assertThat(requests, hasSize(1));
        refresher.refresh(Trigger.TICK, 4, Set.of(shard0));
        assertThat(requests, hasSize(2));
    }

    public void testAFailureToSendIsAFailureOfTheRequest() {
        final var attempts = new int[1];
        final var failing = new ShardGenerationsRefresher("repo", (shardIds, listener) -> {
            attempts[0]++;
            throw new IllegalStateException("simulated");
        }, cache, Runnable::run);
        failing.refresh(Trigger.TICK, 3, Set.of(shard0));
        assertThat(attempts[0], equalTo(1));
        // it is not stuck as in flight: the next tick asks again
        failing.refresh(Trigger.TICK, 3, Set.of(shard0));
        assertThat(attempts[0], equalTo(2));
    }

    public void testNothingIsAskedOnceClosed() {
        refresher.close();
        refresher.refresh(Trigger.TICK, 3, Set.of(shard0));
        assertThat(requests, empty());
    }

    public void testTheAnswerIsHandledOnTheStateExecutor() {
        final var stateTasks = new ArrayList<Runnable>();
        final var onStateExecutor = new ShardGenerationsRefresher(
            "repo",
            (shardIds, listener) -> requests.add(new Request(shardIds, listener)),
            cache,
            stateTasks::add
        );
        onStateExecutor.refresh(Trigger.TICK, 3, Set.of(shard0));
        requests.get(0).listener().onResponse(response(3, Map.of(shard0, gen0)));

        // whichever thread the answer arrives on, it is not used until the state executor runs it
        assertFalse(cache.hasShardGeneration(shard0));
        assertThat(stateTasks, hasSize(1));
        stateTasks.get(0).run();
        assertTrue(cache.hasShardGeneration(shard0));
    }
}
