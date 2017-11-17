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
package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ReplicationOperationTests extends ESTestCase {

    public void testReplication() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState state = stateWithActivePrimary(index, true, randomInt(5));
        IndexMetaData indexMetaData = state.getMetaData().index(index);
        final long primaryTerm = indexMetaData.primaryTerm(0);
        final IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().shardRoutingTable(shardId);
        ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
        if (primaryShard.relocating() && randomBoolean()) {
            // simulate execution of the replication phase on the relocation target node after relocation source was marked as relocated
            state = ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(primaryShard.relocatingNodeId())).build();
            primaryShard = primaryShard.getTargetRelocatingShard();
        }
        // add a few in-sync allocation ids that don't have corresponding routing entries
        Set<String> staleAllocationIds = Sets.newHashSet(generateRandomStringArray(4, 10, false));
        state = ClusterState.builder(state).metaData(MetaData.builder(state.metaData()).put(IndexMetaData.builder(indexMetaData)
            .putInSyncAllocationIds(0, Sets.union(indexMetaData.inSyncAllocationIds(0), staleAllocationIds)))).build();

        final Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, state);

        final Map<ShardRouting, Exception> expectedFailures = new HashMap<>();
        final Set<ShardRouting> expectedFailedShards = new HashSet<>();
        for (ShardRouting replica : expectedReplicas) {
            if (randomBoolean()) {
                Exception t;
                boolean criticalFailure = randomBoolean();
                if (criticalFailure) {
                    t = new CorruptIndexException("simulated", (String) null);
                } else {
                    t = new IndexShardNotStartedException(shardId, IndexShardState.RECOVERING);
                }
                logger.debug("--> simulating failure on {} with [{}]", replica, t.getClass().getSimpleName());
                expectedFailures.put(replica, t);
                if (criticalFailure) {
                    expectedFailedShards.add(replica);
                }
            }
        }

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final TestReplicaProxy replicasProxy = new TestReplicaProxy(primaryTerm, expectedFailures);
        final ClusterState finalState = state;
        final TestPrimary primary = new TestPrimary(primaryShard, () -> finalState);
        final TestReplicationOperation op = new TestReplicationOperation(request,
            primary, listener, replicasProxy);
        op.execute();

        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        assertThat(request.processedOnReplicas, equalTo(expectedReplicas));
        assertThat(replicasProxy.failedReplicas, equalTo(expectedFailedShards));
        assertThat(replicasProxy.markedAsStaleCopies, equalTo(staleAllocationIds));
        assertTrue("listener is not marked as done", listener.isDone());
        ShardInfo shardInfo = listener.actionGet().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(expectedFailedShards.size()));
        assertThat(shardInfo.getFailures(), arrayWithSize(expectedFailedShards.size()));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + expectedReplicas.size() - expectedFailures.size()));
        final List<ShardRouting> unassignedShards =
            indexShardRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
        final int totalShards = 1 + expectedReplicas.size() + unassignedShards.size();
        assertThat(shardInfo.getTotal(), equalTo(totalShards));

        assertThat(primary.knownLocalCheckpoints.remove(primaryShard.allocationId().getId()), equalTo(primary.localCheckpoint));
        assertThat(primary.knownLocalCheckpoints, equalTo(replicasProxy.generatedLocalCheckpoints));
        assertThat(primary.knownGlobalCheckpoints, equalTo(replicasProxy.generatedGlobalCheckpoints));
    }

    public void testDemotedPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState state = stateWithActivePrimary(index, true, 1 + randomInt(2), randomInt(2));
        IndexMetaData indexMetaData = state.getMetaData().index(index);
        final long primaryTerm = indexMetaData.primaryTerm(0);
        ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        if (primaryShard.relocating() && randomBoolean()) {
            // simulate execution of the replication phase on the relocation target node after relocation source was marked as relocated
            state = ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(primaryShard.relocatingNodeId())).build();
            primaryShard = primaryShard.getTargetRelocatingShard();
        }
        // add in-sync allocation id that doesn't have a corresponding routing entry
        state = ClusterState.builder(state).metaData(MetaData.builder(state.metaData()).put(IndexMetaData.builder(indexMetaData)
            .putInSyncAllocationIds(0, Sets.union(indexMetaData.inSyncAllocationIds(0), Sets.newHashSet(randomAlphaOfLength(10))))))
            .build();

        final Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, state);

        final Map<ShardRouting, Exception> expectedFailures = new HashMap<>();
        final ShardRouting failedReplica = randomFrom(new ArrayList<>(expectedReplicas));
        expectedFailures.put(failedReplica, new CorruptIndexException("simulated", (String) null));

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final ClusterState finalState = state;
        final boolean testPrimaryDemotedOnStaleShardCopies = randomBoolean();
        final TestReplicaProxy replicasProxy = new TestReplicaProxy(primaryTerm, expectedFailures) {
            @Override
            public void failShardIfNeeded(ShardRouting replica, String message, Exception exception,
                                          Runnable onSuccess, Consumer<Exception> onPrimaryDemoted,
                                          Consumer<Exception> onIgnoredFailure) {
                if (testPrimaryDemotedOnStaleShardCopies) {
                    super.failShardIfNeeded(replica, message, exception, onSuccess, onPrimaryDemoted, onIgnoredFailure);
                } else {
                    assertThat(replica, equalTo(failedReplica));
                    onPrimaryDemoted.accept(new ElasticsearchException("the king is dead"));
                }
            }

            @Override
            public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, Runnable onSuccess,
                                                     Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
                if (testPrimaryDemotedOnStaleShardCopies) {
                    onPrimaryDemoted.accept(new ElasticsearchException("the king is dead"));
                } else {
                    super.markShardCopyAsStaleIfNeeded(shardId, allocationId, onSuccess, onPrimaryDemoted, onIgnoredFailure);
                }
            }
        };
        AtomicBoolean primaryFailed = new AtomicBoolean();
        final TestPrimary primary = new TestPrimary(primaryShard, () -> finalState) {
            @Override
            public void failShard(String message, Exception exception) {
                assertTrue(primaryFailed.compareAndSet(false, true));
            }
        };
        final TestReplicationOperation op = new TestReplicationOperation(request, primary, listener, replicasProxy);
        op.execute();

        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        assertTrue("listener is not marked as done", listener.isDone());
        assertTrue(primaryFailed.get());
        assertListenerThrows("should throw exception to trigger retry", listener,
            ReplicationOperation.RetryOnPrimaryException.class);
    }

    public void testAddedReplicaAfterPrimaryOperation() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        final ClusterState initialState = stateWithActivePrimary(index, true, 0);
        final ClusterState stateWithAddedReplicas;
        if (randomBoolean()) {
            stateWithAddedReplicas = state(index, true, ShardRoutingState.STARTED,
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.STARTED);
        } else {
            stateWithAddedReplicas = state(index, true, ShardRoutingState.RELOCATING);
        }
        AtomicReference<ClusterState> state = new AtomicReference<>(initialState);
        logger.debug("--> using initial state:\n{}", state.get());
        final long primaryTerm = initialState.getMetaData().index(shardId.getIndexName()).primaryTerm(shardId.id());
        final ShardRouting primaryShard = state.get().routingTable().shardRoutingTable(shardId).primaryShard();
        final TestPrimary primary = new TestPrimary(primaryShard, state::get) {
            @Override
            public Result perform(Request request) throws Exception {
                Result result = super.perform(request);
                state.set(stateWithAddedReplicas);
                logger.debug("--> state after primary operation:\n{}", state.get());
                return result;
            }
        };

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final TestReplicationOperation op = new TestReplicationOperation(request, primary, listener,
            new TestReplicaProxy(primaryTerm));
        op.execute();

        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, state.get());
        assertThat(request.processedOnReplicas, equalTo(expectedReplicas));
    }

    public void testWaitForActiveShards() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        final int assignedReplicas = randomInt(2);
        final int unassignedReplicas = randomInt(2);
        final int totalShards = 1 + assignedReplicas + unassignedReplicas;
        final int activeShardCount = randomIntBetween(0, totalShards);
        Request request = new Request(shardId).waitForActiveShards(
            activeShardCount == totalShards ? ActiveShardCount.ALL : ActiveShardCount.from(activeShardCount));
        final boolean passesActiveShardCheck = activeShardCount <= assignedReplicas + 1;

        ShardRoutingState[] replicaStates = new ShardRoutingState[assignedReplicas + unassignedReplicas];
        for (int i = 0; i < assignedReplicas; i++) {
            replicaStates[i] = randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        }
        for (int i = assignedReplicas; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }

        final ClusterState state = state(index, true, ShardRoutingState.STARTED, replicaStates);
        logger.debug("using active shard count of [{}], assigned shards [{}], total shards [{}]." +
                " expecting op to [{}]. using state: \n{}",
            request.waitForActiveShards(), 1 + assignedReplicas, 1 + assignedReplicas + unassignedReplicas,
            passesActiveShardCheck ? "succeed" : "retry", state);
        final long primaryTerm = state.metaData().index(index).primaryTerm(shardId.id());
        final IndexShardRoutingTable shardRoutingTable = state.routingTable().index(index).shard(shardId.id());
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final ShardRouting primaryShard = shardRoutingTable.primaryShard();
        final TestReplicationOperation op = new TestReplicationOperation(request,
            new TestPrimary(primaryShard, () -> state),
                listener, new TestReplicaProxy(primaryTerm), logger, "test");

        if (passesActiveShardCheck) {
            assertThat(op.checkActiveShardCount(), nullValue());
            op.execute();
            assertTrue("operations should have been performed, active shard count is met",
                request.processedOnPrimary.get());
        } else {
            assertThat(op.checkActiveShardCount(), notNullValue());
            op.execute();
            assertFalse("operations should not have been perform, active shard count is *NOT* met",
                request.processedOnPrimary.get());
            assertListenerThrows("should throw exception to trigger retry", listener, UnavailableShardsException.class);
        }
    }

    public void testPrimaryFailureHandlingReplicaResponse() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        final Request request = new Request(shardId);

        final ClusterState state = stateWithActivePrimary(index, true, 1, 0);
        final IndexMetaData indexMetaData = state.getMetaData().index(index);
        final long primaryTerm = indexMetaData.primaryTerm(0);
        final ShardRouting primaryRouting = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();

        final boolean fatal = randomBoolean();
        final AtomicBoolean primaryFailed = new AtomicBoolean();
        final ReplicationOperation.Primary<Request, Request, TestPrimary.Result> primary =
            new TestPrimary(primaryRouting, () -> state) {

            @Override
            public void failShard(String message, Exception exception) {
                primaryFailed.set(true);
            }

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
                if (primaryRouting.allocationId().getId().equals(allocationId)) {
                    super.updateLocalCheckpointForShard(allocationId, checkpoint);
                } else {
                    if (fatal) {
                        throw new NullPointerException();
                    } else {
                        throw new AlreadyClosedException("already closed");
                    }
                }
            }

        };

        final PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final ReplicationOperation.Replicas<Request> replicas = new TestReplicaProxy(primaryTerm, Collections.emptyMap());
        TestReplicationOperation operation = new TestReplicationOperation(request, primary, listener, replicas);
        operation.execute();

        assertThat(primaryFailed.get(), equalTo(fatal));
        final ShardInfo shardInfo = listener.actionGet().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(0));
        assertThat(shardInfo.getFailures(), arrayWithSize(0));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + getExpectedReplicas(shardId, state).size()));
    }

    private Set<ShardRouting> getExpectedReplicas(ShardId shardId, ClusterState state) {
        Set<ShardRouting> expectedReplicas = new HashSet<>();
        String localNodeId = state.nodes().getLocalNodeId();
        if (state.routingTable().hasIndex(shardId.getIndexName())) {
            for (ShardRouting shardRouting : state.routingTable().shardRoutingTable(shardId)) {
                if (shardRouting.unassigned()) {
                    continue;
                }
                if (localNodeId.equals(shardRouting.currentNodeId()) == false) {
                    expectedReplicas.add(shardRouting);
                }

                if (shardRouting.relocating() && localNodeId.equals(shardRouting.relocatingNodeId()) == false) {
                    expectedReplicas.add(shardRouting.getTargetRelocatingShard());
                }
            }
        }
        return expectedReplicas;
    }


    public static class Request extends ReplicationRequest<Request> {
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public Set<ShardRouting> processedOnReplicas = ConcurrentCollections.newConcurrentSet();

        public Request() {
        }

        Request(ShardId shardId) {
            this();
            this.shardId = shardId;
            this.index = shardId.getIndexName();
            this.waitForActiveShards = ActiveShardCount.NONE;
            // keep things simple
        }

        @Override
        public String toString() {
            return "Request{}";
        }
    }

    static class TestPrimary implements ReplicationOperation.Primary<Request, Request, TestPrimary.Result> {
        final ShardRouting routing;
        final long localCheckpoint;
        final long globalCheckpoint;
        final Supplier<ClusterState> clusterStateSupplier;
        final Map<String, Long> knownLocalCheckpoints = new HashMap<>();
        final Map<String, Long> knownGlobalCheckpoints = new HashMap<>();

        TestPrimary(ShardRouting routing, Supplier<ClusterState> clusterStateSupplier) {
            this.routing = routing;
            this.clusterStateSupplier = clusterStateSupplier;
            this.localCheckpoint = random().nextLong();
            this.globalCheckpoint = randomNonNegativeLong();
        }

        @Override
        public ShardRouting routingEntry() {
            return routing;
        }

        @Override
        public void failShard(String message, Exception exception) {
            throw new AssertionError("should shouldn't be failed with [" + message + "]", exception);
        }

        @Override
        public Result perform(Request request) throws Exception {
            if (request.processedOnPrimary.compareAndSet(false, true) == false) {
                fail("processed [" + request + "] twice");
            }
            return new Result(request);
        }

        static class Result implements ReplicationOperation.PrimaryResult<Request> {
            private final Request request;
            private ShardInfo shardInfo;

            Result(Request request) {
                this.request = request;
            }

            @Override
            public Request replicaRequest() {
                return request;
            }

            @Override
            public void setShardInfo(ShardInfo shardInfo) {
                this.shardInfo = shardInfo;
            }

            public ShardInfo getShardInfo() {
                return shardInfo;
            }
        }

        @Override
        public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
            knownLocalCheckpoints.put(allocationId, checkpoint);
        }

        @Override
        public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
            knownGlobalCheckpoints.put(allocationId, globalCheckpoint);
        }

        @Override
        public long localCheckpoint() {
            return localCheckpoint;
        }

        @Override
        public long globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public ReplicationGroup getReplicationGroup() {
            ClusterState clusterState = clusterStateSupplier.get();
            return new ReplicationGroup(clusterState.routingTable().shardRoutingTable(routing.shardId()),
                clusterState.metaData().index(routing.index()).inSyncAllocationIds(routing.id()));
        }

    }

    static class ReplicaResponse implements ReplicationOperation.ReplicaResponse {
        final long localCheckpoint;
        final long globalCheckpoint;

        ReplicaResponse(long localCheckpoint, long globalCheckpoint) {
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
        }

        @Override
        public long localCheckpoint() {
            return localCheckpoint;
        }

        @Override
        public long globalCheckpoint() {
            return globalCheckpoint;
        }

    }

    static class TestReplicaProxy implements ReplicationOperation.Replicas<Request> {

        final Map<ShardRouting, Exception> opFailures;

        final Set<ShardRouting> failedReplicas = ConcurrentCollections.newConcurrentSet();

        final Map<String, Long> generatedLocalCheckpoints = ConcurrentCollections.newConcurrentMap();

        final Map<String, Long> generatedGlobalCheckpoints = ConcurrentCollections.newConcurrentMap();

        final Set<String> markedAsStaleCopies = ConcurrentCollections.newConcurrentSet();

        final long primaryTerm;

        TestReplicaProxy(long primaryTerm) {
            this(primaryTerm, Collections.emptyMap());
        }

        TestReplicaProxy(long primaryTerm, Map<ShardRouting, Exception> opFailures) {
            this.primaryTerm = primaryTerm;
            this.opFailures = opFailures;
        }

        @Override
        public void performOn(
                final ShardRouting replica,
                final Request request,
                final long globalCheckpoint,
                final ActionListener<ReplicationOperation.ReplicaResponse> listener) {
            assertTrue("replica request processed twice on [" + replica + "]", request.processedOnReplicas.add(replica));
            if (opFailures.containsKey(replica)) {
                listener.onFailure(opFailures.get(replica));
            } else {
                final long generatedLocalCheckpoint = random().nextLong();
                final long generatedGlobalCheckpoint = random().nextLong();
                final String allocationId = replica.allocationId().getId();
                assertNull(generatedLocalCheckpoints.put(allocationId, generatedLocalCheckpoint));
                assertNull(generatedGlobalCheckpoints.put(allocationId, generatedGlobalCheckpoint));
                listener.onResponse(new ReplicaResponse(generatedLocalCheckpoint, generatedGlobalCheckpoint));
            }
        }

        @Override
        public void failShardIfNeeded(ShardRouting replica, String message, Exception exception, Runnable onSuccess,
                                      Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            if (failedReplicas.add(replica) == false) {
                fail("replica [" + replica + "] was failed twice");
            }
            if (opFailures.containsKey(replica)) {
                if (randomBoolean()) {
                    onSuccess.run();
                } else {
                    onIgnoredFailure.accept(new ElasticsearchException("simulated"));
                }
            } else {
                fail("replica [" + replica + "] was failed");
            }
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, Runnable onSuccess,
                                                 Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            if (markedAsStaleCopies.add(allocationId) == false) {
                fail("replica [" + allocationId + "] was marked as stale twice");
            }
            if (randomBoolean()) {
                onSuccess.run();
            } else {
                onIgnoredFailure.accept(new ElasticsearchException("simulated"));
            }
        }
    }

    class TestReplicationOperation extends ReplicationOperation<Request, Request, TestPrimary.Result> {
        TestReplicationOperation(Request request, Primary<Request, Request, TestPrimary.Result> primary,
                ActionListener<TestPrimary.Result> listener, Replicas<Request> replicas) {
            this(request, primary, listener, replicas, ReplicationOperationTests.this.logger, "test");
        }

        TestReplicationOperation(Request request, Primary<Request, Request, TestPrimary.Result> primary,
                                 ActionListener<TestPrimary.Result> listener,
                                 Replicas<Request> replicas, Logger logger, String opType) {
            super(request, primary, listener, replicas, logger, opType);
        }
    }

    <T> void assertListenerThrows(String msg, PlainActionFuture<T> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

}
