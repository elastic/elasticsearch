/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ReplicationOperationTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testReplication() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState initialState = stateWithActivePrimary(index, true, randomInt(5));
        IndexMetadata indexMetadata = initialState.getMetadata().index(index);
        final long primaryTerm = indexMetadata.primaryTerm(0);
        final IndexShardRoutingTable indexShardRoutingTable = initialState.getRoutingTable().shardRoutingTable(shardId);
        ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
        if (primaryShard.relocating() && randomBoolean()) {
            // simulate execution of the replication phase on the relocation target node after relocation source was marked as relocated
            initialState = ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder(initialState.nodes()).localNodeId(primaryShard.relocatingNodeId()))
                .build();
            primaryShard = primaryShard.getTargetRelocatingShard();
        }
        // add a few in-sync allocation ids that don't have corresponding routing entries
        final Set<String> staleAllocationIds = Sets.newHashSet(generateRandomStringArray(4, 10, false));

        final Set<String> inSyncAllocationIds = Sets.union(indexMetadata.inSyncAllocationIds(0), staleAllocationIds);

        final Set<String> trackedShards = new HashSet<>();
        final Set<String> untrackedShards = new HashSet<>();
        addTrackingInfo(indexShardRoutingTable, primaryShard, trackedShards, untrackedShards);
        trackedShards.addAll(staleAllocationIds);

        final ReplicationGroup replicationGroup = new ReplicationGroup(indexShardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, initialState, trackedShards);

        final Map<ShardRouting, Exception> simulatedFailures = new HashMap<>();
        final Map<ShardRouting, Exception> reportedFailures = new HashMap<>();
        for (ShardRouting replica : expectedReplicas) {
            if (randomBoolean()) {
                Exception t;
                boolean criticalFailure = randomBoolean();
                if (criticalFailure) {
                    t = new CorruptIndexException("simulated", (String) null);
                    reportedFailures.put(replica, t);
                } else {
                    t = new IndexShardNotStartedException(shardId, IndexShardState.RECOVERING);
                }
                logger.debug("--> simulating failure on {} with [{}]", replica, t.getClass().getSimpleName());
                simulatedFailures.put(replica, t);
            }
        }

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final TestReplicaProxy replicasProxy = new TestReplicaProxy(simulatedFailures);

        final TestPrimary primary = new TestPrimary(primaryShard, () -> replicationGroup, threadPool);
        final TestReplicationOperation op = new TestReplicationOperation(request, primary, listener, replicasProxy, primaryTerm);
        op.execute();
        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        assertThat(request.processedOnReplicas, equalTo(expectedReplicas));
        assertThat(replicasProxy.failedReplicas, equalTo(simulatedFailures.keySet()));
        assertThat(replicasProxy.markedAsStaleCopies, equalTo(staleAllocationIds));
        assertThat("post replication operations not run on primary", request.runPostReplicationActionsOnPrimary.get(), equalTo(true));
        assertTrue("listener is not marked as done", listener.isDone());
        ShardInfo shardInfo = listener.actionGet().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(reportedFailures.size()));
        assertThat(shardInfo.getFailures(), arrayWithSize(reportedFailures.size()));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + expectedReplicas.size() - simulatedFailures.size()));
        final List<ShardRouting> unassignedShards = indexShardRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
        final int totalShards = 1 + expectedReplicas.size() + unassignedShards.size() + untrackedShards.size();
        assertThat(replicationGroup.toString(), shardInfo.getTotal(), equalTo(totalShards));

        assertThat(primary.knownLocalCheckpoints.remove(primaryShard.allocationId().getId()), equalTo(primary.localCheckpoint));
        assertThat(primary.knownLocalCheckpoints, equalTo(replicasProxy.generatedLocalCheckpoints));
        assertThat(primary.knownGlobalCheckpoints.remove(primaryShard.allocationId().getId()), equalTo(primary.globalCheckpoint));
        assertThat(primary.knownGlobalCheckpoints, equalTo(replicasProxy.generatedGlobalCheckpoints));
    }

    public void testRetryTransientReplicationFailure() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState initialState = stateWithActivePrimary(index, true, randomInt(5));
        IndexMetadata indexMetadata = initialState.getMetadata().index(index);
        final long primaryTerm = indexMetadata.primaryTerm(0);
        final IndexShardRoutingTable indexShardRoutingTable = initialState.getRoutingTable().shardRoutingTable(shardId);
        ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
        if (primaryShard.relocating() && randomBoolean()) {
            // simulate execution of the replication phase on the relocation target node after relocation source was marked as relocated
            initialState = ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder(initialState.nodes()).localNodeId(primaryShard.relocatingNodeId()))
                .build();
            primaryShard = primaryShard.getTargetRelocatingShard();
        }
        // add a few in-sync allocation ids that don't have corresponding routing entries
        final Set<String> staleAllocationIds = Sets.newHashSet(generateRandomStringArray(4, 10, false));

        final Set<String> inSyncAllocationIds = Sets.union(indexMetadata.inSyncAllocationIds(0), staleAllocationIds);

        final Set<String> trackedShards = new HashSet<>();
        final Set<String> untrackedShards = new HashSet<>();
        addTrackingInfo(indexShardRoutingTable, primaryShard, trackedShards, untrackedShards);
        trackedShards.addAll(staleAllocationIds);

        final ReplicationGroup replicationGroup = new ReplicationGroup(indexShardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, initialState, trackedShards);

        final Map<ShardRouting, Exception> simulatedFailures = new HashMap<>();
        for (ShardRouting replica : expectedReplicas) {
            Exception cause;
            Exception exception;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    cause = new CircuitBreakingException("broken", CircuitBreaker.Durability.PERMANENT);
                } else {
                    cause = new EsRejectedExecutionException("rejected");
                }
                exception = new RemoteTransportException("remote", cause);
            } else {
                TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 9300);
                DiscoveryNode node = new DiscoveryNode("replica", address, Version.CURRENT);
                cause = new ConnectTransportException(node, "broken");
                exception = cause;
            }
            logger.debug("--> simulating failure on {} with [{}]", replica, exception.getClass().getSimpleName());
            simulatedFailures.put(replica, exception);
        }

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final TestReplicaProxy replicasProxy = new TestReplicaProxy(simulatedFailures, true);

        final TestPrimary primary = new TestPrimary(primaryShard, () -> replicationGroup, threadPool);
        final TestReplicationOperation op = new TestReplicationOperation(
            request,
            primary,
            listener,
            replicasProxy,
            primaryTerm,
            TimeValue.timeValueMillis(20),
            TimeValue.timeValueSeconds(60)
        );
        op.execute();
        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        assertThat(request.processedOnReplicas, equalTo(expectedReplicas));
        assertThat(replicasProxy.failedReplicas.size(), equalTo(0));
        assertThat(replicasProxy.markedAsStaleCopies, equalTo(staleAllocationIds));
        assertThat("post replication operations not run on primary", request.runPostReplicationActionsOnPrimary.get(), equalTo(true));
        ShardInfo shardInfo = listener.actionGet().getShardInfo();
        assertThat(shardInfo.getSuccessful(), equalTo(1 + expectedReplicas.size()));
        final List<ShardRouting> unassignedShards = indexShardRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
        final int totalShards = 1 + expectedReplicas.size() + unassignedShards.size() + untrackedShards.size();
        assertThat(replicationGroup.toString(), shardInfo.getTotal(), equalTo(totalShards));

        assertThat(primary.knownLocalCheckpoints.remove(primaryShard.allocationId().getId()), equalTo(primary.localCheckpoint));
        assertThat(primary.knownLocalCheckpoints, equalTo(replicasProxy.generatedLocalCheckpoints));
        assertThat(primary.knownGlobalCheckpoints.remove(primaryShard.allocationId().getId()), equalTo(primary.globalCheckpoint));
        assertThat(primary.knownGlobalCheckpoints, equalTo(replicasProxy.generatedGlobalCheckpoints));
    }

    private void addTrackingInfo(
        IndexShardRoutingTable indexShardRoutingTable,
        ShardRouting primaryShard,
        Set<String> trackedShards,
        Set<String> untrackedShards
    ) {
        for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
            ShardRouting shr = indexShardRoutingTable.shard(copy);
            if (shr.unassigned() == false) {
                if (shr.initializing()) {
                    if (randomBoolean()) {
                        trackedShards.add(shr.allocationId().getId());
                    } else {
                        untrackedShards.add(shr.allocationId().getId());
                    }
                } else {
                    trackedShards.add(shr.allocationId().getId());
                    if (shr.relocating()) {
                        if (primaryShard == shr.getTargetRelocatingShard() || randomBoolean()) {
                            trackedShards.add(shr.getTargetRelocatingShard().allocationId().getId());
                        } else {
                            untrackedShards.add(shr.getTargetRelocatingShard().allocationId().getId());
                        }
                    }
                }
            }
        }
    }

    public void testNoLongerPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState initialState = stateWithActivePrimary(index, true, 1 + randomInt(2), randomInt(2));
        IndexMetadata indexMetadata = initialState.getMetadata().index(index);
        final long primaryTerm = indexMetadata.primaryTerm(0);
        final IndexShardRoutingTable indexShardRoutingTable = initialState.getRoutingTable().shardRoutingTable(shardId);
        ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
        if (primaryShard.relocating() && randomBoolean()) {
            // simulate execution of the replication phase on the relocation target node after relocation source was marked as relocated
            initialState = ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder(initialState.nodes()).localNodeId(primaryShard.relocatingNodeId()))
                .build();
            primaryShard = primaryShard.getTargetRelocatingShard();
        }
        // add an in-sync allocation id that doesn't have a corresponding routing entry
        final Set<String> staleAllocationIds = Sets.newHashSet(randomAlphaOfLength(10));
        final Set<String> inSyncAllocationIds = Sets.union(indexMetadata.inSyncAllocationIds(0), staleAllocationIds);
        final Set<String> trackedShards = new HashSet<>();
        addTrackingInfo(indexShardRoutingTable, primaryShard, trackedShards, new HashSet<>());
        trackedShards.addAll(staleAllocationIds);

        final ReplicationGroup replicationGroup = new ReplicationGroup(indexShardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, initialState, trackedShards);

        final Map<ShardRouting, Exception> expectedFailures = new HashMap<>();
        if (expectedReplicas.isEmpty()) {
            return;
        }
        final ShardRouting failedReplica = randomFrom(new ArrayList<>(expectedReplicas));
        expectedFailures.put(failedReplica, new CorruptIndexException("simulated", (String) null));

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final boolean testPrimaryDemotedOnStaleShardCopies = randomBoolean();
        final Exception shardActionFailure;
        if (randomBoolean()) {
            shardActionFailure = new NodeClosedException(new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT));
        } else if (randomBoolean()) {
            DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
            shardActionFailure = new SendRequestTransportException(
                node,
                ShardStateAction.SHARD_FAILED_ACTION_NAME,
                new NodeClosedException(node)
            );
        } else {
            shardActionFailure = new ShardStateAction.NoLongerPrimaryShardException(failedReplica.shardId(), "the king is dead");
        }
        final TestReplicaProxy replicasProxy = new TestReplicaProxy(expectedFailures) {
            @Override
            public void failShardIfNeeded(
                ShardRouting replica,
                long primaryTerm,
                String message,
                Exception exception,
                ActionListener<Void> shardActionListener
            ) {
                if (testPrimaryDemotedOnStaleShardCopies) {
                    super.failShardIfNeeded(replica, primaryTerm, message, exception, shardActionListener);
                } else {
                    assertThat(replica, equalTo(failedReplica));
                    shardActionListener.onFailure(shardActionFailure);
                }
            }

            @Override
            public void markShardCopyAsStaleIfNeeded(
                ShardId shardId,
                String allocationId,
                long primaryTerm,
                ActionListener<Void> shardActionListener
            ) {
                if (testPrimaryDemotedOnStaleShardCopies) {
                    shardActionListener.onFailure(shardActionFailure);
                } else {
                    super.markShardCopyAsStaleIfNeeded(shardId, allocationId, primaryTerm, shardActionListener);
                }
            }
        };
        AtomicBoolean primaryFailed = new AtomicBoolean();
        final TestPrimary primary = new TestPrimary(primaryShard, () -> replicationGroup, threadPool) {
            @Override
            public void failShard(String message, Exception exception) {
                assertThat(exception, instanceOf(ShardStateAction.NoLongerPrimaryShardException.class));
                assertTrue(primaryFailed.compareAndSet(false, true));
            }
        };
        final TestReplicationOperation op = new TestReplicationOperation(request, primary, listener, replicasProxy, primaryTerm);
        op.execute();

        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        assertThat(
            expectThrows(
                ExecutionException.class,
                ReplicationOperation.RetryOnPrimaryException.class,
                () -> listener.get(10, TimeUnit.SECONDS)
            ).getMessage(),
            anyOf(containsString("demoted while failing replica shard"), containsString("shutting down while failing replica shard"))
        );
        if (shardActionFailure instanceof ShardStateAction.NoLongerPrimaryShardException) {
            assertTrue(primaryFailed.get());
        } else {
            assertFalse(primaryFailed.get());
        }
    }

    public void testAddedReplicaAfterPrimaryOperation() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        final ClusterState initialState = stateWithActivePrimary(index, true, 0);
        Set<String> inSyncAllocationIds = initialState.metadata().index(index).inSyncAllocationIds(0);
        IndexShardRoutingTable shardRoutingTable = initialState.getRoutingTable().shardRoutingTable(shardId);
        Set<String> trackedShards = new HashSet<>();
        addTrackingInfo(shardRoutingTable, null, trackedShards, new HashSet<>());
        ReplicationGroup initialReplicationGroup = new ReplicationGroup(shardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final ClusterState stateWithAddedReplicas;
        if (randomBoolean()) {
            stateWithAddedReplicas = state(
                index,
                true,
                ShardRoutingState.STARTED,
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.STARTED
            );
        } else {
            stateWithAddedReplicas = state(index, true, ShardRoutingState.RELOCATING);
        }

        inSyncAllocationIds = stateWithAddedReplicas.metadata().index(index).inSyncAllocationIds(0);
        shardRoutingTable = stateWithAddedReplicas.getRoutingTable().shardRoutingTable(shardId);
        trackedShards = new HashSet<>();
        addTrackingInfo(shardRoutingTable, null, trackedShards, new HashSet<>());

        ReplicationGroup updatedReplicationGroup = new ReplicationGroup(shardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final AtomicReference<ReplicationGroup> replicationGroup = new AtomicReference<>(initialReplicationGroup);
        logger.debug("--> using initial replicationGroup:\n{}", replicationGroup.get());
        final long primaryTerm = initialState.getMetadata().index(shardId.getIndexName()).primaryTerm(shardId.id());
        final ShardRouting primaryShard = updatedReplicationGroup.getRoutingTable().primaryShard();
        final TestPrimary primary = new TestPrimary(primaryShard, replicationGroup::get, threadPool) {
            @Override
            public void perform(Request request, ActionListener<Result> listener) {
                super.perform(request, listener.map(result -> {
                    replicationGroup.set(updatedReplicationGroup);
                    logger.debug("--> state after primary operation:\n{}", replicationGroup.get());
                    return result;
                }));
            }
        };

        Request request = new Request(shardId);
        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final TestReplicationOperation op = new TestReplicationOperation(request, primary, listener, new TestReplicaProxy(), primaryTerm);
        op.execute();

        assertThat("request was not processed on primary", request.processedOnPrimary.get(), equalTo(true));
        Set<ShardRouting> expectedReplicas = getExpectedReplicas(shardId, stateWithAddedReplicas, trackedShards);
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
            activeShardCount == totalShards ? ActiveShardCount.ALL : ActiveShardCount.from(activeShardCount)
        );
        final boolean passesActiveShardCheck = activeShardCount <= assignedReplicas + 1;

        ShardRoutingState[] replicaStates = new ShardRoutingState[assignedReplicas + unassignedReplicas];
        for (int i = 0; i < assignedReplicas; i++) {
            replicaStates[i] = randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        }
        for (int i = assignedReplicas; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }

        final ClusterState state = state(index, true, ShardRoutingState.STARTED, replicaStates);
        logger.debug(
            "using active shard count of [{}], assigned shards [{}], total shards [{}]." + " expecting op to [{}]. using state: \n{}",
            request.waitForActiveShards(),
            1 + assignedReplicas,
            1 + assignedReplicas + unassignedReplicas,
            passesActiveShardCheck ? "succeed" : "retry",
            state
        );
        final long primaryTerm = state.metadata().index(index).primaryTerm(shardId.id());
        final IndexShardRoutingTable shardRoutingTable = state.routingTable().index(index).shard(shardId.id());

        final Set<String> inSyncAllocationIds = state.metadata().index(index).inSyncAllocationIds(0);
        Set<String> trackedShards = new HashSet<>();
        addTrackingInfo(shardRoutingTable, null, trackedShards, new HashSet<>());
        final ReplicationGroup initialReplicationGroup = new ReplicationGroup(shardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        PlainActionFuture<TestPrimary.Result> listener = new PlainActionFuture<>();
        final ShardRouting primaryShard = shardRoutingTable.primaryShard();
        final TestReplicationOperation op = new TestReplicationOperation(
            request,
            new TestPrimary(primaryShard, () -> initialReplicationGroup, threadPool),
            listener,
            new TestReplicaProxy(),
            logger,
            threadPool,
            "test",
            primaryTerm
        );

        if (passesActiveShardCheck) {
            assertThat(op.checkActiveShardCount(), nullValue());
            op.execute();
            assertTrue("operations should have been performed, active shard count is met", request.processedOnPrimary.get());
        } else {
            assertThat(op.checkActiveShardCount(), notNullValue());
            op.execute();
            assertFalse("operations should not have been perform, active shard count is *NOT* met", request.processedOnPrimary.get());
            assertListenerThrows("should throw exception to trigger retry", listener, UnavailableShardsException.class);
        }
    }

    public void testPrimaryFailureHandlingReplicaResponse() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        final Request request = new Request(shardId);

        final ClusterState state = stateWithActivePrimary(index, true, 1, 0);
        final IndexMetadata indexMetadata = state.getMetadata().index(index);
        final long primaryTerm = indexMetadata.primaryTerm(0);
        final ShardRouting primaryRouting = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();

        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(0);
        final IndexShardRoutingTable shardRoutingTable = state.routingTable().index(index).shard(shardId.id());
        final Set<String> trackedShards = shardRoutingTable.getAllAllocationIds();
        final ReplicationGroup initialReplicationGroup = new ReplicationGroup(shardRoutingTable, inSyncAllocationIds, trackedShards, 0);

        final boolean fatal = randomBoolean();
        final AtomicBoolean primaryFailed = new AtomicBoolean();
        final ReplicationOperation.Primary<Request, Request, TestPrimary.Result> primary = new TestPrimary(
            primaryRouting,
            () -> initialReplicationGroup,
            threadPool
        ) {

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
        final ReplicationOperation.Replicas<Request> replicas = new TestReplicaProxy(Collections.emptyMap());
        TestReplicationOperation operation = new TestReplicationOperation(request, primary, listener, replicas, primaryTerm);
        operation.execute();

        assertThat(primaryFailed.get(), equalTo(fatal));
        final ShardInfo shardInfo = listener.actionGet().getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(0));
        assertThat(shardInfo.getFailures(), arrayWithSize(0));
        assertThat(shardInfo.getSuccessful(), equalTo(1 + getExpectedReplicas(shardId, state, trackedShards).size()));
    }

    private Set<ShardRouting> getExpectedReplicas(ShardId shardId, ClusterState state, Set<String> trackedShards) {
        Set<ShardRouting> expectedReplicas = new HashSet<>();
        String localNodeId = state.nodes().getLocalNodeId();
        if (state.routingTable().hasIndex(shardId.getIndexName())) {
            final IndexShardRoutingTable indexShardRoutingTable = state.routingTable().shardRoutingTable(shardId);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                if (shardRouting.unassigned()) {
                    continue;
                }
                if (localNodeId.equals(shardRouting.currentNodeId()) == false) {
                    if (trackedShards.contains(shardRouting.allocationId().getId())) {
                        expectedReplicas.add(shardRouting);
                    }
                }

                if (shardRouting.relocating() && localNodeId.equals(shardRouting.relocatingNodeId()) == false) {
                    if (trackedShards.contains(shardRouting.getTargetRelocatingShard().allocationId().getId())) {
                        expectedReplicas.add(shardRouting.getTargetRelocatingShard());
                    }
                }
            }
        }
        return expectedReplicas;
    }

    public static class Request extends ReplicationRequest<Request> {
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public AtomicBoolean runPostReplicationActionsOnPrimary = new AtomicBoolean();
        public Set<ShardRouting> processedOnReplicas = ConcurrentCollections.newConcurrentSet();

        Request(ShardId shardId) {
            super(shardId);
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
        final long maxSeqNoOfUpdatesOrDeletes;
        final Supplier<ReplicationGroup> replicationGroupSupplier;
        final PendingReplicationActions pendingReplicationActions;
        final Map<String, Long> knownLocalCheckpoints = Collections.synchronizedMap(new HashMap<>());
        final Map<String, Long> knownGlobalCheckpoints = Collections.synchronizedMap(new HashMap<>());

        TestPrimary(ShardRouting routing, Supplier<ReplicationGroup> replicationGroupSupplier, ThreadPool threadPool) {
            this.routing = routing;
            this.replicationGroupSupplier = replicationGroupSupplier;
            this.localCheckpoint = random().nextLong();
            this.globalCheckpoint = randomNonNegativeLong();
            this.maxSeqNoOfUpdatesOrDeletes = randomNonNegativeLong();
            this.pendingReplicationActions = new PendingReplicationActions(routing.shardId(), threadPool);
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
        public void perform(Request request, ActionListener<Result> listener) {
            if (request.processedOnPrimary.compareAndSet(false, true) == false) {
                fail("processed [" + request + "] twice");
            }
            listener.onResponse(new Result(request));
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

            @Override
            public void runPostReplicationActions(ActionListener<Void> listener) {
                if (request.runPostReplicationActionsOnPrimary.compareAndSet(false, true) == false) {
                    fail("processed [" + request + "] twice");
                }
                listener.onResponse(null);
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
        public long computedGlobalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public long maxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public ReplicationGroup getReplicationGroup() {
            return replicationGroupSupplier.get();
        }

        @Override
        public PendingReplicationActions getPendingReplicationActions() {
            pendingReplicationActions.accept(getReplicationGroup());
            return pendingReplicationActions;
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

        private final int attemptsBeforeSuccess;
        private final AtomicInteger attemptsNumber = new AtomicInteger(0);
        final Map<ShardRouting, Exception> opFailures;
        private final boolean retryable;

        final Set<ShardRouting> failedReplicas = ConcurrentCollections.newConcurrentSet();

        final Map<String, Long> generatedLocalCheckpoints = ConcurrentCollections.newConcurrentMap();

        final Map<String, Long> generatedGlobalCheckpoints = ConcurrentCollections.newConcurrentMap();

        final Set<String> markedAsStaleCopies = ConcurrentCollections.newConcurrentSet();

        TestReplicaProxy() {
            this(Collections.emptyMap());
        }

        TestReplicaProxy(Map<ShardRouting, Exception> opFailures) {
            this(opFailures, false);
        }

        TestReplicaProxy(Map<ShardRouting, Exception> opFailures, boolean retryable) {
            this.opFailures = opFailures;
            this.retryable = retryable;
            if (retryable) {
                attemptsBeforeSuccess = randomInt(2) + 1;
            } else {
                attemptsBeforeSuccess = Integer.MAX_VALUE;
            }
        }

        @Override
        public void performOn(
            final ShardRouting replica,
            final Request request,
            final long primaryTerm,
            final long globalCheckpoint,
            final long maxSeqNoOfUpdatesOrDeletes,
            final ActionListener<ReplicationOperation.ReplicaResponse> listener
        ) {
            boolean added = request.processedOnReplicas.add(replica);
            if (retryable == false) {
                assertTrue("replica request processed twice on [" + replica + "]", added);
            }
            // If replication is not retryable OR this is the first attempt, the post replication actions
            // should not have run.
            if (retryable == false || added) {
                assertFalse(
                    "primary post replication actions should run after replication",
                    request.runPostReplicationActionsOnPrimary.get()
                );
            }
            // If this is a retryable scenario and this is the second try, we finish successfully
            int n = attemptsNumber.incrementAndGet();
            if (opFailures.containsKey(replica) && n <= attemptsBeforeSuccess) {
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
        public void failShardIfNeeded(
            ShardRouting replica,
            long primaryTerm,
            String message,
            Exception exception,
            ActionListener<Void> listener
        ) {
            if (failedReplicas.add(replica) == false) {
                fail("replica [" + replica + "] was failed twice");
            }
            if (opFailures.containsKey(replica)) {
                listener.onResponse(null);
            } else {
                fail("replica [" + replica + "] was failed");
            }
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            if (markedAsStaleCopies.add(allocationId) == false) {
                fail("replica [" + allocationId + "] was marked as stale twice");
            }
            listener.onResponse(null);
        }
    }

    class TestReplicationOperation extends ReplicationOperation<Request, Request, TestPrimary.Result> {

        TestReplicationOperation(
            Request request,
            Primary<Request, Request, TestPrimary.Result> primary,
            ActionListener<TestPrimary.Result> listener,
            Replicas<Request> replicas,
            long primaryTerm,
            TimeValue initialRetryBackoffBound,
            TimeValue retryTimeout
        ) {
            this(
                request,
                primary,
                listener,
                replicas,
                ReplicationOperationTests.this.logger,
                threadPool,
                "test",
                primaryTerm,
                initialRetryBackoffBound,
                retryTimeout
            );
        }

        TestReplicationOperation(
            Request request,
            Primary<Request, Request, TestPrimary.Result> primary,
            ActionListener<TestPrimary.Result> listener,
            Replicas<Request> replicas,
            long primaryTerm
        ) {
            this(request, primary, listener, replicas, ReplicationOperationTests.this.logger, threadPool, "test", primaryTerm);
        }

        TestReplicationOperation(
            Request request,
            Primary<Request, Request, TestPrimary.Result> primary,
            ActionListener<TestPrimary.Result> listener,
            Replicas<Request> replicas,
            Logger logger,
            ThreadPool threadPool,
            String opType,
            long primaryTerm
        ) {
            this(
                request,
                primary,
                listener,
                replicas,
                logger,
                threadPool,
                opType,
                primaryTerm,
                TimeValue.timeValueMillis(50),
                TimeValue.timeValueSeconds(1)
            );
        }

        TestReplicationOperation(
            Request request,
            Primary<Request, Request, TestPrimary.Result> primary,
            ActionListener<TestPrimary.Result> listener,
            Replicas<Request> replicas,
            Logger logger,
            ThreadPool threadPool,
            String opType,
            long primaryTerm,
            TimeValue initialRetryBackoffBound,
            TimeValue retryTimeout
        ) {
            super(request, primary, listener, replicas, logger, threadPool, opType, primaryTerm, initialRetryBackoffBound, retryTimeout);
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
