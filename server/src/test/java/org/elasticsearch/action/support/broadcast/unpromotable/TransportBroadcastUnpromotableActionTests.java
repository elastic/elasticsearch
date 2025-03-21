/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.broadcast.unpromotable;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class TransportBroadcastUnpromotableActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;
    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private TestTransportBroadcastUnpromotableAction broadcastUnpromotableAction;
    private ShardStateAction shardStateAction;

    @BeforeClass
    public static void beforeClass() {
        THREAD_POOL = new TestThreadPool(TransportBroadcastUnpromotableActionTests.class.getSimpleName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        shardStateAction = mock(ShardStateAction.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> argument = invocation.getArgument(6);
            argument.onResponse(null);
            return null;
        })
            .when(shardStateAction)
            .remoteShardFailed(any(ShardId.class), anyString(), anyLong(), anyBoolean(), anyString(), any(Exception.class), any());
        broadcastUnpromotableAction = new TestTransportBroadcastUnpromotableAction(shardStateAction);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService, transportService);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    private class TestTransportBroadcastUnpromotableAction extends TransportBroadcastUnpromotableAction<
        TestBroadcastUnpromotableRequest,
        ActionResponse.Empty> {

        TestTransportBroadcastUnpromotableAction(ShardStateAction shardStateAction) {
            super(
                "indices:admin/test",
                TransportBroadcastUnpromotableActionTests.this.clusterService,
                TransportBroadcastUnpromotableActionTests.this.transportService,
                shardStateAction,
                new ActionFilters(Set.of()),
                TestBroadcastUnpromotableRequest::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected void unpromotableShardOperation(
            Task task,
            TestBroadcastUnpromotableRequest request,
            ActionListener<ActionResponse.Empty> listener
        ) {
            assert false : "not reachable in these tests";
        }

        @Override
        protected ActionResponse.Empty combineUnpromotableShardResponses(List<ActionResponse.Empty> empties) {
            return ActionResponse.Empty.INSTANCE;
        }

        @Override
        protected ActionResponse.Empty readResponse(StreamInput in) {
            return ActionResponse.Empty.INSTANCE;
        }

        @Override
        protected ActionResponse.Empty emptyResponse() {
            return ActionResponse.Empty.INSTANCE;
        }
    }

    private static class TestBroadcastUnpromotableRequest extends BroadcastUnpromotableRequest {

        TestBroadcastUnpromotableRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestBroadcastUnpromotableRequest(IndexShardRoutingTable indexShardRoutingTable) {
            super(indexShardRoutingTable);
        }

        TestBroadcastUnpromotableRequest(IndexShardRoutingTable indexShardRoutingTable, boolean failShardOnError) {
            super(indexShardRoutingTable, failShardOnError);
        }
    }

    private static List<ShardRouting.Role> getReplicaRoles(int numPromotableReplicas, int numSearchReplicas) {
        List<ShardRouting.Role> replicaRoles = Stream.concat(
            Collections.nCopies(numPromotableReplicas, randomBoolean() ? ShardRouting.Role.DEFAULT : ShardRouting.Role.INDEX_ONLY).stream(),
            Collections.nCopies(numSearchReplicas, ShardRouting.Role.SEARCH_ONLY).stream()
        ).collect(Collectors.toList());
        Collections.shuffle(replicaRoles, random());
        return replicaRoles;
    }

    private static List<Tuple<ShardRoutingState, ShardRouting.Role>> getReplicaRolesWithRandomStates(
        int numPromotableReplicas,
        int numSearchReplicas,
        ShardRoutingState... possibleStates
    ) {
        return getReplicaRoles(numPromotableReplicas, numSearchReplicas).stream()
            .map(role -> new Tuple<>(getValidStateForRole(role, possibleStates), role))
            .collect(Collectors.toList());
    }

    private static List<Tuple<ShardRoutingState, ShardRouting.Role>> getReplicaRolesWithRandomStates(
        int numPromotableReplicas,
        int numSearchReplicas
    ) {
        return getReplicaRolesWithRandomStates(numPromotableReplicas, numSearchReplicas, ShardRoutingState.values());
    }

    private static ShardRoutingState getValidStateForRole(ShardRouting.Role role, ShardRoutingState... possibleStates) {
        ShardRoutingState state;
        do {
            state = randomFrom(possibleStates);
            // relocation is not possible for unserchable shards until ES-4677 is implemented
        } while (role.isSearchable() == false && state == ShardRoutingState.RELOCATING);
        return state;
    }

    private static List<Tuple<ShardRoutingState, ShardRouting.Role>> getReplicaRolesWithState(
        int numPromotableReplicas,
        int numSearchReplicas,
        ShardRoutingState state
    ) {
        return getReplicaRolesWithRandomStates(numPromotableReplicas, numSearchReplicas, state);
    }

    private int countRequestsForIndex(ClusterState state, String index) {
        PlainActionFuture<ActionResponse.Empty> response = new PlainActionFuture<>();
        state.routingTable().activePrimaryShardsGrouped(new String[] { index }, true).iterator().forEachRemaining(shardId -> {
            logger.debug("--> executing for primary shard id: {}", shardId.shardId());
            ActionTestUtils.execute(
                broadcastUnpromotableAction,
                null,
                new TestBroadcastUnpromotableRequest(state.routingTable().shardRoutingTable(shardId.shardId())),
                response
            );
        });

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int totalRequests = 0;
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            logger.debug("Captured requests for node [{}] are: [{}]", entry.getKey(), entry.getValue());
            totalRequests += entry.getValue().size();
        }
        return totalRequests;
    }

    public void testNotStartedPrimary() {
        final String index = "test";
        final int numPromotableReplicas = randomInt(2);
        final int numSearchReplicas = randomInt(2);
        final ClusterState state = state(
            index,
            randomBoolean(),
            randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED,
            getReplicaRolesWithState(numPromotableReplicas, numSearchReplicas, ShardRoutingState.UNASSIGNED)
        );
        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        assertThat(countRequestsForIndex(state, index), is(equalTo(0)));
    }

    public void testMixOfStartedPromotableAndSearchReplicas() {
        final String index = "test";
        final int numShards = 1 + randomInt(3);
        final int numPromotableReplicas = randomInt(2);
        final int numSearchReplicas = randomInt(2);

        ClusterState state = stateWithAssignedPrimariesAndReplicas(
            new String[] { index },
            numShards,
            getReplicaRoles(numPromotableReplicas, numSearchReplicas)
        );
        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        assertThat(countRequestsForIndex(state, index), is(equalTo(numShards * numSearchReplicas)));
    }

    public void testSearchReplicasWithRandomStates() {
        final String index = "test";
        final int numPromotableReplicas = randomInt(2);
        final int numSearchReplicas = randomInt(6);

        List<Tuple<ShardRoutingState, ShardRouting.Role>> replicas = getReplicaRolesWithRandomStates(
            numPromotableReplicas,
            numSearchReplicas
        );
        int numReachableUnpromotables = replicas.stream().mapToInt(t -> {
            if (t.v2() == ShardRouting.Role.SEARCH_ONLY && t.v1() != ShardRoutingState.UNASSIGNED) {
                if (t.v1() == ShardRoutingState.RELOCATING) {
                    return 2; // accounts for both the RELOCATING and the INITIALIZING copies
                }
                return 1;
            }
            return 0;
        }).sum();

        final ClusterState state = state(index, true, ShardRoutingState.STARTED, replicas);

        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        assertThat(countRequestsForIndex(state, index), is(equalTo(numReachableUnpromotables)));
    }

    public void testInvalidNodes() throws Exception {
        final String index = "test";
        ClusterState state = stateWithAssignedPrimariesAndReplicas(
            new String[] { index },
            randomIntBetween(1, 3),
            getReplicaRoles(randomInt(2), randomIntBetween(1, 2))
        );
        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());

        ShardId shardId = state.routingTable().activePrimaryShardsGrouped(new String[] { index }, true).get(0).shardId();
        IndexShardRoutingTable routingTable = state.routingTable().shardRoutingTable(shardId);
        IndexShardRoutingTable.Builder wrongRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
        for (int i = 0; i < routingTable.size(); i++) {
            ShardRouting shardRouting = routingTable.shard(i);
            String currentNodeId = shardRouting.currentNodeId() + randomIntBetween(10, 100);
            ShardRouting wrongShardRouting = shardRoutingBuilder(shardId, currentNodeId, shardRouting.primary(), shardRouting.state())
                .withRelocatingNodeId(shardRouting.relocatingNodeId())
                .withUnassignedInfo(shardRouting.unassignedInfo())
                .withRole(shardRouting.role())
                .build();
            wrongRoutingTableBuilder.addShard(wrongShardRouting);
        }
        IndexShardRoutingTable wrongRoutingTable = wrongRoutingTableBuilder.build();

        PlainActionFuture<ActionResponse.Empty> response = new PlainActionFuture<>();
        logger.debug("--> executing for wrong shard routing table: {}", wrongRoutingTable);

        // The request fails if we don't mark shards as stale
        assertThat(
            asInstanceOf(NodeNotConnectedException.class, safeAwaitFailure(broadcastUnpromotableRequest(wrongRoutingTable, false)))
                .toString(),
            containsString("discovery node must not be null")
        );
        Mockito.verifyNoInteractions(shardStateAction);

        // We were able to mark shards as stale, so the request finishes successfully
        assertThat(safeAwait(broadcastUnpromotableRequest(wrongRoutingTable, true)), equalTo(ActionResponse.Empty.INSTANCE));
        for (var shardRouting : wrongRoutingTable.assignedUnpromotableShards()) {
            Mockito.verify(shardStateAction)
                .remoteShardFailed(
                    eq(shardRouting.shardId()),
                    eq(shardRouting.allocationId().getId()),
                    eq(state.metadata().getProject().index(index).primaryTerm(shardRouting.shardId().getId())),
                    eq(true),
                    eq("mark unpromotable copy as stale after refresh failure"),
                    any(Exception.class),
                    any()
                );
        }

        Mockito.reset(shardStateAction);
        // If we are unable to mark a shard as stale, then the request fails
        Mockito.doAnswer(invocation -> {
            Exception exception = invocation.getArgument(5);
            ActionListener<Void> argument = invocation.getArgument(6);
            argument.onFailure(exception);
            return null;
        })
            .when(shardStateAction)
            .remoteShardFailed(any(ShardId.class), anyString(), anyLong(), anyBoolean(), anyString(), any(Exception.class), any());
        assertThat(
            asInstanceOf(NodeNotConnectedException.class, safeAwaitFailure(broadcastUnpromotableRequest(wrongRoutingTable, true)))
                .toString(),
            containsString("discovery node must not be null")
        );
    }

    private SubscribableListener<ActionResponse.Empty> broadcastUnpromotableRequest(
        IndexShardRoutingTable wrongRoutingTable,
        boolean failShardOnError
    ) {
        return SubscribableListener.newForked(
            listener -> ActionTestUtils.execute(
                broadcastUnpromotableAction,
                null,
                new TestBroadcastUnpromotableRequest(wrongRoutingTable, failShardOnError),
                listener
            )
        );
    }

    public void testNullIndexShardRoutingTable() {
        assertThat(
            expectThrows(
                NullPointerException.class,
                () -> ActionTestUtils.execute(
                    broadcastUnpromotableAction,
                    null,
                    new TestBroadcastUnpromotableRequest((IndexShardRoutingTable) null),
                    ActionListener.running(ESTestCase::fail)
                )
            ).toString(),
            containsString("index shard routing table is null")
        );
    }

}
