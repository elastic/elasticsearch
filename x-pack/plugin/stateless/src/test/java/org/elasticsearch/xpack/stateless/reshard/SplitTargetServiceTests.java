/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SplitTargetServiceTests extends ESTestCase {
    public void testAcceptHandoff() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = mock(ReshardIndexService.class);
        when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
        var sts = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);

        var shardId = new ShardId("index", "1", 0);
        var sourceNode = new DiscoveryNode(
            "node1",
            "node_1",
            new TransportAddress(TransportAddress.META_ADDRESS, 10000),
            Map.of(),
            Set.of(),
            null
        );
        var targetNode = new DiscoveryNode(
            "node2",
            "node_2",
            new TransportAddress(TransportAddress.META_ADDRESS, 10001),
            Map.of(),
            Set.of(),
            null
        );
        var indexShard = mock(IndexShard.class);
        var split = new SplitTargetService.Split(shardId, sourceNode, targetNode, 2, 2);
        sts.initializeSplitInCloneState(indexShard, split);

        // Any requests to handoff that do not exactly match stored split information should be rejected.
        var newShardId = new ShardId("foo", "1", 0);
        var request = new TransportReshardSplitAction.Request(newShardId, sourceNode, targetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request, ActionListener.noop()));

        var newSourceNode = new DiscoveryNode(
            "node10",
            "node_1",
            new TransportAddress(TransportAddress.META_ADDRESS, 10000),
            Map.of(),
            Set.of(),
            null
        );
        var request2 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), newSourceNode, targetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request2, ActionListener.noop()));

        var newTargetNode = new DiscoveryNode(
            "node2",
            "node_22",
            new TransportAddress(TransportAddress.META_ADDRESS, 10001),
            Map.of(),
            Set.of(),
            null
        );
        var request3 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, newTargetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request3, ActionListener.noop()));

        var request4 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, targetNode, 100, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request4, ActionListener.noop()));

        var request5 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, targetNode, 2, 100);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request5, ActionListener.noop()));
    }

    /// If cancelSplits already ran (removing nothing) and the shard is CLOSED by the time a state machine is registered,
    /// the CLOSED guard must prevent leaving an entry in the map that nothing would ever clean up.
    public void testStateMachineIsNotRegisteredForClosedShard() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = mock(ReshardIndexService.class);
        when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
        var sts = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);

        var split = dummySplit();
        var indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(split.shardId());
        when(indexShard.state()).thenReturn(IndexShardState.CLOSED);

        sts.cancelSplits(indexShard);
        sts.initializeSplitInCloneState(indexShard, split);

        assertTrue(sts.getShardsWithOngoingSplits().isEmpty());
    }

    /// Closing a shard cancels its split. Nothing else answers the refreshes waiting on it, so the state machine's `cancel()` must
    /// fail them.
    public void testWaitingRefreshesAreAnsweredWhenSplitIsCancelled() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = new ReshardIndexService(
            mock(IndicesService.class),
            mock(NodeClient.class),
            ReshardMetrics.NOOP,
            clusterService,
            null,
            null
        );
        var sts = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);

        var split = dummySplit();
        var indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(split.shardId());
        var failureCount = new AtomicInteger();
        reshardIndexService.maybeAwaitSplit(
            IndexReshardingMetadata.newSplitByMultiple(1, 2),
            indexShard,
            ActionListener.wrap(ignored -> fail("should not have completed"), e -> {
                assertThat(e, instanceOf(IndexShardClosedException.class));
                failureCount.incrementAndGet();
            })
        );
        sts.initializeSplitInCloneState(indexShard, split);

        sts.cancelSplits(indexShard);

        assertEquals(1, failureCount.get());
        assertTrue(sts.getShardsWithOngoingSplits().isEmpty());
    }

    public void testAwaitSplitStateAppliedActionRetries() {
        var time = new AtomicInteger(0);
        var threadPool = new ThreadPool() {
            @Override
            public long relativeTimeInMillis() {
                return time.getAndAdd(1000);
            }

            @Override
            public ExecutorService generic() {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                executor.execute(command);
                // Only used for cancellation
                return null;
            }
        };
        var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(threadPool);
        var reshardIndexService = mock(ReshardIndexService.class);

        var requests = new AtomicInteger(0);
        var failingClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.name().equals(TransportAwaitClusterStateVersionAppliedAction.TYPE.name())) {
                    requests.incrementAndGet();
                    listener.onFailure(new TransportException("fail"));
                }
            }
        };

        var settings = Settings.builder().put("reshard.split.split_state_applied_timeout", TimeValue.timeValueMillis(5000)).build();
        var sts = new SplitTargetService(settings, failingClient, clusterService, reshardIndexService);

        // doesn't matter
        var clusterState = ClusterState.builder(new ClusterName("cluster")).build();
        var action = sts.createAwaitSplitStateAppliedAction(clusterState, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // Should be 5 given the custom time implementation.
                // Note that we succeed even though all requests fail.
                // This is to not block refresh for a long time.
                assertEquals(5, requests.get());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should never happen");
            }
        });
        action.run();
    }

    public void testInitiateSplitWithSourceShardActionRetries() {
        var time = new AtomicInteger(0);
        var threadPool = new ThreadPool() {
            @Override
            public long relativeTimeInMillis() {
                return time.getAndAdd(1000);
            }

            @Override
            public ExecutorService generic() {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                executor.execute(command);
                // Only used for cancellation
                return null;
            }
        };
        var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(threadPool);

        var reshardIndexService = mock(ReshardIndexService.class);

        var sourceShardId = new ShardId("index", "uuid", 0);
        var requests = new AtomicInteger(0);
        var failingClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.name().equals(TransportReshardSplitAction.TYPE.name())) {
                    requests.incrementAndGet();

                    var exception = randomFrom(
                        new IndexShardNotStartedException(sourceShardId, IndexShardState.RECOVERING),
                        new IndexNotFoundException("", "index"),
                        new ShardNotFoundException(sourceShardId)
                    );
                    listener.onFailure(exception);
                }
            }
        };

        var sts = new SplitTargetService(Settings.EMPTY, failingClient, clusterService, reshardIndexService);
        var action = sts.createInitiateSplitWithSourceShardAction(
            dummySplit(),
            new AtomicBoolean(false),
            TimeValue.timeValueMillis(5000),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    fail("Should never happen");
                }

                @Override
                public void onFailure(Exception e) {
                    // Should be 5 given the custom time implementation (5 times by 1000 millis exceeds 5000 millis timeout).
                    assertEquals(5, requests.get());
                }
            }
        );

        action.run();
    }

    /// Once the state machine of a target shard has been cancelled (its shard closed, e.g. for a restore over the index), the retrying
    /// action that deletes unowned documents must do nothing, and in particular must not look up a shard by its id, since a different
    /// shard with the same id may have replaced the closed one.
    public void testDeleteUnownedDoesNothingOnceCancelled() {
        var threadPool = directThreadPool();
        var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(threadPool);
        var reshardIndexService = mock(ReshardIndexService.class);
        var sts = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);

        var failures = new ArrayList<Exception>();
        var action = sts.createDeleteUnownedAction(mock(IndexShard.class), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("a cancelled action must not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                failures.add(e);
            }
        }, new AtomicBoolean(true));
        action.run();

        verify(reshardIndexService, never()).deleteUnownedDocuments(any(ShardId.class), any());
        verify(reshardIndexService, never()).deleteUnownedDocuments(any(IndexShard.class), any());
        assertThat(failures, hasSize(1));
        assertThat(failures.get(0), instanceOf(CancellationException.class));
    }

    /// As above, a cancelled action that moves the target to a new state must not send the request: the shard it is for has been
    /// closed, and the request would be a stale one for the master to reject.
    public void testChangeStateSendsNothingOnceCancelled() {
        var threadPool = directThreadPool();
        var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(threadPool);
        var requests = new AtomicInteger(0);
        var countingClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                requests.incrementAndGet();
            }
        };
        var sts = new SplitTargetService(Settings.EMPTY, countingClient, clusterService, mock(ReshardIndexService.class));

        var failures = new ArrayList<Exception>();
        var action = sts.createChangeStateAction(
            dummySplit(),
            IndexReshardingState.Split.TargetShardState.DONE,
            new AtomicBoolean(true),
            new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse response) {
                    fail("a cancelled action must not succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(e);
                }
            }
        );
        action.run();

        assertEquals(0, requests.get());
        assertThat(failures, hasSize(1));
        assertThat(failures.get(0), instanceOf(CancellationException.class));
    }

    /// A target shard waits until its search shards are online, or its primary term has advanced. If a restore has replaced the index
    /// with one that has fewer shards, the target shard is gone and there is nothing to wait for. Looking at its primary term or its
    /// routing must not fail.
    public void testWaitForSearchShardsStopsWhenTargetShardIsGone() {
        var index = new Index("index", "uuid");
        var restored = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), 2, 1).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID()))
            .build();
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(restored, false))
            .build();

        // The split target was shard 2 of a 4 shard index
        assertTrue(SplitTargetService.searchShardsOnlineOrNewPrimaryTerm(state, new ShardId(index, 2), 1L));
        assertTrue(SplitTargetService.searchShardsOnlineOrNewPrimaryTerm(state, new ShardId(index, 3), 1L));
    }

    /// A target shard's recovery can start while the shard is being closed (e.g. a restore replaces the index). If the shard is already
    /// closed when the recovery registers its state machine nothing would ever cancel it, so none is registered, and the recovery
    /// fails because its shard is closed.
    public void testRecoveryOfAlreadyClosedTargetShardFailsWithoutTrackingASplit() {
        var threadPool = directThreadPool();
        var clusterService = mock(ClusterService.class);
        when(clusterService.threadPool()).thenReturn(threadPool);
        when(clusterService.localNode()).thenReturn(dummyNode("node_target", 10001));
        var reshardIndexService = mock(ReshardIndexService.class);
        when(reshardIndexService.getReshardMetrics()).thenReturn(ReshardMetrics.NOOP);
        var sts = new SplitTargetService(Settings.EMPTY, silentClient(threadPool), clusterService, reshardIndexService);

        var indexShard = splitTargetShard(IndexShardState.CLOSED);
        var recovery = new PlainActionFuture<Void>();
        sts.startSplitTargetShardRecovery(indexShard, splitIndexMetadata(), recovery);

        assertThat(sts.getShardsWithOngoingSplits(), empty());
        expectThrows(IndexShardClosedException.class, recovery::actionGet);
    }

    /// A client that never answers, as if the start split request to the source shard were still waiting for the handoff
    private static NoOpClient silentClient(ThreadPool threadPool) {
        return new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {}
        };
    }

    /// A mock of a split target shard (shard 1 of a 2 shard index that is being split from 1 shard)
    private static IndexShard splitTargetShard(IndexShardState state) {
        var indexShard = mock(IndexShard.class);
        var shardId = new ShardId("index", "uuid", 1);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(state);
        when(indexShard.getOperationPrimaryTerm()).thenReturn(2L);
        var recoveryState = mock(RecoveryState.class);
        when(recoveryState.getRecoverySource()).thenReturn(RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        when(recoveryState.getSourceNode()).thenReturn(dummyNode("node_source", 10000));
        when(indexShard.recoveryState()).thenReturn(recoveryState);
        return indexShard;
    }

    private static IndexMetadata splitIndexMetadata() {
        return IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 2, 0).put(IndexMetadata.SETTING_INDEX_UUID, "uuid"))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(1, 2))
            .build();
    }

    private static DiscoveryNode dummyNode(String name, int port) {
        return new DiscoveryNode(name, name, new TransportAddress(TransportAddress.META_ADDRESS, port), Map.of(), Set.of(), null);
    }

    /// A thread pool that runs everything immediately, on the calling thread
    private static ThreadPool directThreadPool() {
        return new ThreadPool() {
            @Override
            public long relativeTimeInMillis() {
                return 0;
            }

            @Override
            public ExecutorService generic() {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                executor.execute(command);
                return null;
            }
        };
    }

    private static SplitTargetService.Split dummySplit() {
        var targetShardId = new ShardId("index", "1", 1);
        var sourceNode = new DiscoveryNode(
            "node1",
            "node_1",
            new TransportAddress(TransportAddress.META_ADDRESS, 10000),
            Map.of(),
            Set.of(),
            null
        );
        var targetNode = new DiscoveryNode(
            "node2",
            "node_2",
            new TransportAddress(TransportAddress.META_ADDRESS, 10001),
            Map.of(),
            Set.of(),
            null
        );
        return new SplitTargetService.Split(targetShardId, sourceNode, targetNode, 2, 2);
    }
}
