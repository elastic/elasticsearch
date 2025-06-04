/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.FakeTransport;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest.DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;

public class TransportIndicesShardStoresActionTests extends ESTestCase {

    public void testEmpty() {
        runTest(new TestHarness() {
            @Override
            void runTest() {
                final var request = new IndicesShardStoresRequest();
                request.shardStatuses("green", "red"); // newly-created shards are in yellow health so this matches none of them
                final var future = new PlainActionFuture<IndicesShardStoresResponse>();
                action.execute(
                    new CancellableTask(1, "transport", TransportIndicesShardStoresAction.TYPE.name(), "", TaskId.EMPTY_TASK_ID, Map.of()),
                    request,
                    future
                );

                final var response = safeGet(future);
                assertThat(response.getFailures(), empty());
                assertThat(response.getStoreStatuses(), anEmptyMap());
                assertThat(shardsWithFailures, empty());
                assertThat(foundShards, empty());
            }
        });
    }

    public void testNonempty() {
        runTest(new TestHarness() {
            @Override
            void runTest() {
                final var request = new IndicesShardStoresRequest();
                request.shardStatuses(randomFrom("yellow", "all")); // newly-created shards are in yellow health so this matches all of them
                final var future = new PlainActionFuture<IndicesShardStoresResponse>();
                action.execute(
                    new CancellableTask(1, "transport", TransportIndicesShardStoresAction.TYPE.name(), "", TaskId.EMPTY_TASK_ID, Map.of()),
                    request,
                    future
                );
                assertFalse(future.isDone());

                deterministicTaskQueue.runAllTasks();
                assertTrue(future.isDone());
                final var response = future.actionGet();

                assertEquals(
                    shardsWithFailures,
                    response.getFailures().stream().map(f -> f.index() + "/" + f.shardId()).collect(Collectors.toSet())
                );

                for (final var indexRoutingTable : clusterState.routingTable()) {
                    final var indexResponse = response.getStoreStatuses().get(indexRoutingTable.getIndex().getName());
                    assertNotNull(indexResponse);
                    for (int shardNum = 0; shardNum < indexRoutingTable.size(); shardNum++) {
                        final var shardResponse = indexResponse.get(shardNum);
                        assertNotNull(shardResponse);
                        if (foundShards.contains(indexRoutingTable.shard(shardNum).shardId())) {
                            assertEquals(1, shardResponse.size());
                            assertSame(localNode, shardResponse.get(0).getNode());
                        } else {
                            assertThat(shardResponse, empty());
                        }
                    }
                }
            }
        });
    }

    public void testCancellation() {
        runTest(new TestHarness() {
            @Override
            void runTest() {
                final var task = new CancellableTask(
                    1,
                    "transport",
                    TransportIndicesShardStoresAction.TYPE.name(),
                    "",
                    TaskId.EMPTY_TASK_ID,
                    Map.of()
                );
                final var request = new IndicesShardStoresRequest();
                request.shardStatuses(randomFrom("yellow", "all"));
                final var future = new PlainActionFuture<IndicesShardStoresResponse>();
                action.execute(task, request, future);
                TaskCancelHelper.cancel(task, "testing");
                listExpected = false;
                assertFalse(future.isDone());
                deterministicTaskQueue.runAllTasks();
                expectThrows(ExecutionException.class, TaskCancelledException.class, future::result);
            }
        });
    }

    public void testFailure() {
        runTest(new TestHarness() {
            @Override
            void runTest() {
                final var request = new IndicesShardStoresRequest();
                request.shardStatuses(randomFrom("yellow", "all"));
                final var future = new PlainActionFuture<IndicesShardStoresResponse>();
                action.execute(
                    new CancellableTask(1, "transport", TransportIndicesShardStoresAction.TYPE.name(), "", TaskId.EMPTY_TASK_ID, Map.of()),
                    request,
                    future
                );
                assertFalse(future.isDone());
                failOneRequest = true;
                deterministicTaskQueue.runAllTasks();
                assertFalse(failOneRequest);
                assertEquals(
                    "simulated",
                    expectThrows(ExecutionException.class, ElasticsearchException.class, future::result).getMessage()
                );
            }
        });
    }

    private static void runTest(TestHarness testHarness) {
        try (testHarness) {
            testHarness.runTest();
        }
    }

    private abstract static class TestHarness implements Closeable {
        final DeterministicTaskQueue deterministicTaskQueue;
        final DiscoveryNode localNode;
        final ClusterState clusterState;
        final HashSet<String> shardsWithFailures = new HashSet<>();
        final HashSet<ShardId> foundShards = new HashSet<>();
        final TransportIndicesShardStoresAction action;
        final ClusterService clusterService;

        boolean listExpected = true;
        boolean failOneRequest = false;

        TestHarness() {
            this.deterministicTaskQueue = new DeterministicTaskQueue();
            this.localNode = DiscoveryNodeUtils.create("local");

            final var threadPool = deterministicTaskQueue.getThreadPool();

            final var settings = Settings.EMPTY;
            final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

            final var transportService = new TransportService(
                settings,
                new FakeTransport(),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> localNode,
                clusterSettings,
                Set.of()
            );

            final var nodes = DiscoveryNodes.builder();
            nodes.add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId());

            final var indexCount = between(1, 100);
            final var metadata = Metadata.builder();
            final var routingTable = RoutingTable.builder();
            for (int i = 0; i < indexCount; i++) {
                final var indexMetadata = IndexMetadata.builder("index-" + i)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                    .numberOfShards(between(1, 3))
                    .numberOfReplicas(between(0, 2))
                    .build();
                metadata.put(indexMetadata, false);

                final var irt = IndexRoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, indexMetadata.getIndex())
                    .initializeAsNew(indexMetadata);
                routingTable.add(irt);
            }
            this.clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(nodes)
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            this.clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool, clusterSettings);

            this.action = new TransportIndicesShardStoresAction(
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(Set.of()),
                TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
                null
            ) {
                private final Semaphore pendingActionPermits = new Semaphore(DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS);

                @Override
                void listShardStores(
                    TransportNodesListGatewayStartedShards.Request request,
                    ActionListener<TransportNodesListGatewayStartedShards.NodesGatewayStartedShards> listener
                ) {
                    assertTrue(pendingActionPermits.tryAcquire());
                    assertTrue(listExpected);
                    deterministicTaskQueue.scheduleNow(() -> {
                        pendingActionPermits.release();

                        final var simulateNodeFailure = rarely();
                        if (simulateNodeFailure) {
                            assertTrue(shardsWithFailures.add(request.shardId().getIndexName() + "/" + request.shardId().getId()));
                        }

                        final var foundShardStore = rarely();
                        if (foundShardStore) {
                            assertTrue(foundShards.add(request.shardId()));
                        }

                        if (failOneRequest) {
                            failOneRequest = false;
                            listener.onFailure(new ElasticsearchException("simulated"));
                        } else {
                            listener.onResponse(
                                new TransportNodesListGatewayStartedShards.NodesGatewayStartedShards(
                                    clusterService.getClusterName(),
                                    foundShardStore
                                        ? List.of(
                                            new TransportNodesListGatewayStartedShards.NodeGatewayStartedShards(
                                                localNode,
                                                randomAlphaOfLength(10),
                                                randomBoolean()
                                            )
                                        )
                                        : List.of(),
                                    simulateNodeFailure
                                        ? List.of(
                                            new FailedNodeException(
                                                randomAlphaOfLength(10),
                                                "test failure",
                                                new ElasticsearchException("simulated")
                                            )
                                        )
                                        : List.of()
                                )
                            );
                        }
                    });
                }
            };
        }

        abstract void runTest();

        @Override
        public void close() {
            clusterService.close();
        }
    }
}
