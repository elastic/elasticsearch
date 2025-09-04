/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardStartedClusterStateTaskExecutorTests extends ESAllocationTestCase {

    private ShardStateAction.ShardStartedClusterStateTaskExecutor executor;

    @SuppressWarnings("unused")
    private static void neverReroutes(String reason, Priority priority, ActionListener<Void> listener) {
        fail("unexpectedly ran a deferred reroute");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AllocationService allocationService = createAllocationService(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE).build()
        );
        executor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(
            allocationService,
            ShardStartedClusterStateTaskExecutorTests::neverReroutes
        );
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        assertSame(clusterState, executeTasks(clusterState, List.of()));
    }

    public void testNonExistentIndexMarkedAsSuccessful() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final StartedShardUpdateTask entry = new StartedShardUpdateTask(
            new StartedShardEntry(
                new ShardId("test", "_na", 0),
                "aId",
                randomNonNegativeLong(),
                "test",
                ShardLongFieldRange.UNKNOWN,
                ShardLongFieldRange.UNKNOWN
            ),
            createTestListener()
        );

        assertSame(clusterState, executeTasks(clusterState, List.of(entry)));
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithActivePrimary(indexName, true, randomInt(2), randomInt(2));

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final List<StartedShardUpdateTask> tasks = Stream.concat(
            // Existent shard id but different allocation id
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(
                    i -> new StartedShardUpdateTask(
                        new StartedShardEntry(
                            new ShardId(indexMetadata.getIndex(), 0),
                            String.valueOf(i),
                            0L,
                            "allocation id",
                            ShardLongFieldRange.UNKNOWN,
                            ShardLongFieldRange.UNKNOWN
                        ),
                        createTestListener()
                    )
                ),
            // Non existent shard id
            IntStream.range(1, randomIntBetween(2, 5))
                .mapToObj(
                    i -> new StartedShardUpdateTask(
                        new StartedShardEntry(
                            new ShardId(indexMetadata.getIndex(), i),
                            String.valueOf(i),
                            0L,
                            "shard id",
                            ShardLongFieldRange.UNKNOWN,
                            ShardLongFieldRange.UNKNOWN
                        ),
                        createTestListener()
                    )
                )

        ).toList();

        assertSame(clusterState, executeTasks(clusterState, tasks));
    }

    public void testNonInitializingShardAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithAssignedPrimariesAndReplicas(new String[] { indexName }, randomIntBetween(2, 10), 1);

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final List<StartedShardUpdateTask> tasks = IntStream.range(0, randomIntBetween(1, indexMetadata.getNumberOfShards()))
            .mapToObj(i -> {
                final ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                final IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().shardRoutingTable(shardId);
                final String allocationId;
                if (randomBoolean()) {
                    allocationId = shardRoutingTable.primaryShard().allocationId().getId();
                } else {
                    allocationId = shardRoutingTable.replicaShards().iterator().next().allocationId().getId();
                }
                final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
                return new StartedShardUpdateTask(
                    new StartedShardEntry(
                        shardId,
                        allocationId,
                        primaryTerm,
                        "test",
                        ShardLongFieldRange.UNKNOWN,
                        ShardLongFieldRange.UNKNOWN
                    ),
                    createTestListener()
                );
            })
            .toList();

        assertSame(clusterState, executeTasks(clusterState, tasks));
    }

    public void testStartPrimary() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        final var task = new StartedShardUpdateTask(
            new StartedShardEntry(
                shardId,
                primaryAllocationId,
                primaryTerm,
                "test",
                ShardLongFieldRange.UNKNOWN,
                ShardLongFieldRange.UNKNOWN
            ),
            createTestListener()
        );

        final var resultingState = executeTasks(clusterState, List.of(task));
        assertNotSame(clusterState, resultingState);
        assertThat(
            resultingState.routingTable()
                .shardRoutingTable(task.getStartedShardEntry().shardId)
                .getByAllocationId(task.getStartedShardEntry().allocationId)
                .state(),
            is(ShardRoutingState.STARTED)
        );
    }

    public void testStartReplica() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();

        final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
        final String replicaAllocationId = replicaShard.allocationId().getId();
        final var task = new StartedShardUpdateTask(
            new StartedShardEntry(
                shardId,
                replicaAllocationId,
                primaryTerm,
                "test",
                ShardLongFieldRange.UNKNOWN,
                ShardLongFieldRange.UNKNOWN
            ),
            createTestListener()
        );

        final var resultingState = executeTasks(clusterState, List.of(task));
        assertNotSame(clusterState, resultingState);
        assertThat(
            resultingState.routingTable()
                .shardRoutingTable(task.getStartedShardEntry().shardId)
                .getByAllocationId(task.getStartedShardEntry().allocationId)
                .state(),
            is(ShardRoutingState.STARTED)
        );
    }

    public void testDuplicateStartsAreOkay() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final ProjectMetadata project = clusterState.metadata().getProject();

        final IndexMetadata indexMetadata = project.index(indexName);
        assertThat(indexMetadata, notNullValue());
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting shardRouting = clusterState.routingTable(project.id()).shardRoutingTable(shardId).primaryShard();
        final String allocationId = shardRouting.allocationId().getId();
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());

        final List<StartedShardUpdateTask> tasks = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(
                i -> new StartedShardUpdateTask(
                    new StartedShardEntry(
                        shardId,
                        allocationId,
                        primaryTerm,
                        "test",
                        ShardLongFieldRange.UNKNOWN,
                        ShardLongFieldRange.UNKNOWN
                    ),
                    createTestListener()
                )
            )
            .toList();

        final var resultingState = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, resultingState);
        for (final var task : tasks) {
            assertThat(
                resultingState.routingTable(project.id())
                    .shardRoutingTable(task.getStartedShardEntry().shardId)
                    .getByAllocationId(task.getStartedShardEntry().allocationId)
                    .state(),
                is(ShardRoutingState.STARTED)
            );
        }
    }

    public void testPrimaryTermsMismatchOnPrimary() throws Exception {
        final String indexName = "test";
        final int shard = 0;
        final int primaryTerm = 2 + randomInt(200);

        ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(
                        IndexMetadata.builder(clusterState.metadata().getProject().index(indexName))
                            .primaryTerm(shard, primaryTerm)
                            .build(),
                        true
                    )
                    .build()
            )
            .build();
        final ShardId shardId = new ShardId(clusterState.metadata().getProject().index(indexName).getIndex(), shard);
        final String primaryAllocationId = clusterState.routingTable().shardRoutingTable(shardId).primaryShard().allocationId().getId();
        {
            final StartedShardUpdateTask task = new StartedShardUpdateTask(
                new StartedShardEntry(
                    shardId,
                    primaryAllocationId,
                    primaryTerm - 1,
                    "primary terms does not match on primary",
                    ShardLongFieldRange.UNKNOWN,
                    ShardLongFieldRange.UNKNOWN
                ),
                createTestListener()
            );

            assertSame(clusterState, executeTasks(clusterState, List.of(task)));
            assertThat(
                clusterState.routingTable()
                    .shardRoutingTable(task.getStartedShardEntry().shardId)
                    .getByAllocationId(task.getStartedShardEntry().allocationId)
                    .state(),
                is(ShardRoutingState.INITIALIZING)
            );
        }
        {
            final StartedShardUpdateTask task = new StartedShardUpdateTask(
                new StartedShardEntry(
                    shardId,
                    primaryAllocationId,
                    primaryTerm,
                    "primary terms match on primary",
                    ShardLongFieldRange.UNKNOWN,
                    ShardLongFieldRange.UNKNOWN
                ),
                createTestListener()
            );

            final var resultingState = executeTasks(clusterState, List.of(task));
            assertNotSame(clusterState, resultingState);
            assertThat(
                resultingState.routingTable()
                    .shardRoutingTable(task.getStartedShardEntry().shardId)
                    .getByAllocationId(task.getStartedShardEntry().allocationId)
                    .state(),
                is(ShardRoutingState.STARTED)
            );
        }
    }

    public void testPrimaryTermsMismatchOnReplica() throws Exception {
        final String indexName = "test";
        final int shard = 0;
        final int primaryTerm = 2 + randomInt(200);

        ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(
                        IndexMetadata.builder(clusterState.metadata().getProject().index(indexName))
                            .primaryTerm(shard, primaryTerm)
                            .build(),
                        true
                    )
                    .build()
            )
            .build();
        final ShardId shardId = new ShardId(clusterState.metadata().getProject().index(indexName).getIndex(), shard);
        {
            final long replicaPrimaryTerm = randomBoolean() ? primaryTerm : primaryTerm - 1;
            final String replicaAllocationId = clusterState.routingTable()
                .shardRoutingTable(shardId)
                .replicaShards()
                .iterator()
                .next()
                .allocationId()
                .getId();

            final StartedShardUpdateTask task = new StartedShardUpdateTask(
                new StartedShardEntry(
                    shardId,
                    replicaAllocationId,
                    replicaPrimaryTerm,
                    "test on replica",
                    ShardLongFieldRange.UNKNOWN,
                    ShardLongFieldRange.UNKNOWN
                ),
                createTestListener()
            );

            final var resultingState = executeTasks(clusterState, List.of(task));
            assertNotSame(clusterState, resultingState);
            assertThat(
                resultingState.routingTable()
                    .shardRoutingTable(task.getStartedShardEntry().shardId)
                    .getByAllocationId(task.getStartedShardEntry().allocationId)
                    .state(),
                is(ShardRoutingState.STARTED)
            );
        }
    }

    public void testExpandsTimestampRangeForPrimary() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        assertThat(indexMetadata.getTimestampRange(), sameInstance(IndexLongFieldRange.NO_SHARDS));
        assertThat(indexMetadata.getEventIngestedRange(), sameInstance(IndexLongFieldRange.NO_SHARDS));

        final ShardLongFieldRange shardTimestampRange = randomBoolean() ? ShardLongFieldRange.UNKNOWN
            : randomBoolean() ? ShardLongFieldRange.EMPTY
            : ShardLongFieldRange.of(1606407943000L, 1606407944000L);

        final ShardLongFieldRange shardEventIngestedRange = randomBoolean() ? ShardLongFieldRange.UNKNOWN
            : randomBoolean() ? ShardLongFieldRange.EMPTY
            : ShardLongFieldRange.of(1606407943000L, 1606407944000L);

        final var task = new StartedShardUpdateTask(
            new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "test", shardTimestampRange, shardEventIngestedRange),
            createTestListener()
        );

        final var resultingState = executeTasks(clusterState, List.of(task));
        assertNotSame(clusterState, resultingState);
        assertThat(
            resultingState.routingTable()
                .shardRoutingTable(task.getStartedShardEntry().shardId)
                .getByAllocationId(task.getStartedShardEntry().allocationId)
                .state(),
            is(ShardRoutingState.STARTED)
        );

        final var timestampRange = resultingState.metadata().getProject().index(indexName).getTimestampRange();
        if (shardTimestampRange == ShardLongFieldRange.UNKNOWN) {
            assertThat(timestampRange, sameInstance(IndexLongFieldRange.UNKNOWN));
        } else if (shardTimestampRange == ShardLongFieldRange.EMPTY) {
            assertThat(timestampRange, sameInstance(IndexLongFieldRange.EMPTY));
        } else {
            assertTrue(timestampRange.isComplete());
            assertThat(timestampRange.getMin(), equalTo(shardTimestampRange.getMin()));
            assertThat(timestampRange.getMax(), equalTo(shardTimestampRange.getMax()));
        }

        final var eventIngestedRange = resultingState.metadata().getProject().index(indexName).getEventIngestedRange();
        if (clusterState.getMinTransportVersion().before(TransportVersions.V_8_15_0)) {
            assertThat(eventIngestedRange, sameInstance(IndexLongFieldRange.UNKNOWN));
        } else {
            if (shardEventIngestedRange == ShardLongFieldRange.UNKNOWN) {
                assertThat(eventIngestedRange, sameInstance(IndexLongFieldRange.UNKNOWN));
            } else if (shardEventIngestedRange == ShardLongFieldRange.EMPTY) {
                assertThat(eventIngestedRange, sameInstance(IndexLongFieldRange.EMPTY));
            } else {
                assertTrue(eventIngestedRange.isComplete());
                assertThat(eventIngestedRange.getMin(), equalTo(shardEventIngestedRange.getMin()));
                assertThat(eventIngestedRange.getMax(), equalTo(shardEventIngestedRange.getMax()));
            }
        }
    }

    public void testExpandsTimestampRangeForReplica() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final long primaryTerm = indexMetadata.primaryTerm(shardId.id());

        assertThat(indexMetadata.getTimestampRange(), sameInstance(IndexLongFieldRange.UNKNOWN));
        assertThat(indexMetadata.getEventIngestedRange(), sameInstance(IndexLongFieldRange.UNKNOWN));

        final ShardLongFieldRange shardTimestampRange = randomBoolean() ? ShardLongFieldRange.UNKNOWN
            : randomBoolean() ? ShardLongFieldRange.EMPTY
            : ShardLongFieldRange.of(1606407943000L, 1606407944000L);

        final ShardLongFieldRange shardEventIngestedRange = randomBoolean() ? ShardLongFieldRange.UNKNOWN
            : randomBoolean() ? ShardLongFieldRange.EMPTY
            : ShardLongFieldRange.of(1606407888888L, 1606407999999L);

        final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
        final String replicaAllocationId = replicaShard.allocationId().getId();
        final var task = new StartedShardUpdateTask(
            new StartedShardEntry(shardId, replicaAllocationId, primaryTerm, "test", shardTimestampRange, shardEventIngestedRange),
            createTestListener()
        );
        final var resultingState = executeTasks(clusterState, List.of(task));
        assertNotSame(clusterState, resultingState);
        assertThat(
            resultingState.routingTable()
                .shardRoutingTable(task.getStartedShardEntry().shardId)
                .getByAllocationId(task.getStartedShardEntry().allocationId)
                .state(),
            is(ShardRoutingState.STARTED)
        );

        final IndexMetadata latestIndexMetadata = resultingState.metadata().getProject().index(indexName);
        assertThat(latestIndexMetadata.getTimestampRange(), sameInstance(IndexLongFieldRange.UNKNOWN));
        assertThat(latestIndexMetadata.getEventIngestedRange(), sameInstance(IndexLongFieldRange.UNKNOWN));
    }

    private ClusterState executeTasks(final ClusterState state, final List<StartedShardUpdateTask> tasks) throws Exception {
        return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, executor, tasks);
    }

    private static <T> ActionListener<T> createTestListener() {
        return ActionTestUtils.assertNoFailureListener(t -> {});
    }
}
