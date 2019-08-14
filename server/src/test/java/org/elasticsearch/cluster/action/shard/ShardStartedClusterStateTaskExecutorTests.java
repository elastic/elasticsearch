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

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ShardStartedClusterStateTaskExecutorTests extends ESAllocationTestCase {

    private ShardStateAction.ShardStartedClusterStateTaskExecutor executor;

    private static void neverReroutes(String reason, Priority priority, ActionListener<Void> listener) {
        fail("unexpectedly ran a deferred reroute");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AllocationService allocationService = createAllocationService(Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .build());
        executor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(allocationService,
            ShardStartedClusterStateTaskExecutorTests::neverReroutes, logger);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, Collections.emptyList());
        assertSame(clusterState, result.resultingState);
    }

    public void testNonExistentIndexMarkedAsSuccessful() throws Exception {
        final ClusterState clusterState = stateWithNoShard();
        final StartedShardEntry entry = new StartedShardEntry(new ShardId("test", "_na", 0), "aId", randomNonNegativeLong(), "test");

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(entry));
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(1));
        assertThat(result.executionResults.containsKey(entry), is(true));
        assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(entry)).isSuccess(), is(true));
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithActivePrimary(indexName, true, randomInt(2), randomInt(2));

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final List<StartedShardEntry> tasks = Stream.concat(
            // Existent shard id but different allocation id
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetaData.getIndex(), 0), String.valueOf(i), 0L, "allocation id")),
            // Non existent shard id
            IntStream.range(1, randomIntBetween(2, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetaData.getIndex(), i), String.valueOf(i), 0L, "shard id"))

        ).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testNonInitializingShardAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = stateWithAssignedPrimariesAndReplicas(new String[]{indexName}, randomIntBetween(2, 10), 1);

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(1, indexMetaData.getNumberOfShards()))
            .mapToObj(i -> {
                final ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                final IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().shardRoutingTable(shardId);
                final String allocationId;
                if (randomBoolean()) {
                    allocationId = shardRoutingTable.primaryShard().allocationId().getId();
                } else {
                    allocationId = shardRoutingTable.replicaShards().iterator().next().allocationId().getId();
                }
                final long primaryTerm = indexMetaData.primaryTerm(shardId.id());
                return new StartedShardEntry(shardId, allocationId, primaryTerm, "test");
            }).collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
        });
    }

    public void testStartedShards() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final ShardId shardId = new ShardId(indexMetaData.getIndex(), 0);
        final long primaryTerm = indexMetaData.primaryTerm(shardId.id());
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        final List<StartedShardEntry> tasks = new ArrayList<>();
        tasks.add(new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "test"));
        if (randomBoolean()) {
            final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
            final String replicaAllocationId = replicaShard.allocationId().getId();
            tasks.add(new StartedShardEntry(shardId, replicaAllocationId, primaryTerm, "test"));
        }
        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testDuplicateStartsAreOkay() throws Exception {
        final String indexName = "test";
        final ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final ShardId shardId = new ShardId(indexMetaData.getIndex(), 0);
        final ShardRouting shardRouting = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String allocationId = shardRouting.allocationId().getId();
        final long primaryTerm = indexMetaData.primaryTerm(shardId.id());

        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> new StartedShardEntry(shardId, allocationId, primaryTerm, "test"))
            .collect(Collectors.toList());

        final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, tasks);
        assertNotSame(clusterState, result.resultingState);
        assertThat(result.executionResults.size(), equalTo(tasks.size()));
        tasks.forEach(task -> {
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

            final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
        });
    }

    public void testPrimaryTermsMismatch() throws Exception {
        final String indexName = "test";
        final int shard = 0;
        final int primaryTerm = 2 + randomInt(200);

        ClusterState clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .put(IndexMetaData.builder(clusterState.metaData().index(indexName))
                    .primaryTerm(shard, primaryTerm)
                    .build(), true)
                .build())
            .build();
        final ShardId shardId = new ShardId(clusterState.metaData().index(indexName).getIndex(), shard);
        final String primaryAllocationId = clusterState.routingTable().shardRoutingTable(shardId).primaryShard().allocationId().getId();
        {
            final StartedShardEntry task =
                new StartedShardEntry(shardId, primaryAllocationId, primaryTerm - 1, "primary terms does not match on primary");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.INITIALIZING));
            assertSame(clusterState, result.resultingState);
        }
        {
            final StartedShardEntry task =
                new StartedShardEntry(shardId, primaryAllocationId, primaryTerm, "primary terms match on primary");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
            clusterState = result.resultingState;
        }
        {
            final long replicaPrimaryTerm = randomBoolean() ? primaryTerm : primaryTerm - 1;
            final String replicaAllocationId = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next()
                .allocationId().getId();

            final StartedShardEntry task = new StartedShardEntry(shardId, replicaAllocationId, replicaPrimaryTerm, "test on replica");

            final ClusterStateTaskExecutor.ClusterTasksResult result = executeTasks(clusterState, singletonList(task));
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(1));
            assertThat(result.executionResults.containsKey(task), is(true));
            assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
            assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            assertNotSame(clusterState, result.resultingState);
        }
    }

    private ClusterStateTaskExecutor.ClusterTasksResult executeTasks(final ClusterState state,
                                                                     final List<StartedShardEntry> tasks) throws Exception {
        final ClusterStateTaskExecutor.ClusterTasksResult<StartedShardEntry> result = executor.execute(state, tasks);
        assertThat(result, notNullValue());
        return result;
    }
}
