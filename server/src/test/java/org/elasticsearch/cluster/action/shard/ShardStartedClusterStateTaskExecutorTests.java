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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
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

    private ClusterState clusterState;
    private ShardStateAction.ShardStartedClusterStateTaskExecutor executor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterState = stateWithNoShard();
        AllocationService allocationService = createAllocationService(Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .build());
        executor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(allocationService, logger);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        assertTasksExecution(Collections.emptyList(), result -> assertSame(clusterState, result.resultingState));
    }

    public void testNonExistentIndexMarkedAsSuccessful() throws Exception {
        final StartedShardEntry entry = new StartedShardEntry(new ShardId("test", "_na", 0), "aId", "test");
        assertTasksExecution(singletonList(entry),
            result -> {
                assertSame(clusterState, result.resultingState);
                assertThat(result.executionResults.size(), equalTo(1));
                assertThat(result.executionResults.containsKey(entry), is(true));
                assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(entry)).isSuccess(), is(true));
            });
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        clusterState = stateWithActivePrimary(indexName, true, randomInt(2), randomInt(2));

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final List<StartedShardEntry> tasks = Stream.concat(
            // Existent shard id but different allocation id
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetaData.getIndex(), 0), String.valueOf(i), "allocation id")),
            // Non existent shard id
            IntStream.range(1, randomIntBetween(2, 5))
                .mapToObj(i -> new StartedShardEntry(new ShardId(indexMetaData.getIndex(), i), String.valueOf(i), "shard id"))

        ).collect(Collectors.toList());

        assertTasksExecution(tasks, result -> {
            assertSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(tasks.size()));
            tasks.forEach(task -> {
                assertThat(result.executionResults.containsKey(task), is(true));
                assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            });
        });
    }

    public void testNonInitializingShardAreMarkedAsSuccessful() throws Exception {
        final String indexName = "test";
        clusterState = stateWithAssignedPrimariesAndReplicas(new String[]{indexName}, randomIntBetween(2, 10), 1);

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
                return new StartedShardEntry(shardId, allocationId, "test");
            }).collect(Collectors.toList());

        assertTasksExecution(tasks, result -> {
            assertSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(tasks.size()));
            tasks.forEach(task -> {
                assertThat(result.executionResults.containsKey(task), is(true));
                assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));
            });
        });
    }

    public void testStartedShards() throws Exception {
        final String indexName = "test";
        clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING, ShardRoutingState.INITIALIZING);

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final ShardId shardId = new ShardId(indexMetaData.getIndex(), 0);
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String primaryAllocationId = primaryShard.allocationId().getId();

        final List<StartedShardEntry> tasks = new ArrayList<>();
        tasks.add(new StartedShardEntry(shardId, primaryAllocationId, "test"));
        if (randomBoolean()) {
            final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().iterator().next();
            final String replicaAllocationId = replicaShard.allocationId().getId();
            tasks.add(new StartedShardEntry(shardId, replicaAllocationId, "test"));
        }
        assertTasksExecution(tasks, result -> {
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(tasks.size()));
            tasks.forEach(task -> {
                assertThat(result.executionResults.containsKey(task), is(true));
                assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

                final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
                assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            });
        });
    }

    public void testDuplicateStartsAreOkay() throws Exception {
        final String indexName = "test";
        clusterState = state(indexName, randomBoolean(), ShardRoutingState.INITIALIZING);

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final ShardId shardId = new ShardId(indexMetaData.getIndex(), 0);
        final ShardRouting shardRouting = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final String allocationId = shardRouting.allocationId().getId();

        final List<StartedShardEntry> tasks = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> new StartedShardEntry(shardId, allocationId, "test"))
            .collect(Collectors.toList());

        assertTasksExecution(tasks, result -> {
            assertNotSame(clusterState, result.resultingState);
            assertThat(result.executionResults.size(), equalTo(tasks.size()));
            tasks.forEach(task -> {
                assertThat(result.executionResults.containsKey(task), is(true));
                assertThat(((ClusterStateTaskExecutor.TaskResult) result.executionResults.get(task)).isSuccess(), is(true));

                final IndexShardRoutingTable shardRoutingTable = result.resultingState.routingTable().shardRoutingTable(task.shardId);
                assertThat(shardRoutingTable.getByAllocationId(task.allocationId).state(), is(ShardRoutingState.STARTED));
            });
        });
    }

    private void assertTasksExecution(final List<StartedShardEntry> tasks,
                                      final Consumer<ClusterStateTaskExecutor.ClusterTasksResult> consumer) throws Exception {
        final ClusterStateTaskExecutor.ClusterTasksResult<StartedShardEntry> result = executor.execute(clusterState, tasks);
        assertThat(result, notNullValue());
        consumer.accept(result);
    }
}
