/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.hamcrest.Matchers.containsString;

// TODO: add tests with failure conditions (onFailure callback) - how will I induce failure?
public class UpdateEventIngestedRangeTransportActionTaskExecutorTests extends ESTestCase {

    private UpdateEventIngestedRangeTransportAction.TaskExecutor executor;

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        executor = new UpdateEventIngestedRangeTransportAction.TaskExecutor();
        ClusterState stateBefore = stateWithNoShard();
        assertSame(stateBefore, executeTasks(stateBefore, List.of()));
    }

    public void testTaskWithSingleIndexAndShard() throws Exception {
        executor = new UpdateEventIngestedRangeTransportAction.TaskExecutor();

        ClusterState clusterState1;  // initial cluster state
        ClusterState clusterState2;  // cluster state after first task runs (building on clusterState1)

        Index blogsIndex = new Index("blogs", UUID.randomUUID().toString());

        // TODO: do I need to try other ShardRoutingStates here, like RELOCATING, UNASSIGNED or INITIALIZING?
        // ClusterState clusterState = state(blogsIndex, true, ShardRoutingState.STARTED);
        clusterState1 = state(1, new String[] { blogsIndex.getName() }, 1);

        Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
        EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
            new ShardId(blogsIndex, 0),
            ShardLongFieldRange.of(1000, 2000)
        );
        eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));

        UpdateEventIngestedRangeRequest rangeUpdateRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);
        var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest);

        clusterState2 = executeTasks(clusterState1, List.of(eventIngestedRangeTask));
        assertNotSame(clusterState1, clusterState2);
        // TODO: change this to blogsIndex
        IndexLongFieldRange eventIngestedRange = clusterState2.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();

        assertEquals(1000L, eventIngestedRange.getMin());
        assertEquals(2000L, eventIngestedRange.getMax());
    }

    public void testTasksWithSingleIndexAndMultipleShards() throws Exception {
        executor = new UpdateEventIngestedRangeTransportAction.TaskExecutor();

        ClusterState clusterState1;  // initial cluster state
        ClusterState clusterState2;  // cluster state after first task batch runs (building on clusterState1)
        ClusterState clusterState3;  // cluster state after second task batch runs (building on clusterState2)
        ClusterState clusterState4;  // cluster state after third task batch runs (building on clusterState3)
        ClusterState clusterState5;  // cluster state after fourth task batch runs (building on clusterState4)

        String indexName = "blogs";
        Index blogsIndex;

        // first task with eventIngestedRange of NO_SHARDS in IndexMetadata for "blogs" index
        {
            // TODO: do I need to try other ShardRoutingStates here, like RELOCATING, UNASSIGNED or INITIALIZING?
            // ClusterState clusterState = state(blogsIndex, true, ShardRoutingState.STARTED);
            clusterState1 = state(1, new String[] { indexName }, 6);
            blogsIndex = clusterState1.metadata().index(indexName).getIndex();

            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
            EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 0),
                ShardLongFieldRange.of(1000, 2000)
            );
            eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));

            UpdateEventIngestedRangeRequest rangeUpdateRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest);

            clusterState2 = executeTasks(clusterState1, List.of(eventIngestedRangeTask));
            assertNotSame(clusterState1, clusterState2);
            IndexLongFieldRange eventIngestedRange = clusterState2.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("1000-2000"));
        }

        // second task with eventIngestedRange of [1000, 2000] in IndexMetadata for "blogs" index should expand to range to [1000, 3000]
        {
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
            EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 1),
                ShardLongFieldRange.of(1000, 3000)
            );
            eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));
            UpdateEventIngestedRangeRequest rangeUpdateRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest);

            clusterState3 = executeTasks(clusterState2, List.of(eventIngestedRangeTask));
            assertNotSame(clusterState2, clusterState3);
            IndexLongFieldRange eventIngestedRange = clusterState3.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("1000-3000"));
        }

        // run two tasks in this batch - should expand range to [500, 4000]
        {
            // first task has two shards
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMapTask1 = new HashMap<>();
            List<EventIngestedRangeClusterStateService.ShardRangeInfo> shardRangeInfos = new ArrayList<>(3);
            shardRangeInfos.add(
                new EventIngestedRangeClusterStateService.ShardRangeInfo(new ShardId(blogsIndex, 2), ShardLongFieldRange.of(500, 650))
            );
            shardRangeInfos.add(
                new EventIngestedRangeClusterStateService.ShardRangeInfo(new ShardId(blogsIndex, 3), ShardLongFieldRange.of(2000, 3800))
            );
            shardRangeInfos.add(
                new EventIngestedRangeClusterStateService.ShardRangeInfo(new ShardId(blogsIndex, 4), ShardLongFieldRange.of(700, 4000))
            );
            Randomness.shuffle(shardRangeInfos);

            eventIngestedRangeMapTask1.put(blogsIndex, shardRangeInfos.subList(0, 2));
            UpdateEventIngestedRangeRequest rangeUpdateRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMapTask1);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest);

            // second task has one shard
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMapTask2 = new HashMap<>();
            eventIngestedRangeMapTask2.put(blogsIndex, shardRangeInfos.subList(2, 3));
            var eventIngestedRangeTask2 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMapTask2)
            );

            clusterState4 = executeTasks(clusterState3, List.of(eventIngestedRangeTask, eventIngestedRangeTask2));
            assertNotSame(clusterState3, clusterState4);
            IndexLongFieldRange eventIngestedRange = clusterState4.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("500-4000"));
        }

        // final task batch - all shard ranges are within that set already so event.ingested range should not change
        {
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
            EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfoA = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 5),
                ShardLongFieldRange.of(1000, 3000)
            );
            EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfoB = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 5),
                ShardLongFieldRange.of(2500, 3500)
            );
            eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfoA, shardRangeInfoB));
            UpdateEventIngestedRangeRequest rangeUpdateRequest = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest);

            clusterState5 = executeTasks(clusterState4, List.of(eventIngestedRangeTask));
            IndexLongFieldRange eventIngestedRange = clusterState5.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            assertEquals(500L, eventIngestedRange.getMin());
            assertEquals(4000L, eventIngestedRange.getMax());
        }
    }

    public void testTasksWithMultipleIndices() throws Exception {
        executor = new UpdateEventIngestedRangeTransportAction.TaskExecutor();

        ClusterState clusterState1;  // initial cluster state
        ClusterState clusterState2;  // cluster state after first task batch runs (building on clusterState1)

        String blogsIndexName = "blogs";
        String webTrafficIndexName = "web_traffic";
        Index blogsIndex;
        Index webTrafficIndex;

        // first task with eventIngestedRange of NO_SHARDS in IndexMetadata for "blogs" index
        {
            // TODO: do I need to try other ShardRoutingStates here, like RELOCATING, UNASSIGNED or INITIALIZING?
            // ClusterState clusterState = state(blogsIndex, true, ShardRoutingState.STARTED);
            clusterState1 = state(1, new String[] { blogsIndexName, webTrafficIndexName }, 3);
            blogsIndex = clusterState1.metadata().index(blogsIndexName).getIndex();
            webTrafficIndex = clusterState1.metadata().index(webTrafficIndexName).getIndex();

            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap1 = new HashMap<>();
            var shardRangeInfoBlogs1A = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 1),
                ShardLongFieldRange.of(1000, 2000)
            );
            var shardRangeInfoBlogs1B = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 0),
                ShardLongFieldRange.of(2500, 99999)
            );
            var shardRangeInfoWeb1A = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(webTrafficIndex, 0),
                ShardLongFieldRange.of(1_000_000, 5_000_000)
            );
            eventIngestedRangeMap1.put(blogsIndex, List.of(shardRangeInfoBlogs1A, shardRangeInfoBlogs1B));
            eventIngestedRangeMap1.put(webTrafficIndex, List.of(shardRangeInfoWeb1A));

            UpdateEventIngestedRangeRequest rangeUpdateRequest1 = new UpdateEventIngestedRangeRequest(eventIngestedRangeMap1);
            var eventIngestedRangeTask1 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(rangeUpdateRequest1);

            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap2 = new HashMap<>();
            var shardRangeInfoBlogs2A = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 2),
                ShardLongFieldRange.of(2000, 2500)
            );
            var shardRangeInfoWeb2A = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(webTrafficIndex, 2),
                ShardLongFieldRange.of(5_000_000, 10_000_000)
            );
            var shardRangeInfoWeb2B = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(webTrafficIndex, 1),
                ShardLongFieldRange.of(6_000_000, 11_000_000)
            );
            eventIngestedRangeMap2.put(blogsIndex, List.of(shardRangeInfoBlogs2A));
            eventIngestedRangeMap2.put(webTrafficIndex, List.of(shardRangeInfoWeb2A, shardRangeInfoWeb2B));

            var eventIngestedRangeTask2 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap2)
            );

            var taskList = new ArrayList<UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask>();
            taskList.add(eventIngestedRangeTask1);
            taskList.add(eventIngestedRangeTask2);
            Randomness.shuffle(taskList);
            clusterState2 = executeTasks(clusterState1, taskList);
            assertNotSame(clusterState1, clusterState2);

            IndexLongFieldRange eventIngestedRangeBlogs = clusterState2.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            assertEquals(1000L, eventIngestedRangeBlogs.getMin());
            assertEquals(99999L, eventIngestedRangeBlogs.getMax());

            IndexLongFieldRange eventIngestedRangeWebTraffic = clusterState2.getMetadata()
                .index(webTrafficIndex.getName())
                .getEventIngestedRange();
            assertEquals(1000000L, eventIngestedRangeWebTraffic.getMin());
            assertEquals(11000000L, eventIngestedRangeWebTraffic.getMax());
        }
    }

    private ClusterState executeTasks(ClusterState state, List<UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask> tasks)
        throws Exception {
        return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, executor, tasks);
    }
}
