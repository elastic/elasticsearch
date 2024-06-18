/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithNoShard;
import static org.hamcrest.Matchers.containsString;

// TODO: add tests with failure conditions (onFailure callback) - how will I induce failure?
public class UpdateEventIngestedRangeTransportActionTaskExecutorTests extends ESTestCase {

    private ClusterStateTaskExecutor<UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask> executor =
        UpdateEventIngestedRangeTransportAction.EVENT_INGESTED_UPDATE_TASK_EXECUTOR;

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        ClusterState stateBefore = stateWithNoShard();
        assertSame(stateBefore, executeTasks(stateBefore, List.of()));
    }

    public void testTaskWithSingleIndexAndShard() throws Exception {
        ClusterState clusterState1;  // initial cluster state
        ClusterState clusterState2;  // cluster state after first task runs (building on clusterState1)

        String indexName = "blogs";
        Index blogsIndex;

        // TODO: do I need to try other ShardRoutingStates here, like RELOCATING, UNASSIGNED or INITIALIZING?
        // ClusterState clusterState = state(blogsIndex, true, ShardRoutingState.STARTED);
        clusterState1 = state(1, new String[] { indexName }, 1);
        blogsIndex = clusterState1.metadata().index(indexName).getIndex();

        Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
        EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
            new ShardId(blogsIndex, 0),
            ShardLongFieldRange.of(1000, 2000)
        );
        eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));

        AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
        var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
            new UpdateEventIngestedRangeRequest(eventIngestedRangeMap),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    ackedResponseFromCallback.set(acknowledgedResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    fail("onFailure should not have been called but received: " + e);
                }
            }
        );

        clusterState2 = executeTasks(clusterState1, List.of(eventIngestedRangeTask));
        assertNotSame(clusterState1, clusterState2);
        IndexLongFieldRange eventIngestedRange = clusterState2.getMetadata().index(blogsIndex).getEventIngestedRange();

        assertEquals(1000L, eventIngestedRange.getMin());
        assertEquals(2000L, eventIngestedRange.getMax());
        assertNotNull("onResponse should have been called", ackedResponseFromCallback.get());
        assertTrue("AcknowledgedResponse should have true setting", ackedResponseFromCallback.get().isAcknowledged());
    }

    public void testTasksWithSingleIndexAndMultipleShards() throws Exception {
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

            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

            clusterState2 = executeTasks(clusterState1, List.of(eventIngestedRangeTask));
            assertNotSame(clusterState1, clusterState2);
            IndexLongFieldRange eventIngestedRange = clusterState2.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("1000-2000"));
            assertNotNull("onResponse should have been called", ackedResponseFromCallback.get());
            assertTrue("AcknowledgedResponse should have true setting", ackedResponseFromCallback.get().isAcknowledged());
        }

        // second task with eventIngestedRange of [1000, 2000] in IndexMetadata for "blogs" index should expand to range to [1000, 3000]
        {
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
            EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
                new ShardId(blogsIndex, 1),
                ShardLongFieldRange.of(1000, 3000)
            );
            eventIngestedRangeMap.put(blogsIndex, List.of(shardRangeInfo));
            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

            clusterState3 = executeTasks(clusterState2, List.of(eventIngestedRangeTask));
            assertNotSame(clusterState2, clusterState3);
            IndexLongFieldRange eventIngestedRange = clusterState3.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("1000-3000"));
            assertNotNull("onResponse should have been called", ackedResponseFromCallback.get());
            assertTrue("AcknowledgedResponse should have true setting", ackedResponseFromCallback.get().isAcknowledged());
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
            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMapTask1),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

            // second task has one shard
            Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMapTask2 = new HashMap<>();
            eventIngestedRangeMapTask2.put(blogsIndex, shardRangeInfos.subList(2, 3));

            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback2 = new AtomicReference<>(null);
            var eventIngestedRangeTask2 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMapTask2),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback2.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

            clusterState4 = executeTasks(clusterState3, List.of(eventIngestedRangeTask, eventIngestedRangeTask2));
            assertNotSame(clusterState3, clusterState4);
            IndexLongFieldRange eventIngestedRange = clusterState4.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            // you can't actually call getMin or getMax right now - those throw errors since not all shards are accounted for
            assertThat(eventIngestedRange.toString(), containsString("500-4000"));
            assertNotNull("onResponse CB1 should have been called", ackedResponseFromCallback.get());
            assertTrue("AcknowledgedResponse CB1 should have true setting", ackedResponseFromCallback.get().isAcknowledged());
            assertNotNull("onResponse CB2 should have been called", ackedResponseFromCallback2.get());
            assertTrue("AcknowledgedResponse CB2 should have true setting", ackedResponseFromCallback2.get().isAcknowledged());
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
            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
            var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

            clusterState5 = executeTasks(clusterState4, List.of(eventIngestedRangeTask));
            IndexLongFieldRange eventIngestedRange = clusterState5.getMetadata().index(blogsIndex.getName()).getEventIngestedRange();
            assertEquals(500L, eventIngestedRange.getMin());
            assertEquals(4000L, eventIngestedRange.getMax());
            assertNotNull("onResponse should have been called", ackedResponseFromCallback.get());
            assertTrue("AcknowledgedResponse should have true setting", ackedResponseFromCallback.get().isAcknowledged());
        }
    }

    public void testTasksWithMultipleIndices() throws Exception {
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

            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
            var eventIngestedRangeTask1 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap1),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
            );

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

            AtomicReference<AcknowledgedResponse> ackedResponseFromCallback2 = new AtomicReference<>(null);
            var eventIngestedRangeTask2 = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
                new UpdateEventIngestedRangeRequest(eventIngestedRangeMap2),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        ackedResponseFromCallback2.set(acknowledgedResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("onFailure should not have been called but received: " + e);
                    }
                }
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

            assertNotNull("onResponse CB1 should have been called", ackedResponseFromCallback.get());
            assertTrue("AcknowledgedResponse CB1 should have true setting", ackedResponseFromCallback.get().isAcknowledged());
            assertNotNull("onResponse CB2 should have been called", ackedResponseFromCallback2.get());
            assertTrue("AcknowledgedResponse CB2 should have true setting", ackedResponseFromCallback2.get().isAcknowledged());
        }
    }

    // TODO: want to see how the onFailure callback is called
    public void testErrorInTaskExecutor() throws Exception {
        ClusterState clusterState1;  // initial cluster state
        ClusterState clusterState2;  // cluster state after first task runs (building on clusterState1)

        String indexName = "blogs";
        Index blogsIndex; // TODO: LEFTOFF - change this to a self-created index

        // TODO: do I need to try other ShardRoutingStates here, like RELOCATING, UNASSIGNED or INITIALIZING?
        // ClusterState clusterState = state(blogsIndex, true, ShardRoutingState.STARTED);
        clusterState1 = state(1, new String[] { indexName }, 1);
        blogsIndex = clusterState1.metadata().index(indexName).getIndex();

        Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap = new HashMap<>();
        EventIngestedRangeClusterStateService.ShardRangeInfo shardRangeInfo = new EventIngestedRangeClusterStateService.ShardRangeInfo(
            new ShardId(blogsIndex, 0),
            ShardLongFieldRange.of(1000, 2000)
        );
        eventIngestedRangeMap.put(new Index("wrong_idx", UUID.randomUUID().toString()), List.of(shardRangeInfo));

        AtomicReference<AcknowledgedResponse> ackedResponseFromCallback = new AtomicReference<>(null);
        var eventIngestedRangeTask = new UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask(
            new UpdateEventIngestedRangeRequest(eventIngestedRangeMap),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    ackedResponseFromCallback.set(acknowledgedResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    fail("onFailure should not have been called but received: " + e);
                }
            }
        );

        clusterState2 = executeTasks(clusterState1, List.of(eventIngestedRangeTask));
        assertNotSame(clusterState1, clusterState2);
        IndexLongFieldRange eventIngestedRange = clusterState2.getMetadata().index(blogsIndex).getEventIngestedRange();

        assertEquals(1000L, eventIngestedRange.getMin());
        assertEquals(2000L, eventIngestedRange.getMax());
        assertNotNull("onResponse should have been called", ackedResponseFromCallback.get());
        assertTrue("AcknowledgedResponse should have true setting", ackedResponseFromCallback.get().isAcknowledged());
    }

    private ClusterState executeTasks(ClusterState state, List<UpdateEventIngestedRangeTransportAction.EventIngestedRangeTask> tasks)
        throws Exception {
        return ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(state, executor, tasks);
    }
}
