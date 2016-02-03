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
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for task management API
 * <p>
 * We need at least 2 nodes so we have a master node a non-master node
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class TasksIT extends ESIntegTestCase {

    private Map<Tuple<String, String>, RecordingTaskManagerListener> listeners = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.getKey(), true)
            .build();
    }

    public void testTaskCounts() {
        // Run only on data nodes
        ListTasksResponse response = client().admin().cluster().prepareListTasks("data:true").setActions(ListTasksAction.NAME + "[n]").get();
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(cluster().numDataNodes()));
    }

    public void testMasterNodeOperationTasks() {
        registerTaskManageListeners(ClusterHealthAction.NAME);

        // First run the health on the master node - should produce only one task on the master node
        internalCluster().masterClient().admin().cluster().prepareHealth().get();
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, event -> event.v1() == false)); // counting only unregistration events

        resetTaskManageListeners(ClusterHealthAction.NAME);

        // Now run the health on a non-master node - should produce one task on master and one task on another node
        internalCluster().nonMasterClient().admin().cluster().prepareHealth().get();
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, event -> event.v1() == false)); // counting only unregistration events
        List<TaskInfo> tasks = findEvents(ClusterHealthAction.NAME, Tuple::v1);

        // Verify that one of these tasks is a parent of another task
        if (tasks.get(0).getParentNode() == null) {
            assertParentTask(Collections.singletonList(tasks.get(1)), tasks.get(0));
        } else {
            assertParentTask(Collections.singletonList(tasks.get(0)), tasks.get(1));
        }
    }

    public void testTransportReplicationAllShardsTasks() {
        registerTaskManageListeners(PercolateAction.NAME);  // main task
        registerTaskManageListeners(PercolateAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().preparePercolate().setIndices("test").setDocumentType("foo").setSource("{}").get();

        // the percolate operation should produce one main task
        NumShards numberOfShards = getNumShards("test");
        assertEquals(1, numberOfEvents(PercolateAction.NAME, Tuple::v1));
        // and then one operation per shard
        assertEquals(numberOfShards.totalNumShards, numberOfEvents(PercolateAction.NAME + "[s]", Tuple::v1));

        // the shard level tasks should have the main task as a parent
        assertParentTask(findEvents(PercolateAction.NAME + "[s]", Tuple::v1), findEvents(PercolateAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportBroadcastByNodeTasks() {
        registerTaskManageListeners(UpgradeAction.NAME);  // main task
        registerTaskManageListeners(UpgradeAction.NAME + "[n]"); // node level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareUpgrade("test").get();

        // the percolate operation should produce one main task
        assertEquals(1, numberOfEvents(UpgradeAction.NAME, Tuple::v1));
        // and then one operation per each node where shards are located
        assertEquals(internalCluster().nodesInclude("test").size(), numberOfEvents(UpgradeAction.NAME + "[n]", Tuple::v1));

        // all node level tasks should have the main task as a parent
        assertParentTask(findEvents(UpgradeAction.NAME + "[n]", Tuple::v1), findEvents(UpgradeAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportReplicationSingleShardTasks() {
        registerTaskManageListeners(ValidateQueryAction.NAME);  // main task
        registerTaskManageListeners(ValidateQueryAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareValidateQuery("test").get();

        // the validate operation should produce one main task
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, Tuple::v1));
        // and then one operation
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1));
        // the shard level operation should have the main task as its parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1), findEvents(ValidateQueryAction.NAME, Tuple::v1).get(0));
    }


    public void testTransportBroadcastReplicationTasks() {
        registerTaskManageListeners(RefreshAction.NAME);  // main task
        registerTaskManageListeners(RefreshAction.NAME + "[s]"); // shard level tasks
        registerTaskManageListeners(RefreshAction.NAME + "[s][*]"); // primary and replica shard tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareRefresh("test").get();

        // the refresh operation should produce one main task
        NumShards numberOfShards = getNumShards("test");

        logger.debug("number of shards, total: [{}], primaries: [{}] ", numberOfShards.totalNumShards, numberOfShards.numPrimaries);
        logger.debug("main events {}", numberOfEvents(RefreshAction.NAME, Tuple::v1));
        logger.debug("main event node {}", findEvents(RefreshAction.NAME, Tuple::v1).get(0).getNode().name());
        logger.debug("[s] events {}", numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1));
        logger.debug("[s][*] events {}", numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));
        logger.debug("nodes with the index {}", internalCluster().nodesInclude("test"));

        assertEquals(1, numberOfEvents(RefreshAction.NAME, Tuple::v1));
        // Because it's broadcast replication action we will have as many [s] level requests
        // as we have primary shards on the coordinating node plus we will have one task per primary outside of the
        // coordinating node due to replication.
        // If all primaries are on the coordinating node, the number of tasks should be equal to the number of primaries
        // If all primaries are not on the coordinating node, the number of tasks should be equal to the number of primaries times 2
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), greaterThanOrEqualTo(numberOfShards.numPrimaries));
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), lessThanOrEqualTo(numberOfShards.numPrimaries * 2));

        // Verify that all [s] events have the proper parent
        // This is complicated because if the shard task runs on the same node it has main task as a parent
        // but if it runs on non-coordinating node it would have another intermediate [s] task on the coordinating node as a parent
        TaskInfo mainTask = findEvents(RefreshAction.NAME, Tuple::v1).get(0);
        List<TaskInfo> sTasks = findEvents(RefreshAction.NAME + "[s]", Tuple::v1);
        for (TaskInfo taskInfo : sTasks) {
            if (mainTask.getNode().equals(taskInfo.getNode())) {
                // This shard level task runs on the same node as a parent task - it should have the main task as a direct parent
                assertParentTask(Collections.singletonList(taskInfo), mainTask);
            } else {
                String description = taskInfo.getDescription();
                // This shard level task runs on another node - it should have a corresponding shard level task on the node where main task is running
                List<TaskInfo> sTasksOnRequestingNode = findEvents(RefreshAction.NAME + "[s]",
                    event -> event.v1() && mainTask.getNode().equals(event.v2().getNode()) && description.equals(event.v2().getDescription()));
                // There should be only one parent task
                assertEquals(1, sTasksOnRequestingNode.size());
                assertParentTask(Collections.singletonList(taskInfo), sTasksOnRequestingNode.get(0));
            }
        }

        // we will have as many [s][p] and [s][r] tasks as we have primary and replica shards
        assertEquals(numberOfShards.totalNumShards, numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));

        // we the [s][p] and [s][r] tasks should have a corresponding [s] task on the same node as a parent
        List<TaskInfo> spEvents = findEvents(RefreshAction.NAME + "[s][*]", Tuple::v1);
        for (TaskInfo taskInfo : spEvents) {
            List<TaskInfo> sTask;
            if (taskInfo.getAction().endsWith("[s][p]")) {
                // A [s][p] level task should have a corresponding [s] level task on the same node
                sTask = findEvents(RefreshAction.NAME + "[s]",
                    event -> event.v1() && taskInfo.getNode().equals(event.v2().getNode()) && taskInfo.getDescription().equals(event.v2().getDescription()));
            } else {
                // A [s][r] level task should have a corresponding [s] level task on the a different node (where primary is located)
                sTask = findEvents(RefreshAction.NAME + "[s]",
                    event -> event.v1() && taskInfo.getParentNode().equals(event.v2().getNode().getId()) && taskInfo.getDescription().equals(event.v2().getDescription()));
            }
            // There should be only one parent task
            assertEquals(1, sTask.size());
            assertParentTask(Collections.singletonList(taskInfo), sTask.get(0));
        }
    }

    @Override
    public void tearDown() throws Exception {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            ((MockTaskManager)internalCluster().getInstance(ClusterService.class, entry.getKey().v1()).getTaskManager()).removeListener(entry.getValue());
        }
        listeners.clear();
        super.tearDown();
    }

    /**
     * Registers recording task event listeners with the given action mask on all nodes
     */
    private void registerTaskManageListeners(String actionMasks) {
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            DiscoveryNode node = clusterService.localNode();
            RecordingTaskManagerListener listener = new RecordingTaskManagerListener(node, Strings.splitStringToArray(actionMasks, ','));
            ((MockTaskManager)clusterService.getTaskManager()).addListener(listener);
            RecordingTaskManagerListener oldListener = listeners.put(new Tuple<>(node.name(), actionMasks), listener);
            assertNull(oldListener);
        }
    }

    /**
     * Resets all recording task event listeners with the given action mask on all nodes
     */
    private void resetTaskManageListeners(String actionMasks) {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                entry.getValue().reset();
            }
        }
    }

    /**
     * Returns the number of events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return number of events that satisfy the criteria
     */
    private int numberOfEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        return findEvents(actionMasks, criteria).size();
    }

    /**
     * Returns all events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return number of events that satisfy the criteria
     */
    private List<TaskInfo> findEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        List<TaskInfo> events = new ArrayList<>();
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                for (Tuple<Boolean, TaskInfo> taskEvent : entry.getValue().getEvents()) {
                    if (criteria.apply(taskEvent)) {
                        events.add(taskEvent.v2());
                    }
                }
            }
        }
        return events;
    }

    /**
     * Asserts that all tasks in the tasks list have the same parentTask
     */
    private void assertParentTask(List<TaskInfo> tasks, TaskInfo parentTask) {
        for (TaskInfo task : tasks) {
            assertNotNull(task.getParentNode());
            assertEquals(parentTask.getNode().getId(), task.getParentNode());
            assertEquals(parentTask.getId(), task.getParentId());
        }
    }
}
