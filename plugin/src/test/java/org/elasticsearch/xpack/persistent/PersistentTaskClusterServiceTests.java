/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.TestPersistentActionPlugin.TestRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PersistentTaskClusterServiceTests extends ESTestCase {

    public void testReassignmentRequired() {
        int numberOfIterations = randomIntBetween(10, 100);
        ClusterState clusterState = initialState();
        for (int i = 0; i < numberOfIterations; i++) {
            boolean significant = randomBoolean();
            ClusterState previousState = clusterState;
            logger.info("inter {} significant: {}", i, significant);
            if (significant) {
                clusterState = significantChange(clusterState);
            } else {
                clusterState = insignificantChange(clusterState);
            }
            ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, previousState);
            assertThat(dumpEvent(event), significant, equalTo(PersistentTaskClusterService.reassignmentRequired(event,
                    new PersistentTaskClusterService.ExecutorNodeDecider() {
                        @Override
                        public <Request extends PersistentActionRequest> String executorNode(
                                String action, ClusterState currentState, Request request) {
                            return randomNode(currentState.nodes());
                        }
                    })));
        }
    }

    public void testReassignTasksWithNoTasks() {
        ClusterState clusterState = initialState();
        assertThat(reassign(clusterState).metaData().custom(PersistentTasksInProgress.TYPE), nullValue());
    }

    public void testReassignConsidersClusterStateUpdates() {
        ClusterState clusterState = initialState();
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksInProgress.Builder tasks = PersistentTasksInProgress.builder(
                clusterState.metaData().custom(PersistentTasksInProgress.TYPE));
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        addTestNodes(nodes, randomIntBetween(1, 10));
        int numberOfTasks = randomIntBetween(2, 40);
        for (int i = 0; i < numberOfTasks; i++) {
            addTask(tasks, "should_assign", "assign_one", randomBoolean() ? null : "no_longer_exits", false);
        }

        MetaData.Builder metaData = MetaData.builder(clusterState.metaData()).putCustom(PersistentTasksInProgress.TYPE, tasks.build());
        clusterState = builder.metaData(metaData).nodes(nodes).build();
        ClusterState newClusterState = reassign(clusterState);

        PersistentTasksInProgress tasksInProgress = newClusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        assertThat(tasksInProgress, notNullValue());

    }

    public void testReassignTasks() {
        ClusterState clusterState = initialState();
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksInProgress.Builder tasks = PersistentTasksInProgress.builder(
                clusterState.metaData().custom(PersistentTasksInProgress.TYPE));
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        addTestNodes(nodes, randomIntBetween(1, 10));
        int numberOfTasks = randomIntBetween(0, 40);
        for (int i = 0; i < numberOfTasks; i++) {
            switch (randomInt(3)) {
                case 0:
                    // add an unassigned task that should get assigned because it's assigned to a non-existing node or unassigned
                    addTask(tasks, "should_assign", "assign_me", randomBoolean() ? null : "no_longer_exits", false);
                    break;
                case 1:
                    // add a task assigned to non-existing node that should not get assigned
                    addTask(tasks, "should_not_assign", "dont_assign_me", randomBoolean() ? null : "no_longer_exits", false);
                    break;
                case 2:
                    // add a stopped task assigned to non-existing node that should not get assigned
                    addTask(tasks, "should_not_assign", "fail_me_if_called", null, true);
                    break;
                case 3:
                    addTask(tasks, "assign_one", "assign_one", randomBoolean() ? null : "no_longer_exits", false);
                    break;

            }
        }
        MetaData.Builder metaData = MetaData.builder(clusterState.metaData()).putCustom(PersistentTasksInProgress.TYPE, tasks.build());
        clusterState = builder.metaData(metaData).nodes(nodes).build();
        ClusterState newClusterState = reassign(clusterState);

        PersistentTasksInProgress tasksInProgress = newClusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        assertThat(tasksInProgress, notNullValue());

        assertThat("number of tasks shouldn't change as a result or reassignment",
                numberOfTasks, equalTo(tasksInProgress.tasks().size()));

        int assignOneCount = 0;

        for (PersistentTaskInProgress<?> task : tasksInProgress.tasks()) {
            if (task.isStopped()) {
                assertThat("stopped tasks should be never assigned", task.getExecutorNode(), nullValue());
            } else {
                switch (task.getAction()) {
                    case "should_assign":
                        assertThat(task.getExecutorNode(), notNullValue());
                        if (clusterState.nodes().nodeExists(task.getExecutorNode()) == false) {
                            logger.info(clusterState.metaData().custom(PersistentTasksInProgress.TYPE).toString());
                        }
                        assertThat("task should be assigned to a node that is in the cluster, was assigned to " + task.getExecutorNode(),
                                clusterState.nodes().nodeExists(task.getExecutorNode()), equalTo(true));
                        break;
                    case "should_not_assign":
                        assertThat(task.getExecutorNode(), nullValue());
                        break;
                    case "assign_one":
                        if (task.getExecutorNode() != null) {
                            assignOneCount++;
                            assertThat("more than one assign_one tasks are assigned", assignOneCount, lessThanOrEqualTo(1));
                        }
                        break;
                    default:
                        fail("Unknown action " + task.getAction());
                }
            }
        }
    }


    private void addTestNodes(DiscoveryNodes.Builder nodes, int nonLocalNodesCount) {
        for (int i = 0; i < nonLocalNodesCount; i++) {
            nodes.add(new DiscoveryNode("other_node_" + i, buildNewFakeTransportAddress(), Version.CURRENT));
        }
    }

    private ClusterState reassign(ClusterState clusterState) {
        return PersistentTaskClusterService.reassignTasks(clusterState, logger,
                new PersistentTaskClusterService.ExecutorNodeDecider() {
                    @Override
                    public <Request extends PersistentActionRequest> String executorNode(
                            String action, ClusterState currentState, Request request) {
                        TestRequest testRequest = (TestRequest) request;
                        switch (testRequest.getTestParam()) {
                            case "assign_me":
                                return randomNode(currentState.nodes());
                            case "dont_assign_me":
                                return null;
                            case "fail_me_if_called":
                                fail("the decision decider shouldn't be called on this task");
                                return null;
                            case "assign_one":
                                return assignOnlyOneTaskAtATime(currentState);
                            default:
                                fail("unknown param " + testRequest.getTestParam());
                        }
                        return null;
                    }
                });

    }

    private String assignOnlyOneTaskAtATime(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.nodes();
        PersistentTasksInProgress tasksInProgress = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        if (tasksInProgress.findTasks("assign_one",
                task -> task.isStopped() == false && nodes.nodeExists(task.getExecutorNode())).isEmpty()) {
            return randomNode(clusterState.nodes());
        } else {
            return null;
        }
    }

    private String randomNode(DiscoveryNodes nodes) {
        if (nodes.getNodes().isEmpty()) {
            return null;
        }
        List<String> nodeList = new ArrayList<>();
        for (ObjectCursor<String> node : nodes.getNodes().keys()) {
            nodeList.add(node.value);
        }
        return randomFrom(nodeList);

    }

    private String dumpEvent(ClusterChangedEvent event) {
        return "nodes_changed: " + event.nodesChanged() +
                " nodes_removed:" + event.nodesRemoved() +
                " routing_table_changed:" + event.routingTableChanged() +
                " tasks: " + event.state().metaData().custom(PersistentTasksInProgress.TYPE);
    }

    private ClusterState significantChange(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        if (tasks != null) {
            if (randomBoolean()) {
                //
                boolean removedNode = false;
                for (PersistentTaskInProgress<?> task : tasks.tasks()) {
                    if (task.getExecutorNode() != null && clusterState.nodes().nodeExists(task.getExecutorNode())) {
                        logger.info("removed node {}", task.getExecutorNode());
                        builder.nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(task.getExecutorNode()));
                        return builder.build();
                    }
                }
            }
        }
        boolean tasksOrNodesChanged = false;
        // add a new unassigned task
        if (hasUnassigned(tasks, clusterState.nodes()) == false) {
            // we don't have any unassigned tasks - add some
            logger.info("added random task");
            addRandomTask(builder, MetaData.builder(clusterState.metaData()), PersistentTasksInProgress.builder(tasks), null, false);
            tasksOrNodesChanged = true;
        }
        // add a node if there are unassigned tasks
        if (clusterState.nodes().getNodes().isEmpty()) {
            logger.info("added random node");
            builder.nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(randomAsciiOfLength(10))));
            tasksOrNodesChanged = true;
        }

        if (tasksOrNodesChanged == false) {
            // change routing table to simulate a change
            logger.info("changed routing table");
            MetaData.Builder metaData = MetaData.builder(clusterState.metaData());
            RoutingTable.Builder routingTable = RoutingTable.builder(clusterState.routingTable());
            changeRoutingTable(metaData, routingTable);
            builder.metaData(metaData).routingTable(routingTable.build());
        }
        return builder.build();
    }

    private ClusterState insignificantChange(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksInProgress tasks = clusterState.getMetaData().custom(PersistentTasksInProgress.TYPE);
        if (randomBoolean()) {
            if (hasUnassigned(tasks, clusterState.nodes()) == false) {
                // we don't have any unassigned tasks - adding a node or changing a routing table shouldn't affect anything
                if (randomBoolean()) {
                    logger.info("added random node");
                    builder.nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(randomAsciiOfLength(10))));
                }
                if (randomBoolean()) {
                    // add unassigned task in stopped state
                    logger.info("added random stopped task");
                    addRandomTask(builder, MetaData.builder(clusterState.metaData()), PersistentTasksInProgress.builder(tasks), null, true);
                    return builder.build();
                } else {
                    logger.info("changed routing table");
                    MetaData.Builder metaData = MetaData.builder(clusterState.metaData());
                    RoutingTable.Builder routingTable = RoutingTable.builder(clusterState.routingTable());
                    changeRoutingTable(metaData, routingTable);
                    builder.metaData(metaData).routingTable(routingTable.build());
                }
                return builder.build();
            }
        }
        if (randomBoolean()) {
            // remove a node that doesn't have any tasks assigned to it and it's not the master node
            for (DiscoveryNode node : clusterState.nodes()) {
                if (hasTasksAssignedTo(tasks, node.getId()) == false && "this_node".equals(node.getId()) == false) {
                    logger.info("removed unassigned node {}", node.getId());
                    return builder.nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(node.getId())).build();
                }
            }
        }

        if (randomBoolean()) {
            // clear the task
            if (randomBoolean()) {
                logger.info("removed all tasks");
                MetaData.Builder metaData = MetaData.builder(clusterState.metaData()).putCustom(PersistentTasksInProgress.TYPE,
                        PersistentTasksInProgress.builder().build());
                return builder.metaData(metaData).build();
            } else {
                logger.info("set task custom to null");
                MetaData.Builder metaData = MetaData.builder(clusterState.metaData()).removeCustom(PersistentTasksInProgress.TYPE);
                return builder.metaData(metaData).build();
            }
        }
        logger.info("removed all unassigned tasks and changed routing table");
        PersistentTasksInProgress.Builder tasksBuilder = PersistentTasksInProgress.builder(tasks);
        if (tasks != null) {
            for (PersistentTaskInProgress<?> task : tasks.tasks()) {
                if (task.getExecutorNode() == null) {
                    tasksBuilder.removeTask(task.getId());
                }
            }
        }
        // Just add a random index - that shouldn't change anything
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAsciiOfLength(10))
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        MetaData.Builder metaData = MetaData.builder(clusterState.metaData()).put(indexMetaData, false)
                .putCustom(PersistentTasksInProgress.TYPE, tasksBuilder.build());
        return builder.metaData(metaData).build();
    }

    private boolean hasUnassigned(PersistentTasksInProgress tasks, DiscoveryNodes discoveryNodes) {
        if (tasks == null || tasks.tasks().isEmpty()) {
            return false;
        }
        return tasks.tasks().stream().anyMatch(task ->
                task.isStopped() == false &&
                        (task.getExecutorNode() == null || discoveryNodes.nodeExists(task.getExecutorNode())));
    }

    private boolean hasTasksAssignedTo(PersistentTasksInProgress tasks, String nodeId) {
        return tasks != null && tasks.tasks().stream().anyMatch(
                task -> nodeId.equals(task.getExecutorNode())) == false;
    }

    private ClusterState.Builder addRandomTask(ClusterState.Builder clusterStateBuilder,
                                               MetaData.Builder metaData, PersistentTasksInProgress.Builder tasks,
                                               String node,
                                               boolean stopped) {
        return clusterStateBuilder.metaData(metaData.putCustom(PersistentTasksInProgress.TYPE,
                tasks.addTask(randomAsciiOfLength(10), new TestRequest(randomAsciiOfLength(10)), stopped, randomBoolean(), node).build()));
    }

    private void addTask(PersistentTasksInProgress.Builder tasks, String action, String param, String node, boolean stopped) {
        tasks.addTask(action, new TestRequest(param), stopped, randomBoolean(), node);
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(),
                Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))),
                Version.CURRENT);
    }


    private ClusterState initialState() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        int randomIndices = randomIntBetween(0, 5);
        for (int i = 0; i < randomIndices; i++) {
            changeRoutingTable(metaData, routingTable);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), "this_node"));
        nodes.localNodeId("this_node");
        nodes.masterNodeId("this_node");

        return ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .routingTable(routingTable.build())
                .build();
    }

    private void changeRoutingTable(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAsciiOfLength(10))
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        metaData.put(indexMetaData, false);
        routingTable.addAsNew(indexMetaData);
    }
}
