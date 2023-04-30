/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.TRANSFORM_ROLE;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class TransformNodesTests extends ESTestCase {

    public void testTransformNodes() {
        String transformIdFoo = "df-id-foo";
        String transformIdBar = "df-id-bar";
        String transformIdFailed = "df-id-failed";
        String transformIdBaz = "df-id-baz";
        String transformIdOther = "df-id-other";
        String transformIdStopped = "df-id-stopped";

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            transformIdFoo,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdFoo, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            transformIdBar,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdBar, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask("test-task1", "testTasks", new PersistentTaskParams() {
            @Override
            public String getWriteableName() {
                return "testTasks";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) {

            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                return null;
            }
        }, new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment"));
        tasksBuilder.addTask(
            transformIdFailed,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdFailed, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment(null, "awaiting reassignment after node loss")
        );
        tasksBuilder.addTask(
            transformIdBaz,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdBaz, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            transformIdOther,
            TransformField.TASK_NAME,
            new TransformTaskParams(transformIdOther, Version.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment")
        );

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();

        // don't ask for transformIdOther
        TransformNodeAssignments transformNodeAssignments = TransformNodes.transformTaskNodes(
            Arrays.asList(transformIdFoo, transformIdBar, transformIdFailed, transformIdBaz, transformIdStopped),
            cs
        );
        assertEquals(2, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-2"));
        assertFalse(transformNodeAssignments.getExecutorNodes().contains(null));
        assertFalse(transformNodeAssignments.getExecutorNodes().contains("node-3"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(3, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBar));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBaz));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        assertEquals(1, transformNodeAssignments.getStopped().size());
        assertTrue(transformNodeAssignments.getStopped().contains(transformIdStopped));

        transformNodeAssignments = TransformNodes.transformTaskNodes(Arrays.asList(transformIdFoo, transformIdFailed), cs);

        assertEquals(1, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(1, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        assertEquals(0, transformNodeAssignments.getStopped().size());

        // test simple matching
        transformNodeAssignments = TransformNodes.findPersistentTasks("df-id-f*", cs);
        assertEquals(1, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(1, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        assertEquals(0, transformNodeAssignments.getStopped().size());

        // test matching none
        transformNodeAssignments = TransformNodes.findPersistentTasks("df-id-z*", cs);
        assertEquals(0, transformNodeAssignments.getExecutorNodes().size());
        assertEquals(0, transformNodeAssignments.getWaitingForAssignment().size());
        assertEquals(0, transformNodeAssignments.getAssigned().size());
        assertEquals(0, transformNodeAssignments.getStopped().size());

        // test matching all
        transformNodeAssignments = TransformNodes.findPersistentTasks("df-id-*", cs);
        assertEquals(3, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-2"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-3"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(4, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBar));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBaz));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdOther));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        // stopped tasks are not reported when matching against _running_ tasks
        assertEquals(0, transformNodeAssignments.getStopped().size());

        // test matching all with _all
        transformNodeAssignments = TransformNodes.findPersistentTasks("_all", cs);
        assertEquals(3, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-2"));
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-3"));
        assertEquals(1, transformNodeAssignments.getWaitingForAssignment().size());
        assertTrue(transformNodeAssignments.getWaitingForAssignment().contains(transformIdFailed));
        assertEquals(4, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBar));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdBaz));
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdOther));
        assertFalse(transformNodeAssignments.getAssigned().contains(transformIdFailed));
        // stopped tasks are not reported when matching against _running_ tasks
        assertEquals(0, transformNodeAssignments.getStopped().size());

        // test matching exact
        transformNodeAssignments = TransformNodes.findPersistentTasks(transformIdFoo, cs);
        assertEquals(1, transformNodeAssignments.getExecutorNodes().size());
        assertTrue(transformNodeAssignments.getExecutorNodes().contains("node-1"));
        assertEquals(0, transformNodeAssignments.getWaitingForAssignment().size());
        assertEquals(1, transformNodeAssignments.getAssigned().size());
        assertTrue(transformNodeAssignments.getAssigned().contains(transformIdFoo));
        // stopped tasks are not reported when matching against _running_ tasks
        assertEquals(0, transformNodeAssignments.getStopped().size());
    }

    public void testTransformNodes_NoTasks() {
        ClusterState emptyState = ClusterState.builder(new ClusterName("_name")).build();
        TransformNodeAssignments transformNodeAssignments = TransformNodes.transformTaskNodes(
            Collections.singletonList("df-id"),
            emptyState
        );

        assertEquals(0, transformNodeAssignments.getExecutorNodes().size());
        assertEquals(1, transformNodeAssignments.getStopped().size());
        assertTrue(transformNodeAssignments.getStopped().contains("df-id"));

        transformNodeAssignments = TransformNodes.findPersistentTasks("df-*", emptyState);

        assertEquals(0, transformNodeAssignments.getExecutorNodes().size());
        assertEquals(0, transformNodeAssignments.getWaitingForAssignment().size());
        assertEquals(0, transformNodeAssignments.getAssigned().size());
        assertEquals(0, transformNodeAssignments.getStopped().size());
    }

    public void testSelectAnyNodeThatCanRunThisTransform() {
        DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, true), isEmpty());
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, false), isEmpty());

        nodes = DiscoveryNodes.builder()
            .add(newDiscoveryNode("node-2", Version.V_7_13_0, TRANSFORM_ROLE))
            .add(newDiscoveryNode("node-3", Version.V_7_13_0, REMOTE_CLUSTER_CLIENT_ROLE))
            .build();
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, true), isEmpty());
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, false).get().getId(), is(equalTo("node-2")));

        nodes = DiscoveryNodes.builder()
            .add(newDiscoveryNode("node-2", Version.V_7_13_0, TRANSFORM_ROLE))
            .add(newDiscoveryNode("node-3", Version.V_7_13_0, REMOTE_CLUSTER_CLIENT_ROLE))
            .add(newDiscoveryNode("node-4", Version.V_7_13_0, TRANSFORM_ROLE, REMOTE_CLUSTER_CLIENT_ROLE))
            .build();
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, true).get().getId(), is(equalTo("node-4")));
        assertThat(TransformNodes.selectAnyNodeThatCanRunThisTransform(nodes, false).get().getId(), is(oneOf("node-2", "node-4")));
    }

    public void testHasAnyTransformNode() {
        {
            DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
            assertThat(TransformNodes.hasAnyTransformNode(nodes), is(false));
            expectThrows(ElasticsearchStatusException.class, () -> TransformNodes.throwIfNoTransformNodes(newClusterState(nodes)));
        }
        {
            DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(newDiscoveryNode("node-1", Version.V_7_12_0))
                .add(newDiscoveryNode("node-2", Version.V_7_13_0))
                .add(newDiscoveryNode("node-3", Version.V_7_13_0))
                .build();
            assertThat(TransformNodes.hasAnyTransformNode(nodes), is(false));
            expectThrows(ElasticsearchStatusException.class, () -> TransformNodes.throwIfNoTransformNodes(newClusterState(nodes)));
        }
        {
            DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(newDiscoveryNode("node-1", Version.V_7_12_0))
                .add(newDiscoveryNode("node-2", Version.V_7_13_0, TRANSFORM_ROLE))
                .add(newDiscoveryNode("node-3", Version.V_7_13_0, REMOTE_CLUSTER_CLIENT_ROLE))
                .add(newDiscoveryNode("node-4", Version.V_7_13_0))
                .build();
            assertThat(TransformNodes.hasAnyTransformNode(nodes), is(true));
            TransformNodes.throwIfNoTransformNodes(newClusterState(nodes));
        }
    }

    public void testGetAssignment() {
        TransformTaskParams transformTaskParams1 = new TransformTaskParams(
            "transform-1",
            Version.CURRENT,
            TimeValue.timeValueSeconds(10),
            false
        );
        TransformTaskParams transformTaskParams2 = new TransformTaskParams(
            "transform-2",
            Version.CURRENT,
            TimeValue.timeValueSeconds(10),
            false
        );
        PersistentTasksCustomMetadata.Assignment assignment2 = new PersistentTasksCustomMetadata.Assignment(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        PersistentTasksCustomMetadata.TYPE,
                        PersistentTasksCustomMetadata.builder()
                            .addTask("transform-1", TransformTaskParams.NAME, transformTaskParams1, null)
                            .addTask("transform-2", TransformTaskParams.NAME, transformTaskParams2, assignment2)
                            .build()
                    )
            )
            .build();
        assertThat(TransformNodes.getAssignment("transform-1", clusterState), is(nullValue()));
        assertThat(TransformNodes.getAssignment("transform-2", clusterState), is(equalTo(assignment2)));
        assertThat(TransformNodes.getAssignment("transform-3", clusterState), is(equalTo(INITIAL_ASSIGNMENT)));
    }

    private static ClusterState newClusterState(DiscoveryNodes nodes) {
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).nodes(nodes).build();
    }

    private static DiscoveryNode newDiscoveryNode(String id, Version version, DiscoveryNodeRole... roles) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), emptyMap(), new HashSet<>(Arrays.asList(roles)), version);
    }
}
