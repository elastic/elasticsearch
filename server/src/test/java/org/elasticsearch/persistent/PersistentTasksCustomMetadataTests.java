/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.ProjectCustom;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.State;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentTasksCustomMetadataTests extends BasePersistentTasksCustomMetadataTests<ProjectCustom> {

    @Override
    protected Writeable.Reader<ProjectCustom> instanceReader() {
        return PersistentTasksCustomMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new Entry(Metadata.ProjectCustom.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new),
                new Entry(NamedDiff.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::readDiffFrom),
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
            )
        );
    }

    @Override
    protected Writeable.Reader<Diff<ProjectCustom>> diffReader() {
        return PersistentTasksCustomMetadata::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetadata doParseInstance(XContentParser parser) {
        return PersistentTasksCustomMetadata.fromXContent(parser);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    TestParams::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskState.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    State::fromXContent
                )
            )
        );
    }

    @Override
    protected PersistentTasks.Builder<?> builder() {
        return PersistentTasksCustomMetadata.builder();
    }

    @Override
    protected PersistentTasks.Builder<?> builder(ProjectCustom testInstance) {
        return PersistentTasksCustomMetadata.builder((PersistentTasksCustomMetadata) testInstance);
    }

    public void testDisassociateDeadNodes_givenNoPersistentTasks() {
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests")).build();
        if (randomBoolean()) {
            originalState = ClusterState.builder(originalState)
                .metadata(Metadata.builder(originalState.metadata()).put(ProjectMetadata.builder(randomProjectIdOrDefault())))
                .build();
        }
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));
    }

    public void testDisassociateDeadNodes_givenAssignedPersistentTask() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .localNodeId("node1")
            .masterNodeId("node1")
            .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "task-id",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("node1", "test assignment")
            );

        final ProjectId projectId = randomProjectIdOrDefault();
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(
                Metadata.builder(Metadata.EMPTY_METADATA)
                    .put(ProjectMetadata.builder(projectId).putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            )
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.get(originalState.metadata().getProject(projectId));
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.get(returnedState.metadata().getProject(projectId));
        assertEquals(originalTasks, returnedTasks);
    }

    public void testDisassociateDeadNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .localNodeId("node1")
            .masterNodeId("node1")
            .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "assigned-task",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("node1", "test assignment")
            )
            .addTask(
                "task-on-deceased-node",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("left-the-cluster", "test assignment")
            );

        final ProjectId projectId = randomProjectIdOrDefault();
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(
                Metadata.builder(Metadata.EMPTY_METADATA)
                    .put(ProjectMetadata.builder(projectId).putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            )
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, not(sameInstance(returnedState)));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.get(originalState.metadata().getProject(projectId));
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.get(returnedState.metadata().getProject(projectId));
        assertNotEquals(originalTasks, returnedTasks);

        assertEquals(originalTasks.getTask("assigned-task"), returnedTasks.getTask("assigned-task"));
        assertNotEquals(originalTasks.getTask("task-on-deceased-node"), returnedTasks.getTask("task-on-deceased-node"));
        assertEquals(PersistentTasks.LOST_NODE_ASSIGNMENT, returnedTasks.getTask("task-on-deceased-node").getAssignment());
    }

    private PersistentTaskParams emptyTaskParams(String taskName) {
        return new PersistentTaskParams() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) {

            }

            @Override
            public String getWriteableName() {
                return taskName;
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.current();
            }
        };
    }
}
