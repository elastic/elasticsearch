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
import org.elasticsearch.test.TransportVersionUtils;
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
                new PersistentTasksCustomMetadata.Assignment(
                    "node1",
                    PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
                    "test assignment"
                )
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
                new PersistentTasksCustomMetadata.Assignment(
                    "node1",
                    PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
                    "test assignment"
                )
            )
            .addTask(
                "task-on-deceased-node",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment(
                    "left-the-cluster",
                    PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
                    "test assignment"
                )
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

    public void testAssignmentReasonFromExplanation() {
        // All well-known explanation strings
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.TASK_CREATED,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "waiting for initial assignment")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.NODE_LEFT,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "awaiting reassignment after node loss")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNMENT_DISABLED,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(
                null,
                "no persistent task assignments are allowed due to cluster settings"
            )
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.NO_NODE_FOUND,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "no appropriate nodes found for the assignment")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.AWAITING_LAZY_ASSIGNMENT,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "persistent task is awaiting node assignment.")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.AWAITING_UPGRADE,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(
                null,
                "persistent task cannot be assigned while upgrade mode is enabled."
            )
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.RESET_IN_PROGRESS,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(
                null,
                "persistent task will not be assigned as a feature reset is in progress."
            )
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.AWAITING_LAZY_ASSIGNMENT,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "datafeed awaiting job assignment.")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.AWAITING_LAZY_ASSIGNMENT,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "datafeed awaiting job relocation.")
        );

        // Unknown explanation defaults to UNEXPECTED_PRE_9_5
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.UNEXPECTED_PRE_9_5,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation(null, "some unknown reason")
        );

        // Non-null executor node always returns ASSIGNED regardless of explanation
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation("node1", "")
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
            PersistentTasksCustomMetadata.Assignment.Reason.fromExplanation("node1", "waiting for initial assignment")
        );
    }

    public void testAssignmentReasonFromOrdinal() {
        for (PersistentTasksCustomMetadata.Assignment.Reason reason : PersistentTasksCustomMetadata.Assignment.Reason.values()) {
            assertEquals(reason, PersistentTasksCustomMetadata.Assignment.Reason.fromOrdinal(reason.ordinal()));
        }
        // Out of bounds returns UNEXPECTED_PRE_9_5
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.UNEXPECTED_PRE_9_5,
            PersistentTasksCustomMetadata.Assignment.Reason.fromOrdinal(-1)
        );
        assertEquals(
            PersistentTasksCustomMetadata.Assignment.Reason.UNEXPECTED_PRE_9_5,
            PersistentTasksCustomMetadata.Assignment.Reason.fromOrdinal(PersistentTasksCustomMetadata.Assignment.Reason.values().length)
        );
    }

    public void testAssignmentReasonWireRoundTrip() throws Exception {
        for (PersistentTasksCustomMetadata.Assignment.Reason reason : PersistentTasksCustomMetadata.Assignment.Reason.values()) {
            String executorNode = reason == PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED ? randomAlphaOfLength(10) : null;
            PersistentTasksCustomMetadata.PersistentTask<TestPersistentTasksPlugin.TestParams> task =
                new PersistentTasksCustomMetadata.PersistentTask<>(
                    "task-id",
                    TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
                    new TestPersistentTasksPlugin.TestParams(randomAlphaOfLength(10)),
                    1L,
                    new PersistentTasksCustomMetadata.Assignment(executorNode, reason, randomAlphaOfLength(10))
                );
            PersistentTasksCustomMetadata.PersistentTask<?> copy = copyWriteable(
                task,
                getNamedWriteableRegistry(),
                PersistentTasksCustomMetadata.PersistentTask::new
            );
            assertEquals(reason, copy.getAssignment().getReason());
            assertEquals(executorNode, copy.getAssignment().getExecutorNode());
            assertEquals(task.getAssignment().getExplanation(), copy.getAssignment().getExplanation());
        }
    }

    public void testAssignmentReasonWireBackwardCompatibility() throws Exception {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("persistent_task_assignment_reason")
        );

        // Well-known explanation: fromExplanation should infer TASK_CREATED
        PersistentTasksCustomMetadata.PersistentTask<TestPersistentTasksPlugin.TestParams> task =
            new PersistentTasksCustomMetadata.PersistentTask<>(
                "task-id",
                TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
                new TestPersistentTasksPlugin.TestParams("test"),
                1L,
                new PersistentTasksCustomMetadata.Assignment(
                    null,
                    PersistentTasksCustomMetadata.Assignment.Reason.TASK_CREATED,
                    "waiting for initial assignment"
                )
            );
        PersistentTasksCustomMetadata.PersistentTask<?> copy = copyWriteable(
            task,
            getNamedWriteableRegistry(),
            PersistentTasksCustomMetadata.PersistentTask::new,
            oldVersion
        );
        assertEquals(PersistentTasksCustomMetadata.Assignment.Reason.TASK_CREATED, copy.getAssignment().getReason());

        // Assigned task: fromExplanation returns ASSIGNED when executorNode is non-null
        task = new PersistentTasksCustomMetadata.PersistentTask<>(
            "task-id",
            TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
            new TestPersistentTasksPlugin.TestParams("test"),
            1L,
            new PersistentTasksCustomMetadata.Assignment(
                "node1",
                PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED,
                "test assignment"
            )
        );
        copy = copyWriteable(task, getNamedWriteableRegistry(), PersistentTasksCustomMetadata.PersistentTask::new, oldVersion);
        assertEquals(PersistentTasksCustomMetadata.Assignment.Reason.ASSIGNED, copy.getAssignment().getReason());

        // Unknown explanation: fromExplanation returns UNEXPECTED_PRE_9_5
        task = new PersistentTasksCustomMetadata.PersistentTask<>(
            "task-id",
            TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
            new TestPersistentTasksPlugin.TestParams("test"),
            1L,
            new PersistentTasksCustomMetadata.Assignment(
                null,
                PersistentTasksCustomMetadata.Assignment.Reason.UNEXPECTED_PRE_9_5,
                "some dynamic reason"
            )
        );
        copy = copyWriteable(task, getNamedWriteableRegistry(), PersistentTasksCustomMetadata.PersistentTask::new, oldVersion);
        assertEquals(PersistentTasksCustomMetadata.Assignment.Reason.UNEXPECTED_PRE_9_5, copy.getAssignment().getReason());
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
