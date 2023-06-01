/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Builder;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.State;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_GATEWAY;
import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.TransportVersionUtils.getFirstVersion;
import static org.elasticsearch.test.TransportVersionUtils.getNextVersion;
import static org.elasticsearch.test.TransportVersionUtils.getPreviousVersion;
import static org.elasticsearch.test.TransportVersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentTasksCustomMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Custom> {

    @Override
    protected PersistentTasksCustomMetadata createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        for (int i = 0; i < numberOfTasks; i++) {
            String taskId = UUIDs.base64UUID();
            tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskState(taskId, new State(randomAlphaOfLength(10)));
            }
        }
        return tasks.build();
    }

    @Override
    protected Custom mutateInstance(Custom instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return PersistentTasksCustomMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new Entry(Metadata.Custom.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new),
                new Entry(NamedDiff.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::readDiffFrom),
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
            )
        );
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        Builder builder = PersistentTasksCustomMetadata.builder((PersistentTasksCustomMetadata) testInstance);
        switch (randomInt(3)) {
            case 0:
                addRandomTask(builder);
                break;
            case 1:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.reassignTask(pickRandomTask(builder), randomAssignment());
                }
                break;
            case 2:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.updateTaskState(pickRandomTask(builder), randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                }
                break;
            case 3:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.removeTask(pickRandomTask(builder));
                }
                break;
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return PersistentTasksCustomMetadata::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetadata doParseInstance(XContentParser parser) {
        return PersistentTasksCustomMetadata.fromXContent(parser);
    }

    private String addRandomTask(Builder builder) {
        String taskId = UUIDs.base64UUID();
        builder.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
        return taskId;
    }

    private String pickRandomTask(PersistentTasksCustomMetadata.Builder testInstance) {
        return randomFrom(new ArrayList<>(testInstance.getCurrentTaskIds()));
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

    @SuppressWarnings("unchecked")
    public void testSerializationContext() throws Exception {
        PersistentTasksCustomMetadata testInstance = createTestInstance();
        for (int i = 0; i < randomInt(10); i++) {
            testInstance = (PersistentTasksCustomMetadata) makeTestChanges(testInstance);
        }

        ToXContent.MapParams params = new ToXContent.MapParams(
            Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, randomFrom(CONTEXT_MODE_SNAPSHOT, CONTEXT_MODE_GATEWAY))
        );

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference shuffled = toShuffledXContent(asXContent(testInstance), xContentType, params, false);

        PersistentTasksCustomMetadata newInstance;
        try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
            newInstance = doParseInstance(parser);
        }
        assertNotSame(newInstance, testInstance);

        assertEquals(testInstance.tasks().size(), newInstance.tasks().size());
        for (PersistentTask<?> testTask : testInstance.tasks()) {
            PersistentTask<TestParams> newTask = (PersistentTask<TestParams>) newInstance.getTask(testTask.getId());
            assertNotNull(newTask);

            // Things that should be serialized
            assertEquals(testTask.getTaskName(), newTask.getTaskName());
            assertEquals(testTask.getId(), newTask.getId());
            assertEquals(testTask.getState(), newTask.getState());
            assertEquals(testTask.getParams(), newTask.getParams());

            // Things that shouldn't be serialized
            assertEquals(0, newTask.getAllocationId());
            assertNull(newTask.getExecutorNode());
        }
    }

    public void testBuilder() {
        PersistentTasksCustomMetadata persistentTasks = null;
        String lastKnownTask = "";
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final Builder builder;
            if (randomBoolean()) {
                builder = PersistentTasksCustomMetadata.builder();
            } else {
                builder = PersistentTasksCustomMetadata.builder(persistentTasks);
            }
            boolean changed = false;
            for (int j = 0; j < randomIntBetween(1, 10); j++) {
                switch (randomInt(3)) {
                    case 0:
                        lastKnownTask = addRandomTask(builder);
                        changed = true;
                        break;
                    case 1:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.reassignTask(lastKnownTask, randomAssignment());
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.reassignTask(fLastKnownTask, randomAssignment()));
                        }
                        break;
                    case 2:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.updateTaskState(lastKnownTask, randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.updateTaskState(fLastKnownTask, null));
                        }
                        break;
                    case 3:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.removeTask(lastKnownTask);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.removeTask(fLastKnownTask));
                        }
                        break;
                }
            }
            assertEquals(changed, builder.isChanged());
            persistentTasks = builder.build();
        }
    }

    public void testMinVersionSerialization() throws IOException {
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();

        TransportVersion minVersion = getFirstVersion();
        TransportVersion streamVersion = randomVersionBetween(random(), minVersion, getPreviousVersion(TransportVersion.CURRENT));
        tasks.addTask(
            "test_compatible_version",
            TestPersistentTasksExecutor.NAME,
            new TestParams(
                null,
                randomVersionBetween(random(), minVersion, streamVersion),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        tasks.addTask(
            "test_incompatible_version",
            TestPersistentTasksExecutor.NAME,
            new TestParams(
                null,
                randomVersionBetween(random(), getNextVersion(streamVersion), TransportVersion.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        final BytesStreamOutput out = new BytesStreamOutput();

        out.setTransportVersion(streamVersion);
        tasks.build().writeTo(out);

        final StreamInput input = out.bytes().streamInput();
        input.setTransportVersion(streamVersion);
        PersistentTasksCustomMetadata read = new PersistentTasksCustomMetadata(
            new NamedWriteableAwareStreamInput(input, getNamedWriteableRegistry())
        );

        assertThat(read.taskMap().keySet(), contains("test_compatible_version"));
    }

    public void testDisassociateDeadNodes_givenNoPersistentTasks() {
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests")).build();
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

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
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

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, not(sameInstance(returnedState)));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
        assertNotEquals(originalTasks, returnedTasks);

        assertEquals(originalTasks.getTask("assigned-task"), returnedTasks.getTask("assigned-task"));
        assertNotEquals(originalTasks.getTask("task-on-deceased-node"), returnedTasks.getTask("task-on-deceased-node"));
        assertEquals(PersistentTasksCustomMetadata.LOST_NODE_ASSIGNMENT, returnedTasks.getTask("task-on-deceased-node").getAssignment());
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
                return TransportVersion.CURRENT;
            }
        };
    }

    private Assignment randomAssignment() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return NO_NODE_FOUND;
            } else {
                return new Assignment(null, randomAlphaOfLength(10));
            }
        }
        return new Assignment(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
