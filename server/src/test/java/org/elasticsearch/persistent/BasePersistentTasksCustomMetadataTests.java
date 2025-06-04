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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
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

public abstract class BasePersistentTasksCustomMetadataTests<T extends Metadata.MetadataCustom<T>> extends
    ChunkedToXContentDiffableSerializationTestCase<T> {

    @SuppressWarnings("unchecked")
    @Override
    protected T createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasks.Builder<?> tasks = builder();
        for (int i = 0; i < numberOfTasks; i++) {
            String taskId = UUIDs.base64UUID();
            tasks.addTask(
                taskId,
                TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
                new TestPersistentTasksPlugin.TestParams(randomAlphaOfLength(10)),
                randomAssignment()
            );
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskState(taskId, new TestPersistentTasksPlugin.State(randomAlphaOfLength(10)));
            }
        }
        return (T) tasks.build();
    }

    @Override
    protected T mutateInstance(T instance) throws IOException {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T makeTestChanges(T testInstance) {
        final PersistentTasks.Builder<?> builder = builder(testInstance);
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
                    builder.updateTaskState(
                        pickRandomTask(builder),
                        randomBoolean() ? new TestPersistentTasksPlugin.State(randomAlphaOfLength(10)) : null
                    );
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
        return (T) builder.build();
    }

    @SuppressWarnings("unchecked")
    public void testSerializationContext() throws Exception {
        T testInstance = createTestInstance();
        for (int i = 0; i < randomInt(10); i++) {
            testInstance = makeTestChanges(testInstance);
        }

        ToXContent.MapParams params = new ToXContent.MapParams(
            Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, randomFrom(CONTEXT_MODE_SNAPSHOT, CONTEXT_MODE_GATEWAY))
        );

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference shuffled = toShuffledXContent(asXContent(testInstance), xContentType, params, false);

        T newInstance;
        try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
            newInstance = doParseInstance(parser);
        }
        assertNotSame(newInstance, testInstance);

        assertEquals(asPersistentTasks(testInstance).tasks().size(), asPersistentTasks(newInstance).tasks().size());
        for (PersistentTasksCustomMetadata.PersistentTask<?> testTask : asPersistentTasks(testInstance).tasks()) {
            PersistentTasksCustomMetadata.PersistentTask<TestPersistentTasksPlugin.TestParams> newTask =
                (PersistentTasksCustomMetadata.PersistentTask<TestPersistentTasksPlugin.TestParams>) asPersistentTasks(newInstance).getTask(
                    testTask.getId()
                );
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

    @SuppressWarnings("unchecked")
    public void testBuilder() {
        T persistentTasks = null;
        String lastKnownTask = "";
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final PersistentTasks.Builder<?> builder;
            if (randomBoolean()) {
                builder = builder();
            } else {
                builder = builder(persistentTasks);
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
                            builder.updateTaskState(
                                lastKnownTask,
                                randomBoolean() ? new TestPersistentTasksPlugin.State(randomAlphaOfLength(10)) : null
                            );
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
            persistentTasks = (T) builder.build();
        }
    }

    @SuppressWarnings("unchecked")
    public void testMinVersionSerialization() throws IOException {
        PersistentTasks.Builder<?> tasks = builder();

        TransportVersion minVersion = getFirstVersion();
        TransportVersion streamVersion = randomVersionBetween(random(), minVersion, getPreviousVersion(TransportVersion.current()));
        tasks.addTask(
            "test_compatible_version",
            TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
            new TestPersistentTasksPlugin.TestParams(
                null,
                randomVersionBetween(random(), minVersion, streamVersion),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        tasks.addTask(
            "test_incompatible_version",
            TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
            new TestPersistentTasksPlugin.TestParams(
                null,
                randomVersionBetween(random(), getNextVersion(streamVersion), TransportVersion.current()),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        final BytesStreamOutput out = new BytesStreamOutput();

        out.setTransportVersion(streamVersion);
        ((T) tasks.build()).writeTo(out);

        final StreamInput input = out.bytes().streamInput();
        input.setTransportVersion(streamVersion);
        PersistentTasksCustomMetadata read = new PersistentTasksCustomMetadata(
            new NamedWriteableAwareStreamInput(input, getNamedWriteableRegistry())
        );

        assertThat(read.taskMap().keySet(), contains("test_compatible_version"));
    }

    private PersistentTasks asPersistentTasks(T testInstance) {
        return (PersistentTasks) testInstance;
    }

    protected String addRandomTask(PersistentTasks.Builder<?> builder) {
        String taskId = UUIDs.base64UUID();
        builder.addTask(
            taskId,
            TestPersistentTasksPlugin.TestPersistentTasksExecutor.NAME,
            new TestPersistentTasksPlugin.TestParams(randomAlphaOfLength(10)),
            randomAssignment()
        );
        return taskId;
    }

    protected String pickRandomTask(PersistentTasks.Builder<?> testInstance) {
        return randomFrom(new ArrayList<>(testInstance.getCurrentTaskIds()));
    }

    protected PersistentTasksCustomMetadata.Assignment randomAssignment() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return NO_NODE_FOUND;
            } else {
                return new PersistentTasksCustomMetadata.Assignment(null, randomAlphaOfLength(10));
            }
        }
        return new PersistentTasksCustomMetadata.Assignment(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    protected abstract PersistentTasks.Builder<?> builder();

    protected abstract PersistentTasks.Builder<?> builder(T testInstance);

}
