/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Builder;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.Status;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xpack.persistent.TestPersistentTasksPlugin.TestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.MetaData.CONTEXT_MODE_GATEWAY;
import static org.elasticsearch.cluster.metadata.MetaData.CONTEXT_MODE_SNAPSHOT;
import static org.elasticsearch.xpack.persistent.PersistentTasksExecutor.NO_NODE_FOUND;

public class PersistentTasksCustomMetaDataTests extends AbstractDiffableSerializationTestCase<Custom> {

    @Override
    protected PersistentTasksCustomMetaData createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();
        for (int i = 0; i < numberOfTasks; i++) {
            boolean stopped = randomBoolean();
            tasks.addTask(TestPersistentTasksExecutor.NAME, new TestRequest(randomAsciiOfLength(10)),
                    stopped, randomBoolean(), stopped ? new Assignment(null, "stopped") : randomAssignment());
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskStatus(tasks.getCurrentId(), new Status(randomAsciiOfLength(10)));
            }
        }
        return tasks.build();
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return PersistentTasksCustomMetaData::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays.asList(
                new Entry(MetaData.Custom.class, PersistentTasksCustomMetaData.TYPE, PersistentTasksCustomMetaData::new),
                new Entry(NamedDiff.class, PersistentTasksCustomMetaData.TYPE, PersistentTasksCustomMetaData::readDiffFrom),
                new Entry(PersistentTaskRequest.class, TestPersistentTasksExecutor.NAME, TestRequest::new),
                new Entry(Task.Status.class, Status.NAME, Status::new)
        ));
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        PersistentTasksCustomMetaData tasksInProgress = (PersistentTasksCustomMetaData) testInstance;
        Builder builder = new Builder();
        switch (randomInt(3)) {
            case 0:
                addRandomTask(builder);
                break;
            case 1:
                if (tasksInProgress.tasks().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.reassignTask(pickRandomTask(tasksInProgress), randomAssignment());
                }
                break;
            case 2:
                if (tasksInProgress.tasks().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.updateTaskStatus(pickRandomTask(tasksInProgress), randomBoolean() ? new Status(randomAsciiOfLength(10)) : null);
                }
                break;
            case 3:
                if (tasksInProgress.tasks().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.removeTask(pickRandomTask(tasksInProgress));
                }
                break;
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return PersistentTasksCustomMetaData::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetaData doParseInstance(XContentParser parser) throws IOException {
        return PersistentTasksCustomMetaData.fromXContent(parser);
    }

    @Override
    protected XContentBuilder toXContent(Custom instance, XContentType contentType) throws IOException {
        return toXContent(instance, contentType, new ToXContent.MapParams(
                Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM, MetaData.XContentContext.API.toString())));
    }

    protected XContentBuilder toXContent(Custom instance, XContentType contentType, ToXContent.MapParams params) throws IOException {
        // We need all attribute to be serialized/de-serialized for testing
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        if (instance.isFragment()) {
            builder.startObject();
        }
        instance.toXContent(builder, params);
        if (instance.isFragment()) {
            builder.endObject();
        }
        return builder;
    }

    private Builder addRandomTask(Builder builder) {
        boolean stopped = randomBoolean();
        builder.addTask(TestPersistentTasksExecutor.NAME, new TestRequest(randomAsciiOfLength(10)), stopped, randomBoolean(),
                stopped ? new Assignment(null, "stopped") : randomAssignment());
        return builder;
    }

    private long pickRandomTask(PersistentTasksCustomMetaData testInstance) {
        return randomFrom(new ArrayList<>(testInstance.tasks())).getId();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskRequest.class, new ParseField(TestPersistentTasksExecutor.NAME),
                        TestRequest::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(Status.NAME), Status::fromXContent)
        ));
    }

    @SuppressWarnings("unchecked")
    public void testSerializationContext() throws Exception {
        PersistentTasksCustomMetaData testInstance = createTestInstance();
        for (int i = 0; i < randomInt(10); i++) {
            testInstance = (PersistentTasksCustomMetaData) makeTestChanges(testInstance);
        }

        ToXContent.MapParams params = new ToXContent.MapParams(
                Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM, randomFrom(CONTEXT_MODE_SNAPSHOT, CONTEXT_MODE_GATEWAY)));

        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = toXContent(testInstance, xContentType, params);
        XContentBuilder shuffled = shuffleXContent(builder);

        XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled.bytes());
        PersistentTasksCustomMetaData newInstance = doParseInstance(parser);
        assertNotSame(newInstance, testInstance);

        assertEquals(testInstance.tasks().size(), newInstance.tasks().size());
        for (PersistentTask<?> testTask : testInstance.tasks()) {
            PersistentTask<TestRequest> newTask = (PersistentTask<TestRequest>) newInstance.getTask(testTask.getId());
            assertNotNull(newTask);

            // Things that should be serialized
            assertEquals(testTask.getTaskName(), newTask.getTaskName());
            assertEquals(testTask.getId(), newTask.getId());
            assertEquals(testTask.getStatus(), newTask.getStatus());
            assertEquals(testTask.getRequest(), newTask.getRequest());
            assertEquals(testTask.isStopped(), newTask.isStopped());

            // Things that shouldn't be serialized
            assertEquals(0, newTask.getAllocationId());
            assertNull(newTask.getExecutorNode());
        }
    }

    public void testBuilder() {
        PersistentTasksCustomMetaData persistentTasks = null;
        long lastKnownTask = -1;
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final Builder builder;
            if (randomBoolean()) {
                builder = new Builder();
            } else {
                builder = new Builder(persistentTasks);
            }
            boolean changed = false;
            for (int j = 0; j < randomIntBetween(1, 10); j++) {
                switch (randomInt(5)) {
                    case 0:
                        lastKnownTask = addRandomTask(builder).getCurrentId();
                        changed = true;
                        break;
                    case 1:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                        }
                        if (randomBoolean()) {
                            builder.reassignTask(lastKnownTask, randomAssignment());
                        } else {
                            builder.reassignTask(lastKnownTask, (s, request) -> randomAssignment());
                        }
                        break;
                    case 2:
                        if (builder.hasTask(lastKnownTask)) {
                            PersistentTask<?> task = builder.build().getTask(lastKnownTask);
                            if (randomBoolean()) {
                                // Trying to reassign to the same node
                                builder.assignTask(lastKnownTask, (s, request) -> task.getAssignment());
                                // should change if the task was stopped AND unassigned
                                if (task.getExecutorNode() == null && task.isStopped()) {
                                    changed = true;
                                }
                            } else {
                                // Trying to reassign to a different node
                                Assignment randomAssignment = randomAssignment();
                                builder.assignTask(lastKnownTask, (s, request) -> randomAssignment);
                                // should change if the task was unassigned and was reassigned to a different node or started
                                if ((task.isAssigned() == false && randomAssignment.isAssigned()) || task.isStopped()) {
                                    changed = true;
                                }
                            }
                        } else {
                            // task doesn't exist - shouldn't change
                            builder.assignTask(lastKnownTask, (s, request) -> randomAssignment());
                        }
                        break;
                    case 3:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                        }
                        builder.updateTaskStatus(lastKnownTask, randomBoolean() ? new Status(randomAsciiOfLength(10)) : null);
                        break;
                    case 4:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                        }
                        builder.removeTask(lastKnownTask);
                        break;
                    case 5:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                        }
                        builder.finishTask(lastKnownTask);
                        break;
                }
            }
            assertEquals(changed, builder.isChanged());
            persistentTasks = builder.build();
        }

    }

    private Assignment randomAssignment() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return NO_NODE_FOUND;
            } else {
                return new Assignment(null, randomAsciiOfLength(10));
            }
        }
        return new Assignment(randomAsciiOfLength(10), randomAsciiOfLength(10));
    }
}