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
package org.elasticsearch.persistent;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Builder;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.Status;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.MetaData.CONTEXT_MODE_GATEWAY;
import static org.elasticsearch.cluster.metadata.MetaData.CONTEXT_MODE_SNAPSHOT;
import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.VersionUtils.allReleasedVersions;
import static org.elasticsearch.test.VersionUtils.compatibleFutureVersion;
import static org.elasticsearch.test.VersionUtils.getFirstVersion;
import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;

public class PersistentTasksCustomMetaDataTests extends AbstractDiffableSerializationTestCase<Custom> {

    @Override
    protected PersistentTasksCustomMetaData createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();
        for (int i = 0; i < numberOfTasks; i++) {
            String taskId = UUIDs.base64UUID();
            tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)),
                    randomAssignment());
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskStatus(taskId, new Status(randomAlphaOfLength(10)));
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
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(Task.Status.class, TestPersistentTasksExecutor.NAME, Status::new)
        ));
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        Builder builder = PersistentTasksCustomMetaData.builder((PersistentTasksCustomMetaData) testInstance);
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
                    builder.updateTaskStatus(pickRandomTask(builder), randomBoolean() ? new Status(randomAlphaOfLength(10)) : null);
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
        return PersistentTasksCustomMetaData::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetaData doParseInstance(XContentParser parser) {
        return PersistentTasksCustomMetaData.fromXContent(parser);
    }

    private String addRandomTask(Builder builder) {
        String taskId = UUIDs.base64UUID();
        builder.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
        return taskId;
    }

    private String pickRandomTask(PersistentTasksCustomMetaData.Builder testInstance) {
        return randomFrom(new ArrayList<>(testInstance.getCurrentTaskIds()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TestPersistentTasksExecutor.NAME),
                        TestParams::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(TestPersistentTasksExecutor.NAME), Status::fromXContent)
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
        BytesReference shuffled = toShuffledXContent(testInstance, xContentType, params, false);

        XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled);
        PersistentTasksCustomMetaData newInstance = doParseInstance(parser);
        assertNotSame(newInstance, testInstance);

        assertEquals(testInstance.tasks().size(), newInstance.tasks().size());
        for (PersistentTask<?> testTask : testInstance.tasks()) {
            PersistentTask<TestParams> newTask = (PersistentTask<TestParams>) newInstance.getTask(testTask.getId());
            assertNotNull(newTask);

            // Things that should be serialized
            assertEquals(testTask.getTaskName(), newTask.getTaskName());
            assertEquals(testTask.getId(), newTask.getId());
            assertEquals(testTask.getStatus(), newTask.getStatus());
            assertEquals(testTask.getParams(), newTask.getParams());

            // Things that shouldn't be serialized
            assertEquals(0, newTask.getAllocationId());
            assertNull(newTask.getExecutorNode());
        }
    }

    public void testBuilder() {
        PersistentTasksCustomMetaData persistentTasks = null;
        String lastKnownTask = "";
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final Builder builder;
            if (randomBoolean()) {
                builder = PersistentTasksCustomMetaData.builder();
            } else {
                builder = PersistentTasksCustomMetaData.builder(persistentTasks);
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
                            builder.updateTaskStatus(lastKnownTask, randomBoolean() ? new Status(randomAlphaOfLength(10)) : null);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.updateTaskStatus(fLastKnownTask, null));
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
        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();

        Version minVersion = allReleasedVersions().stream().filter(Version::isRelease).findFirst().orElseThrow(NoSuchElementException::new);
        final Version streamVersion = randomVersionBetween(random(), minVersion, getPreviousVersion(Version.CURRENT));
        tasks.addTask("test_compatible_version", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, streamVersion),
                randomBoolean() ? Optional.empty() : Optional.of("test")),
            randomAssignment());
        tasks.addTask("test_incompatible_version", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), compatibleFutureVersion(streamVersion), Version.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("test")),
            randomAssignment());
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(streamVersion);
        Set<String> features = new HashSet<>();
        final boolean transportClient = randomBoolean();
        if (transportClient) {
            features.add(TransportClient.TRANSPORT_CLIENT_FEATURE);
        }
        // if a transport client, then it must have the feature otherwise we add the feature randomly
        if (transportClient || randomBoolean()) {
            features.add("test");
        }
        out.setFeatures(features);
        tasks.build().writeTo(out);

        final StreamInput input = out.bytes().streamInput();
        input.setVersion(streamVersion);
        PersistentTasksCustomMetaData read =
            new PersistentTasksCustomMetaData(new NamedWriteableAwareStreamInput(input, getNamedWriteableRegistry()));

        assertThat(read.taskMap().keySet(), equalTo(Collections.singleton("test_compatible_version")));
    }

    public void testFeatureSerialization() throws IOException {
        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();

        Version minVersion = getFirstVersion();
        tasks.addTask("test_compatible", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, Version.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("existing")),
            randomAssignment());
        tasks.addTask("test_incompatible", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, Version.CURRENT), Optional.of("non_existing")),
            randomAssignment());
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.CURRENT);
        Set<String> features = new HashSet<>();
        features.add("existing");
        features.add(TransportClient.TRANSPORT_CLIENT_FEATURE);
        out.setFeatures(features);
        tasks.build().writeTo(out);

        PersistentTasksCustomMetaData read = new PersistentTasksCustomMetaData(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry()));
        assertThat(read.taskMap().keySet(), equalTo(Collections.singleton("test_compatible")));
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
