/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasks;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksExecutorRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MetadataPersistentTasksTests extends ESTestCase {

    private Set<Long> assignedAllocationIds;
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedWriteableRegistry namedWriteableRegistryBwc;

    @Before
    public void initializeRegistries() {
        assignedAllocationIds = new HashSet<>();
        new PersistentTasksExecutorRegistry(
            List.of(
                new TestClusterPersistentTasksExecutor(TestClusterPersistentTasksParams.NAME, null),
                new TestProjectPersistentTasksExecutor(TestProjectPersistentTasksParams.NAME, null)
            )
        );
        namedWriteableRegistry = new NamedWriteableRegistry(
            Stream.concat(
                ClusterModule.getNamedWriteables().stream(),
                Stream.of(
                    new NamedWriteableRegistry.Entry(
                        PersistentTaskParams.class,
                        TestClusterPersistentTasksParams.NAME,
                        TestClusterPersistentTasksParams::new
                    ),
                    new NamedWriteableRegistry.Entry(
                        PersistentTaskParams.class,
                        TestProjectPersistentTasksParams.NAME,
                        TestProjectPersistentTasksParams::new
                    )
                )
            ).toList()
        );
        namedWriteableRegistryBwc = new NamedWriteableRegistry(
            Stream.concat(
                ClusterModule.getNamedWriteables()
                    .stream()
                    .filter(entry -> entry.name.equals(ClusterPersistentTasksCustomMetadata.TYPE) == false),
                Stream.of(
                    new NamedWriteableRegistry.Entry(
                        PersistentTaskParams.class,
                        TestClusterPersistentTasksParams.NAME,
                        TestClusterPersistentTasksParams::new
                    ),
                    new NamedWriteableRegistry.Entry(
                        PersistentTaskParams.class,
                        TestProjectPersistentTasksParams.NAME,
                        TestProjectPersistentTasksParams::new
                    )
                )
            ).toList()
        );
    }

    public void testPersistentTasksSerialization() throws IOException {
        final Metadata orig = randomMetadataWithPersistentTasks();

        final BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        final Metadata fromStream = Metadata.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertTrue(Metadata.isGlobalStateEquals(orig, fromStream));
    }

    public void testPersistentTasksDiffSerialization() throws IOException {
        final Tuple<Metadata, Metadata> tuple = randomMetadataAndUpdate();
        final Metadata before = tuple.v1();
        final Metadata after = tuple.v2();

        final Diff<Metadata> diff = after.diff(before);

        final BytesStreamOutput out = new BytesStreamOutput();
        diff.writeTo(out);

        final Diff<Metadata> diffFromStream = Metadata.readDiffFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        final Metadata metadataFromDiff = diffFromStream.apply(before);

        assertTrue(Metadata.isGlobalStateEquals(after, metadataFromDiff));
    }

    public void testPersistentTasksSerializationBwc() throws IOException {
        final Metadata orig = randomMetadataWithPersistentTasks();

        final BytesStreamOutput out = new BytesStreamOutput();
        final var previousVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(previousVersion);
        orig.writeTo(out);

        final var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistryBwc);
        in.setTransportVersion(previousVersion);
        final Metadata fromStream = Metadata.readFrom(in);

        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(orig);
        final var projectTasks = PersistentTasksCustomMetadata.get(orig.getProject(Metadata.DEFAULT_PROJECT_ID));

        final var streamClusterTasks = ClusterPersistentTasksCustomMetadata.get(fromStream);
        final var streamProjectTasks = PersistentTasksCustomMetadata.get(fromStream.getProject(Metadata.DEFAULT_PROJECT_ID));

        // Tasks should be preserved
        assertThat(clusterTasks.taskMap(), equalTo(streamClusterTasks.taskMap()));
        assertThat(projectTasks.taskMap(), equalTo(streamProjectTasks.taskMap()));

        // The allocationId is merged to be the maximum between cluster and project scoped tasks
        assertThat(streamClusterTasks.getLastAllocationId(), equalTo(streamProjectTasks.getLastAllocationId()));
        assertThat(
            streamClusterTasks.getLastAllocationId(),
            equalTo(Math.max(clusterTasks.getLastAllocationId(), projectTasks.getLastAllocationId()))
        );
    }

    public void testPersistentTasksDiffSerializationBwc() throws IOException {
        final Tuple<Metadata, Metadata> tuple = randomMetadataAndUpdate();
        final Metadata before = tuple.v1();
        final Metadata after = tuple.v2();

        final Diff<Metadata> diff = after.diff(before);

        final BytesStreamOutput out = new BytesStreamOutput();
        final var previousVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(previousVersion);
        diff.writeTo(out);

        final var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistryBwc);
        in.setTransportVersion(previousVersion);
        final Diff<Metadata> diffFromStream = Metadata.readDiffFrom(in);
        final Metadata metadataFromDiff = diffFromStream.apply(before);

        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(after);
        final var projectTasks = PersistentTasksCustomMetadata.get(after.getProject(Metadata.DEFAULT_PROJECT_ID));

        final var streamClusterTasks = ClusterPersistentTasksCustomMetadata.get(metadataFromDiff);
        final var streamProjectTasks = PersistentTasksCustomMetadata.get(metadataFromDiff.getProject(Metadata.DEFAULT_PROJECT_ID));

        // Tasks should be preserved
        assertThat(clusterTasks.taskMap(), equalTo(streamClusterTasks.taskMap()));
        assertThat(projectTasks.taskMap(), equalTo(streamProjectTasks.taskMap()));

        // The allocationId is merged to be the maximum between cluster and project scoped tasks
        assertThat(streamClusterTasks.getLastAllocationId(), equalTo(streamProjectTasks.getLastAllocationId()));
        assertThat(
            streamClusterTasks.getLastAllocationId(),
            equalTo(Math.max(clusterTasks.getLastAllocationId(), projectTasks.getLastAllocationId()))
        );
    }

    private long randomUniqueAllocationIdBetween(long min, long max) {
        final long allocationId = randomValueOtherThanMany(id -> assignedAllocationIds.contains(id), () -> randomLongBetween(min, max));
        assignedAllocationIds.add(allocationId);
        return allocationId;
    }

    private <T extends PersistentTasks> T mutatePersistentTasks(
        PersistentTasks persistentTasks,
        BiFunction<String, Long, PersistentTask<?>> oneTaskFunc,
        BiFunction<Long, Map<String, PersistentTask<?>>, T> tasksFunc
    ) {
        final long updatedLastAllocationId = persistentTasks.getLastAllocationId() + randomLongBetween(100, 1000);
        final Map<String, PersistentTask<?>> tasks = persistentTasks.taskMap();

        final var updatedTasks = new HashMap<>(tasks);
        final boolean addNewTask = randomBoolean() || tasks.isEmpty();
        if (addNewTask) {
            IntStream.range(0, between(1, 3)).forEach(i -> {
                final String taskId = randomTaskId();
                updatedTasks.put(
                    taskId,
                    oneTaskFunc.apply(
                        taskId,
                        randomUniqueAllocationIdBetween(persistentTasks.getLastAllocationId() + i + 1, updatedLastAllocationId)
                    )
                );
            });
        } else {
            final int n = between(1, updatedTasks.size());
            for (int i = 0; i < n; i++) {
                updatedTasks.remove(randomFrom(updatedTasks.keySet()));
            }
        }
        return tasksFunc.apply(updatedLastAllocationId, updatedTasks);
    }

    private static String randomTaskId() {
        return randomAlphaOfLength(15);
    }

    private Metadata randomMetadataWithPersistentTasks() {
        final long lastAllocationIdCluster = randomLongBetween(0, Long.MAX_VALUE / 2);
        final var clusterPersistentTasksCustomMetadata = new ClusterPersistentTasksCustomMetadata(
            lastAllocationIdCluster,
            randomClusterPersistentTasks(lastAllocationIdCluster)
        );

        final long lastAllocationIdProject = randomLongBetween(0, Long.MAX_VALUE / 2);
        final var persistentTasksCustomMetadata = new PersistentTasksCustomMetadata(
            lastAllocationIdProject,
            randomProjectPersistentTasks(lastAllocationIdProject)
        );

        return Metadata.builder()
            .putCustom(ClusterPersistentTasksCustomMetadata.TYPE, clusterPersistentTasksCustomMetadata)
            .projectMetadata(
                Map.of(
                    Metadata.DEFAULT_PROJECT_ID,
                    ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
                        .putCustom(PersistentTasksCustomMetadata.TYPE, persistentTasksCustomMetadata)
                        .build()
                )
            )
            .build();
    }

    private static PersistentTask<?> oneClusterPersistentTask(String taskId, long allocationId) {
        return new PersistentTask<>(
            taskId,
            TestClusterPersistentTasksParams.NAME,
            new TestClusterPersistentTasksParams(randomInt()),
            allocationId,
            new Assignment(randomAlphaOfLength(10), randomAlphaOfLengthBetween(10, 20))
        );
    }

    private static PersistentTask<?> oneProjectPersistentTask(String taskId, long allocationId) {
        return new PersistentTask<>(
            taskId,
            TestProjectPersistentTasksParams.NAME,
            new TestProjectPersistentTasksParams(randomInt()),
            allocationId,
            new Assignment(randomAlphaOfLength(10), randomAlphaOfLengthBetween(10, 20))
        );
    }

    private Map<String, PersistentTask<?>> randomClusterPersistentTasks(long allocationId) {
        return randomMap(0, 5, () -> {
            final String taskId = randomTaskId();
            return new Tuple<>(taskId, oneClusterPersistentTask(taskId, randomUniqueAllocationIdBetween(0, allocationId)));
        });
    }

    private Map<String, PersistentTask<?>> randomProjectPersistentTasks(long allocationId) {
        return randomMap(0, 5, () -> {
            final String taskId = randomTaskId();
            return new Tuple<>(taskId, oneProjectPersistentTask(taskId, randomUniqueAllocationIdBetween(0, allocationId)));
        });
    }

    private Tuple<Metadata, Metadata> randomMetadataAndUpdate() {
        final Metadata before = randomMetadataWithPersistentTasks();
        final Metadata after = Metadata.builder(before)
            .putCustom(
                ClusterPersistentTasksCustomMetadata.TYPE,
                this.<ClusterPersistentTasksCustomMetadata>mutatePersistentTasks(
                    ClusterPersistentTasksCustomMetadata.get(before),
                    MetadataPersistentTasksTests::oneClusterPersistentTask,
                    ClusterPersistentTasksCustomMetadata::new
                )
            )
            .putProjectCustom(
                PersistentTasksCustomMetadata.TYPE,
                mutatePersistentTasks(
                    PersistentTasksCustomMetadata.get(before.getProject(Metadata.DEFAULT_PROJECT_ID)),
                    MetadataPersistentTasksTests::oneProjectPersistentTask,
                    PersistentTasksCustomMetadata::new
                )
            )
            .build();
        return new Tuple<>(before, after);
    }

    record TestClusterPersistentTasksParams(int value) implements PersistentTaskParams {
        static final String NAME = "test_cluster_persistent_tasks";

        TestClusterPersistentTasksParams(StreamInput in) throws IOException {
            this(in.readVInt());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("value", value);
            builder.endObject();
            return builder;
        }
    }

    record TestProjectPersistentTasksParams(int value) implements PersistentTaskParams {
        static final String NAME = "test_project_persistent_tasks";

        TestProjectPersistentTasksParams(StreamInput in) throws IOException {
            this(in.readVInt());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("value", value);
            builder.endObject();
            return builder;
        }
    }

    static class TestClusterPersistentTasksExecutor extends PersistentTasksExecutor<TestClusterPersistentTasksParams> {
        protected TestClusterPersistentTasksExecutor(String taskName, Executor executor) {
            super(taskName, executor);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestClusterPersistentTasksParams params, PersistentTaskState state) {}

        @Override
        public Scope scope() {
            return Scope.CLUSTER;
        }
    }

    static class TestProjectPersistentTasksExecutor extends PersistentTasksExecutor<TestProjectPersistentTasksParams> {
        protected TestProjectPersistentTasksExecutor(String taskName, Executor executor) {
            super(taskName, executor);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestProjectPersistentTasksParams params, PersistentTaskState state) {}

        @Override
        public Scope scope() {
            return Scope.PROJECT;
        }
    }
}
