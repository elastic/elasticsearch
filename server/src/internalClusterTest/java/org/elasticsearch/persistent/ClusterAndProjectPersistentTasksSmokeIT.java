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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class ClusterAndProjectPersistentTasksSmokeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPersistentTasksPlugin.class);
    }

    public void testCoexistenceOfClusterAndProjectPersistentTasks() throws Exception {
        internalCluster().startNode();
        ensureGreen();
        final var persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final var clusterService = internalCluster().getInstance(ClusterService.class);

        // Start a project persistent task
        final PersistentTask<TestEmptyProjectParams> projectTask;
        {
            final PlainActionFuture<PersistentTask<TestEmptyProjectParams>> future = new PlainActionFuture<>();
            persistentTasksService.sendStartRequest(
                randomUUID(),
                TestProjectPersistentTasksExecutor.NAME,
                TestEmptyProjectParams.INSTANCE,
                TEST_REQUEST_TIMEOUT,
                future
            );
            projectTask = safeGet(future);
        }

        // Start a cluster persistent task
        final PersistentTask<TestEmptyClusterParams> clusterTask;
        {
            final PlainActionFuture<PersistentTask<TestEmptyClusterParams>> future = new PlainActionFuture<>();
            persistentTasksService.sendStartRequest(
                randomUUID(),
                TestClusterPersistentTasksExecutor.NAME,
                TestEmptyClusterParams.INSTANCE,
                TEST_REQUEST_TIMEOUT,
                future
            );
            clusterTask = safeGet(future);
        }

        // Two tasks have different allocation IDs
        assertThat(projectTask.getAllocationId(), not(equalTo(clusterTask.getAllocationId())));
        // They are found in different section of the cluster state
        assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 1);
        assertClusterStateHasTaskSize(clusterService, null, 1);

        // List tasks work correctly for both of them
        final List<TaskInfo> tasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
            .stream()
            .filter(taskInfo -> "persistent".equals(taskInfo.type()) && taskInfo.action().startsWith("test-"))
            .toList();
        assertThat(tasks.toString(), tasks, hasSize(2));
        assertThat(
            tasks.stream().map(TaskInfo::action).toList(),
            containsInAnyOrder(TestProjectPersistentTasksExecutor.NAME + "[c]", TestClusterPersistentTasksExecutor.NAME + "[c]")
        );
        assertThat(
            tasks.stream().map(taskinfo -> taskinfo.parentTaskId().getId()).toList(),
            containsInAnyOrder(projectTask.getAllocationId(), clusterTask.getAllocationId())
        );

        // Start remove the tasks
        if (randomBoolean()) {
            // Remove project task first
            final PlainActionFuture<PersistentTask<?>> future1 = new PlainActionFuture<>();
            persistentTasksService.sendRemoveRequest(projectTask.getId(), TEST_REQUEST_TIMEOUT, future1);
            safeGet(future1);

            assertBusy(() -> {
                final List<TaskInfo> remainingTasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
                    .stream()
                    .filter(
                        taskInfo -> "persistent".equals(taskInfo.type())
                            && taskInfo.action().startsWith("test-")
                            && taskInfo.cancelled() == false
                    )
                    .toList();
                assertThat(remainingTasks.toString(), remainingTasks, hasSize(1));
                assertThat(remainingTasks.getFirst().parentTaskId().getId(), equalTo(clusterTask.getAllocationId()));

            });
            assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 0);
            assertClusterStateHasTaskSize(clusterService, null, 1);

            final PlainActionFuture<PersistentTask<?>> future2 = new PlainActionFuture<>();
            persistentTasksService.sendRemoveRequest(clusterTask.getId(), TEST_REQUEST_TIMEOUT, future2);
            safeGet(future2);
        } else {
            // Remove cluster task first
            final PlainActionFuture<PersistentTask<?>> future1 = new PlainActionFuture<>();
            persistentTasksService.sendRemoveRequest(clusterTask.getId(), TEST_REQUEST_TIMEOUT, future1);
            safeGet(future1);

            assertBusy(() -> {
                final List<TaskInfo> remainingTasks = safeGet(clusterAdmin().prepareListTasks().execute()).getTasks()
                    .stream()
                    .filter(
                        taskInfo -> "persistent".equals(taskInfo.type())
                            && taskInfo.action().startsWith("test-")
                            && taskInfo.cancelled() == false
                    )
                    .toList();
                assertThat(remainingTasks.toString(), remainingTasks, hasSize(1));
                assertThat(remainingTasks.getFirst().parentTaskId().getId(), equalTo(projectTask.getAllocationId()));
            });

            assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 1);
            assertClusterStateHasTaskSize(clusterService, null, 0);

            final PlainActionFuture<PersistentTask<?>> future2 = new PlainActionFuture<>();
            persistentTasksService.sendRemoveRequest(projectTask.getId(), TEST_REQUEST_TIMEOUT, future2);
            safeGet(future2);
        }

        assertClusterStateHasTaskSize(clusterService, Metadata.DEFAULT_PROJECT_ID, 0);
        assertClusterStateHasTaskSize(clusterService, null, 0);
    }

    private void assertClusterStateHasTaskSize(ClusterService clusterService, @Nullable ProjectId projectId, int size) {
        assertThat(
            PersistentTasks.getTasks(clusterService.state(), projectId)
                .tasks()
                .stream()
                .filter(task -> task.getTaskName().startsWith("test-"))
                .toList(),
            hasSize(size)
        );
    }

    public static class TestPersistentTasksPlugin extends Plugin implements PersistentTaskPlugin {
        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return List.of(new TestProjectPersistentTasksExecutor(clusterService), new TestClusterPersistentTasksExecutor(clusterService));
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestProjectPersistentTasksExecutor.NAME,
                    ignore -> TestEmptyProjectParams.INSTANCE
                ),
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestClusterPersistentTasksExecutor.NAME,
                    ignore -> TestEmptyClusterParams.INSTANCE
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestProjectPersistentTasksExecutor.NAME),
                    TestEmptyProjectParams::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestClusterPersistentTasksExecutor.NAME),
                    TestEmptyClusterParams::fromXContent
                )
            );
        }
    }

    public static class TestEmptyProjectParams implements PersistentTaskParams {

        public static final TestEmptyProjectParams INSTANCE = new TestEmptyProjectParams();
        public static final ObjectParser<TestEmptyProjectParams, Void> PARSER = new ObjectParser<>(
            TestProjectPersistentTasksExecutor.NAME,
            true,
            () -> INSTANCE
        );

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String getWriteableName() {
            return TestProjectPersistentTasksExecutor.NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        public static TestEmptyProjectParams fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    public static class TestProjectPersistentTasksExecutor extends PersistentTasksExecutor<TestEmptyProjectParams> {
        static final String NAME = "test-project-persistent-task";

        protected TestProjectPersistentTasksExecutor(ClusterService clusterService) {
            super(NAME, clusterService.threadPool().generic());
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestEmptyProjectParams params, PersistentTaskState state) {}
    }

    public static class TestEmptyClusterParams implements PersistentTaskParams {

        public static final TestEmptyClusterParams INSTANCE = new TestEmptyClusterParams();
        public static final ObjectParser<TestEmptyClusterParams, Void> PARSER = new ObjectParser<>(
            TestClusterPersistentTasksExecutor.NAME,
            true,
            () -> INSTANCE
        );

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public String getWriteableName() {
            return TestClusterPersistentTasksExecutor.NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        public static TestEmptyClusterParams fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    public static class TestClusterPersistentTasksExecutor extends PersistentTasksExecutor<TestEmptyClusterParams> {
        static final String NAME = "test-cluster-persistent-task";

        protected TestClusterPersistentTasksExecutor(ClusterService clusterService) {
            super(NAME, clusterService.threadPool().generic());
        }

        @Override
        public Scope scope() {
            return Scope.CLUSTER;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestEmptyClusterParams params, PersistentTaskState state) {}
    }
}
