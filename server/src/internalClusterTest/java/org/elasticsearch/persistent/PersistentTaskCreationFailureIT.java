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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PersistentTaskCreationFailureIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FailingCreationPersistentTasksPlugin.class);
    }

    private static boolean hasPersistentTask(ClusterState clusterState) {
        return findTasks(clusterState, FailingCreationPersistentTaskExecutor.TASK_NAME).isEmpty() == false;
    }

    public void testPersistentTasksThatFailDuringCreationAreRemovedFromClusterState() {

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(FailingCreationPersistentTasksPlugin.class))
            .toList();
        plugins.forEach(plugin -> plugin.hasFailedToCreateTask.set(false));

        final var taskCreatedListener = ClusterServiceUtils.addTemporaryStateListener(
            masterClusterService,
            PersistentTaskCreationFailureIT::hasPersistentTask
        );

        taskCreatedListener.andThenAccept(v -> {
            // enqueue some higher-priority cluster state updates to check that they do not cause retries of the failing task creation step
            for (int i = 0; i < 5; i++) {
                masterClusterService.submitUnbatchedStateUpdateTask("test", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        assertTrue(hasPersistentTask(currentState));

                        assertTrue(waitUntil(() -> {
                            final var completePersistentTaskPendingTasksCount = masterClusterService.getMasterService()
                                .pendingTasks()
                                .stream()
                                .filter(
                                    pendingClusterTask -> pendingClusterTask.getSource()
                                        .string()
                                        .matches("finish project .* persistent task \\[.*] \\(failed\\)")
                                )
                                .count();
                            assertThat(completePersistentTaskPendingTasksCount, lessThanOrEqualTo(1L));
                            return completePersistentTaskPendingTasksCount == 1L;
                        }));

                        return currentState.copyAndUpdateMetadata(
                            mdb -> mdb.putCustom(
                                PersistentTasksCustomMetadata.TYPE,
                                PersistentTasksCustomMetadata.builder(
                                    PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(currentState)
                                )
                                    // create and remove a fake task just to force a change in lastAllocationId so that
                                    // PersistentTasksNodeService checks for changes and potentially retries
                                    .addTask("test", "test", null, PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT)
                                    .removeTask("test")
                                    .build()
                            )
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail(e);
                    }
                });
            }
        });

        safeAwait(
            l -> internalCluster().getInstance(PersistentTasksService.class)
                .sendStartRequest(
                    UUIDs.base64UUID(),
                    FailingCreationPersistentTaskExecutor.TASK_NAME,
                    new FailingCreationTaskParams(),
                    TEST_REQUEST_TIMEOUT,
                    l.map(ignored -> null)
                )
        );

        safeAwait(
            taskCreatedListener.<Void>andThen(
                (l, v) -> ClusterServiceUtils.addTemporaryStateListener(
                    masterClusterService,
                    clusterState -> hasPersistentTask(clusterState) == false
                ).addListener(l)
            )
        );

        assertEquals(1L, plugins.stream().filter(plugin -> plugin.hasFailedToCreateTask.get()).count());
    }

    public static class FailingCreationPersistentTasksPlugin extends Plugin implements PersistentTaskPlugin {

        private final AtomicBoolean hasFailedToCreateTask = new AtomicBoolean();

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return List.of(new FailingCreationPersistentTaskExecutor(hasFailedToCreateTask));
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    FailingCreationPersistentTaskExecutor.TASK_NAME,
                    FailingCreationTaskParams::new
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(FailingCreationPersistentTaskExecutor.TASK_NAME),
                    p -> {
                        p.skipChildren();
                        return new FailingCreationTaskParams();
                    }
                )
            );
        }
    }

    public static class FailingCreationTaskParams implements PersistentTaskParams {
        public FailingCreationTaskParams() {}

        public FailingCreationTaskParams(StreamInput in) {}

        @Override
        public String getWriteableName() {
            return FailingCreationPersistentTaskExecutor.TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }
    }

    static class FailingCreationPersistentTaskExecutor extends PersistentTasksExecutor<FailingCreationTaskParams> {
        static final String TASK_NAME = "cluster:admin/persistent/test_creation_failure";

        private final AtomicBoolean hasFailedToCreateTask;

        FailingCreationPersistentTaskExecutor(AtomicBoolean hasFailedToCreateTask) {
            super(TASK_NAME, r -> fail("execution is unexpected"));
            this.hasFailedToCreateTask = hasFailedToCreateTask;
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            PersistentTasksCustomMetadata.PersistentTask<FailingCreationTaskParams> taskInProgress,
            Map<String, String> headers
        ) {
            assertTrue("already failed before", hasFailedToCreateTask.compareAndSet(false, true));
            throw new RuntimeException("simulated");
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, FailingCreationTaskParams params, PersistentTaskState state) {
            fail("execution is unexpected");
        }
    }
}
