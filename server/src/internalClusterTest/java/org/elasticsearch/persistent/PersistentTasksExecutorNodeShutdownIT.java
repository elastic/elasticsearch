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
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeShutdownTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.safeAwait;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests verifying the behavior of task reassignment on node shutdown depending on
 * the {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} flag value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class PersistentTasksExecutorNodeShutdownIT extends ESIntegTestCase {

    static class TestNodeShutdownParams implements PersistentTaskParams {

        static final String OPT_IN_NAME = "cluster:admin/persistent/test_shutdown_opt_in";
        static final String OPT_OUT_NAME = "cluster:admin/persistent/test_shutdown_opt_out";

        private final String name;

        TestNodeShutdownParams(String name) {
            this.name = name;
        }

        TestNodeShutdownParams(String name, StreamInput in) {
            this.name = name;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        public static TestNodeShutdownParams fromXContent(String name, XContentParser parser) throws IOException {
            parser.skipChildren();
            return new TestNodeShutdownParams(name);
        }
    }

    static class TestShutdownNodeExecutor extends PersistentTasksExecutor<TestNodeShutdownParams> {

        private final boolean automaticReassignment;

        TestShutdownNodeExecutor(String name, boolean automaticReassignment, ThreadPool threadPool) {
            super(name, threadPool.generic());
            this.automaticReassignment = automaticReassignment;
        }

        @Override
        public boolean automaticReassignmentOnShutdown() {
            return automaticReassignment;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestNodeShutdownParams params, PersistentTaskState state) {
            safeAwait(l -> task.addListener(() -> l.onResponse(null)));
            task.markAsCompleted();
        }
    }

    public static class TestNodeShutdownPlugin extends Plugin implements PersistentTaskPlugin {

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return List.of(
                new TestShutdownNodeExecutor(TestNodeShutdownParams.OPT_IN_NAME, true, threadPool),
                new TestShutdownNodeExecutor(TestNodeShutdownParams.OPT_OUT_NAME, false, threadPool)
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestNodeShutdownParams.OPT_IN_NAME,
                    in -> new TestNodeShutdownParams(TestNodeShutdownParams.OPT_IN_NAME, in)
                ),
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestNodeShutdownParams.OPT_OUT_NAME,
                    in -> new TestNodeShutdownParams(TestNodeShutdownParams.OPT_OUT_NAME, in)
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestNodeShutdownParams.OPT_IN_NAME),
                    p -> TestNodeShutdownParams.fromXContent(TestNodeShutdownParams.OPT_IN_NAME, p)
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestNodeShutdownParams.OPT_OUT_NAME),
                    p -> TestNodeShutdownParams.fromXContent(TestNodeShutdownParams.OPT_OUT_NAME, p)
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestNodeShutdownPlugin.class);
    }

    @After
    public void assertNoRunningTasks() throws Exception {
        for (String taskName : List.of(TestNodeShutdownParams.OPT_IN_NAME, TestNodeShutdownParams.OPT_OUT_NAME)) {
            assertBusy(() -> {
                assertThat(clusterAdmin().prepareListTasks().setActions(taskName + "[c]").get().getTasks(), empty());
                assertThat(findTasks(internalCluster().clusterService().state(), taskName), empty());
            });
        }
    }

    /**
     * Executor's {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} is {@code true}
     * The task should be proactively moved off a node that is marked for graceful shutdown, without waiting for
     * the node to physically leave the cluster.
     */
    public void testOptInTaskIsReassignedOnShutdown() throws Exception {
        final var taskName = TestNodeShutdownParams.OPT_IN_NAME;
        final var taskId = startTask(taskName);
        waitForTaskToStart(taskName);

        final var task = assertClusterStateHasTask(taskId, taskName);
        final var originalNodeId = task.getAssignment().getExecutorNode();
        assertNotNull("task should be assigned before shutdown", originalNodeId);

        NodeShutdownTestUtils.putShutdownMetadata(
            originalNodeId,
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            EnumSet.of(
                SingleNodeShutdownMetadata.Type.REMOVE,
                SingleNodeShutdownMetadata.Type.RESTART,
                SingleNodeShutdownMetadata.Type.SIGTERM
            )
        );
        try {
            awaitClusterState(state -> {
                final var tasks = findTasks(state, taskName);
                assertTrue(tasks.size() == 1 && taskId.equals(tasks.getFirst().getId()));
                final var executorNode = tasks.getFirst().getAssignment().getExecutorNode();
                return executorNode != null && originalNodeId.equals(executorNode) == false;
            });
            waitForTaskToStart(taskName);
        } finally {
            NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            cancelTask(taskName);
        }
    }

    /**
     * Executor's {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} is {@code false}
     * The task should remain on its current node when that node is marked for graceful shutdown.
     */
    public void testOptOutTaskIsNotReassignedOnShutdown() throws Exception {
        final var taskName = TestNodeShutdownParams.OPT_OUT_NAME;
        final var taskId = startTask(taskName);
        waitForTaskToStart(taskName);

        final var task = assertClusterStateHasTask(taskId, taskName);
        final var originalNodeId = task.getAssignment().getExecutorNode();
        assertNotNull("task should be assigned before shutdown", originalNodeId);

        NodeShutdownTestUtils.putShutdownMetadata(
            originalNodeId,
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
            EnumSet.of(
                SingleNodeShutdownMetadata.Type.REMOVE,
                SingleNodeShutdownMetadata.Type.RESTART,
                SingleNodeShutdownMetadata.Type.SIGTERM
            )
        );
        try {
            waitNoPendingTasksOnAll();
            final var taskAfterShutdown = assertClusterStateHasTask(taskId, taskName);
            assertThat(
                "task should remain on the shutting-down node",
                taskAfterShutdown.getAssignment().getExecutorNode(),
                equalTo(originalNodeId)
            );
        } finally {
            NodeShutdownTestUtils.clearShutdownMetadata(internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
            cancelTask(taskName);
        }
    }

    private String startTask(String taskName) throws Exception {
        final var persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final var future = new PlainActionFuture<PersistentTask<TestNodeShutdownParams>>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.sendStartRequest(taskId, taskName, new TestNodeShutdownParams(taskName), TEST_REQUEST_TIMEOUT, future);
        future.get();
        return taskId;
    }

    private static void waitForTaskToStart(String taskName) throws Exception {
        assertBusy(() -> assertThat(clusterAdmin().prepareListTasks().setActions(taskName + "[c]").get().getTasks(), hasSize(1)));
    }

    private static PersistentTask<?> assertClusterStateHasTask(String taskId, String taskName) {
        final var state = internalCluster().clusterService().state();
        final var tasks = findTasks(state, taskName);
        assertThat(tasks, hasSize(1));
        final PersistentTask<?> task = tasks.iterator().next();
        assertThat(task.getId(), equalTo(taskId));
        return task;
    }

    private void cancelTask(String taskName) {
        final var tasks = clusterAdmin().prepareListTasks().setActions(taskName + "[c]").get().getTasks();
        if (tasks.isEmpty()) {
            return;
        }
        assertThat(tasks, hasSize(1));
        clusterAdmin().prepareCancelTasks().setTargetTaskId(tasks.getFirst().taskId()).get();
    }
}
