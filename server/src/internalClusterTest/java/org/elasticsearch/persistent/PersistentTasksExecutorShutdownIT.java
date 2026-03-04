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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
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
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests verifying the behavior of persistent tasks on node shutdown and local abort,
 * depending on the {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} flag value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class PersistentTasksExecutorShutdownIT extends ESIntegTestCase {

    static class TestShutdownParams implements PersistentTaskParams {

        static final String OPT_IN_NAME = "cluster:admin/persistent/test_shutdown_opt_in";
        static final String OPT_OUT_NAME = "cluster:admin/persistent/test_shutdown_opt_out";

        private final String name;

        TestShutdownParams(String name) {
            this.name = name;
        }

        TestShutdownParams(String name, StreamInput in) {
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

        public static TestShutdownParams fromXContent(String name, XContentParser parser) throws IOException {
            parser.skipChildren();
            return new TestShutdownParams(name);
        }
    }

    static class TestShutdownNodeExecutor extends PersistentTasksExecutor<TestShutdownParams> {

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
        protected void nodeOperation(AllocatedPersistentTask task, TestShutdownParams params, PersistentTaskState state) {
            safeAwait(l -> task.addListener(() -> l.onResponse(null)));
            task.markAsCompleted();
        }
    }

    /**
     * An executor that registers all running tasks so the test can abort specific ones via {@link #abortTask(String)}.
     * <p>
     * {@link #automaticReassignmentOnShutdown()} is set to {@code false}. This executor manages its own shutdown
     * behavior.
     */
    static class TestLocalAbortExecutor extends PersistentTasksExecutor<TestShutdownParams> {

        static final String NAME = "cluster:admin/persistent/test_local_abort";

        private static final ConcurrentHashMap<String, TaskEntry> runningTasks = new ConcurrentHashMap<>();

        static volatile boolean allowAssignment = true;

        /**
         * Waits until the task with the given persistent-task ID is running, then signals its
         * {@code nodeOperation} to call {@link AllocatedPersistentTask#markAsLocallyAborted}.
         */
        static void abortTask(String taskId) throws Exception {
            assertBusy(() -> assertNotNull(runningTasks.get(taskId)));
            runningTasks.get(taskId).signal().onResponse(null);
        }

        TestLocalAbortExecutor(ThreadPool threadPool) {
            super(NAME, threadPool.generic());
        }

        @Override
        public boolean automaticReassignmentOnShutdown() {
            return false;
        }

        @Override
        protected Assignment doGetAssignment(
            TestShutdownParams params,
            Collection<DiscoveryNode> candidateNodes,
            ClusterState clusterState,
            ProjectId projectId
        ) {
            return allowAssignment ? super.doGetAssignment(params, candidateNodes, clusterState, projectId) : NO_NODE_FOUND;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestShutdownParams params, PersistentTaskState state) {
            final var signal = new SubscribableListener<Void>();
            runningTasks.put(task.getPersistentTaskId(), new TaskEntry(task, signal));
            safeAwait(l -> {
                // Canceled (test cleanup)
                task.addListener(() -> {
                    if (runningTasks.remove(task.getPersistentTaskId()) != null) {
                        task.markAsCompleted();
                        l.onResponse(null);
                    }
                });
                // Locally aborted
                signal.addListener(ActionListener.running(() -> {
                    if (runningTasks.remove(task.getPersistentTaskId()) != null) {
                        task.markAsLocallyAborted("test local abort");
                        l.onResponse(null);
                    }
                }));
            });
        }

        private record TaskEntry(AllocatedPersistentTask task, SubscribableListener<Void> signal) {}
    }

    public static class TestShutdownPlugin extends Plugin implements PersistentTaskPlugin {

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return List.of(
                new TestShutdownNodeExecutor(TestShutdownParams.OPT_IN_NAME, true, threadPool),
                new TestShutdownNodeExecutor(TestShutdownParams.OPT_OUT_NAME, false, threadPool),
                new TestLocalAbortExecutor(threadPool)
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestShutdownParams.OPT_IN_NAME,
                    in -> new TestShutdownParams(TestShutdownParams.OPT_IN_NAME, in)
                ),
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestShutdownParams.OPT_OUT_NAME,
                    in -> new TestShutdownParams(TestShutdownParams.OPT_OUT_NAME, in)
                ),
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    TestLocalAbortExecutor.NAME,
                    in -> new TestShutdownParams(TestLocalAbortExecutor.NAME, in)
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestShutdownParams.OPT_IN_NAME),
                    p -> TestShutdownParams.fromXContent(TestShutdownParams.OPT_IN_NAME, p)
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestShutdownParams.OPT_OUT_NAME),
                    p -> TestShutdownParams.fromXContent(TestShutdownParams.OPT_OUT_NAME, p)
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestLocalAbortExecutor.NAME),
                    p -> TestShutdownParams.fromXContent(TestLocalAbortExecutor.NAME, p)
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestShutdownPlugin.class);
    }

    @Before
    public void resetLocalAbortState() {
        TestLocalAbortExecutor.allowAssignment = true;
        TestLocalAbortExecutor.runningTasks.clear();
    }

    @After
    public void assertNoRunningTasks() throws Exception {
        for (String taskName : List.of(TestShutdownParams.OPT_IN_NAME, TestShutdownParams.OPT_OUT_NAME, TestLocalAbortExecutor.NAME)) {
            assertBusy(() -> {
                assertThat(clusterAdmin().prepareListTasks().setActions(taskName + "[c]").get().getTasks(), empty());
                assertThat(findTasks(internalCluster().clusterService().state(), taskName), empty());
            });
        }
    }

    /**
     * Executor's {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} is {@code true}.
     * The task should be proactively moved off a node that is marked for graceful shutdown,
     * without waiting for the node to physically leave the cluster.
     */
    public void testOptInTaskIsReassignedOnShutdown() throws Exception {
        final var taskName = TestShutdownParams.OPT_IN_NAME;
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
     * Executor's {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} is {@code false}.
     * The task should remain on its current node when that node is marked for graceful shutdown.
     */
    public void testOptOutTaskIsNotReassignedOnShutdown() throws Exception {
        final var taskName = TestShutdownParams.OPT_OUT_NAME;
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

    /**
     * Executor's {@link PersistentTasksExecutor#automaticReassignmentOnShutdown()} is {@code false}.
     * Verifies that when an executor calls {@link AllocatedPersistentTask#markAsLocallyAborted}, the
     * persistent task is unassigned in the cluster state with the abort reason recorded, and is
     * subsequently reassigned and restarts cleanly.
     */
    public void testLocalAbort() throws Exception {
        internalCluster().getInstance(PersistentTasksClusterService.class, internalCluster().getMasterName())
            .setRecheckInterval(TimeValue.timeValueMillis(1));
        try {
            final var taskId = startTask(TestLocalAbortExecutor.NAME);
            waitForTaskToStart(TestLocalAbortExecutor.NAME);

            // Block reassignment so we can observe the unassigned state
            TestLocalAbortExecutor.allowAssignment = false;
            TestLocalAbortExecutor.abortTask(taskId);

            awaitClusterState(state -> {
                final var tasks = findTasks(state, TestLocalAbortExecutor.NAME);
                return tasks.size() == 1
                    && taskId.equals(tasks.getFirst().getId())
                    && tasks.getFirst().getAssignment().getExecutorNode() == null
                    && "test local abort".equals(tasks.getFirst().getAssignment().getExplanation());
            });
            assertThat(clusterAdmin().prepareListTasks().setActions(TestLocalAbortExecutor.NAME + "[c]").get().getTasks(), empty());

            TestLocalAbortExecutor.allowAssignment = true;
            awaitClusterState(state -> {
                final var tasks = findTasks(state, TestLocalAbortExecutor.NAME);
                return tasks.size() == 1 && "test local abort".equals(tasks.getFirst().getAssignment().getExplanation()) == false;
            });
            waitForTaskToStart(TestLocalAbortExecutor.NAME);
        } finally {
            cancelTask(TestLocalAbortExecutor.NAME);
            resetLocalAbortState();
        }
    }

    private String startTask(String taskName) throws Exception {
        final var persistentTasksService = internalCluster().getInstance(PersistentTasksService.class);
        final var future = new PlainActionFuture<PersistentTask<TestShutdownParams>>();
        String taskId = UUIDs.base64UUID();
        persistentTasksService.sendStartRequest(taskId, taskName, new TestShutdownParams(taskName), TEST_REQUEST_TIMEOUT, future);
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
        final PersistentTask<?> task = tasks.getFirst();
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
