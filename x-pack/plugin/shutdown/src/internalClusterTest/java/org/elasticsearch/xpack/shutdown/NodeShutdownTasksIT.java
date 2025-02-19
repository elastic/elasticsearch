/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;

/**
 * This class is for testing that when shutting down a node, persistent tasks
 * are not assigned to that node.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeShutdownTasksIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(NodeShutdownTasksIT.class);
    private static final AtomicBoolean startTask = new AtomicBoolean(false);
    private static final AtomicBoolean taskCompleted = new AtomicBoolean(false);
    private static final AtomicReference<Collection<DiscoveryNode>> candidates = new AtomicReference<>(null);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class, TaskPlugin.class);
    }

    public void testTasksAreNotAssignedToShuttingDownNode() throws Exception {
        // Start two nodes, one will be marked as shutting down
        final String node1 = internalCluster().startNode();
        final String node2 = internalCluster().startNode();

        final String shutdownNode;
        final String candidateNode;
        final String node1Id = getNodeId(node1);
        final String node2Id = getNodeId(node2);

        if (randomBoolean()) {
            shutdownNode = node1Id;
            candidateNode = node2Id;
        } else {
            shutdownNode = node2Id;
            candidateNode = node1Id;
        }
        logger.info("--> node {} will be shut down, {} will remain", shutdownNode, candidateNode);

        // Mark the node as shutting down
        client().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                shutdownNode,
                SingleNodeShutdownMetadata.Type.REMOVE,
                "removal for testing",
                null,
                null,
                null
            )
        ).get();

        // Tell the persistent task executor it can start allocating the task
        startTask.set(true);
        // Issue a new cluster state update to force task assignment
        ClusterRerouteUtils.reroute(client());
        // Wait until the task has been assigned to a node
        assertBusy(() -> assertNotNull("expected to have candidate nodes chosen for task", candidates.get()));
        // Check that the node that is not shut down is the only candidate
        assertThat(candidates.get().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()), contains(candidateNode));
        assertThat(candidates.get().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()), not(contains(shutdownNode)));
    }

    public static class TaskPlugin extends Plugin implements PersistentTaskPlugin {

        TaskExecutor taskExecutor;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            taskExecutor = new TaskExecutor(services.client(), services.clusterService(), services.threadPool());
            return Collections.singletonList(taskExecutor);
        }

        @Override
        public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule,
            IndexNameExpressionResolver expressionResolver
        ) {
            return Collections.singletonList(taskExecutor);
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Collections.singletonList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, "task_name", TestTaskParams::new)
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return Collections.singletonList(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    TestTaskParams.TASK_NAME,
                    (p, c) -> TestTaskParams.fromXContent(p)
                )
            );
        }
    }

    public static final class TaskExecutor extends PersistentTasksExecutor<TestTaskParams> implements ClusterStateListener {

        private final PersistentTasksService persistentTasksService;

        protected TaskExecutor(Client client, ClusterService clusterService, ThreadPool threadPool) {
            super("task_name", threadPool.generic());
            persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
            clusterService.addListener(this);
        }

        @Override
        public PersistentTasksCustomMetadata.Assignment getAssignment(
            TestTaskParams params,
            Collection<DiscoveryNode> candidateNodes,
            ClusterState clusterState
        ) {
            candidates.set(candidateNodes);
            return super.getAssignment(params, candidateNodes, clusterState);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestTaskParams params, PersistentTaskState state) {
            logger.info("--> executing the task");
            taskCompleted.compareAndSet(false, true);
        }

        private void startTask() {
            logger.info("--> sending start request");
            persistentTasksService.sendStartRequest(
                "task_id",
                "task_name",
                new TestTaskParams(),
                TEST_REQUEST_TIMEOUT,
                ActionListener.wrap(r -> {}, e -> {
                    if (e instanceof ResourceAlreadyExistsException == false) {
                        logger.error("failed to create task", e);
                        fail("failed to create task");
                    }
                })
            );
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            // Check if it's true, setting it to false if we are going to start task
            if (startTask.compareAndSet(true, false)) {
                startTask();
            }
        }
    }

    public static class TestTaskParams implements PersistentTaskParams {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        public static final ParseField TASK_NAME = new ParseField("task_name");
        public static final ObjectParser<TestTaskParams, Void> PARSER = new ObjectParser<TestTaskParams, Void>(
            TASK_NAME.getPreferredName(),
            true,
            () -> new TestTaskParams()
        );

        public static TestTaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public TestTaskParams() {}

        public TestTaskParams(StreamInput in) {}

        @Override
        public String getWriteableName() {
            return TASK_NAME.getPreferredName();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
