/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A plugin that adds a test persistent task.
 */
public class TestPersistentTasksPlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(new ActionHandler<>(TestTaskAction.INSTANCE, TransportTestTaskAction.class));
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService,
                                                                       ThreadPool threadPool,
                                                                       Client client,
                                                                       SettingsModule settingsModule,
                                                                       IndexNameExpressionResolver expressionResolver) {
        return Collections.singletonList(new TestPersistentTasksExecutor(clusterService));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class,
                    new ParseField(TestPersistentTasksExecutor.NAME), TestParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class,
                    new ParseField(TestPersistentTasksExecutor.NAME), State::fromXContent)
        );
    }

    public static class TestParams implements PersistentTaskParams {

        public static final ConstructingObjectParser<TestParams, Void> REQUEST_PARSER =
                new ConstructingObjectParser<>(TestPersistentTasksExecutor.NAME, args -> new TestParams((String) args[0]));

        static {
            REQUEST_PARSER.declareString(constructorArg(), new ParseField("param"));
        }

        private final Version minVersion;
        private final Optional<String> feature;

        private String executorNodeAttr = null;

        private String responseNode = null;

        private String testParam = null;

        public TestParams() {
            this((String)null);
        }

        public TestParams(String testParam) {
            this(testParam, Version.CURRENT, Optional.empty());
        }

        public TestParams(String testParam, Version minVersion, Optional<String> feature) {
            this.testParam = testParam;
            this.minVersion = minVersion;
            this.feature = feature;
        }

        public TestParams(StreamInput in) throws IOException {
            executorNodeAttr = in.readOptionalString();
            responseNode = in.readOptionalString();
            testParam = in.readOptionalString();
            minVersion = Version.readVersion(in);
            feature = Optional.ofNullable(in.readOptionalString());
        }

        @Override
        public String getWriteableName() {
            return TestPersistentTasksExecutor.NAME;
        }

        public void setExecutorNodeAttr(String executorNodeAttr) {
            this.executorNodeAttr = executorNodeAttr;
        }

        public void setTestParam(String testParam) {
            this.testParam = testParam;
        }

        public String getExecutorNodeAttr() {
            return executorNodeAttr;
        }

        public String getTestParam() {
            return testParam;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(executorNodeAttr);
            out.writeOptionalString(responseNode);
            out.writeOptionalString(testParam);
            Version.writeVersion(minVersion, out);
            out.writeOptionalString(feature.orElse(null));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("param", testParam);
            builder.endObject();
            return builder;
        }

        public static TestParams fromXContent(XContentParser parser) throws IOException {
            return REQUEST_PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestParams that = (TestParams) o;
            return Objects.equals(executorNodeAttr, that.executorNodeAttr) &&
                    Objects.equals(responseNode, that.responseNode) &&
                    Objects.equals(testParam, that.testParam);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executorNodeAttr, responseNode, testParam);
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return minVersion;
        }

    }

    public static class State implements PersistentTaskState {

        private final String phase;

        public static final ConstructingObjectParser<State, Void> STATE_PARSER =
                new ConstructingObjectParser<>(TestPersistentTasksExecutor.NAME, args -> new State((String) args[0]));

        static {
            STATE_PARSER.declareString(constructorArg(), new ParseField("phase"));
        }

        public State(String phase) {
            this.phase = requireNonNull(phase, "Phase cannot be null");
        }

        public State(StreamInput in) throws IOException {
            phase = in.readString();
        }

        @Override
        public String getWriteableName() {
            return TestPersistentTasksExecutor.NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("phase", phase);
            builder.endObject();
            return builder;
        }

        public static PersistentTaskState fromXContent(XContentParser parser) throws IOException {
            return STATE_PARSER.parse(parser, null);
        }

        @Override
        public boolean isFragment() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(phase);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        // Implements equals and hashcode for testing
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != State.class) {
                return false;
            }
            State other = (State) obj;
            return phase.equals(other.phase);
        }

        @Override
        public int hashCode() {
            return phase.hashCode();
        }
    }

    public static class TestPersistentTasksExecutor extends PersistentTasksExecutor<TestParams> {

        private static final Logger logger = LogManager.getLogger(TestPersistentTasksExecutor.class);

        public static final String NAME = "cluster:admin/persistent/test";
        private final ClusterService clusterService;

        private static volatile boolean nonClusterStateCondition = true;

        public TestPersistentTasksExecutor(ClusterService clusterService) {
            super(NAME, ThreadPool.Names.GENERIC);
            this.clusterService = clusterService;
        }

        public static void setNonClusterStateCondition(boolean nonClusterStateCondition) {
            TestPersistentTasksExecutor.nonClusterStateCondition = nonClusterStateCondition;
        }

        @Override
        public Assignment getAssignment(TestParams params, Collection<DiscoveryNode> candidateNodes, ClusterState clusterState) {
            if (nonClusterStateCondition == false) {
                return new Assignment(null, "non cluster state condition prevents assignment");
            }
            if (params == null || params.getExecutorNodeAttr() == null) {
                return super.getAssignment(params, candidateNodes, clusterState);
            } else {
                DiscoveryNode executorNode = selectLeastLoadedNode(clusterState, candidateNodes,
                        discoveryNode -> params.getExecutorNodeAttr().equals(discoveryNode.getAttributes().get("test_attr")));
                if (executorNode != null) {
                    return new Assignment(executorNode.getId(), "test assignment");
                } else {
                    return NO_NODE_FOUND;
                }
            }
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestParams params, PersistentTaskState state) {
            logger.info("started node operation for the task {}", task);
            try {
                TestTask testTask = (TestTask) task;
                AtomicInteger phase = new AtomicInteger();
                while (true) {
                    // wait for something to happen
                    try {
                        assertBusy(() -> assertTrue(testTask.isCancelled() ||
                                testTask.getOperation() != null ||
                                clusterService.lifecycleState() != Lifecycle.State.STARTED),   // speedup finishing on closed nodes
                            45, TimeUnit.SECONDS); // This can take a while during large cluster restart
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }

                    if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
                        return;
                    }
                    if ("finish".equals(testTask.getOperation())) {
                        task.markAsCompleted();
                        return;
                    } else if ("fail".equals(testTask.getOperation())) {
                        task.markAsFailed(new RuntimeException("Simulating failure"));
                        return;
                    }  else if ("abort_locally".equals(testTask.getOperation())) {
                        task.markAsLocallyAborted("Simulating local abort");
                        return;
                    } else if ("update_status".equals(testTask.getOperation())) {
                        testTask.setOperation(null);
                        CountDownLatch latch = new CountDownLatch(1);
                        State newState = new State("phase " + phase.incrementAndGet());
                        logger.info("updating the task state to {}", newState);
                        task.updatePersistentTaskState(newState, new ActionListener<PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTask<?> persistentTask) {
                                logger.info("updating was successful");
                                latch.countDown();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.info("updating failed", e);
                                latch.countDown();
                                fail(e.toString());
                            }
                        });
                        assertTrue(latch.await(10, TimeUnit.SECONDS));
                    } else if (testTask.isCancelled()) {
                        // Cancellation make cause different ways for the task to finish
                        if (randomBoolean()) {
                            if (randomBoolean()) {
                                task.markAsFailed(new TaskCancelledException(testTask.getReasonCancelled()));
                            } else {
                                task.markAsCompleted();
                            }
                        } else {
                            task.markAsFailed(new RuntimeException(testTask.getReasonCancelled()));
                        }
                        return;
                    } else {
                        fail("We really shouldn't be here");
                    }
                }
            } catch (InterruptedException e) {
                task.markAsFailed(e);
            }
        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTask<TestParams> task, Map<String, String> headers) {
            return new TestTask(id, type, action, getDescription(task), parentTaskId, headers);
        }
    }

    public static class TestTaskAction extends ActionType<TestTasksResponse> {

        public static final TestTaskAction INSTANCE = new TestTaskAction();
        public static final String NAME = "cluster:admin/persistent/task_test";

        private TestTaskAction() {
            super(NAME, TestTasksResponse::new);
        }
    }


    public static class TestTask extends AllocatedPersistentTask {
        private volatile String operation;

        public TestTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
            super(id, type, action, description, parentTask, headers);
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        @Override
        public String toString() {
            return "TestTask[" + this.getId() + ", " + this.getParentTaskId() + ", " + this.getOperation() + "]";
        }
    }

    static class TestTaskResponse implements Writeable {

        TestTaskResponse() {

        }

        TestTaskResponse(StreamInput in) throws IOException {
            in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(true);
        }
    }

    public static class TestTasksRequest extends BaseTasksRequest<TestTasksRequest> {
        private String operation;

        public TestTasksRequest() {
        }

        public TestTasksRequest(StreamInput in) throws IOException {
            super(in);
            operation = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(operation);
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public String getOperation() {
            return operation;
        }

    }

    public static class TestTasksRequestBuilder extends TasksRequestBuilder<TestTasksRequest, TestTasksResponse, TestTasksRequestBuilder> {

        protected TestTasksRequestBuilder(ElasticsearchClient client) {
            super(client, TestTaskAction.INSTANCE, new TestTasksRequest());
        }

        public TestTasksRequestBuilder setOperation(String operation) {
            request.setOperation(operation);
            return this;
        }
    }

    public static class TestTasksResponse extends BaseTasksResponse {

        private List<TestTaskResponse> tasks;

        public TestTasksResponse(List<TestTaskResponse> tasks, List<TaskOperationFailure> taskFailures,
                                 List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : List.copyOf(tasks);
        }

        public TestTasksResponse(StreamInput in) throws IOException {
            super(in);
            tasks = in.readList(TestTaskResponse::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(tasks);
        }

        public List<TestTaskResponse> getTasks() {
            return tasks;
        }
    }

    public static class TransportTestTaskAction extends TransportTasksAction<TestTask,
            TestTasksRequest, TestTasksResponse, TestTaskResponse> {

        @Inject
        public TransportTestTaskAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
            super(TestTaskAction.NAME, clusterService, transportService, actionFilters,
                TestTasksRequest::new, TestTasksResponse::new, TestTaskResponse::new, ThreadPool.Names.MANAGEMENT);
        }

        @Override
        protected TestTasksResponse newResponse(TestTasksRequest request, List<TestTaskResponse> tasks,
                                                List<TaskOperationFailure> taskOperationFailures,
                                                List<FailedNodeException> failedNodeExceptions) {
            return new TestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected void taskOperation(TestTasksRequest request, TestTask task, ActionListener<TestTaskResponse> listener) {
            task.setOperation(request.operation);
            listener.onResponse(new TestTaskResponse());
        }

    }

}
