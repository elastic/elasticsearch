/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.test.ESTestCase.awaitBusy;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A plugin that adds a test persistent task.
 */
public class TestPersistentActionPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(TestPersistentAction.INSTANCE, TransportTestPersistentAction.class),
                new ActionHandler<>(TestTaskAction.INSTANCE, TransportTestTaskAction.class),
                new ActionHandler<>(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class),
                new ActionHandler<>(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class),
                new ActionHandler<>(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class),
                new ActionHandler<>(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class)
        );
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry) {

        PersistentActionService persistentActionService = new PersistentActionService(Settings.EMPTY, threadPool, clusterService, client);
        PersistentActionRegistry persistentActionRegistry = new PersistentActionRegistry(Settings.EMPTY);
        return Arrays.asList(
                persistentActionService,
                persistentActionRegistry,
                new PersistentTaskClusterService(Settings.EMPTY, persistentActionRegistry, clusterService)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                new NamedWriteableRegistry.Entry(PersistentActionRequest.class, TestPersistentAction.NAME, TestRequest::new),
                new NamedWriteableRegistry.Entry(Task.Status.class,
                        PersistentActionCoordinator.Status.NAME, PersistentActionCoordinator.Status::new),
                new NamedWriteableRegistry.Entry(ClusterState.Custom.class, PersistentTasksInProgress.TYPE, PersistentTasksInProgress::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, PersistentTasksInProgress.TYPE, PersistentTasksInProgress::readDiffFrom),
                new NamedWriteableRegistry.Entry(Task.Status.class, Status.NAME, Status::new)
        );
    }

    public static class TestRequest extends PersistentActionRequest {

        private String executorNodeAttr = null;

        private String responseNode = null;

        private String testParam = null;

        public TestRequest() {

        }

        public TestRequest(String testParam) {
            this.testParam = testParam;
        }

        public TestRequest(StreamInput in) throws IOException {
            readFrom(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getWriteableName() {
            return TestPersistentAction.NAME;
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
            super.writeTo(out);
            out.writeOptionalString(executorNodeAttr);
            out.writeOptionalString(responseNode);
            out.writeOptionalString(testParam);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            executorNodeAttr = in.readOptionalString();
            responseNode = in.readOptionalString();
            testParam = in.readOptionalString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestRequest that = (TestRequest) o;
            return Objects.equals(executorNodeAttr, that.executorNodeAttr) &&
                    Objects.equals(responseNode, that.responseNode) &&
                    Objects.equals(testParam, that.testParam);
        }

        @Override
        public int hashCode() {
            return Objects.hash(executorNodeAttr, responseNode, testParam);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new TestTask(id, type, action, getDescription(), parentTaskId);
        }
    }

    public static class TestPersistentTaskRequestBuilder extends
            ActionRequestBuilder<TestRequest, PersistentActionResponse, TestPersistentTaskRequestBuilder> {

        protected TestPersistentTaskRequestBuilder(ElasticsearchClient client, Action<TestRequest, PersistentActionResponse,
                TestPersistentTaskRequestBuilder> action, TestRequest request) {
            super(client, action, request);
        }

        public TestPersistentTaskRequestBuilder testParam(String testParam) {
            request.setTestParam(testParam);
            return this;
        }

        public TestPersistentTaskRequestBuilder executorNodeAttr(String targetNode) {
            request.setExecutorNodeAttr(targetNode);
            return this;
        }

    }

    public static class TestPersistentAction extends Action<TestRequest, PersistentActionResponse, TestPersistentTaskRequestBuilder> {

        public static final TestPersistentAction INSTANCE = new TestPersistentAction();
        public static final String NAME = "cluster:admin/persistent/test";

        private TestPersistentAction() {
            super(NAME);
        }

        @Override
        public PersistentActionResponse newResponse() {
            return new PersistentActionResponse();
        }

        @Override
        public TestPersistentTaskRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new TestPersistentTaskRequestBuilder(client, this, new TestRequest());
        }
    }

    public static class Status implements Task.Status {
        public static final String NAME = "test";

        private final String phase;

        public Status(String phase) {
            this.phase = requireNonNull(phase, "Phase cannot be null");
        }

        public Status(StreamInput in) throws IOException {
            phase = in.readString();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("phase", phase);
            builder.endObject();
            return builder;
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
            if (obj == null || obj.getClass() != Status.class) {
                return false;
            }
            Status other = (Status) obj;
            return phase.equals(other.phase);
        }

        @Override
        public int hashCode() {
            return phase.hashCode();
        }
    }


    public static class TransportTestPersistentAction extends TransportPersistentAction<TestRequest> {

        private final TransportService transportService;

        @Inject
        public TransportTestPersistentAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                             PersistentActionService persistentActionService,
                                             PersistentActionRegistry persistentActionRegistry, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, TestPersistentAction.NAME, false, threadPool, transportService, persistentActionService,
                    persistentActionRegistry, actionFilters, indexNameExpressionResolver, TestRequest::new,
                    ThreadPool.Names.MANAGEMENT);
            this.transportService = transportService;
        }

        @Override
        public DiscoveryNode executorNode(TestRequest request, ClusterState clusterState) {
            if (request.getExecutorNodeAttr() == null) {
                return super.executorNode(request, clusterState);
            } else {
                return selectLeastLoadedNode(clusterState,
                        discoveryNode -> request.getExecutorNodeAttr().equals(discoveryNode.getAttributes().get("test_attr")));
            }
        }

        @Override
        protected void nodeOperation(PersistentTask task, TestRequest request, ActionListener<Empty> listener) {
            logger.info("started node operation for the task {}", task);
            try {
                TestTask testTask = (TestTask) task;
                AtomicInteger phase = new AtomicInteger();
                while (true) {
                    // wait for something to happen
                    assertTrue(awaitBusy(() -> testTask.isCancelled() ||
                            testTask.getOperation() != null ||
                            transportService.lifecycleState() != Lifecycle.State.STARTED)); // speedup finishing on closed nodes
                    if (transportService.lifecycleState() != Lifecycle.State.STARTED) {
                        return;
                    }
                    if ("finish".equals(testTask.getOperation())) {
                        listener.onResponse(Empty.INSTANCE);
                        return;
                    } else if ("fail".equals(testTask.getOperation())) {
                        listener.onFailure(new RuntimeException("Simulating failure"));
                        return;
                    } else if ("update_status".equals(testTask.getOperation())) {
                        testTask.setOperation(null);
                        CountDownLatch latch = new CountDownLatch(1);
                        Status status = new Status("phase " + phase.incrementAndGet());
                        logger.info("updating the task status to {}", status);
                        updatePersistentTaskStatus(task, status, new ActionListener<Empty>() {
                            @Override
                            public void onResponse(Empty empty) {
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
                                listener.onFailure(new TaskCancelledException(testTask.getReasonCancelled()));
                            } else {
                                listener.onResponse(Empty.INSTANCE);
                            }
                        } else {
                            listener.onFailure(new RuntimeException(testTask.getReasonCancelled()));
                        }
                        return;
                    } else {
                        fail("We really shouldn't be here");
                    }
                }
            } catch (InterruptedException e) {
                listener.onFailure(e);
            }
        }
    }

    public static class TestTaskAction extends Action<TestTasksRequest, TestTasksResponse, TestTasksRequestBuilder> {

        public static final TestTaskAction INSTANCE = new TestTaskAction();
        public static final String NAME = "cluster:admin/persistent/task_test";

        private TestTaskAction() {
            super(NAME);
        }

        @Override
        public TestTasksResponse newResponse() {
            return new TestTasksResponse();
        }

        @Override
        public TestTasksRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new TestTasksRequestBuilder(client);
        }
    }


    public static class TestTask extends PersistentTask {
        private volatile String operation;

        public TestTask(long id, String type, String action, String description, TaskId parentTask) {
            super(id, type, action, description, parentTask);
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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

        public TestTasksResponse() {

        }

        public TestTasksResponse(List<TestTaskResponse> tasks, List<TaskOperationFailure> taskFailures,
                                 List<? extends FailedNodeException> nodeFailures) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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
        public TransportTestTaskAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, String nodeExecutor) {
            super(settings, TestTaskAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                    TestTasksRequest::new, TestTasksResponse::new, ThreadPool.Names.MANAGEMENT);
        }

        @Override
        protected TestTasksResponse newResponse(TestTasksRequest request, List<TestTaskResponse> tasks,
                                                List<TaskOperationFailure> taskOperationFailures,
                                                List<FailedNodeException> failedNodeExceptions) {
            return new TestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected TestTaskResponse readTaskResponse(StreamInput in) throws IOException {
            return new TestTaskResponse(in);
        }

        @Override
        protected void taskOperation(TestTasksRequest request, TestTask task, ActionListener<TestTaskResponse> listener) {
            task.setOperation(request.operation);
            listener.onResponse(new TestTaskResponse());
        }

        @Override
        protected boolean accumulateExceptions() {
            return false;
        }
    }


}