/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestResponseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RestReindexRethrottleActionTests extends RestActionTestCase {

    private static final Settings STATEFUL_SETTINGS = Settings.EMPTY;
    private static final Settings STATELESS_SETTINGS = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();

    private String nodeId;
    private TaskId taskId;
    private float requestsPerSecond;
    private ExecutorService transportExecutor;

    @Before
    public void initializeRequestParameters() {
        nodeId = randomIdentifier();
        taskId = new TaskId(nodeId, randomLongBetween(1, 1000));
        requestsPerSecond = randomIntBetween(1, 1000); // use an int to be sure it round-trips via the URL parameter
    }

    @Before
    public void setUpFakeTransportLayer() {
        verifyingClient.setExecuteVerifier(this::fakeTransportAction);
    }

    @Before
    public void initializeTransportExecutor() {
        transportExecutor = newSingleThreadExecutor(this::createTransportThread);
    }

    @After
    public void shutdownTransportExecutor() {
        transportExecutor.shutdown();
    }

    public void testStateful_groupByDefault_isGroupedByNode() throws Exception {
        registerHandler(STATEFUL_SETTINGS);
        RestRequest request = createRestRequest(null);
        ObjectPath body = execute(request, RestStatus.OK);
        // Expect structure "nodes" -> nodeId -> "tasks" -> taskId -> taskInfo
        assertTaskInfo(body.evaluate("nodes." + nodeId + ".tasks." + taskId));
    }

    public void testStateful_groupByNone() throws Exception {
        registerHandler(STATEFUL_SETTINGS);
        RestRequest request = createRestRequest("none");
        ObjectPath body = execute(request, RestStatus.OK);
        // Expect structure "nodes" -> list of taskInfo
        assertTaskInfo(body.evaluate("tasks.0"));
    }

    public void testStateless_groupByDefault_isNotGrouped() throws Exception {
        registerHandler(STATELESS_SETTINGS);
        RestRequest request = createRestRequest(null);
        ObjectPath body = execute(request, RestStatus.OK);
        // Expect structure "nodes" -> list of taskInfo
        assertTaskInfo(body.evaluate("tasks.0"));
    }

    public void testStateless_groupByNodes_fails() throws Exception {
        registerHandler(STATELESS_SETTINGS);
        RestRequest request = createRestRequest("nodes");
        // Expect a 400 response, with a body where "error" -> "reason" mentions the illegal "group_by" parameter
        ObjectPath body = execute(request, RestStatus.BAD_REQUEST);
        String reason = body.evaluate("error.reason");
        assertThat(reason, containsString("group_by"));
    }

    /**
     * Fakes the behavior of the rethrottle transport action. Returns a {@link ListTasksResponse} containing a single {@link TaskInfo} with
     * the task ID and requests-per-second values from the {@link RethrottleRequest}, just like the real transport action would.
     */
    private ListTasksResponse fakeTransportAction(ActionType<ListTasksResponse> actionType, ActionRequest actionRequest) {
        assertThat(actionType, equalTo(ReindexPlugin.RETHROTTLE_ACTION));
        RethrottleRequest rethrottleRequest = asInstanceOf(RethrottleRequest.class, actionRequest);
        TaskInfo task = new TaskInfo(
            rethrottleRequest.getTargetTaskId(),
            "transport",
            nodeId,
            ReindexPlugin.RETHROTTLE_ACTION.name(),
            "doing a reindex",
            new BulkByPaginatedSearchTask.Status(
                0,
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomIntBetween(1, 100),
                randomTimeValue(),
                rethrottleRequest.getRequestsPerSecond(),
                null,
                randomTimeValue()
            ),
            randomMillisUpToYear9999(),
            randomLongBetween(1, 1000000000),
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        return new ListTasksResponse(List.of(task), List.of(), List.of());
    }

    /**
     * Registers the rethrottle REST handler.
     */
    private void registerHandler(Settings settings) {
        controller().registerHandler(new RestReindexRethrottleAction(() -> DiscoveryNodes.EMPTY_NODES, settings));
    }

    /**
     * Creates a REST request to the rethrottle endpoint, using the stored {@link #taskId} and {@link #requestsPerSecond}, and the
     * {@code groupBy} specified if non-null.
     */
    private RestRequest createRestRequest(@Nullable String groupBy) {
        HashMap<String, String> params = new HashMap<>();
        params.put("requests_per_second", Float.toString(requestsPerSecond));
        if (groupBy != null) {
            params.put("group_by", groupBy);
        }
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_reindex/" + taskId + "/_rethrottle")
            .withParams(params)
            .build();
    }

    /**
     * Dispatches the given {@link RestRequest} to the REST controller, asserts that the response has the expected status, and returns the
     * contents of the response body as an {@link ObjectPath}.
     */
    private ObjectPath execute(RestRequest request, RestStatus expectedStatus) throws Exception {
        FakeRestChannel channel = new FakeRestChannel(request, true);
        ThreadContext threadContext = verifyingClient.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            controller().dispatchRequest(request, channel, threadContext);
            try (RestResponse response = channel.capturedResponse()) {
                assertThat(response.status(), equalTo(expectedStatus));
                // The response is chunked, so we have to extract it on a thread which the RestController thinks is a transport thread:
                BytesReference bodyContent = transportExecutor.submit(() -> RestResponseUtils.getBodyContent(response)).get();
                XContent xContent = requireNonNull(XContentType.fromMediaType(response.contentType())).xContent();
                return ObjectPath.createFromXContent(xContent, bodyContent);
            }
        }
    }

    /**
     * Creates a thread to execute the given {@link Runnable}. The thread will have a name which means the REST controller will recognize as
     * a transport thread.
     */
    private Thread createTransportThread(Runnable runnable) {
        return new Thread(runnable, Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + "_" + randomIdentifier());
    }

    private void assertTaskInfo(Map<String, Object> taskInfoMap) {
        assertThat(taskInfoMap, notNullValue());
        assertThat(taskInfoMap.get("node"), equalTo(nodeId));
        assertThat(asInstanceOf(Number.class, taskInfoMap.get("id")).longValue(), equalTo(taskId.getId()));
        Map<?, ?> status = asInstanceOf(Map.class, taskInfoMap.get("status"));
        assertThat(asInstanceOf(Number.class, status.get("requests_per_second")).floatValue(), equalTo(requestsPerSecond));
    }
}
