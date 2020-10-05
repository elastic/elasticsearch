/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestSubmitAsyncSearchActionTests extends RestActionTestCase {

    private RestSubmitAsyncSearchAction action;
    private ActionRequest lastCapturedRequest;
    private RestController controller;
    private NodeClient nodeClient;

    @Before
    public void setUpController() {
        nodeClient = new NodeClient(Settings.EMPTY, null) {

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                    Request request, ActionListener<Response> listener) {
                lastCapturedRequest = request;
                return new Task(1L, "type", "action", "description", null, null);
            }
        };
        nodeClient.initialize(new HashMap<>(), () -> "local", null, new NamedWriteableRegistry(Collections.emptyList()));
        controller = new RestController(Collections.emptySet(), null,
            nodeClient,
            new NoneCircuitBreakerService(),
            new UsageService());
        action = new RestSubmitAsyncSearchAction();
        controller.registerHandler(action);
    }

    /**
     * Check that the appropriate defaults are set on the {@link SubmitAsyncSearchRequest} if
     * no parameters are specified on the rest request itself.
     */
    public void testRequestParameterDefaults() throws IOException {
            RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.POST)
                .withPath("/test_index/_async_search")
                .withContent(new BytesArray("{}"), XContentType.JSON)
                .build();
            dispatchRequest(submitAsyncRestRequest);
            SubmitAsyncSearchRequest submitRequest = (SubmitAsyncSearchRequest) lastCapturedRequest;
            assertEquals(TimeValue.timeValueSeconds(1), submitRequest.getWaitForCompletionTimeout());
            assertFalse(submitRequest.isKeepOnCompletion());
            assertEquals(TimeValue.timeValueDays(5), submitRequest.getKeepAlive());
            // check parameters we implicitly set in the SubmitAsyncSearchRequest ctor
            assertFalse(submitRequest.getSearchRequest().isCcsMinimizeRoundtrips());
            assertEquals(5, submitRequest.getSearchRequest().getBatchedReduceSize());
            assertEquals(true, submitRequest.getSearchRequest().requestCache());
            assertEquals(1, submitRequest.getSearchRequest().getPreFilterShardSize().intValue());
    }

    public void testParameters() throws IOException {
        String tvString = randomTimeValue(1, 100);
        doTestParameter("keep_alive", tvString, TimeValue.parseTimeValue(tvString, ""), SubmitAsyncSearchRequest::getKeepAlive);
        doTestParameter("wait_for_completion_timeout", tvString, TimeValue.parseTimeValue(tvString, ""),
                SubmitAsyncSearchRequest::getWaitForCompletionTimeout);
        boolean keepOnCompletion = randomBoolean();
        doTestParameter("keep_on_completion", Boolean.toString(keepOnCompletion), keepOnCompletion,
                SubmitAsyncSearchRequest::isKeepOnCompletion);
        boolean requestCache = randomBoolean();
        doTestParameter("request_cache", Boolean.toString(requestCache), requestCache,
                r -> r.getSearchRequest().requestCache());
        int batchedReduceSize = randomIntBetween(2, 50);
        doTestParameter("batched_reduce_size", Integer.toString(batchedReduceSize), batchedReduceSize,
                r -> r.getSearchRequest().getBatchedReduceSize());
    }

    private <T> void doTestParameter(String paramName, String paramValue, T expectedValue,
            Function<SubmitAsyncSearchRequest, T> valueAccessor) {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> {
            assertThat(request, instanceOf(SubmitAsyncSearchRequest.class));
            assertThat(valueAccessor.apply((SubmitAsyncSearchRequest) request), equalTo(expectedValue));
            executeCalled.set(true);
            return null;
        });
        Map<String, String> params = new HashMap<>();
        params.put(paramName, paramValue);
        RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/test_index/_async_search")
            .withParams(params)
            .withContent(new BytesArray("{}"), XContentType.JSON).build();

        // Get a new context each time, so we don't get exceptions due to trying to add the same header multiple times
        try (ThreadContext.StoredContext context = verifyingClient.threadPool().getThreadContext().stashContext()) {
            dispatchRequest(submitAsyncRestRequest);
        }
        assertThat(executeCalled.get(), equalTo(true));
        verifyingClient.reset();
    }

    /**
     * Sends the given request to the test controller
     */
    protected void dispatchRequest(RestRequest request) {
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller.dispatchRequest(request, channel, threadContext);
    }
}
