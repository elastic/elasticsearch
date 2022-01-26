/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestSubmitAsyncSearchActionTests extends RestActionTestCase {

    private RestSubmitAsyncSearchAction action;

    @Before
    public void setUpAction() {
        action = new RestSubmitAsyncSearchAction();
        controller().registerHandler(action);
    }

    /**
     * Check that the appropriate defaults are set on the {@link SubmitAsyncSearchRequest} if
     * no parameters are specified on the rest request itself.
     */
    @SuppressWarnings("unchecked")
    public void testRequestParameterDefaults() throws IOException {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> {
            assertThat(request, instanceOf(SubmitAsyncSearchRequest.class));
            SubmitAsyncSearchRequest submitRequest = (SubmitAsyncSearchRequest) request;
            assertThat(submitRequest.getWaitForCompletionTimeout(), equalTo(TimeValue.timeValueSeconds(1)));
            assertThat(submitRequest.isKeepOnCompletion(), equalTo(false));
            assertThat(submitRequest.getKeepAlive(), equalTo(TimeValue.timeValueDays(5)));
            // check parameters we implicitly set in the SubmitAsyncSearchRequest ctor
            assertThat(submitRequest.getSearchRequest().isCcsMinimizeRoundtrips(), equalTo(false));
            assertThat(submitRequest.getSearchRequest().getBatchedReduceSize(), equalTo(5));
            assertThat(submitRequest.getSearchRequest().requestCache(), equalTo(true));
            assertThat(submitRequest.getSearchRequest().getPreFilterShardSize().intValue(), equalTo(1));
            executeCalled.set(true);
            return mock(ActionResponse.class);
        });
        RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/test_index/_async_search")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(submitAsyncRestRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testParameters() throws IOException {
        String tvString = randomTimeValue(1, 100);
        doTestParameter("keep_alive", tvString, TimeValue.parseTimeValue(tvString, ""), SubmitAsyncSearchRequest::getKeepAlive);
        doTestParameter(
            "wait_for_completion_timeout",
            tvString,
            TimeValue.parseTimeValue(tvString, ""),
            SubmitAsyncSearchRequest::getWaitForCompletionTimeout
        );
        boolean keepOnCompletion = randomBoolean();
        doTestParameter(
            "keep_on_completion",
            Boolean.toString(keepOnCompletion),
            keepOnCompletion,
            SubmitAsyncSearchRequest::isKeepOnCompletion
        );
        boolean requestCache = randomBoolean();
        doTestParameter("request_cache", Boolean.toString(requestCache), requestCache, r -> r.getSearchRequest().requestCache());
        int batchedReduceSize = randomIntBetween(2, 50);
        doTestParameter(
            "batched_reduce_size",
            Integer.toString(batchedReduceSize),
            batchedReduceSize,
            r -> r.getSearchRequest().getBatchedReduceSize()
        );
    }

    private <T> void doTestParameter(
        String paramName,
        String paramValue,
        T expectedValue,
        Function<SubmitAsyncSearchRequest, T> valueAccessor
    ) {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> {
            assertThat(request, instanceOf(SubmitAsyncSearchRequest.class));
            assertThat(valueAccessor.apply((SubmitAsyncSearchRequest) request), equalTo(expectedValue));
            executeCalled.set(true);
            return new AsyncSearchResponse("", randomBoolean(), randomBoolean(), 0L, 0L);
        });
        Map<String, String> params = new HashMap<>();
        params.put(paramName, paramValue);
        RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/test_index/_async_search")
            .withParams(params)
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

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
        controller().dispatchRequest(request, channel, threadContext);
    }
}
