/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

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
            RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.POST)
                .withPath("/test_index/_async_search")
                .withContent(new BytesArray("{}"), XContentType.JSON)
                .build();
            dispatchRequest(submitAsyncRestRequest);
            ArgumentCaptor<SubmitAsyncSearchRequest> argumentCaptor = ArgumentCaptor.forClass(SubmitAsyncSearchRequest.class);
            verify(nodeClient).executeLocally(any(ActionType.class), argumentCaptor.capture(), any(ActionListener.class));
            SubmitAsyncSearchRequest submitRequest = argumentCaptor.getValue();
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

    @SuppressWarnings("unchecked")
    private <T> void doTestParameter(String paramName, String paramValue, T expectedValue,
            Function<SubmitAsyncSearchRequest, T> valueAccessor) {
        Map<String, String> params = new HashMap<>();
        params.put(paramName, paramValue);
        RestRequest submitAsyncRestRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/test_index/_async_search")
                .withParams(params)
                .withContent(new BytesArray("{}"), XContentType.JSON).build();
        ArgumentCaptor<SubmitAsyncSearchRequest> argumentCaptor = ArgumentCaptor.forClass(SubmitAsyncSearchRequest.class);
        dispatchRequest(submitAsyncRestRequest);
        verify(nodeClient).executeLocally(any(ActionType.class), argumentCaptor.capture(), any(ActionListener.class));
        SubmitAsyncSearchRequest submitRequest = argumentCaptor.getValue();
        assertEquals(expectedValue, valueAccessor.apply(submitRequest));
        reset(nodeClient);
    }
}
