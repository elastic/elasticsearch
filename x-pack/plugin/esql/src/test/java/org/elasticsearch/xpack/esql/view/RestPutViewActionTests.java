/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestPutViewActionTests extends ESTestCase {

    private static final String WARNING = "view [v] query is not valid: Unknown index [missing]";

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /**
     * Both the validation and the put complete on another thread, so the warning is already produced before the handler goes on to the put
     * and must still be on the response when it is sent.
     */
    public void testValidationWarningIsOnResponseWhenActionsCompleteOnAnotherThread() throws Exception {
        assertWarnings(true);
    }

    public void testValidationWarningIsOnResponseWhenActionsCompleteSynchronously() throws Exception {
        assertWarnings(false);
    }

    public void testNoWarningWithoutValidationWarning() throws Exception {
        assertThat(sendPut(true, true, null), nullValue());
    }

    public void testNoWarningWhenValidationIsDisabled() throws Exception {
        assertThat(sendPut(randomBoolean(), false, WARNING), nullValue());
    }

    private void assertWarnings(boolean async) throws Exception {
        List<String> warnings = sendPut(async, true, WARNING);
        assertThat(warnings, notNullValue());
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString(WARNING));
    }

    /**
     * Sends a PUT request through {@link RestPutViewAction} and returns the {@code Warning} response headers as they are when the REST
     * response is sent.
     */
    private List<String> sendPut(boolean async, boolean validate, String validationWarning) throws Exception {
        final var client = new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                // A real client hands back to the context of the caller, whatever the thread the action completes on.
                final ActionListener<Response> restoring = new ContextPreservingActionListener<>(
                    threadContext.newRestorableContext(false),
                    listener
                );
                final Response response;
                if (action == ValidateViewAction.INSTANCE) {
                    response = (Response) new ValidateViewAction.Response(validationWarning);
                } else if (action == PutViewAction.INSTANCE) {
                    response = (Response) AcknowledgedResponse.TRUE;
                } else {
                    throw new AssertionError("unexpected action [" + action.name() + "]");
                }
                if (async) {
                    threadPool.generic().execute(() -> restoring.onResponse(response));
                } else {
                    restoring.onResponse(response);
                }
            }
        };

        final var sent = new CountDownLatch(1);
        final var headers = new AtomicReference<List<String>>();
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_query/view/v")
            .withParams(Map.of("name", "v", "validate", Boolean.toString(validate)))
            .withContent(new BytesArray("{\"query\":\"FROM missing\"}"), XContentType.JSON)
            .build();
        final var channel = new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {
                headers.set(threadContext.getResponseHeaders().get("Warning"));
                sent.countDown();
            }
        };

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            new RestPutViewAction().handleRequest(request, channel, client);
        }
        assertTrue("REST response was not sent", sent.await(10, TimeUnit.SECONDS));
        return headers.get();
    }
}
