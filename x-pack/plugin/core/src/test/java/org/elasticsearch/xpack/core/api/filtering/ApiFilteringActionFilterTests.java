/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.api.filtering;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class ApiFilteringActionFilterTests extends ESTestCase {

    public void testApply() {
        boolean isOperator = randomBoolean();
        final ThreadContext threadContext = getTestThreadContext(isOperator);
        String action = "test.action";
        ApiFilteringActionFilter<TestResponse> filter = new TestFilter(threadContext);
        Task task = null;
        TestRequest request = new TestRequest();
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        AtomicBoolean responseModified = new AtomicBoolean(false);
        ActionListener<TestResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                listenerCalled.set(true);
                responseModified.set(testResponse.modified);
            }

            @Override
            public void onFailure(Exception e) {
                fail(Strings.format("Unexpected exception: %s", e.getMessage()));
            }
        };
        ActionFilterChain<TestRequest, TestResponse> chain = (task1, action1, request1, listener1) -> {
            listener1.onResponse(new TestResponse());
        };
        filter.apply(task, "wrong.action", request, listener, chain);
        assertThat(listenerCalled.get(), equalTo(true));
        assertThat(responseModified.get(), equalTo(false));
        filter.apply(task, action, request, listener, chain);
        assertThat(listenerCalled.get(), equalTo(true));
        // The response should only have been modified if we are not an operator user
        assertThat(responseModified.get(), equalTo(isOperator == false));
    }

    public void testApplyWithException() {
        /*
         * This test makes sure that we have correct behavior if the filter function throws an exception. In that case we expect
         * onFailure() to be called on the delegate listener for a non-operator user. For an operator user, we expect onResponse() to be
         * called on the delegate listener because the filter function will never have been called.
         */
        boolean isOperator = randomBoolean();
        final ThreadContext threadContext = getTestThreadContext(isOperator);
        String action = "test.exception.action";
        ApiFilteringActionFilter<TestResponse> filter = new ExceptionTestFilter(threadContext);
        Task task = null;
        TestRequest request = new TestRequest();
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        ActionListener<TestResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                listenerCalled.set(true);
                assertThat(isOperator, equalTo(true));
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalled.set(true);
                assertThat(isOperator, equalTo(false));
            }
        };
        ActionFilterChain<TestRequest, TestResponse> chain = (task1, action1, request1, listener1) -> {
            listener1.onResponse(new TestResponse());
        };
        filter.apply(task, action, request, listener, chain);
        assertThat(listenerCalled.get(), equalTo(true));
    }

    private static class TestFilter extends ApiFilteringActionFilter<TestResponse> {

        TestFilter(ThreadContext threadContext) {
            super(threadContext, "test.action", TestResponse.class);
        }

        @Override
        protected TestResponse filterResponse(TestResponse response) {
            response.modified = true;
            return response;
        }
    }

    private static class ExceptionTestFilter extends ApiFilteringActionFilter<TestResponse> {

        ExceptionTestFilter(ThreadContext threadContext) {
            super(threadContext, "test.exception.action", TestResponse.class);
        }

        @Override
        protected TestResponse filterResponse(TestResponse response) {
            throw new RuntimeException("Throwing expected exception");
        }
    }

    private ThreadContext getTestThreadContext(boolean isOperator) {
        Settings settings;
        if (isOperator) {
            settings = Settings.builder().put("request.headers._security_privilege_category", "operator").build();
        } else {
            settings = Settings.EMPTY;
        }
        return new ThreadContext(settings);
    }

    private static class TestRequest extends LegacyActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {
        boolean modified = false;

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
