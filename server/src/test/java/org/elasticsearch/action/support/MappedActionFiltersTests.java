/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

public class MappedActionFiltersTests extends ESTestCase {

    static class DummyRequest extends LegacyActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class DummyResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public void testNoMappedFilters() {
        var actionFilter = new MappedActionFilters(List.of());
        AtomicBoolean proceedCalled = new AtomicBoolean();
        var chain = new ActionFilterChain<DummyRequest, DummyResponse>() {
            @Override
            public void proceed(Task task, String action, DummyRequest request, ActionListener<DummyResponse> listener) {
                proceedCalled.set(true);
            }
        };
        actionFilter.apply(null, "dnm", null, null, chain);
        assertThat(proceedCalled.get(), is(true));
    }

    public void testSingleMappedFilters() {
        AtomicBoolean applyCalled = new AtomicBoolean();
        AtomicBoolean proceedCalled = new AtomicBoolean();

        MappedActionFilter filter = new MappedActionFilter() {
            @Override
            public String actionName() {
                return "dummyAction";
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                applyCalled.set(true);
                chain.proceed(task, action, request, listener);
            }
        };
        var actionFilter = new MappedActionFilters(List.of(filter));

        var chain = new ActionFilterChain<DummyRequest, DummyResponse>() {
            @Override
            public void proceed(Task task, String action, DummyRequest request, ActionListener<DummyResponse> listener) {
                assertThat("mapped filter should be called first", applyCalled.get(), is(true));
                proceedCalled.set(true);
            }
        };
        actionFilter.apply(null, "dummyAction", null, null, chain);
        assertThat(proceedCalled.get(), is(true));
    }

    public void testMultipleMappedFilters() {
        AtomicBoolean apply1Called = new AtomicBoolean();
        AtomicBoolean apply2Called = new AtomicBoolean();
        AtomicBoolean proceedCalled = new AtomicBoolean();

        MappedActionFilter filter1 = new MappedActionFilter() {
            @Override
            public String actionName() {
                return "dummyAction";
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                apply1Called.set(true);
                chain.proceed(task, action, request, listener);
            }
        };
        MappedActionFilter filter2 = new MappedActionFilter() {
            @Override
            public String actionName() {
                return "dummyAction";
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                assertThat("filter1 should be called first", apply1Called.get(), is(true));
                apply2Called.set(true);
                chain.proceed(task, action, request, listener);
            }
        };
        var actionFilter = new MappedActionFilters(List.of(filter1, filter2));

        var chain = new ActionFilterChain<DummyRequest, DummyResponse>() {
            @Override
            public void proceed(Task task, String action, DummyRequest request, ActionListener<DummyResponse> listener) {
                assertThat("filter2 should be called before outer proceed", apply2Called.get(), is(true));
                proceedCalled.set(true);
            }
        };
        actionFilter.apply(null, "dummyAction", null, null, chain);
        assertThat(proceedCalled.get(), is(true));
    }

    public void testSkipOtherAction() {
        AtomicBoolean applyCalled = new AtomicBoolean();
        AtomicBoolean proceedCalled = new AtomicBoolean();

        MappedActionFilter filter = new MappedActionFilter() {
            @Override
            public String actionName() {
                return "dummyAction";
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                applyCalled.set(true);
                chain.proceed(task, action, request, listener);
            }
        };
        var actionFilter = new MappedActionFilters(List.of(filter));

        var chain = new ActionFilterChain<DummyRequest, DummyResponse>() {
            @Override
            public void proceed(Task task, String action, DummyRequest request, ActionListener<DummyResponse> listener) {
                proceedCalled.set(true);
            }
        };
        actionFilter.apply(null, "differentAction", null, null, chain);
        assertThat(applyCalled.get(), is(false));
        assertThat(proceedCalled.get(), is(true));
    }
}
