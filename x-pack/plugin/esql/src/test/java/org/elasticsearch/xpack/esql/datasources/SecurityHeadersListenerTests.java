/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SecurityHeadersListenerTests extends ESTestCase {

    private static final String AUTH = AuthenticationField.AUTHENTICATION_KEY;
    private static final String PROJECT = Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER;
    private static final String OPAQUE = Task.X_OPAQUE_ID_HTTP_HEADER;

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    private List<String> observe() {
        return Arrays.asList(threadContext.getHeader(AUTH), threadContext.getHeader(PROJECT), threadContext.getHeader(OPAQUE));
    }

    public void testInstallsIssuingHeadersOverStaleCompletingThread() {
        threadContext.putHeader(AUTH, "issuing-user");
        threadContext.putHeader(PROJECT, "issuing-project");
        List<List<String>> seen = new ArrayList<>();
        ActionListener<String> listener = SecurityHeadersListener.wrap(threadContext, ActionListener.wrap(r -> seen.add(observe()), e -> {
            throw new AssertionError(e);
        }));

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            threadContext.putHeader(AUTH, "stale-user");
            threadContext.putHeader(PROJECT, "stale-project");
            threadContext.putHeader(OPAQUE, "stale-opaque");
            listener.onResponse("ok");
        }

        assertThat(seen, hasSize(1));
        assertThat(seen.get(0), contains("issuing-user", "issuing-project", null));
    }

    public void testNoSecurityOnIssuingThreadDoesNotLeakStaleIdentity() {
        List<List<String>> seen = new ArrayList<>();
        ActionListener<String> listener = SecurityHeadersListener.wrap(threadContext, ActionListener.wrap(r -> seen.add(observe()), e -> {
            throw new AssertionError(e);
        }));

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            threadContext.putHeader(AUTH, "stale-user");
            threadContext.putHeader(PROJECT, "stale-project");
            listener.onResponse("ok");
        }

        assertThat(seen.get(0), contains(null, null, null));
    }

    public void testFailureRunsUnderIssuingHeaders() {
        threadContext.putHeader(AUTH, "issuing-user");
        List<List<String>> seen = new ArrayList<>();
        ActionListener<String> listener = SecurityHeadersListener.wrap(threadContext, ActionListener.wrap(r -> {
            throw new AssertionError("unexpected response");
        }, e -> seen.add(observe())));

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            threadContext.putHeader(AUTH, "stale-user");
            listener.onFailure(new IllegalStateException("boom"));
        }

        assertThat(seen.get(0), contains("issuing-user", null, null));
    }

    public void testCompletingThreadContextIsRestoredEvenWhenListenerThrows() {
        threadContext.putHeader(AUTH, "issuing-user");
        ActionListener<String> throwing = new ActionListener<>() {
            @Override
            public void onResponse(String response) {
                throw new IllegalStateException("listener failed");
            }

            @Override
            public void onFailure(Exception e) {}
        };
        ActionListener<String> listener = SecurityHeadersListener.wrap(threadContext, throwing);

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            threadContext.putHeader(AUTH, "completing-user");
            expectThrows(IllegalStateException.class, () -> listener.onResponse("ok"));
            assertThat(threadContext.getHeader(AUTH), equalTo("completing-user"));
        }
    }

    public void testResponseHeadersWrittenByListenerAreDiscarded() {
        threadContext.putHeader(AUTH, "issuing-user");
        ActionListener<String> listener = SecurityHeadersListener.wrap(
            threadContext,
            ActionListener.wrap(r -> threadContext.addResponseHeader("Warning", "from-continuation"), e -> {})
        );

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            listener.onResponse("ok");
            assertThat(threadContext.getResponseHeaders().get("Warning"), nullValue());
        }
    }
}
