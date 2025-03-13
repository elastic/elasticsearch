/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecondaryAuthorizationUtilsTests extends ESTestCase {

    private static final String AUTH_KEY = AuthenticationField.AUTHENTICATION_KEY;
    private static final String SECONDARY_AUTH_KEY = SecondaryAuthentication.THREAD_CTX_KEY;
    private static final String NOT_AN_AUTH_KEY = "not-an-auth-key";

    private static final String JOHN_HEADER =
        "45XtAwAXdHJhbnNmb3JtX2FkbWluX25vX2RhdGEBD3RyYW5zZm9ybV9hZG1pbgoAAAABAA5qYXZhUmVzdFRlc3QtMA5kZWZhdWx0X25hdGl2ZQZuYXRpdmUAAAAA";
    private static final String BILL_HEADER =
        "45XtAwARdHJhbnNmb3JtX2FkbWluXzICD3RyYW5zZm9ybV9hZG1pbhJ0ZXN0X2RhdGFfYWNjZXNzXzIKAAAAAQAOamF2YVJlc3RUZXN0LTAOZGVmYXVsdF9uYXRp"
            + "dmUGbmF0aXZlAAAAAA==";
    private static final String NOT_AN_AUTH_HEADER = "not-an-auth-header";

    public void testGetSecurityHeadersPreferringSecondary() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);

        Map<String, String> filteredHeaders = SecondaryAuthorizationUtils.getSecurityHeadersPreferringSecondary(
            threadPool,
            securityContext,
            ClusterState.EMPTY_STATE
        );
        assertThat(filteredHeaders.keySet(), is(empty()));

        threadContext.setHeaders(
            Tuple.tuple(Map.of(AUTH_KEY, JOHN_HEADER, SECONDARY_AUTH_KEY, BILL_HEADER, NOT_AN_AUTH_KEY, NOT_AN_AUTH_HEADER), Map.of())
        );
        filteredHeaders = SecondaryAuthorizationUtils.getSecurityHeadersPreferringSecondary(
            threadPool,
            securityContext,
            ClusterState.EMPTY_STATE
        );
        assertThat(filteredHeaders.keySet(), contains(AUTH_KEY));
    }

    public void testUseSecondaryAuthIfAvailable() {
        // Counter used to verify that the runnable has been run
        AtomicInteger counter = new AtomicInteger(0);

        SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable(null, () -> counter.incrementAndGet());
        assertThat(counter.get(), is(equalTo(1)));

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);

        SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable(securityContext, () -> counter.incrementAndGet());
        assertThat(counter.get(), is(equalTo(2)));

        threadContext.setHeaders(
            Tuple.tuple(Map.of(AUTH_KEY, JOHN_HEADER, SECONDARY_AUTH_KEY, BILL_HEADER, NOT_AN_AUTH_KEY, NOT_AN_AUTH_HEADER), Map.of())
        );
        SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable(securityContext, () -> {
            counter.incrementAndGet();
            // The only remaining header is the secondary auth header under the auth key
            assertThat(threadContext.getHeaders(), is(equalTo(Map.of(AUTH_KEY, BILL_HEADER))));
        });
        assertThat(counter.get(), is(equalTo(3)));
        // The headers are restored
        assertThat(
            threadContext.getHeaders(),
            is(equalTo(Map.of(AUTH_KEY, JOHN_HEADER, SECONDARY_AUTH_KEY, BILL_HEADER, NOT_AN_AUTH_KEY, NOT_AN_AUTH_HEADER)))
        );
    }
}
