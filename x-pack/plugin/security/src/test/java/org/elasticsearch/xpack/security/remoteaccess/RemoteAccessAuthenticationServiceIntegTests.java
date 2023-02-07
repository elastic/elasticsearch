/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.remoteaccess;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;
import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteAccessAuthenticationServiceIntegTests extends SecurityIntegTestCase {

    public void testInvalidHeaders() throws InterruptedException, IOException {
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "abc");
            authenticateAndAssertExpectedFailure(nodeName, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo(
                        "remote access header ["
                            + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY
                            + "] value must be a valid API key credential"
                    )
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(AUTHENTICATION_KEY, AuthenticationTestHelper.builder().build().encode());
            // Optionally include remote access headers; the request should fail due to authentication header either way
            if (randomBoolean()) {
                threadContext.putHeader(
                    REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY,
                    // Validly-formatted, non-existent API key
                    "ApiKey WTdac3lvVUIzblhJMzR1RzNMWjI6VHc4RXpJZ0NUZTY5T3drZkUxT3hDQQ=="
                );
                AuthenticationTestHelper.randomRemoteAccessAuthentication().writeToContext(threadContext);
            }
            authenticateAndAssertExpectedFailure(nodeName, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(ex.getCause().getMessage(), equalTo("authentication header is not allowed with remote access"));
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(
                REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY,
                // Validly-formatted, non-existent API key
                "ApiKey WTdac3lvVUIzblhJMzR1RzNMWjI6VHc4RXpJZ0NUZTY5T3drZkUxT3hDQQ=="
            );
            authenticateAndAssertExpectedFailure(nodeName, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required")
                );
            });
        }
    }

    private void authenticateAndAssertExpectedFailure(String nodeName, Consumer<Exception> assertions) throws InterruptedException {
        final RemoteAccessAuthenticationService service = internalCluster().getInstance(RemoteAccessAuthenticationService.class, nodeName);
        final AtomicReference<Exception> actual = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        service.authenticate(SearchAction.NAME, new SearchRequest(), false, new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Authentication authentication) {
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                actual.set(e);
            }
        }, latch));
        latch.await();
        final Exception actualException = actual.get();
        assertNotNull(actual);
        assertions.accept(actualException);
    }
}
