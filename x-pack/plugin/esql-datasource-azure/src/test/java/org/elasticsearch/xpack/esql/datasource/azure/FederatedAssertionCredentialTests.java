/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientAssertionCredentialBuilder;
import com.azure.identity.CredentialUnavailableException;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link FederatedAssertionCredential}. These exercise only the async-assertion bridge this class owns:
 * when the assertion supplier fails, resolution short-circuits before the inner {@link com.azure.identity.ClientAssertionCredential}
 * attempts any token exchange, so the assertions are deterministic and require no network access.
 */
public class FederatedAssertionCredentialTests extends ESTestCase {

    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final TokenRequestContext REQUEST = new TokenRequestContext().addScopes("https://storage.azure.com/.default");

    private static FederatedAssertionCredential credentialWith(Supplier<CompletableFuture<String>> assertionSupplier) {
        // tenant/client id are required by ClientAssertionCredentialBuilder.build(); the values are never used because every
        // test fails during assertion resolution, before the delegate's token exchange.
        return new FederatedAssertionCredential(
            new ClientAssertionCredentialBuilder().tenantId(TENANT_ID).clientId(CLIENT_ID),
            assertionSupplier
        );
    }

    public void testGetTokenWrapsAssertionFailure() {
        IOException failure = new IOException("issuer unavailable");
        FederatedAssertionCredential credential = credentialWith(() -> CompletableFuture.failedFuture(failure));

        CredentialUnavailableException e = expectThrows(CredentialUnavailableException.class, () -> credential.getToken(REQUEST).block());
        assertThat(e.getMessage(), containsString("Failed to obtain a client assertion"));
        assertSame(failure, e.getCause());
    }

    public void testGetTokenSyncWrapsAssertionFailure() {
        IOException failure = new IOException("issuer unavailable");
        FederatedAssertionCredential credential = credentialWith(() -> CompletableFuture.failedFuture(failure));

        CredentialUnavailableException e = expectThrows(CredentialUnavailableException.class, () -> credential.getTokenSync(REQUEST));
        assertSame(failure, e.getCause());
    }

    public void testExistingCredentialUnavailableExceptionIsNotRewrapped() {
        CredentialUnavailableException original = new CredentialUnavailableException("boom");
        FederatedAssertionCredential credential = credentialWith(() -> CompletableFuture.failedFuture(original));

        CredentialUnavailableException e = expectThrows(CredentialUnavailableException.class, () -> credential.getToken(REQUEST).block());
        assertSame(original, e);
    }

    public void testAssertionSupplierIsLazyAndResolvedPerSubscription() {
        AtomicInteger calls = new AtomicInteger();
        FederatedAssertionCredential credential = credentialWith(() -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new IOException("issuer unavailable"));
        });

        assertEquals("construction must not resolve the assertion", 0, calls.get());
        expectThrows(CredentialUnavailableException.class, () -> credential.getToken(REQUEST).block());
        expectThrows(CredentialUnavailableException.class, () -> credential.getToken(REQUEST).block());
        assertEquals("each subscription resolves the assertion afresh", 2, calls.get());
    }
}
