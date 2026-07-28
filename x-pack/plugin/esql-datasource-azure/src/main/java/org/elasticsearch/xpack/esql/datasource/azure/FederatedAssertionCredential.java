/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import reactor.core.publisher.Mono;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientAssertionCredential;
import com.azure.identity.ClientAssertionCredentialBuilder;
import com.azure.identity.CredentialUnavailableException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * <p>A {@link TokenCredential} that authenticates a service principal using a client assertion (an OIDC/federated JWT)
 * that is produced <strong>asynchronously</strong> via a {@link Supplier} of {@link CompletableFuture}.</p>
 *
 * <p>Adapted from the Azure SDK for Java ({@code com.azure.identity}) into this plugin's package so it can be wired to
 * the Elasticsearch workload-identity issuer client. It is intended for scenarios where obtaining the client assertion
 * requires network I/O that is encapsulated by an existing transport component. Unlike {@link ClientAssertionCredential},
 * whose {@code Supplier<String>} is invoked inline on whatever thread happens to drive the reactive chain (a Reactor
 * event-loop thread, the {@link java.util.concurrent.ForkJoinPool#commonPool() common pool}, or the calling thread),
 * this credential resolves the assertion through the supplied {@link CompletableFuture} so the I/O runs on the
 * transport's own threads and never blocks a thread that matters.</p>
 *
 * <p>The actual token exchange (talking to Microsoft Entra ID, access-token caching, instance discovery, CAE, claims,
 * sync and async support) is delegated to an inner {@link ClientAssertionCredential}, so none of that machinery is
 * duplicated here. This class only bridges the asynchronous assertion supplier to the non-blocking
 * {@code Supplier<String>} that the delegate reads.</p>
 *
 * <p>No assertion caching is performed here: the supplied {@code assertionSupplier} is expected to return an
 * already-cached assertion (the Elasticsearch workload-identity issuer client mints short-lived JWTs and serves them
 * from its own cache), so resolving on each token request is cheap.</p>
 *
 * @see ClientAssertionCredential
 */
public final class FederatedAssertionCredential implements TokenCredential {

    private final ClientAssertionCredential delegate;
    private final Supplier<CompletableFuture<String>> assertionSupplier;

    // Holds the most recently resolved assertion so the delegate's (synchronous, inline) Supplier<String> can read it
    // without blocking. The value is identity-scoped (clientId/tenantId), so concurrent requests observe an equivalent
    // assertion.
    private final AtomicReference<String> latestAssertion = new AtomicReference<>();

    /**
     * Creates an instance of {@link FederatedAssertionCredential}.
     *
     * @param delegateBuilder a configured {@link ClientAssertionCredentialBuilder} (client id, tenant id and any
     *     transport/authority options already applied); its {@code clientAssertion} supplier is wired by this
     *     constructor.
     * @param assertionSupplier supplies a {@link CompletableFuture} that completes with the client assertion JWT.
     */
    FederatedAssertionCredential(ClientAssertionCredentialBuilder delegateBuilder, Supplier<CompletableFuture<String>> assertionSupplier) {
        this.assertionSupplier = assertionSupplier;
        // The delegate reads the already-resolved value, so its inline Supplier<String> never blocks.
        this.delegate = delegateBuilder.clientAssertion(latestAssertion::get).build();
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        // Resolve the assertion first, on the transport's own threads, then delegate the exchange.
        // The delegate logs token success/exchange errors with its own client options.
        return resolveAssertion().then(Mono.defer(() -> delegate.getToken(request)));
    }

    @Override
    public AccessToken getTokenSync(TokenRequestContext request) {
        // The sync path runs on the caller's own thread, where blocking to resolve the assertion is acceptable.
        resolveAssertion().block();
        return delegate.getTokenSync(request);
    }

    private Mono<String> resolveAssertion() {
        return Mono.defer(() -> Mono.fromFuture(assertionSupplier.get()))
            .onErrorMap(
                e -> (e instanceof CredentialUnavailableException) == false,
                e -> new CredentialUnavailableException("Failed to obtain a client assertion from the configured supplier.", e)
            )
            .doOnNext(latestAssertion::set);
    }
}
