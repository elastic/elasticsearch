/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.identity.aws;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * An {@link IdentityProvider} of {@link AwsCredentialsIdentity} that obtains temporary credentials via STS
 * {@code AssumeRoleWithWebIdentity} <em>without ever blocking the calling thread</em>.
 *
 * <p>The stock {@code StsAssumeRoleWithWebIdentityCredentialsProvider} only implements the synchronous
 * {@code resolveCredentials()} and backs it with a blocking cache, so when it is used with an async
 * client such as {@code S3AsyncClient} the default {@code resolveIdentity()} bridge runs the blocking
 * fetch inline on the request thread. This provider instead implements {@link #resolveIdentity} directly to
 * return a {@link CompletableFuture} that is completed by asynchronous I/O. Because the AWS SDK resolves
 * credentials for async clients exclusively through {@link IdentityProvider#resolveIdentity}, no synchronous
 * {@code resolveCredentials()} bridge is provided; this provider is async-only.
 *
 * <h2>Refresh model</h2>
 * Credentials are cached together with two instants derived from the STS expiry (mirroring the stock
 * provider's defaults):
 * <ul>
 *   <li>{@code prefetchAt = expiry - prefetchTime} (default 5 minutes): once passed, a background refresh
 *       is started but the still-valid cached credentials are served immediately;</li>
 *   <li>{@code staleAt = expiry - staleTime} (default 1 minute): once passed (or when nothing is cached),
 *       callers receive the in-flight refresh future and complete only when it does, asynchronously,
 *       so no thread is parked.</li>
 * </ul>
 * A single in-flight refresh is shared by all concurrent callers (single-flight), so a burst of requests
 * triggers at most one STS exchange.
 *
 * <h2>Token sourcing</h2>
 * The OIDC token is supplied through a {@link Consumer} of {@link ActionListener} (the Elasticsearch async
 * idiom): each refresh invokes it afresh, which both matches the rotation of projected service-account
 * tokens and lets the implementation perform the read off the event loop.
 *
 * <p>This class is thread-safe. The {@link StsAsyncClient} is owned by the caller; {@link #close()} is a
 * no-op and does not shut it down.
 */
public final class AsyncWebIdentityCredentialsProvider implements IdentityProvider<AwsCredentialsIdentity>, SdkAutoCloseable {

    private static final Logger logger = LogManager.getLogger(AsyncWebIdentityCredentialsProvider.class);

    private static final Duration DEFAULT_PREFETCH_TIME = Duration.ofMinutes(5);
    private static final Duration DEFAULT_STALE_TIME = Duration.ofMinutes(1);

    private final String roleArn;
    private final String roleSessionName;
    private final Consumer<ActionListener<String>> tokenSupplier;
    private final StsAsyncClient stsAsyncClient;
    private final Duration prefetchTime;
    private final Duration staleTime;
    private final Clock clock;

    private final AtomicReference<Cached> cache = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Cached>> inFlight = new AtomicReference<>();

    private AsyncWebIdentityCredentialsProvider(Builder builder) {
        this.roleArn = Objects.requireNonNull(builder.roleArn, "roleArn must not be null");
        this.roleSessionName = Objects.requireNonNull(builder.roleSessionName, "roleSessionName must not be null");
        this.tokenSupplier = Objects.requireNonNull(builder.tokenSupplier, "tokenSupplier must not be null");
        this.stsAsyncClient = Objects.requireNonNull(builder.stsAsyncClient, "stsAsyncClient must not be null");
        this.prefetchTime = builder.prefetchTime != null ? builder.prefetchTime : DEFAULT_PREFETCH_TIME;
        this.staleTime = builder.staleTime != null ? builder.staleTime : DEFAULT_STALE_TIME;
        this.clock = builder.clock != null ? builder.clock : Clock.systemUTC();
        if (staleTime.isNegative() || staleTime.isZero()) {
            throw new IllegalArgumentException("staleTime must be a positive duration but was [" + staleTime + "]");
        }
        if (prefetchTime.isNegative() || prefetchTime.isZero()) {
            throw new IllegalArgumentException("prefetchTime must be a positive duration but was [" + prefetchTime + "]");
        }
        // prefetchTime must start no later than staleTime, otherwise prefetchAt would fall after staleAt and the
        // background-refresh window in resolveIdentity() would be unreachable.
        if (prefetchTime.compareTo(staleTime) < 0) {
            throw new IllegalArgumentException(
                "prefetchTime [" + prefetchTime + "] must be greater than or equal to staleTime [" + staleTime + "]"
            );
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
        Cached current = cache.get();
        Instant now = clock.instant();
        if (current == null || now.isAfter(current.staleAt())) {
            // Nothing usable cached: wait on a refresh, but asynchronously, so the caller's thread is not parked.
            return refresh().thenApply(Cached::credentials);
        }
        if (now.isAfter(current.prefetchAt())) {
            // Still valid but close to expiry: serve the cached value and refresh in the background. The
            // outcome is logged by startRefresh, and the cached credentials are retained on failure.
            refresh();
        }
        return CompletableFuture.completedFuture(current.credentials());
    }

    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return AwsCredentialsIdentity.class;
    }

    /**
     * Returns the in-flight refresh if one is running, otherwise starts exactly one and returns it. The
     * compare-and-set loop guarantees single-flight semantics under concurrent callers.
     */
    private CompletableFuture<Cached> refresh() {
        while (true) {
            CompletableFuture<Cached> existing = inFlight.get();
            if (existing != null) {
                return existing;
            }
            CompletableFuture<Cached> promise = new CompletableFuture<>();
            if (inFlight.compareAndSet(null, promise)) {
                startRefresh(promise);
                return promise;
            }
            // Lost the race to another caller; retry and join their in-flight refresh.
        }
    }

    private void startRefresh(CompletableFuture<Cached> promise) {
        logger.trace("refreshing workload-identity credentials for role [{}]", roleArn);
        requestToken().thenCompose(this::requestCredentials).thenApply(this::toCached).whenComplete((cached, failure) -> {
            // Clear the in-flight slot first so a later call can start a fresh refresh regardless of outcome.
            inFlight.set(null);
            if (failure != null) {
                // Keep whatever is cached untouched; only the in-flight slot is reset above.
                Throwable unwrapped = unwrap(failure);
                logger.warn("failed to refresh workload-identity credentials for role [{}]", roleArn, unwrapped);
                promise.completeExceptionally(unwrapped);
            } else {
                logger.debug(
                    "refreshed workload-identity credentials for role [{}]; new credentials expire at [{}]",
                    roleArn,
                    cached.credentials().expirationTime().orElse(null)
                );
                cache.set(cached);
                promise.complete(cached);
            }
        });
    }

    /**
     * Bridges the {@link ActionListener}-based token supplier (the one place this idiom is used, mirroring
     * {@code WorkloadIdentityIssuerClient#issueToken}) into the {@link CompletableFuture} pipeline. A synchronous
     * throw from the supplier is routed to the future rather than escaping to the caller.
     */
    private CompletableFuture<String> requestToken() {
        CompletableFuture<String> future = new CompletableFuture<>();
        ActionListener.run(ActionListener.<String>wrap(future::complete, future::completeExceptionally), tokenSupplier::accept);
        return future;
    }

    private CompletableFuture<AssumeRoleWithWebIdentityResponse> requestCredentials(String token) {
        AssumeRoleWithWebIdentityRequest request = AssumeRoleWithWebIdentityRequest.builder()
            .roleArn(roleArn)
            .roleSessionName(roleSessionName)
            .webIdentityToken(token)
            .build();
        return stsAsyncClient.assumeRoleWithWebIdentity(request);
    }

    private Cached toCached(AssumeRoleWithWebIdentityResponse response) {
        Credentials credentials = response.credentials();
        if (credentials == null) {
            throw new IllegalStateException("STS AssumeRoleWithWebIdentity response did not include credentials");
        }
        Instant expiry = credentials.expiration();
        if (expiry == null) {
            throw new IllegalStateException("STS AssumeRoleWithWebIdentity response did not include a credential expiry");
        }
        Instant now = clock.instant();
        if (expiry.isAfter(now) == false) {
            throw new IllegalStateException("STS returned credentials that are already expired at [" + expiry + "] (now [" + now + "])");
        }
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
            .accessKeyId(credentials.accessKeyId())
            .secretAccessKey(credentials.secretAccessKey())
            .sessionToken(credentials.sessionToken())
            .expirationTime(expiry)
            .build();
        return new Cached(sessionCredentials, expiry.minus(prefetchTime), expiry.minus(staleTime));
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }

    /**
     * No-op: the {@link StsAsyncClient} is owned and closed by the caller.
     */
    @Override
    public void close() {}

    /**
     * Cached credentials plus the two refresh thresholds derived from the STS expiry.
     *
     * @param credentials the temporary session credentials
     * @param prefetchAt  instant after which a background refresh should be started
     * @param staleAt     instant after which callers must wait for a refresh
     */
    private record Cached(AwsSessionCredentials credentials, Instant prefetchAt, Instant staleAt) {}

    /**
     * Builder for {@link AsyncWebIdentityCredentialsProvider}. {@code roleArn}, {@code roleSessionName},
     * {@code tokenSupplier}, and {@code stsAsyncClient} are required; the timing and clock fields are optional.
     */
    public static final class Builder {
        private String roleArn;
        private String roleSessionName;
        private Consumer<ActionListener<String>> tokenSupplier;
        private StsAsyncClient stsAsyncClient;
        private Duration prefetchTime;
        private Duration staleTime;
        private Clock clock;

        private Builder() {}

        /** The ARN of the role to assume. */
        public Builder roleArn(String roleArn) {
            this.roleArn = roleArn;
            return this;
        }

        /** The session name attached to the assumed-role session. */
        public Builder roleSessionName(String roleSessionName) {
            this.roleSessionName = roleSessionName;
            return this;
        }

        /**
         * Source of the OIDC web-identity token, invoked once per refresh. The implementation must eventually
         * call {@link ActionListener#onResponse} with the token or {@link ActionListener#onFailure}, and is
         * expected to do any blocking work (file read, network call) off the event loop.
         */
        public Builder tokenSupplier(Consumer<ActionListener<String>> tokenSupplier) {
            this.tokenSupplier = tokenSupplier;
            return this;
        }

        /** The async STS client used to perform the {@code AssumeRoleWithWebIdentity} exchange. */
        public Builder stsAsyncClient(StsAsyncClient stsAsyncClient) {
            this.stsAsyncClient = stsAsyncClient;
            return this;
        }

        /**
         * Time before expiry at which a background refresh starts. Must be a positive duration and no smaller
         * than {@link #staleTime}. Defaults to 5 minutes.
         */
        public Builder prefetchTime(Duration prefetchTime) {
            this.prefetchTime = prefetchTime;
            return this;
        }

        /** Time before expiry after which callers wait for a refresh. Must be a positive duration. Defaults to 1 minute. */
        public Builder staleTime(Duration staleTime) {
            this.staleTime = staleTime;
            return this;
        }

        /** Clock used to evaluate the refresh windows. Defaults to {@link Clock#systemUTC()}. */
        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public AsyncWebIdentityCredentialsProvider build() {
            return new AsyncWebIdentityCredentialsProvider(this);
        }
    }
}
