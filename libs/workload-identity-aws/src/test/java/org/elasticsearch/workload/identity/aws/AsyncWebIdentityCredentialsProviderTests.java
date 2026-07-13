/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.identity.aws;

import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncWebIdentityCredentialsProviderTests extends ESTestCase {

    private static final Duration PREFETCH = Duration.ofMinutes(5);
    private static final Duration STALE = Duration.ofMinutes(1);
    private static final Duration LIFETIME = Duration.ofMinutes(10);

    private MutableClock clock;
    private AtomicInteger tokenCalls;
    private Consumer<ActionListener<String>> tokenSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clock = new MutableClock(Instant.parse("2026-05-29T00:00:00Z"));
        tokenCalls = new AtomicInteger();
        tokenSupplier = listener -> {
            tokenCalls.incrementAndGet();
            listener.onResponse("oidc-jwt");
        };
    }

    public void testColdResolveTriggersSingleExchange() throws Exception {
        CountingExchanger exchanger = new CountingExchanger(clock::instant);
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        AwsCredentialsIdentity identity = resolve(provider);

        assertEquals("AK-1", identity.accessKeyId());
        assertEquals(1, exchanger.calls.get());
        assertEquals(1, tokenCalls.get());
    }

    public void testFreshCredentialsServedWithoutExchange() throws Exception {
        CountingExchanger exchanger = new CountingExchanger(clock::instant);
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        resolve(provider); // prime the cache
        AwsCredentialsIdentity identity = resolve(provider); // still well within validity

        assertEquals("AK-1", identity.accessKeyId());
        assertEquals("no second exchange while fresh", 1, exchanger.calls.get());
    }

    public void testPrefetchWindowServesCachedAndRefreshesInBackground() throws Exception {
        CountingExchanger exchanger = new CountingExchanger(clock::instant);
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        resolve(provider); // cache AK-1, expires in 10m

        // Move into the prefetch window: past (expiry - 5m) but before (expiry - 1m).
        clock.advance(Duration.ofMinutes(6));
        AwsCredentialsIdentity served = resolve(provider);

        // The still-valid cached credentials are served immediately...
        assertEquals("AK-1", served.accessKeyId());
        // ...while a background refresh runs exactly once.
        assertEquals(2, exchanger.calls.get());

        // A subsequent resolve now sees the refreshed credentials.
        assertEquals("AK-2", resolve(provider).accessKeyId());
    }

    public void testStaleCredentialsWaitForRefresh() throws Exception {
        CountingExchanger exchanger = new CountingExchanger(clock::instant);
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        resolve(provider); // cache AK-1

        // Past the stale threshold (expiry - 1m): callers must get the refreshed value.
        clock.advance(Duration.ofMinutes(9).plusSeconds(30));
        AwsCredentialsIdentity refreshed = resolve(provider);

        assertEquals("AK-2", refreshed.accessKeyId());
        assertEquals(2, exchanger.calls.get());
    }

    public void testSingleFlightUnderConcurrentColdCallers() {
        HeldExchanger exchanger = new HeldExchanger(clock.instant().plus(LIFETIME));
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        ResolveIdentityRequest request = ResolveIdentityRequest.builder().build();
        CompletableFuture<? extends AwsCredentialsIdentity> first = provider.resolveIdentity(request);
        CompletableFuture<? extends AwsCredentialsIdentity> second = provider.resolveIdentity(request);

        assertFalse(first.isDone());
        assertFalse(second.isDone());
        assertEquals("both callers share one in-flight exchange", 1, exchanger.calls.get());
        assertEquals(1, tokenCalls.get());

        exchanger.release();

        assertEquals("AK-1", first.join().accessKeyId());
        assertEquals("AK-1", second.join().accessKeyId());
    }

    public void testFailurePropagatesButRetainsLastGoodCache() throws Exception {
        SwitchableExchanger exchanger = new SwitchableExchanger(clock::instant);
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        // Prime a good value, then force the next exchange to fail while credentials are stale.
        resolve(provider);
        clock.advance(Duration.ofMinutes(9).plusSeconds(30));
        exchanger.failNext(new RuntimeException("sts boom"));

        CompletableFuture<? extends AwsCredentialsIdentity> failing = provider.resolveIdentity(ResolveIdentityRequest.builder().build());
        ExecutionException thrown = expectThrows(ExecutionException.class, failing::get);
        assertEquals("sts boom", thrown.getCause().getMessage());

        // The provider recovers on the next successful exchange (cache was not corrupted). Calls so far:
        // 1 = initial prime, 2 = the forced failure, 3 = this successful refresh.
        exchanger.succeed();
        assertEquals("AK-3", resolve(provider).accessKeyId());
    }

    public void testTokenSupplierFailurePropagatesToFuture() {
        Exception checked = new Exception("token read failed");
        AsyncWebIdentityCredentialsProvider provider = baseBuilder(new CountingExchanger(clock::instant)).tokenSupplier(
            listener -> listener.onFailure(checked)
        ).build();

        // A failure sourcing the OIDC token must surface as the future's failure, unwrapped from any CompletionException.
        CompletableFuture<? extends AwsCredentialsIdentity> failing = provider.resolveIdentity(ResolveIdentityRequest.builder().build());
        ExecutionException thrown = expectThrows(ExecutionException.class, failing::get);
        assertSame(checked, thrown.getCause());
    }

    /** Returns the same already-completed response for every exchange. */
    private static StubStsAsyncClient respondingWith(AssumeRoleWithWebIdentityResponse response) {
        return new StubStsAsyncClient() {
            @Override
            public CompletableFuture<AssumeRoleWithWebIdentityResponse> assumeRoleWithWebIdentity(
                AssumeRoleWithWebIdentityRequest request
            ) {
                return CompletableFuture.completedFuture(response);
            }
        };
    }

    public void testRejectsPrefetchSmallerThanStale() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> baseBuilder(new CountingExchanger(clock::instant)).prefetchTime(STALE).staleTime(PREFETCH).build()
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must be greater than or equal to staleTime"));
    }

    public void testRejectsNonPositiveStaleTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> baseBuilder(new CountingExchanger(clock::instant)).staleTime(Duration.ZERO).build()
        );
        assertTrue(e.getMessage(), e.getMessage().contains("staleTime must be a positive duration"));
    }

    public void testRejectsNegativePrefetchTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> baseBuilder(new CountingExchanger(clock::instant)).prefetchTime(Duration.ofMinutes(-1)).build()
        );
        assertTrue(e.getMessage(), e.getMessage().contains("prefetchTime must be a positive duration"));
    }

    public void testRejectsAlreadyExpiredStsCredentials() {
        StubStsAsyncClient exchanger = respondingWith(response("AK-stale", clock.instant().minus(Duration.ofSeconds(1))));
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        CompletableFuture<? extends AwsCredentialsIdentity> failing = provider.resolveIdentity(ResolveIdentityRequest.builder().build());
        ExecutionException thrown = expectThrows(ExecutionException.class, failing::get);
        assertTrue(thrown.getCause() instanceof IllegalStateException);
        assertTrue(thrown.getCause().getMessage(), thrown.getCause().getMessage().contains("already expired"));
    }

    public void testRejectsResponseWithoutCredentials() {
        StubStsAsyncClient exchanger = respondingWith(AssumeRoleWithWebIdentityResponse.builder().build());
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        CompletableFuture<? extends AwsCredentialsIdentity> failing = provider.resolveIdentity(ResolveIdentityRequest.builder().build());
        ExecutionException thrown = expectThrows(ExecutionException.class, failing::get);
        assertTrue(thrown.getCause() instanceof IllegalStateException);
        assertTrue(thrown.getCause().getMessage(), thrown.getCause().getMessage().contains("did not include credentials"));
    }

    public void testRejectsCredentialsWithoutExpiry() {
        StubStsAsyncClient exchanger = respondingWith(
            AssumeRoleWithWebIdentityResponse.builder()
                .credentials(Credentials.builder().accessKeyId("AK").secretAccessKey("secret").sessionToken("token").build())
                .build()
        );
        AsyncWebIdentityCredentialsProvider provider = newProvider(exchanger);

        CompletableFuture<? extends AwsCredentialsIdentity> failing = provider.resolveIdentity(ResolveIdentityRequest.builder().build());
        ExecutionException thrown = expectThrows(ExecutionException.class, failing::get);
        assertTrue(thrown.getCause() instanceof IllegalStateException);
        assertTrue(thrown.getCause().getMessage(), thrown.getCause().getMessage().contains("did not include a credential expiry"));
    }

    private AsyncWebIdentityCredentialsProvider newProvider(StsAsyncClient stsAsyncClient) {
        return baseBuilder(stsAsyncClient).build();
    }

    private AsyncWebIdentityCredentialsProvider.Builder baseBuilder(StsAsyncClient stsAsyncClient) {
        return AsyncWebIdentityCredentialsProvider.builder()
            .roleArn("arn:aws:iam::123456789012:role/test")
            .roleSessionName("es-test")
            .tokenSupplier(tokenSupplier)
            .stsAsyncClient(stsAsyncClient)
            .prefetchTime(PREFETCH)
            .staleTime(STALE)
            .clock(clock);
    }

    private static AwsCredentialsIdentity resolve(AsyncWebIdentityCredentialsProvider provider) throws Exception {
        return provider.resolveIdentity(ResolveIdentityRequest.builder().build()).get();
    }

    private static AssumeRoleWithWebIdentityResponse response(String accessKeyId, Instant expiry) {
        return AssumeRoleWithWebIdentityResponse.builder()
            .credentials(
                Credentials.builder().accessKeyId(accessKeyId).secretAccessKey("secret").sessionToken("token").expiration(expiry).build()
            )
            .build();
    }

    /**
     * Minimal {@link StsAsyncClient} test double. AWS SDK v2 service-client interfaces declare every
     * operation as a {@code default} method throwing {@link UnsupportedOperationException}, so subclasses
     * only need to implement {@link #serviceName()}/{@link #close()} and override the single operation
     * under test.
     */
    private abstract static class StubStsAsyncClient implements StsAsyncClient {
        @Override
        public String serviceName() {
            return "sts";
        }

        @Override
        public void close() {}
    }

    /** Returns an already-completed response per call, numbering the access keys so refreshes are observable. */
    private static final class CountingExchanger extends StubStsAsyncClient {
        private final Supplier<Instant> now;
        private final AtomicInteger calls = new AtomicInteger();

        CountingExchanger(Supplier<Instant> now) {
            this.now = now;
        }

        @Override
        public CompletableFuture<AssumeRoleWithWebIdentityResponse> assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest request) {
            int n = calls.incrementAndGet();
            return CompletableFuture.completedFuture(response("AK-" + n, now.get().plus(LIFETIME)));
        }
    }

    /** Holds its single response until {@link #release()} is called, to exercise single-flight behaviour. */
    private static final class HeldExchanger extends StubStsAsyncClient {
        private final Instant expiry;
        private final AtomicInteger calls = new AtomicInteger();
        private final CompletableFuture<AssumeRoleWithWebIdentityResponse> held = new CompletableFuture<>();

        HeldExchanger(Instant expiry) {
            this.expiry = expiry;
        }

        @Override
        public CompletableFuture<AssumeRoleWithWebIdentityResponse> assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest request) {
            calls.incrementAndGet();
            return held;
        }

        void release() {
            held.complete(response("AK-1", expiry));
        }
    }

    /** Succeeds by default, can be told to fail the next exchange exactly once. */
    private static final class SwitchableExchanger extends StubStsAsyncClient {
        private final Supplier<Instant> now;
        private final AtomicInteger calls = new AtomicInteger();
        private volatile RuntimeException nextFailure;

        SwitchableExchanger(Supplier<Instant> now) {
            this.now = now;
        }

        void failNext(RuntimeException failure) {
            this.nextFailure = failure;
        }

        void succeed() {
            this.nextFailure = null;
        }

        @Override
        public CompletableFuture<AssumeRoleWithWebIdentityResponse> assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest request) {
            int n = calls.incrementAndGet();
            RuntimeException failure = nextFailure;
            if (failure != null) {
                nextFailure = null;
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(response("AK-" + n, now.get().plus(LIFETIME)));
        }
    }

    /** A {@link Clock} whose instant can be advanced by tests. */
    private static final class MutableClock extends Clock {
        private volatile Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
            this.instant = this.instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
