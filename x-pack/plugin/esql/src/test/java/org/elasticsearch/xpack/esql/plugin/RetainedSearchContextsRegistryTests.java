/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.junit.After;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class RetainedSearchContextsRegistryTests extends ESTestCase {
    private static final Predicate<Authentication> ALLOW_ALL = ignored -> true;

    private final RetainedSearchContextsRegistry registry = new RetainedSearchContextsRegistry();

    @After
    public void assertNoLeakedSessions() {
        assertThat(registry.retainedSessions(), equalTo(0));
    }

    public void testLeaseRetainsContextsAfterRegistrationCloses() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null)) {
            assertTrue(registry.isRetained("session-1"));
            assertThat(registration.searchContexts().size(), equalTo(1));

            lease = registry.acquire("session-1", ALLOW_ALL);
        }

        assertTrue(registry.isRetained("session-1"));
        assertThat(registry.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());

        lease.close();

        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testDuplicateRegistrationRejected() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);
        SearchContext duplicateSearchContext = createSearchContext();
        AcquiredSearchContexts duplicateContexts = createContexts(duplicateSearchContext);

        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts, null)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> registry.register("session-1", duplicateContexts, null)
            );
            assertEquals("search contexts already retained for session [session-1]", e.getMessage());
        }

        duplicateContexts.close();
        assertTrue(searchContext.isClosed());
        assertTrue(duplicateSearchContext.isClosed());
    }

    public void testAcquireUnknownSessionRejected() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.acquire("missing", ALLOW_ALL));
        assertEquals("no retained search contexts for session [missing]", e.getMessage());
    }

    public void testAcquireChecksRetainedSessionOwner() {
        SearchContext searchContext = createSearchContext();
        Authentication creator = AuthenticationTestHelper.builder().realm().build(false);
        AtomicReference<Authentication> checkedCreator = new AtomicReference<>();

        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", createContexts(searchContext), creator)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> registry.acquire("session-1", retainedSessionCreator -> {
                    checkedCreator.set(retainedSessionCreator);
                    return false;
                })
            );
            assertEquals("no retained search contexts for session [session-1]", e.getMessage());
            assertSame(creator, checkedCreator.get());
            assertFalse(searchContext.isClosed());

            try (
                RetainedSearchContextsRegistry.Handle ignored = registry.acquire(
                    "session-1",
                    retainedSessionCreator -> retainedSessionCreator == creator
                )
            ) {
                assertFalse(searchContext.isClosed());
            }
        }

        assertTrue(searchContext.isClosed());
    }

    public void testReleaseChecksRetainedSessionOwner() {
        SearchContext searchContext = createSearchContext();
        Authentication creator = AuthenticationTestHelper.builder().realm().build(false);
        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", createContexts(searchContext), creator);

        registry.closeRegistration("session-1", retainedSessionCreator -> false);
        assertTrue(registry.isRetained("session-1"));
        assertFalse(searchContext.isClosed());

        registry.closeRegistration("session-1", retainedSessionCreator -> retainedSessionCreator == creator);
        registration.close();

        assertFalse(registry.isRetained("session-1"));
        assertTrue(searchContext.isClosed());
    }

    public void testLeaseCloseIsIdempotent() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts, null)) {
            lease = registry.acquire("session-1", ALLOW_ALL);
        }

        lease.close();
        lease.close();

        assertTrue(searchContext.isClosed());
    }

    public void testRegistrationKeepsSearchContextAliveUntilClosed() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null)) {
            assertNotNull(registration.searchContexts().get(0));
            assertFalse(searchContext.isClosed());
        }

        assertTrue(searchContext.isClosed());
    }

    public void testCloseRegistrationBySessionIdReleasesContexts() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        registry.register("session-1", contexts, null);
        assertTrue(registry.isRetained("session-1"));

        registry.closeRegistration("session-1", ALLOW_ALL);

        assertFalse(registry.isRetained("session-1"));
        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testMultipleLeasesThenRegistrationClose() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease1;
        RetainedSearchContextsRegistry.Handle lease2;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts, null)) {
            lease1 = registry.acquire("session-1", ALLOW_ALL);
            lease2 = registry.acquire("session-1", ALLOW_ALL);
        }

        assertThat(registry.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());

        lease1.close();
        assertThat(registry.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());

        lease2.close();
        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testRegistrationCloseAndCloseBySessionIdAreIdempotent() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        RetainedSearchContextsRegistry.Handle lease = registry.acquire("session-1", ALLOW_ALL);

        registration.close();
        registry.closeRegistration("session-1", ALLOW_ALL);

        assertFalse(searchContext.isClosed());
        assertTrue(registry.isRetained("session-1"));

        lease.close();
        assertTrue(searchContext.isClosed());
    }

    public void testCloseBySessionIdThenRegistrationClose() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);

        registry.closeRegistration("session-1", ALLOW_ALL);
        registration.close();

        assertTrue(searchContext.isClosed());
    }

    public void testAcquireAfterRegistrationCloseRejected() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        registry.register("session-1", contexts, null);
        registry.closeRegistration("session-1", ALLOW_ALL);

        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1", ALLOW_ALL));
        assertTrue(searchContext.isClosed());
    }

    public void testClosedRegistrationRejectsAcquireWithActiveLease() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts, null)) {
            lease = registry.acquire("session-1", ALLOW_ALL);
        }

        assertFalse(searchContext.isClosed());
        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1", ALLOW_ALL));

        lease.close();
        assertTrue(searchContext.isClosed());
    }

    public void testExpireReleasesIdleRegistration() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry expiringRegistry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = expiringRegistry.register("session-1", contexts, null);
        registration.finishRegistration();
        now[0] = 11L;

        expiringRegistry.expire();

        assertThat(expiringRegistry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testExpireSkipsActiveProducer() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry expiringRegistry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = expiringRegistry.register("session-1", contexts, null);
        now[0] = 11L;

        expiringRegistry.expire();

        assertThat(expiringRegistry.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());

        registration.close();

        assertThat(expiringRegistry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testExpireKeepsRegistrationOpenWithActiveLease() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry expiringRegistry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = expiringRegistry.register("session-1", contexts, null);
        registration.finishRegistration();
        RetainedSearchContextsRegistry.Handle lease = expiringRegistry.acquire("session-1", ALLOW_ALL);
        now[0] = 11L;

        expiringRegistry.expire();

        assertThat(expiringRegistry.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());

        RetainedSearchContextsRegistry.Handle secondLease = expiringRegistry.acquire("session-1", ALLOW_ALL);
        registration.close();
        secondLease.close();
        assertFalse(searchContext.isClosed());

        lease.close();

        assertThat(expiringRegistry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testQueryScopedReleaseKeepsActiveFetchLeaseAlive() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        List<String> released = new ArrayList<>();
        RemoteFetchService.RetainedSessionReleaser releaser = new RemoteFetchService.RetainedSessionReleaser((targetNode, sessionId) -> {
            released.add(targetNode.getId() + "/" + sessionId);
            registry.closeRegistration(sessionId, ALLOW_ALL);
        });

        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null)) {
            RetainedSearchContextsRegistry.Handle fetchLease = registry.acquire("session-1", ALLOW_ALL);
            releaser.track(node, "session-1");

            releaser.close();

            assertThat(released, equalTo(List.of("node-1/session-1")));
            assertFalse(searchContext.isClosed());
            expectThrows(IllegalStateException.class, () -> registry.acquire("session-1", ALLOW_ALL));

            fetchLease.close();

            assertThat(registry.retainedSessions(), equalTo(0));
            assertTrue(searchContext.isClosed());
        }
    }

    /**
     * Cancellation closes the registration asynchronously while compute drivers may still be running. The compute's
     * own lease must keep the contexts alive until the compute completes, even though new leases are rejected as soon
     * as the registration closes.
     */
    public void testRegistrationCloseDuringComputeKeepsContextsAliveUntilComputeLeaseCloses() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        RetainedSearchContextsRegistry.Handle computeLease = registry.acquire("session-1", ALLOW_ALL);

        // Simulates the cancellation listener firing mid-compute.
        registration.close();

        assertFalse(searchContext.isClosed());
        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1", ALLOW_ALL));

        // Simulates the compute finishing and the response listener releasing its lease.
        computeLease.close();

        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentAcquireAndClose() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        int threads = randomIntBetween(4, 16);
        startInParallel(threads, i -> {
            RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1", ALLOW_ALL);
            handle.close();
        });

        assertFalse(searchContext.isClosed());
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentAcquireDuringRegistrationClose() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        int acquirers = randomIntBetween(4, 16);
        CopyOnWriteArrayList<RetainedSearchContextsRegistry.Handle> acquired = new CopyOnWriteArrayList<>();
        startInParallel(acquirers + 1, i -> {
            if (i == 0) {
                registration.close();
            } else {
                try {
                    RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1", ALLOW_ALL);
                    acquired.add(handle);
                } catch (IllegalStateException expected) {
                    // acquire after refcount reached zero
                }
            }
        });

        for (RetainedSearchContextsRegistry.Handle handle : acquired) {
            handle.close();
        }

        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentHandleCloseOnSameHandle() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1", ALLOW_ALL);

        int threads = randomIntBetween(4, 16);
        startInParallel(threads, i -> handle.close());

        assertFalse(searchContext.isClosed());
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentCloseRegistrationFromTwoPaths() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts, null);
        startInParallel(2, i -> {
            if (i == 0) {
                registration.close();
            } else {
                registry.closeRegistration("session-1", ALLOW_ALL);
            }
        });

        assertTrue(searchContext.isClosed());
    }

    private static AcquiredSearchContexts createContexts(SearchContext searchContext) {
        AcquiredSearchContexts contexts = new AcquiredSearchContexts(1);
        contexts.newSubRangeView(List.of(searchContext));
        return contexts;
    }

    private static SearchContext createSearchContext() {
        return new TestSearchContext(Mockito.mock(SearchExecutionContext.class, Mockito.withSettings().stubOnly()));
    }
}
