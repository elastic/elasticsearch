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
import org.junit.After;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.equalTo;

public class RetainedSearchContextsRegistryTests extends ESTestCase {
    private final RetainedSearchContextsRegistry registry = new RetainedSearchContextsRegistry();

    @After
    public void assertNoLeakedSessions() {
        assertThat(registry.retainedSessions(), equalTo(0));
    }

    public void testLeaseRetainsContextsAfterRegistrationCloses() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts)) {
            assertTrue(registry.isRetained("session-1"));
            assertThat(registration.searchContexts().size(), equalTo(1));

            lease = registry.acquire("session-1");
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

        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts)) {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.register("session-1", duplicateContexts));
            assertEquals("search contexts already retained for session [session-1]", e.getMessage());
        }

        duplicateContexts.close();
        assertTrue(searchContext.isClosed());
        assertTrue(duplicateSearchContext.isClosed());
    }

    public void testAcquireUnknownSessionRejected() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> registry.acquire("missing"));
        assertEquals("no retained search contexts for session [missing]", e.getMessage());
    }

    public void testLeaseCloseIsIdempotent() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts)) {
            lease = registry.acquire("session-1");
        }

        lease.close();
        lease.close();

        assertTrue(searchContext.isClosed());
    }

    public void testRegistrationKeepsSearchContextAliveUntilClosed() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        try (RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts)) {
            assertNotNull(registration.searchContexts().get(0));
            assertFalse(searchContext.isClosed());
        }

        assertTrue(searchContext.isClosed());
    }

    public void testCloseRegistrationBySessionIdReleasesContexts() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        registry.register("session-1", contexts);
        assertTrue(registry.isRetained("session-1"));

        registry.closeRegistration("session-1");

        assertFalse(registry.isRetained("session-1"));
        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testMultipleLeasesThenRegistrationClose() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease1;
        RetainedSearchContextsRegistry.Handle lease2;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts)) {
            lease1 = registry.acquire("session-1");
            lease2 = registry.acquire("session-1");
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

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);
        RetainedSearchContextsRegistry.Handle lease = registry.acquire("session-1");

        registration.close();
        registry.closeRegistration("session-1");

        assertFalse(searchContext.isClosed());
        assertTrue(registry.isRetained("session-1"));

        lease.close();
        assertTrue(searchContext.isClosed());
    }

    public void testCloseBySessionIdThenRegistrationClose() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);

        registry.closeRegistration("session-1");
        registration.close();

        assertTrue(searchContext.isClosed());
    }

    public void testAcquireAfterRegistrationCloseRejected() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        registry.register("session-1", contexts);
        registry.closeRegistration("session-1");

        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1"));
        assertTrue(searchContext.isClosed());
    }

    public void testAcquireAfterRegistrationCloseRejectedWhileLeaseIsActive() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle lease;
        try (RetainedSearchContextsRegistry.Handle ignored = registry.register("session-1", contexts)) {
            lease = registry.acquire("session-1");
        }

        assertFalse(searchContext.isClosed());
        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1"));

        lease.close();
        assertTrue(searchContext.isClosed());
    }

    public void testExpireReleasesIdleRegistration() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry expiringRegistry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        expiringRegistry.register("session-1", contexts);
        now[0] = 11L;

        expiringRegistry.expire();

        assertThat(expiringRegistry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testExpireDoesNotReleaseActiveLease() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry expiringRegistry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = expiringRegistry.register("session-1", contexts);
        RetainedSearchContextsRegistry.Handle lease = expiringRegistry.acquire("session-1");
        registration.close();
        now[0] = 11L;

        expiringRegistry.expire();

        assertThat(expiringRegistry.retainedSessions(), equalTo(1));
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
            registry.closeRegistration(sessionId);
        });

        registry.register("session-1", contexts);
        RetainedSearchContextsRegistry.Handle fetchLease = registry.acquire("session-1");
        releaser.track(node, "session-1");

        releaser.close();

        assertThat(released, equalTo(List.of("node-1/session-1")));
        assertFalse(searchContext.isClosed());
        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1"));

        fetchLease.close();

        assertThat(registry.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentAcquireAndClose() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);
        int threads = randomIntBetween(4, 16);
        startInParallel(threads, i -> {
            RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1");
            handle.close();
        });

        assertFalse(searchContext.isClosed());
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentAcquireDuringRegistrationClose() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);
        int acquirers = randomIntBetween(4, 16);
        CopyOnWriteArrayList<RetainedSearchContextsRegistry.Handle> acquired = new CopyOnWriteArrayList<>();
        startInParallel(acquirers + 1, i -> {
            if (i == 0) {
                registration.close();
            } else {
                try {
                    RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1");
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

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);
        RetainedSearchContextsRegistry.Handle handle = registry.acquire("session-1");

        int threads = randomIntBetween(4, 16);
        startInParallel(threads, i -> handle.close());

        assertFalse(searchContext.isClosed());
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    public void testConcurrentCloseRegistrationFromTwoPaths() throws InterruptedException {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        RetainedSearchContextsRegistry.Handle registration = registry.register("session-1", contexts);
        startInParallel(2, i -> {
            if (i == 0) {
                registration.close();
            } else {
                registry.closeRegistration("session-1");
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
