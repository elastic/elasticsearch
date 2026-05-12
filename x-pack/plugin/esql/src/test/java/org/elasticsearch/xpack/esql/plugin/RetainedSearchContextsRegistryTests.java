/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.After;
import org.mockito.Mockito;

import java.util.List;

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

        RetainedSearchContextsRegistry.Lease lease;
        try (RetainedSearchContextsRegistry.Registration registration = registry.register("session-1", contexts)) {
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

        try (RetainedSearchContextsRegistry.Registration ignored = registry.register("session-1", contexts)) {
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

        RetainedSearchContextsRegistry.Lease lease;
        try (RetainedSearchContextsRegistry.Registration ignored = registry.register("session-1", contexts)) {
            lease = registry.acquire("session-1");
        }

        lease.close();
        lease.close();

        assertTrue(searchContext.isClosed());
    }

    public void testRegistrationKeepsSearchContextAliveWhileExtraReferenceReleases() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        try (RetainedSearchContextsRegistry.Registration registration = registry.register("session-1", contexts)) {
            ComputeSearchContext retainedContext = registration.searchContexts().get(0);
            retainedContext.incRef();
            retainedContext.decRef();

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

        RetainedSearchContextsRegistry.Lease lease1;
        RetainedSearchContextsRegistry.Lease lease2;
        try (RetainedSearchContextsRegistry.Registration ignored = registry.register("session-1", contexts)) {
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

    public void testAcquireAfterRegistrationCloseRejected() {
        SearchContext searchContext = createSearchContext();
        AcquiredSearchContexts contexts = createContexts(searchContext);

        registry.register("session-1", contexts);
        registry.closeRegistration("session-1");

        expectThrows(IllegalStateException.class, () -> registry.acquire("session-1"));
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
