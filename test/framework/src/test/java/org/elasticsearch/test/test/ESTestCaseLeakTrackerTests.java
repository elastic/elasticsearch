/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.test;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.LeakTracker;

import static org.hamcrest.Matchers.containsString;

public class ESTestCaseLeakTrackerTests extends ESTestCase {

    public void testWrappedRefCountedReleasedPassesTeardown() {
        var rc = LeakTracker.wrap(new AbstractRefCounted() {
            @Override
            protected void closeInternal() {}
        });
        assertTrue(rc.decRef());
    }

    public void testUnclosedWrapFailsVerify() throws Exception {
        new UnclosedWrapFixture().assertTeardownDetectsLeak();
    }

    public void testFailureMessageMentionsLeak() throws Exception {
        new UnclosedWrapFixture().assertMessageContainsCreatedAt();
    }

    public void testSearchHitDecRefSatisfiesTeardown() {
        SearchHit hit = new SearchHit(0);
        assertTrue(hit.decRef());
    }

    public void testUnclosedSearchHitFailsVerify() throws Exception {
        new UnclosedSearchHitFixture().assertTeardownDetectsLeak();
    }

    public void testFreshCollectorAfterClearIsEmpty() {
        LeakTracker.clearTestLeakCollector();
        LeakTracker.installTestLeakCollector();
        try {
            SearchHit hit = new SearchHit(0);
            assertTrue(hit.decRef());
            LeakTracker.verifyNoLeaksAndClear();
        } finally {
            LeakTracker.clearTestLeakCollector();
        }
    }

    private static final class UnclosedWrapFixture extends ESTestCase {
        void assertTeardownDetectsLeak() throws Exception {
            before();
            var rc = LeakTracker.wrap(new AbstractRefCounted() {
                @Override
                protected void closeInternal() {}
            });
            try {
                after();
                expectThrows(AssertionError.class, LeakTracker::verifyNoLeaksAndClear);
            } finally {
                rc.decRef(); // cancel the Cleaner to avoid spurious LEAK log after GC
                LeakTracker.clearTestLeakCollector();
            }
        }

        void assertMessageContainsCreatedAt() throws Exception {
            before();
            var rc = LeakTracker.wrap(new AbstractRefCounted() {
                @Override
                protected void closeInternal() {}
            });
            try {
                after();
                AssertionError e = expectThrows(AssertionError.class, LeakTracker::verifyNoLeaksAndClear);
                assertThat(e.getMessage(), containsString("Leaked resources"));
                assertThat(e.getMessage(), containsString("Created at:"));
            } finally {
                rc.decRef(); // cancel the Cleaner to avoid spurious LEAK log after GC
                LeakTracker.clearTestLeakCollector();
            }
        }
    }

    // Note: only SearchHit(int) and SearchHit(int, String, ...) reach LeakTracker.wrap() via refCounted==null.
    // SearchHit.unpooled() passes ALWAYS_REFERENCED directly and is NOT tracked by this registry.
    private static final class UnclosedSearchHitFixture extends ESTestCase {
        void assertTeardownDetectsLeak() throws Exception {
            before();
            SearchHit hit = new SearchHit(0);
            try {
                after();
                AssertionError e = expectThrows(AssertionError.class, LeakTracker::verifyNoLeaksAndClear);
                assertThat(e.getMessage(), containsString("Leaked resources"));
            } finally {
                hit.decRef(); // cancel the Cleaner to avoid spurious LEAK log after GC
                LeakTracker.clearTestLeakCollector();
            }
        }
    }
}
