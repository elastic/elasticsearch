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

    public void testEnableLeakTrackerCheckOptOut() throws Exception {
        new LeakCheckOptOutFixture().leakAndTeardown();
    }

    public void testSearchHitDecRefSatisfiesTeardown() {
        SearchHit hit = new SearchHit(0);
        assertTrue(hit.decRef());
    }

    public void testUnclosedSearchHitFailsVerify() throws Exception {
        new UnclosedSearchHitFixture().assertTeardownDetectsLeak();
    }

    /**
     * {@link LeakCheckOptOutFixture} intentionally leaves a wrapped resource untracked (collector never installed).
     * After it finishes, a fresh collector must be empty — the leak must not appear as if it were registered in a
     * subsequent tracked phase on the same thread.
     */
    public void testPriorOptOutDoesNotBleedIntoNextTrackedCollector() throws Exception {
        new LeakCheckOptOutFixture().leakAndTeardown();

        LeakTracker.installTestLeakCollector();
        try {
            LeakTracker.verifyNoLeaksAndClear();
            LeakTracker.installTestLeakCollector();
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
            try {
                LeakTracker.wrap(new AbstractRefCounted() {
                    @Override
                    protected void closeInternal() {}
                });
                after();
                expectThrows(AssertionError.class, this::verifyNoOutstandingLeakTrackerLeaks);
            } finally {
                LeakTracker.clearTestLeakCollector();
            }
        }

        void assertMessageContainsCreatedAt() throws Exception {
            before();
            try {
                LeakTracker.wrap(new AbstractRefCounted() {
                    @Override
                    protected void closeInternal() {}
                });
                after();
                AssertionError e = expectThrows(AssertionError.class, this::verifyNoOutstandingLeakTrackerLeaks);
                assertThat(e.getMessage(), containsString("Leaked resources"));
                assertThat(e.getMessage(), containsString("Created at:"));
            } finally {
                LeakTracker.clearTestLeakCollector();
            }
        }
    }

    /**
     * Opt-out: intentional leak must not fail {@link #verifyNoOutstandingLeakTrackerLeaks}.
     */
    private static final class LeakCheckOptOutFixture extends ESTestCase {
        @Override
        protected boolean enableLeakTrackerCheck() {
            return false;
        }

        void leakAndTeardown() throws Exception {
            before();
            try {
                LeakTracker.wrap(new AbstractRefCounted() {
                    @Override
                    protected void closeInternal() {}
                });
                after();
                verifyNoOutstandingLeakTrackerLeaks();
            } finally {
                LeakTracker.clearTestLeakCollector();
            }
        }
    }

    private static final class UnclosedSearchHitFixture extends ESTestCase {
        void assertTeardownDetectsLeak() throws Exception {
            before();
            try {
                new SearchHit(0);
                after();
                AssertionError e = expectThrows(AssertionError.class, this::verifyNoOutstandingLeakTrackerLeaks);
                assertThat(e.getMessage(), containsString("Leaked resources"));
            } finally {
                LeakTracker.clearTestLeakCollector();
            }
        }
    }
}
