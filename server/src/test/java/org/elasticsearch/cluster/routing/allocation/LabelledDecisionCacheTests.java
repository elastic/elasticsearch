/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class LabelledDecisionCacheTests extends ESTestCase {

    private final LabelledDecisionCache cache = new LabelledDecisionCache();

    public void testInterestingDecisionsAreLabelled() {
        for (Decision decision : new Decision[] { Decision.NO, Decision.NOT_PREFERRED }) {
            final var label = randomAlphaOfLength(8);
            final var result = (Decision.Single) cache.get(decision, label);
            assertThat(result.type(), equalTo(decision.type()));
            assertThat(result.label(), equalTo(label));
        }
    }

    public void testInterestingDecisionsAreCachedPerTypeAndLabel() {
        final var label = randomAlphaOfLength(8);
        final var no = cache.get(Decision.NO, label);
        assertThat(cache.get(Decision.NO, label), sameInstance(no));
        assertThat(cache.get(Decision.NO, randomAlphaOfLength(9)), not(sameInstance(no)));
        assertThat(cache.get(Decision.NOT_PREFERRED, label), not(sameInstance(no)));
    }

    public void testUninterestingDecisionsAreReturnedUnchanged() {
        assertThat(cache.get(Decision.YES, randomAlphaOfLength(8)), sameInstance(Decision.YES));
        assertThat(cache.get(Decision.THROTTLE, randomAlphaOfLength(8)), sameInstance(Decision.THROTTLE));
    }

    public void testNullLabelReturnsDecisionUnchanged() {
        for (Decision decision : new Decision[] { Decision.NO, Decision.NOT_PREFERRED, Decision.YES, Decision.THROTTLE }) {
            assertThat(cache.get(decision, null), sameInstance(decision));
        }
    }

    public void testFullCacheStillServesCachedLabelsButDoesNotCacheNewOnes() {
        final var first = cache.get(Decision.NO, "label-0");
        for (int i = 1; i < LabelledDecisionCache.DECIDER_SIZE_LIMIT; i++) {
            cache.get(Decision.NO, "label-" + i);
        }

        // Cache is full: known labels are still served from the cache
        assertThat(cache.get(Decision.NO, "label-0"), sameInstance(first));

        // Unknown labels fall back to the original decision, and are not added
        assertThat(cache.get(Decision.NO, "one-more"), sameInstance(Decision.NO));
        assertThat(cache.get(Decision.NO, "one-more"), sameInstance(Decision.NO));
    }

    public void testWarnsOnlyOnceWhenAnyCacheFills() {
        final var loggerName = LabelledDecisionCache.class.getCanonicalName();
        try (var mockLog = MockLog.capture(LabelledDecisionCache.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("not full yet", loggerName, Level.WARN, "labelled decision cache*is full*")
            );
            for (int i = 0; i < LabelledDecisionCache.DECIDER_SIZE_LIMIT; i++) {
                cache.get(Decision.NO, "label-" + i);
            }
            mockLog.assertAllExpectationsMatched();
        }

        try (var mockLog = MockLog.capture(LabelledDecisionCache.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation("cache full", loggerName, Level.WARN, "labelled decision cache for [NO]*is full*")
            );
            cache.get(Decision.NO, "label-0");
            mockLog.assertAllExpectationsMatched();
        }

        try (var mockLog = MockLog.capture(LabelledDecisionCache.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no repeat warning", loggerName, Level.WARN, "labelled decision cache*is full*")
            );
            cache.get(Decision.NO, "label-0");
            cache.get(Decision.NO, "one-more");
            // a different cache filling up does not warn again
            for (int i = 0; i <= LabelledDecisionCache.DECIDER_SIZE_LIMIT; i++) {
                cache.get(Decision.NOT_PREFERRED, "label-" + i);
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSizeLimitIsAppliedPerDecisionType() {
        for (int i = 0; i < LabelledDecisionCache.DECIDER_SIZE_LIMIT; i++) {
            cache.get(Decision.NO, "label-" + i);
        }
        assertThat(cache.get(Decision.NO, "one-more"), sameInstance(Decision.NO));

        final var notPreferred = (Decision.Single) cache.get(Decision.NOT_PREFERRED, "one-more");
        assertThat(notPreferred.label(), equalTo("one-more"));
    }
}
