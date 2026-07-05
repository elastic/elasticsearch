/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.node.ResponseCollectorService.ComputedNodeStats;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the core calculation used for ranking nodes in adaptive replica selection.
 */
public class ComputedNodeStatsTests extends ESTestCase {

    private static final boolean ARS_ADJUSTMENT = ResponseCollectorService.ARS_FORMULA_ADJUSTMENT_FEATURE_FLAG.isEnabled();

    public void testBasicInvariants() {
        if (ARS_ADJUSTMENT) {
            // With the adjustment, rank = qHatS^3 * muBarSInverse.
            // When queueSize=0 and outstandingRequests=0: qHatS = 1, so rank = muBarSInverse = serviceTime.
            ComputedNodeStats stats = createStats(0, 150, 100);
            assertThat(stats.rank(0), equalTo(100.0));

            stats = createStats(0, 20, 19);
            assertThat(stats.rank(0), equalTo(19.0));
        } else {
            // Without the adjustment, rank = rS - muBarSInverse + qHatS^3 * muBarSInverse.
            // When queueSize=0 and outstandingRequests=0: qHatS = 1, so rank = rS.
            ComputedNodeStats stats = createStats(0, 150, 100);
            assertThat(stats.rank(0), equalTo(150.0));

            stats = createStats(0, 20, 19);
            assertThat(stats.rank(0), equalTo(20.0));
        }
    }

    public void testParameterScaling() {
        // With non-zero queue size estimate, a larger service time should result in a larger rank.
        ComputedNodeStats first = createStats(0, 150, 100);
        ComputedNodeStats second = createStats(0, 150, 120);
        assertTrue(first.rank(1) < second.rank(1));

        first = createStats(1, 200, 199);
        second = createStats(1, 200, 200);
        assertTrue(first.rank(3) < second.rank(3));

        if (ARS_ADJUSTMENT == false) {
            // A larger response time should always result in a larger rank (only when the response time term is included).
            first = createStats(2, 150, 100);
            second = createStats(2, 200, 100);
            assertTrue(first.rank(1) < second.rank(1));

            first = createStats(2, 150, 150);
            second = createStats(2, 200, 150);
            assertTrue(first.rank(1) < second.rank(1));
        }

        // More queued requests should always result in a larger rank.
        first = createStats(2, 150, 100);
        second = createStats(3, 150, 100);
        assertTrue(first.rank(1) < second.rank(1));

        // More pending requests should always result in a larger rank.
        first = createStats(2, 150, 100);
        second = createStats(2, 150, 100);
        assertTrue(first.rank(0) < second.rank(1));
    }

    private ComputedNodeStats createStats(int queueSize, int responseTimeMillis, int serviceTimeMillis) {
        return new ComputedNodeStats("node0", 5, queueSize, 1_000_000 * responseTimeMillis, 1_000_000 * serviceTimeMillis);
    }
}
