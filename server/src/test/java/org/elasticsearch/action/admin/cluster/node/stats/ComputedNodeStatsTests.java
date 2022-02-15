/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.node.ResponseCollectorService.ComputedNodeStats;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the core calculation used for ranking nodes in adaptive replica selection.
 */
public class ComputedNodeStatsTests extends ESTestCase {

    public void testBasicInvariants() {
        // When queue size estimate is 0, the rank should equal response time.
        ComputedNodeStats stats = createStats(0, 150, 100);
        assertThat(stats.rank(0), equalTo(150.0));

        stats = createStats(0, 20, 19);
        assertThat(stats.rank(0), equalTo(20.0));
    }

    public void testParameterScaling() {
        // With non-zero queue size estimate, a larger service time should result in a larger rank.
        ComputedNodeStats first = createStats(0, 150, 100);
        ComputedNodeStats second = createStats(0, 150, 120);
        assertTrue(first.rank(1) < second.rank(1));

        first = createStats(1, 200, 199);
        second = createStats(1, 200, 200);
        assertTrue(first.rank(3) < second.rank(3));

        // A larger response time should always result in a larger rank.
        first = createStats(2, 150, 100);
        second = createStats(2, 200, 100);
        assertTrue(first.rank(1) < second.rank(1));

        first = createStats(2, 150, 150);
        second = createStats(2, 200, 150);
        assertTrue(first.rank(1) < second.rank(1));

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
