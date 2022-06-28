/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.search.aggregations.bucket.AbstractNXYSignificanceHeuristicTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MutualInformationTests extends AbstractNXYSignificanceHeuristicTestCase {
    @Override
    protected SignificanceHeuristic getHeuristic(boolean includeNegatives, boolean backgroundIsSuperset) {
        return new MutualInformation(includeNegatives, backgroundIsSuperset);
    }

    @Override
    public void testAssertions() {
        testBackgroundAssertions(new MutualInformation(true, true), new MutualInformation(true, false));
    }

    public void testScoreMutual() {
        SignificanceHeuristic heuristic = new MutualInformation(true, true);
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(2, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(0, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(2, 2, 4, 4), equalTo(0.0));
        assertThat(heuristic.getScore(1, 2, 2, 4), equalTo(0.0));
        assertThat(heuristic.getScore(3, 6, 9, 18), equalTo(0.0));

        double score = 0.0;
        try {
            long a = randomLong();
            long b = randomLong();
            long c = randomLong();
            long d = randomLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (IllegalArgumentException e) {}
        assertThat(score, lessThanOrEqualTo(1.0));
        assertThat(score, greaterThanOrEqualTo(0.0));
        heuristic = new MutualInformation(false, true);
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(Double.NEGATIVE_INFINITY));

        heuristic = new MutualInformation(true, false);
        score = heuristic.getScore(2, 3, 1, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
        score = heuristic.getScore(1, 4, 2, 3);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
        score = heuristic.getScore(1, 3, 4, 4);
        assertThat(score, greaterThanOrEqualTo(0.0));
        assertThat(score, lessThanOrEqualTo(1.0));
    }

}
