/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.search.aggregations.bucket.AbstractSignificanceHeuristicTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GNDTests extends AbstractSignificanceHeuristicTestCase {
    @Override
    protected SignificanceHeuristic getHeuristic() {
        return new GND(randomBoolean());
    }

    @Override
    public void testAssertions() {
        testBackgroundAssertions(new GND(true), new GND(false));
    }

    /**
     * term is only in the subset, not at all in the other set but that is because the other set is empty.
     * this should actually not happen because only terms that are in the subset are considered now,
     * however, in this case the score should be 0 because a term that does not exist cannot be relevant...
     */
    public void testGNDCornerCases() {
        GND gnd = new GND(true);
        assertThat(gnd.getScore(0, randomIntBetween(1, 2), 0, randomIntBetween(2,3)), equalTo(0.0));
        // the terms do not co-occur at all - should be 0
        assertThat(gnd.getScore(0, randomIntBetween(1, 2), randomIntBetween(2, 3), randomIntBetween(5,6)), equalTo(0.0));
        // comparison between two terms that do not exist - probably not relevant
        assertThat(gnd.getScore(0, 0, 0, randomIntBetween(1,2)), equalTo(0.0));
        // terms co-occur perfectly - should be 1
        assertThat(gnd.getScore(1, 1, 1, 1), equalTo(1.0));
        gnd = new GND(false);
        assertThat(gnd.getScore(0, 0, 0, 0), equalTo(0.0));
    }
}
