/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

/**
 * Abstract test case for testing NXY significant term heuristics
 */
public abstract class AbstractNXYSignificanceHeuristicTestCase extends AbstractSignificanceHeuristicTestCase {

    /**
     * @return A random instance of the heuristic to test
     */
    @Override
    protected SignificanceHeuristic getHeuristic() {
        return getHeuristic(randomBoolean(), randomBoolean());
    }

    /**
     * @param includeNegatives value for this test run, should the scores include negative values.
     * @param backgroundIsSuperset value for this test run, indicates in NXY significant terms if the background is indeed
     *                             a superset of the the subset, or is instead a disjoint set
     * @return  A random instance of an NXY heuristic to test
     */
    protected abstract SignificanceHeuristic getHeuristic(boolean includeNegatives, boolean backgroundIsSuperset);

    @Override
    public void testBasicScoreProperties() {
        testBasicScoreProperties(getHeuristic(true, true), false);
    }

}
