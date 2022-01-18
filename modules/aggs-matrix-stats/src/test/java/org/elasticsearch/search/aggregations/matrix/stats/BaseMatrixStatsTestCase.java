/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;

public abstract class BaseMatrixStatsTestCase extends ESTestCase {
    protected final int numObs = atLeast(10000);
    protected final ArrayList<Double> fieldA = new ArrayList<>(numObs);
    protected final ArrayList<Double> fieldB = new ArrayList<>(numObs);
    protected final MultiPassStats actualStats = new MultiPassStats(fieldAKey, fieldBKey);
    protected static final String fieldAKey = "fieldA";
    protected static final String fieldBKey = "fieldB";

    @Before
    public void setup() {
        createStats();
    }

    public void createStats() {
        for (int n = 0; n < numObs; ++n) {
            fieldA.add(randomDouble());
            fieldB.add(randomDouble());
        }
        actualStats.computeStats(fieldA, fieldB);
    }

}
