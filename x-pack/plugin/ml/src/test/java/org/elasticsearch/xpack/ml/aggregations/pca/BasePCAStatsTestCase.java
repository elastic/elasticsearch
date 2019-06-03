/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.search.aggregations.matrix.stats.MultiPassStats;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;

public class BasePCAStatsTestCase extends ESTestCase {
    protected final int numObs = 10000; /** = 100000;*/ /** = 500000;*/ /** = 1000000;*/  //atLeast(10000);
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
//        for (int n = 1, k = 2; n < 7; n+=2, k +=2) {
//            fieldA.add((double)n);
//            fieldB.add((double)k);
//        }
        for (int n = 0; n < numObs; ++n) {
            fieldA.add(randomDouble());
            fieldB.add(randomDouble());
        }
        actualStats.computeStats(fieldA, fieldB);
    }
}
