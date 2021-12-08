/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.math;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.closeTo;

public class PCGTests extends ESTestCase {

    public void testUncorrelatedRandomStreams() {
        long seed = randomLong();
        PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
        double[][] randomStreams = new double[50][];
        for (int i = 0; i < 50; i++) {
            PCG pcg = new PCG(seed, randomLong());
            randomStreams[i] = DoubleStream.generate(pcg::nextInt).limit(1000).toArray();
        }
        for (int i = 0; i < 49; i++) {
            for (int j = i + 1; j < 50; j++) {
                assertThat(pearsonsCorrelation.correlation(randomStreams[i], randomStreams[j]), closeTo(0.0, 1e8));
            }
        }
    }

}
