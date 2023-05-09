/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static org.elasticsearch.tdigest.ScaleFunction.*;

public class ReproTest {

    @Test
    public void testRepro() {
        Random gen = new Random();
        gen.setSeed(1);
        double[] data = new double[10000];
        for (int i = 0; i < data.length; i++) {
            // these samples are truncated and thus have lots of duplicates
            // this can wreak havoc with the t-digest invariants
            data[i] = Math.floor(gen.nextDouble() * 10);
        }

        for (ScaleFunction sf : ScaleFunction.values()) {
            if (sf.toString().contains("NO_NORM")) {
                continue;
            }
            TDigest distLow = new MergingDigest(100);
            TDigest distMedian = new MergingDigest(100);
            TDigest distHigh = new MergingDigest(100);
            for (int i = 0; i < 500; i++) {
                MergingDigest d1 = new MergingDigest(100);
                d1.setScaleFunction(K_2);
                for (double x : data) {
                    d1.add(x);
                }
                d1.compress();
                distLow.add(d1.quantile(0.001));
                distMedian.add(d1.quantile(0.5));
                distHigh.add(d1.quantile(0.999));
            }
            Assert.assertEquals(0, distLow.quantile(0.0), 0);
            Assert.assertEquals(0, distLow.quantile(0.5), 0);
            Assert.assertEquals(0, distLow.quantile(1.0), 0);
            Assert.assertEquals(9, distHigh.quantile(0.0), 0);
            Assert.assertEquals(9, distHigh.quantile(0.5), 0);
            Assert.assertEquals(9, distHigh.quantile(1.0), 0);
            System.out.printf("%s,%.3f,%.5f,%.5f,%.5f\n",
                    sf, 0.5,
                    distMedian.quantile(0.01), distMedian.quantile(0.5), distMedian.quantile(0.99));
        }
    }
}
