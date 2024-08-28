/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.h3;

import org.elasticsearch.h3.H3;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Random;

@State(Scope.Benchmark)
public class H3State {

    double[][] points = new double[1000][2];
    long[] h3 = new long[1000];

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        Random random = new Random(1234);
        for (int i = 0; i < points.length; i++) {
            points[i][0] = random.nextDouble() * 180 - 90;   // lat
            points[i][1] = random.nextDouble() * 360 - 180;  // lon
            int res = random.nextInt(16);            // resolution
            h3[i] = H3.geoToH3(points[i][0], points[i][1], res);
        }
    }
}
