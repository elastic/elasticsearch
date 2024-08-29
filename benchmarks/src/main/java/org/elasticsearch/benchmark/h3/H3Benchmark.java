/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.h3;

import org.elasticsearch.h3.H3;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class H3Benchmark {

    @Benchmark
    public void pointToH3(H3State state, Blackhole bh) {
        for (int i = 0; i < state.points.length; i++) {
            for (int res = 0; res <= 15; res++) {
                bh.consume(H3.geoToH3(state.points[i][0], state.points[i][1], res));
            }
        }
    }

    @Benchmark
    public void h3Boundary(H3State state, Blackhole bh) {
        for (int i = 0; i < state.h3.length; i++) {
            bh.consume(H3.h3ToGeoBoundary(state.h3[i]));
        }
    }

    public static void main(String[] args) throws Exception {
        Main.main(args);
    }
}
