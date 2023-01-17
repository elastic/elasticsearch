/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import java.io.IOException;
import java.util.Random;

public class DecodeFloatingPointBenchmark extends DecodeBenchmark {

    public DecodeFloatingPointBenchmark() {
        super(new Random(17));
    }

    @Override
    public void setupIteration() throws IOException {
        double min = random.nextDouble(0.0D, 1000.0D);
        this.input = generateFloatingPointInput(min, min + random.nextDouble(1000.0D, 2000.0D));
    }
}
