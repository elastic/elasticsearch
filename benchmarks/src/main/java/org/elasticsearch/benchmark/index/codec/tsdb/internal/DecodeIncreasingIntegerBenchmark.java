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

public class DecodeIncreasingIntegerBenchmark extends DecodeBenchmark {

    public DecodeIncreasingIntegerBenchmark() {
        super(new Random(17));
    }

    @Override
    public void setupIteration() throws IOException {
        this.input = generateMonotonicIncreasingInput(random.nextInt(1, 10), random.nextInt(1, 100));
    }
}
