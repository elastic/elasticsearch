/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataOutput;

import java.util.Random;

public class EncodeIncreasingIntegerBenchmark extends EncodeBenchmark {

    private final Random random;

    public EncodeIncreasingIntegerBenchmark(int seed) {
        this.random = new Random(seed);
    }

    public void setupIteration() {
        this.input = generateMonotonicIncreasingInput(() -> random.nextLong(0, 10), random.nextLong(0, 100));
        this.dataOutput = new ByteArrayDataOutput(new byte[Long.BYTES * blockSize]);
    }
}
