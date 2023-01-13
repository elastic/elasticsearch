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

public class EncodeFloatingPointBenchmark extends EncodeBenchmark {

    public EncodeFloatingPointBenchmark() {
        super(new Random(17));
    }

    public void setupIteration(int bitsPerValue) {
        double min = random.nextDouble(0.0D, 1000.0D);
        this.input = generateFloatingPointInput(min, min + random.nextDouble(1000.0D, 2000.0D));
        this.dataOutput = new ByteArrayDataOutput(new byte[Long.BYTES * blockSize]);
    }
}
