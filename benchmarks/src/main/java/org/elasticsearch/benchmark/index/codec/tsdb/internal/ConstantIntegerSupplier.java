/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import java.util.Arrays;
import java.util.Random;

public class ConstantIntegerSupplier extends AbstractLongArraySupplier {
    private final Random random;

    public ConstantIntegerSupplier(int seed, int bitsPerValue, int size) {
        super(bitsPerValue, size);
        this.random = new Random(seed);
    }

    @Override
    public long[] get() {
        long[] data = new long[size];
        long max = 1L << bitsPerValue;
        Arrays.fill(data, random.nextLong(max));
        return data;
    }
}
