/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public abstract class AbstractDocValuesForUtilBenchmark {
    protected final DocValuesForUtil forUtil;
    protected final Random random;
    protected final int blockSize;

    public AbstractDocValuesForUtilBenchmark(final Random random) {
        this.random = random;
        this.forUtil = new DocValuesForUtil();
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
    }

    protected long[] generateConstantInput(int value) {
        long[] data = new long[blockSize];
        Arrays.fill(data, value);
        return data;
    }

    protected long[] generateMonotonicIncreasingInput(int step, int start) {
        final long[] data = new long[blockSize];
        data[0] = start;
        for (int i = 1; i < blockSize; i++) {
            data[i] = data[i - 1] + random.nextInt(step);
        }
        return data;
    }

    protected long[] generateMonotonicDecreasingInput(int step, int start) {
        final long[] data = new long[blockSize];
        data[blockSize - 1] = start;
        for (int i = blockSize - 2; i >= 0; i--) {
            data[i] = data[i + 1] - random.nextInt(step);
        }
        return data;
    }

    protected long[] generateFloatingPointInput(double min, double max) {
        long[] data = new long[blockSize];
        Arrays.fill(data, Double.doubleToLongBits(random.nextDouble(min, max)));
        return data;
    }

    public abstract void benchmark(int bitsPerValue) throws IOException;

    public abstract void setupIteration(int bitsPerValue) throws IOException;

    public abstract void setupInvocation(int bitsPerValue) throws IOException;
}
