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
import java.util.function.Supplier;

public abstract class AbstractDocValuesForUtilBenchmark {
    protected final DocValuesForUtil forUtil;
    protected final int blockSize;

    public AbstractDocValuesForUtilBenchmark() {
        this.forUtil = new DocValuesForUtil();
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
    }

    protected long[] generateConstantInput(long value) {
        long[] data = new long[blockSize];
        Arrays.fill(data, value);
        return data;
    }

    protected long[] generateMonotonicIncreasingInput(final Supplier<Long> stepSupplier, long start) {
        final long[] data = new long[blockSize];
        data[0] = start;
        for (int i = 1; i < blockSize; i++) {
            data[i] = data[i - 1] + stepSupplier.get();
        }
        return data;
    }

    protected long[] generateMonotonicDecreasingInput(final Supplier<Long> stepSupplier, long start) {
        final long[] data = new long[blockSize];
        data[blockSize - 1] = start;
        for (int i = blockSize - 2; i >= 0; i--) {
            data[i] = data[i + 1] + stepSupplier.get();
        }
        return data;
    }

    protected long[] generateFloatingPointInput(final Supplier<Double> valueSupplier) {
        final long[] data = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = Double.doubleToLongBits(valueSupplier.get());
        }
        return data;
    }

    public abstract void benchmark(int bitsPerValue) throws IOException;

    public abstract void setupIteration() throws IOException;

    public abstract void setupInvocation(int bitsPerValue) throws IOException;
}
