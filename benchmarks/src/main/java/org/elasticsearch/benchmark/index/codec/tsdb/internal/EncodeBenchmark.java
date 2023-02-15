/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Supplier;

public class EncodeBenchmark extends AbstractDocValuesForUtilBenchmark {
    protected ByteArrayDataOutput dataOutput;
    protected long[] input;
    protected byte[] output;

    @Override
    public void setupIteration(int unUsedBitsPerValue, Supplier<long[]> arraySupplier) throws IOException {
        this.input = arraySupplier.get();
        this.output = new byte[Long.BYTES * blockSize];
        this.dataOutput = new ByteArrayDataOutput(this.output);
    }

    @Override
    public void setupInvocation(int unusedBitsPerValue) {
        dataOutput.reset(this.output);
    }

    @Override
    public void benchmark(int bitsPerValue, Blackhole bh) throws IOException {
        forUtil.encode(this.input, bitsPerValue, this.dataOutput);
        bh.consume(this.dataOutput);
    }
}
