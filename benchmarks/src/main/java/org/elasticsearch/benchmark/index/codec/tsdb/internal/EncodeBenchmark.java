/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.store.ByteArrayDataOutput;

import java.io.IOException;
import java.util.Random;

public abstract class EncodeBenchmark extends AbstractDocValuesForUtilBenchmark {
    protected ByteArrayDataOutput dataOutput;
    protected long[] input;

    public EncodeBenchmark(final Random random) {
        super(random);
    }

    @Override
    public void setupInvocation(int bitsPerValue) throws IOException {
        this.dataOutput.reset(new byte[Long.BYTES * blockSize]);
    }

    @Override
    public void benchmark(int bitsPerValue) throws IOException {
        forUtil.encode(this.input, bitsPerValue, this.dataOutput);
    }
}
