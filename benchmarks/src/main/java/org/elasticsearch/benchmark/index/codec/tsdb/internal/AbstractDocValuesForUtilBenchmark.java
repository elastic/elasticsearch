/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Supplier;

public abstract class AbstractDocValuesForUtilBenchmark {
    protected final DocValuesForUtil forUtil;
    protected final int blockSize;

    public AbstractDocValuesForUtilBenchmark() {
        this.forUtil = new DocValuesForUtil(ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
        this.blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
    }

    public abstract void benchmark(int bitsPerValue, Blackhole bh) throws IOException;

    public abstract void setupIteration(int bitsPerValue, final Supplier<long[]> arraySupplier) throws IOException;

    public abstract void setupInvocation(int bitsPerValue) throws IOException;
}
