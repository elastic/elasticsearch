/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Configuration holder for compression metrics in TSDB codec benchmarks.
 * Inject into {@code @Setup} methods; use {@link CompressionMetrics} for benchmark methods.
 */
@State(Scope.Benchmark)
public class MetricsConfig {

    private int blockSize;
    private int encodedBytesPerBlock;
    private int nominalBitsPerValue;

    public void configure(int blockSize, int encodedBytesPerBlock, int nominalBitsPerValue) {
        this.blockSize = blockSize;
        this.encodedBytesPerBlock = encodedBytesPerBlock;
        this.nominalBitsPerValue = nominalBitsPerValue;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getEncodedBytesPerBlock() {
        return encodedBytesPerBlock;
    }

    public int getNominalBitsPerValue() {
        return nominalBitsPerValue;
    }
}
