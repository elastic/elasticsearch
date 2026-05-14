/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;

/**
 * Builder for {@link BlockFactory} instances.
 */
public class BlockFactoryBuilder {
    final BigArrays bigArrays;
    @Nullable
    CircuitBreaker breaker;
    long maxPrimitiveArraySize = BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes();
    long bytesRefRamOverestimateThreshold = BlockFactory.DEFAULT_BYTES_REF_RAM_OVERESTIMATE_THRESHOLD.getBytes();
    double bytesRefRamOverestimateFactor = BlockFactory.DEFAULT_BYTES_REF_RAM_OVERESTIMATE_FACTOR;

    BlockFactoryBuilder(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    /**
     * Override the breaker. If you don't call this the {@code breaker} will default
     * to the {@code REQUEST} {@link CircuitBreaker} in the {@link BigArrays}.
     */
    public BlockFactoryBuilder breaker(CircuitBreaker breaker) {
        this.breaker = breaker;
        return this;
    }

    /**
     * Override the maximum size we use for block of primitive arrays. If you don't
     * call this it'll default to {@link BlockFactory#DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE}.
     */
    public BlockFactoryBuilder maxPrimitiveArraySize(ByteSizeValue maxPrimitiveArraySize) {
        this.maxPrimitiveArraySize = maxPrimitiveArraySize.getBytes();
        return this;
    }

    /**
     * Override the maximum size we use for block of primitive arrays. If you don't
     * call this it'll default to {@link BlockFactory#DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE}.
     */
    public BlockFactoryBuilder maxPrimitiveArraySize(long maxPrimitiveArraySize) {
        this.maxPrimitiveArraySize = maxPrimitiveArraySize;
        return this;
    }

    /**
     * Override the threshold over which {@link BytesRefBlock}s use an inflated size in the
     * {@link CircuitBreaker}. If you don't call this it'll default to
     * {@link BlockFactory#DEFAULT_BYTES_REF_RAM_OVERESTIMATE_FACTOR}.
     */
    public BlockFactoryBuilder bytesRefRamOverestimateThreshold(ByteSizeValue bytesRefRamOverestimateThreshold) {
        this.bytesRefRamOverestimateThreshold = bytesRefRamOverestimateThreshold.getBytes();
        return this;
    }

    /**
     * Override the threshold over which {@link BytesRefBlock}s use an inflated size in the
     * {@link CircuitBreaker}. If you don't call this it'll default to
     * {@link BlockFactory#DEFAULT_BYTES_REF_RAM_OVERESTIMATE_FACTOR}.
     */
    public BlockFactoryBuilder bytesRefRamOverestimateThreshold(long bytesRefRamOverestimateThreshold) {
        this.bytesRefRamOverestimateThreshold = bytesRefRamOverestimateThreshold;
        return this;
    }

    /**
     * Override the inflation factor which large {@link BytesRefBlock}s use to inflate
     * the size in the {@link CircuitBreaker}. If you don't call this it'll default to
     * {@link BlockFactory#DEFAULT_BYTES_REF_RAM_OVERESTIMATE_THRESHOLD}.
     */
    public BlockFactoryBuilder bytesRefRamOverestimateFactor(double bytesRefRamOverestimateFactor) {
        this.bytesRefRamOverestimateFactor = bytesRefRamOverestimateFactor;
        return this;
    }

    /**
     * Build the {@link BlockFactory}.
     */
    public BlockFactory build() {
        return new BlockFactory(this, null);
    }
}
