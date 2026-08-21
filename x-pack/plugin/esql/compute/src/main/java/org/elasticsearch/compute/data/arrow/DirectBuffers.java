/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Objects;

/**
 * Allocator plus the node-lifetime {@link DirectBufferPool} that reuses its buffers across
 * queries. {@link BufferAllocator} is Arrow's type; this record is the unit passed where both
 * are required instead of two parameters.
 */
public record DirectBuffers(BufferAllocator allocator, DirectBufferPool pool) {

    public DirectBuffers {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(pool, "pool");
    }

    public ArrowBuf buffer(long size) {
        return allocator.buffer(size);
    }

    public ArrowBuf borrow(int minCapacity) {
        return pool.borrow(allocator, minCapacity);
    }

    public void returnBuf(ArrowBuf buf) {
        pool.returnBuf(buf);
    }

    public void releaseIdle() {
        pool.releaseIdle();
    }
}
