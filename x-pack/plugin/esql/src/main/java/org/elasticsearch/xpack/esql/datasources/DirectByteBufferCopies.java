/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.nio.ByteBuffer;

/**
 * Utilities for copying streaming {@link ByteBuffer} chunks into a pre-sized direct destination.
 */
public final class DirectByteBufferCopies {

    private DirectByteBufferCopies() {}

    /**
     * Copies {@code chunk}'s remaining bytes into {@code destination} at {@code offset} without
     * changing the destination's caller-visible position.
     */
    public static void copyChunkIntoDestination(ByteBuffer destination, int offset, ByteBuffer chunk) {
        int remaining = chunk.remaining();
        if (chunk.hasArray()) {
            int savedPosition = destination.position();
            destination.position(offset);
            destination.put(chunk.array(), chunk.arrayOffset() + chunk.position(), remaining);
            destination.position(savedPosition);
            chunk.position(chunk.position() + remaining);
        } else {
            int savedPosition = destination.position();
            destination.position(offset);
            destination.put(chunk);
            destination.position(savedPosition);
        }
    }
}
