/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

// We don't have _prefetch available; hence, we have to use speculative reads to enable prefetch
final class PrefetchBarrier {
    private static final VarHandle SINK_HANDLE;
    static {
        try {
            SINK_HANDLE = MethodHandles.lookup().findVarHandle(PrefetchBarrier.class, "barrier", int.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private int barrier;

    void consume(int prefetch) {
        this.barrier = prefetch;
    }

    void flush() {
        SINK_HANDLE.setOpaque(this, barrier);
    }
}
