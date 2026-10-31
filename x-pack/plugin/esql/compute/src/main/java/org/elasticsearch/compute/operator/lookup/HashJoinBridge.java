/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * Shares the completed hash-join build side with probe and payload-loading drivers.
 */
public final class HashJoinBridge extends AbstractRefCounted {
    private final SubscribableListener<Void> ready = new SubscribableListener<>();
    private boolean completed;

    private volatile RowInTableLookup lookup;
    private volatile Block[] payload;
    private volatile Exception failure;

    public HashJoinBridge() {}

    public SubscribableListener<Void> ready() {
        return ready;
    }

    void publish(RowInTableLookup lookup, Block[] payload) {
        Objects.requireNonNull(lookup);
        Block[] publishedPayload = Objects.requireNonNull(payload).clone();
        for (Block block : publishedPayload) {
            Objects.requireNonNull(block).allowPassingToDifferentDriver();
        }
        synchronized (this) {
            if (completed) {
                ensureNotFailed();
                throw new IllegalStateException("hash join build side has already completed");
            }
            completed = true;
            this.lookup = lookup;
            this.payload = publishedPayload;
        }
        ready.onResponse(null);
    }

    /** Unblocks every probe when the build cannot finish, including cancellation before the build driver starts. */
    public void fail(Exception failure) {
        Objects.requireNonNull(failure);
        synchronized (this) {
            if (completed) {
                return;
            }
            completed = true;
            this.failure = failure;
        }
        ready.onFailure(failure);
    }

    RowInTableLookup lookup() {
        ensureReady();
        return lookup;
    }

    Block payload(int index) {
        ensureReady();
        return payload[index];
    }

    private void ensureReady() {
        if (ready.isDone() == false) {
            throw new IllegalStateException("hash join build side is not ready");
        }
        ensureNotFailed();
    }

    private void ensureNotFailed() {
        Exception failure = this.failure;
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new ElasticsearchException("hash join build side failed", failure);
        }
    }

    @Override
    protected void closeInternal() {
        Releasables.close(lookup, payload == null ? null : Releasables.wrap(payload));
    }
}
