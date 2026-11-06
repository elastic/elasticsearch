/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.compute.spill.spi.ChunkSink;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A deterministic sink for the serialization fixture: records every chunk in order, takes its own reference under the
 * borrow rule and releases it before completing, and either completes inline or holds the completion until the test
 * releases it. It records contract violations, the one-outstanding-append rule and a dead reference, rather than
 * asserting on the codec thread, where an assertion error would be an {@link Error} the task cannot report; the test
 * thread reads {@link #violations()}. It also tracks the bytes it holds, which is what bounds a task's memory in flight.
 */
final class DeferredChunkSink implements ChunkSink {

    enum Mode {
        INLINE,
        DEFERRED
    }

    private final Mode mode;
    private final ByteArrayOutputStream received = new ByteArrayOutputStream();
    private final Deque<Runnable> held = new ArrayDeque<>();
    private final List<String> violations = new ArrayList<>();
    private int outstanding;
    private int maxOutstanding;
    private long bytesHeld;
    private long maxBytesHeld;
    private int appends;
    private Exception failNext;

    DeferredChunkSink(Mode mode) {
        this.mode = mode;
    }

    @Override
    public void append(ReleasableBytesReference chunk, ActionListener<Void> listener) {
        final int length = chunk.length();
        final Exception failure;
        synchronized (this) {
            if (chunk.hasReferences() == false) {
                violations.add("append " + appends + " received a chunk without a live reference");
            }
            appends++;
            outstanding++;
            if (outstanding != 1) {
                violations.add("append " + appends + " found " + outstanding + " appends outstanding");
            }
            maxOutstanding = Math.max(maxOutstanding, outstanding);
            bytesHeld += length;
            maxBytesHeld = Math.max(maxBytesHeld, bytesHeld);
            received.write(BytesReference.toBytes(chunk), 0, length);
            failure = failNext;
            failNext = null;
        }
        final ReleasableBytesReference mine = chunk.retain();
        Runnable complete = () -> {
            mine.close();
            synchronized (this) {
                outstanding--;
                bytesHeld -= length;
            }
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(null);
            }
        };
        if (mode == Mode.INLINE) {
            complete.run();
        } else {
            synchronized (this) {
                held.add(complete);
            }
        }
    }

    /**
     * Completes up to {@code n} held appends, oldest first; returns how many were completed.
     */
    int release(int n) {
        int done = 0;
        while (done < n) {
            Runnable next;
            synchronized (this) {
                next = held.poll();
            }
            if (next == null) {
                break;
            }
            next.run();
            done++;
        }
        return done;
    }

    synchronized int heldCount() {
        return held.size();
    }

    synchronized int appends() {
        return appends;
    }

    synchronized int maxOutstanding() {
        return maxOutstanding;
    }

    synchronized long bytesHeld() {
        return bytesHeld;
    }

    synchronized long maxBytesHeld() {
        return maxBytesHeld;
    }

    synchronized byte[] bytes() {
        return received.toByteArray();
    }

    synchronized List<String> violations() {
        return List.copyOf(violations);
    }

    synchronized void failNext(Exception e) {
        failNext = e;
    }
}
