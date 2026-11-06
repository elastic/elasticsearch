/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cuts a synchronous serialization into chunks and appends each to a {@link ChunkSink} with one append outstanding: the
 * next chunk is serialized into a second buffer while the first append is in flight, and the thread waits, at the chunk
 * boundary, until that append has completed before it sends the second. Only the codec thread runs this, so the waits
 * never land on a driver, transport or I/O thread.
 */
final class ChunkingOutputStream extends OutputStream {

    private final ChunkBuffers buffers;
    private final ChunkSink sink;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition appendDone = lock.newCondition();
    private boolean appendPending;
    private Exception appendFailure;
    private boolean cancelled;

    private byte[] buffer;
    private int position;
    private volatile long bytesWritten;
    private volatile int chunksAppended;

    ChunkingOutputStream(ChunkBuffers buffers, ChunkSink sink) {
        this.buffers = buffers;
        this.sink = sink;
    }

    @Override
    public void write(int b) throws IOException {
        ensureBuffer();
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            ensureBuffer();
            int n = Math.min(len, buffer.length - position);
            System.arraycopy(b, off, buffer, position, n);
            position += n;
            off += n;
            len -= n;
        }
    }

    private void ensureBuffer() throws IOException {
        if (buffer == null) {
            buffer = buffers.acquire();
            position = 0;
        } else if (position == buffer.length) {
            appendCurrent();
            buffer = buffers.acquire();
            position = 0;
        }
    }

    /**
     * Sends the current chunk, after waiting for the previous append to complete.
     */
    private void appendCurrent() throws IOException {
        if (buffer == null || position == 0) {
            return;
        }
        awaitAppend();
        final byte[] chunk = buffer;
        final int length = position;
        buffer = null;
        position = 0;
        lock.lock();
        try {
            appendPending = true;
        } finally {
            lock.unlock();
        }
        final ReleasableBytesReference reference = new ReleasableBytesReference(
            new BytesArray(chunk, 0, length),
            () -> buffers.release(chunk)
        );
        bytesWritten += length;
        chunksAppended++;
        sink.append(reference, ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                lock.lock();
                try {
                    appendPending = false;
                    appendDone.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onFailure(Exception e) {
                lock.lock();
                try {
                    appendPending = false;
                    if (appendFailure == null) {
                        appendFailure = e;
                    }
                    appendDone.signalAll();
                } finally {
                    lock.unlock();
                }
                buffers.fail(e);
            }
        }, reference::close));
    }

    private void awaitAppend() throws IOException {
        lock.lock();
        try {
            while (true) {
                if (appendFailure != null) {
                    throw new IOException("spill append failed", appendFailure);
                }
                if (cancelled) {
                    throw new TaskCancelledException("spill cancelled");
                }
                if (appendPending == false) {
                    return;
                }
                try {
                    appendDone.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskCancelledException("spill interrupted");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends the last partial chunk and waits for every append to complete; after it returns, the sink has acknowledged
     * every byte.
     */
    long finish() throws IOException {
        appendCurrent();
        awaitAppend();
        return bytesWritten;
    }

    /**
     * Wakes a waiting codec thread, which then fails at the next chunk boundary.
     */
    void cancel() {
        lock.lock();
        try {
            cancelled = true;
            appendDone.signalAll();
        } finally {
            lock.unlock();
        }
        buffers.cancel();
    }

    long bytesWritten() {
        return bytesWritten;
    }

    int chunksAppended() {
        return chunksAppended;
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffers.release(buffer);
            buffer = null;
        }
    }
}
