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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.compute.spill.spi.ChunkSink;
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
 * <p>
 * With trailers on, every chunk carries the eight-byte trailer of {@link FramedRunLayout}, payload length and CRC32,
 * so the chunk's payload is {@code chunkSize} bytes and its physical size eight more; {@link #logicalPosition()} counts
 * payload bytes only, which is what a run's index records. {@link #appendRaw} sends bytes outside the chunking, for an
 * index and a footer, after the chunk in progress has been flushed.
 */
final class ChunkingOutputStream extends OutputStream {

    private final ChunkBuffers buffers;
    private final ChunkSink sink;
    private final boolean trailers;
    private final int payloadCapacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition appendDone = lock.newCondition();
    private boolean appendPending;
    private Exception appendFailure;
    private boolean cancelled;

    private byte[] buffer;
    private int position;
    private volatile long bytesWritten;
    private volatile int chunksAppended;

    /**
     * @param trailers whether each chunk ends in the layout's length-and-CRC trailer, which then occupies the last eight
     *                 bytes of every buffer
     */
    ChunkingOutputStream(ChunkBuffers buffers, ChunkSink sink, boolean trailers) {
        this.buffers = buffers;
        this.sink = sink;
        this.trailers = trailers;
        this.payloadCapacity = trailers ? buffers.chunkSize() - FramedRunLayout.CHUNK_TRAILER_BYTES : buffers.chunkSize();
        if (payloadCapacity <= 0) {
            throw new IllegalArgumentException("chunk size [" + buffers.chunkSize() + "] leaves no room for a payload");
        }
    }

    /**
     * Payload bytes per chunk: the buffer, less the trailer when there is one.
     */
    int payloadCapacity() {
        return payloadCapacity;
    }

    /**
     * Payload bytes written so far, the offset of the next byte in the run's logical stream.
     */
    long logicalPosition() {
        return bytesWritten + position;
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
            int n = Math.min(len, payloadCapacity - position);
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
        } else if (position == payloadCapacity) {
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
        final int payload = position;
        int length = payload;
        if (trailers) {
            ByteUtils.writeIntLE(payload, chunk, payload);
            ByteUtils.writeIntLE(FramedRunLayout.crc32(chunk, 0, payload), chunk, payload + Integer.BYTES);
            length = payload + FramedRunLayout.CHUNK_TRAILER_BYTES;
        }
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
        bytesWritten += payload;
        chunksAppended++;
        sink.append(reference, appendListener(reference));
    }

    /**
     * The listener for one append: records the completion or the failure, then releases the stream's own reference.
     */
    private ActionListener<Void> appendListener(ReleasableBytesReference reference) {
        return ActionListener.runAfter(new ActionListener<>() {
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
        }, reference::close);
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
     * Sends bytes outside the chunking, after the chunk in progress: the run's index and footer. The bytes belong to the
     * caller until the append completes, which this waits for.
     */
    void appendRaw(byte[] bytes) throws IOException {
        appendCurrent();
        awaitAppend();
        lock.lock();
        try {
            appendPending = true;
        } finally {
            lock.unlock();
        }
        final ReleasableBytesReference reference = ReleasableBytesReference.wrap(new BytesArray(bytes));
        sink.append(reference, appendListener(reference));
        awaitAppend();
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
