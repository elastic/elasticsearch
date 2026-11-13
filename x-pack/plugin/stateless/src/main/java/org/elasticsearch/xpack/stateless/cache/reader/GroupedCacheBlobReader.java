/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link CacheBlobReader} that lets several consecutive cache regions of the same blob be filled from a single object store read,
 * instead of issuing one read per region.
 * <p>
 * The first {@link #getRangeInputStream} call opens a stream from the requested position up to {@code groupEnd}. Later calls are served
 * from that same stream, skipping over data that is not needed. The group is expected to hold only regions that need data, so skipping
 * is only needed when a region got cached after the group was planned. If the next requested position lies before the current stream
 * position, the stream is closed and a new one is opened.
 * <p>
 * Calls must be sequential and in increasing position order, with the previous returned stream no longer being read. This is
 * guaranteed by filling the regions of a group one after the other. Streams returned from this reader must be closed as usual, but this
 * does not close the shared stream; {@link #close()} does and must be called once the group is done.
 * <p>
 * Only use this on a delegate for which {@link CacheBlobReader#readsFromObjectStore()} is {@code true}: other readers cannot be asked
 * for more than the range returned by {@link CacheBlobReader#getRange}.
 */
public class GroupedCacheBlobReader implements CacheBlobReader, Releasable {

    private final CacheBlobReader delegate;
    private final long groupEnd;

    // Accessed by one thread at a time, with happens-before between calls given by the sequential filling of the regions
    private InputStream sharedStream;
    private long sharedStreamPosition;

    /**
     * @param delegate the reader to open the shared stream with
     * @param groupEnd the (exclusive) end position of the blob data that the group of regions needs
     */
    public GroupedCacheBlobReader(CacheBlobReader delegate, long groupEnd) {
        this.delegate = delegate;
        this.groupEnd = groupEnd;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        return delegate.getRange(position, length, remainingFileLength);
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        if (sharedStream != null && position >= sharedStreamPosition) {
            ActionListener.completeWith(listener, () -> {
                try {
                    sharedStream.skipNBytes(position - sharedStreamPosition);
                    sharedStreamPosition = position;
                    return new SharedStreamView(length);
                } catch (EOFException e) {
                    // The blob has less data than the range we were asked for, which is allowed: there is nothing left to read
                    IOUtils.closeWhileHandlingException(sharedStream);
                    sharedStream = null;
                    return InputStream.nullInputStream();
                }
            });
            return;
        }
        IOUtils.closeWhileHandlingException(sharedStream);
        sharedStream = null;
        final long end = Math.max(position + length, groupEnd);
        delegate.getRangeInputStream(position, Math.toIntExact(end - position), listener.map(stream -> {
            sharedStream = stream;
            sharedStreamPosition = position;
            return new SharedStreamView(length);
        }));
    }

    @Override
    public boolean readsFromObjectStore() {
        return delegate.readsFromObjectStore();
    }

    @Override
    public String executorName() {
        return delegate.executorName();
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(sharedStream);
        sharedStream = null;
    }

    /**
     * Exposes at most {@code length} bytes of the shared stream and keeps track of its position. Closing it leaves the shared stream open.
     */
    private class SharedStreamView extends FilterInputStream {
        private int remaining;

        private SharedStreamView(int length) {
            super(sharedStream);
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            final int b = in.read();
            if (b != -1) {
                remaining--;
                sharedStreamPosition++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (remaining <= 0) {
                return -1;
            }
            final int read = in.read(b, off, Math.min(len, remaining));
            if (read > 0) {
                remaining -= read;
                sharedStreamPosition += read;
            }
            return read;
        }

        @Override
        public long skip(long n) throws IOException {
            final long skipped = in.skip(Math.min(n, remaining));
            if (skipped > 0) {
                remaining -= (int) skipped;
                sharedStreamPosition += skipped;
            }
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return Math.min(in.available(), remaining);
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() {
            // the underlying stream is shared with the next regions of the group and is closed by GroupedCacheBlobReader#close
        }
    }
}
