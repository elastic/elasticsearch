/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.core.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * One channel over one range, one read outstanding. The buffer a read returns is charged to the scope's breaker before
 * the I/O is scheduled and released when the caller closes the reference. A failed read poisons the reader, except a
 * breaker trip, which leaves it where it was.
 */
final class DiskSpillReader implements SpillReader {

    private final DiskSpillObject object;
    private final FileChannel channel;
    private final long end;
    private final CircuitBreaker breaker;
    private long position;
    private boolean outstanding;
    private boolean failed;
    private boolean closed;

    DiskSpillReader(DiskSpillObject object, FileChannel channel, long start, long end, CircuitBreaker breaker) {
        this.object = object;
        this.channel = channel;
        this.position = start;
        this.end = end;
        this.breaker = breaker;
    }

    /**
     * The channel, for tests that check a close let go of it.
     */
    FileChannel channel() {
        return channel;
    }

    @Override
    public void read(int maxBytes, ActionListener<SpillRead> listener) {
        final int length;
        final long at;
        try {
            synchronized (this) {
                length = admit(maxBytes);
                at = position;
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (length == 0) {
            listener.onResponse(SpillRead.eof());
            return;
        }
        object.scope().io(new ActionListener<>() {
            @Override
            public void onResponse(SpillRead read) {
                synchronized (DiskSpillReader.this) {
                    outstanding = false;
                    position = at + read.data().length();
                }
                listener.onResponse(read);
            }

            @Override
            public void onFailure(Exception e) {
                breaker.addWithoutBreaking(-length);
                synchronized (DiskSpillReader.this) {
                    outstanding = false;
                    failed = true;
                }
                listener.onFailure(e);
            }
        }, () -> {
            byte[] bytes = new byte[length];
            try {
                // the range lies inside the file, so a short read means the file was cut from under the reader
                Channels.readFromFileChannelWithEofException(channel, at, bytes, 0, length);
            } catch (EOFException e) {
                throw new IOException("file [" + object.path() + "] ended before [" + (at + length) + "], expected [" + end + "] bytes", e);
            }
            return SpillRead.of(new ReleasableBytesReference(new BytesArray(bytes), () -> breaker.addWithoutBreaking(-length)));
        });
    }

    /**
     * Checks the reader's state and charges the next read's buffer; zero means the range is exhausted. Called under the
     * monitor; a breaker trip propagates without poisoning the reader.
     */
    private int admit(int maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got [" + maxBytes + "]");
        }
        if (closed) {
            throw new IllegalStateException("reader is closed");
        }
        if (failed) {
            throw new IllegalStateException("reader failed earlier");
        }
        if (outstanding) {
            failed = true;
            throw new IllegalStateException("a read is outstanding");
        }
        if (object.deleted()) {
            throw new IllegalStateException("object is deleted");
        }
        if (position == end) {
            return 0;
        }
        int length = (int) Math.min(maxBytes, end - position);
        breaker.addEstimateBytesAndMaybeBreak(length, "disk spill read");
        outstanding = true;
        return length;
    }

    @Override
    public void close(ActionListener<Void> listener) {
        boolean first;
        synchronized (this) {
            first = closed == false;
            closed = true;
        }
        if (first == false) {
            listener.onResponse(null);
            return;
        }
        object.scope().ioUncancellable(listener, () -> {
            IOUtils.close(channel);
            return null;
        });
    }
}
