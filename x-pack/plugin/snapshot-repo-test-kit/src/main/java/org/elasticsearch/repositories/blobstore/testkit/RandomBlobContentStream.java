/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import java.io.InputStream;

/**
 * An {@link InputStream} that's the same random ~8kB of data repeatedly, for use as a dummy payload for testing a blob store repo.
 */
class RandomBlobContentStream extends InputStream {

    private final long length;
    private final RandomBlobContent randomBlobContent;
    private long position;
    private long markPosition;

    /**
     * @param randomBlobContent The (simulated) content of the blob.
     * @param length The length of this stream.
     */
    RandomBlobContentStream(RandomBlobContent randomBlobContent, long length) {
        assert 0 < length;
        this.randomBlobContent = randomBlobContent;
        this.length = length;
    }

    private int bufferPosition() {
        return Math.toIntExact(position % randomBlobContent.buffer.length);
    }

    @Override
    public int read() {
        randomBlobContent.ensureNotCancelled(position + "/" + length);

        if (length <= position) {
            return -1;
        }

        final int b = Byte.toUnsignedInt(randomBlobContent.buffer[bufferPosition()]);
        position += 1;

        if (position == length) {
            randomBlobContent.onLastRead();
        }

        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        randomBlobContent.ensureNotCancelled(position + "+" + len + "/" + length);

        if (length <= position) {
            return -1;
        }

        len = Math.toIntExact(Math.min(len, length - position));
        int remaining = len;
        while (0 < remaining) {
            assert position < length : position + " vs " + length;

            final int bufferPosition = bufferPosition();
            final int copied = Math.min(randomBlobContent.buffer.length - bufferPosition, remaining);
            System.arraycopy(randomBlobContent.buffer, bufferPosition, b, off, copied);
            off += copied;
            remaining -= copied;
            position += copied;
        }

        assert position <= length : position + " vs " + length;

        if (position == length) {
            randomBlobContent.onLastRead();
        }

        return len;
    }

    @Override
    public void close() {
        randomBlobContent.onLastRead();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        markPosition = position;
    }

    @Override
    public synchronized void reset() {
        position = markPosition;
    }

    @Override
    public long skip(long n) {
        final long oldPosition = position;
        position = Math.min(length, position + n);
        return position - oldPosition;
    }

    @Override
    public int available() {
        return Math.toIntExact(Math.min(length - position, Integer.MAX_VALUE));
    }
}
