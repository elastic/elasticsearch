/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.elasticsearch.common.Nullable;

/**
 * A {@code ChainingInputStream} concatenates several input streams into a single one.
 * It starts reading from the first input stream until it's exhausted, whereupon
 * it closes it and starts reading from the next one, until the last component stream is
 * exhausted.
 * <p>
 * The implementing subclass must provide the component input streams and describe the
 * chaining order, by implementing the {@link #nextElement(InputStream)} and
 * {@link #hasNextElement(InputStream)} abstract methods. The {@code ChainingInputStream}
 * assumes ownership of the component input streams as they are generated. They should
 * not be accessed by any other callers and they will be closed when they are exhausted
 * and before requesting the next one. The previous element instance is passed to the
 * {@code nextElement} method to obtain the next component.
 * <p>
 * This stream does support {@code mark} and {@code reset} but it expects that the component
 * streams also support it. Otherwise the implementing subclass must override
 * {@code markSupported} to return {@code false}.
 * <p>
 * The {@code close} call will close all the element streams that are generated (the same way
 * as if they would be iterated during a read all operation) and any subsequent {@code read},
 * {@code skip}, {@code available} and {@code reset} calls will throw {@code IOException}s.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 */
public abstract class ChainingInputStream extends InputStream {

    /**
     * The instance of the currently in use component input stream,
     * i.e. the instance servicing the read and skip calls on the {@code ChainingInputStream}
     */
    private InputStream currentIn;
    /**
     * The instance of the component input stream at the time of the last {@code mark} call.
     */
    private InputStream markIn;
    private boolean closed;

    /**
     * This method is passed the current element input stream and must return {@code true}
     * if there exists a successive one, or {@code false} otherwise. It passes {@code null}
     * at the start, when no element input stream has yet been obtained.
     */
    abstract boolean hasNextElement(@Nullable InputStream currentElementIn);

    abstract InputStream nextElement(@Nullable InputStream currentElementIn) throws IOException;

    @Override
    public int read() throws IOException {
        ensureOpen();
        do {
            int byteVal = currentIn == null ? -1 : currentIn.read();
            if (byteVal != -1) {
                return byteVal;
            }
            if (false == hasNextElement(currentIn)) {
                return -1;
            }
            nextIn();
        } while (true);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        do {
            int bytesRead = currentIn == null ? -1 : currentIn.read(b, off, len);
            if (bytesRead != -1) {
                return bytesRead;
            }
            if (false == hasNextElement(currentIn)) {
                return -1;
            }
            nextIn();
        } while (true);
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        long bytesRemaining = n;
        while (bytesRemaining > 0) {
            long bytesSkipped = currentIn == null ? 0 : currentIn.skip(bytesRemaining);
            if (bytesSkipped == 0) {
                int byteRead = read();
                if (byteRead == -1) {
                    break;
                } else {
                    bytesRemaining--;
                }
            } else {
                bytesRemaining -= bytesSkipped;
            }
        }
        return n - bytesRemaining;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return currentIn == null ? 0 : currentIn.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        if (markSupported()) {
            if (markIn != null && currentIn != markIn) {
                try {
                    markIn.close();
                } catch (IOException e) {
                }
            }
            markIn = currentIn;
            if (markIn != null) {
                markIn.mark(readlimit);
            }
        }
    }

    @Override
    public void reset() throws IOException {
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        ensureOpen();
        currentIn = markIn;
        if (currentIn != null) {
            currentIn.reset();
        }
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (currentIn != null) {
                currentIn.close();
            }
            if (markIn != null) {
                markIn.close();
            }
            while (hasNextElement(currentIn)) {
                nextIn();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private void nextIn() throws IOException {
        if (currentIn != null && currentIn != markIn) {
            currentIn.close();
        }
        currentIn = nextElement(currentIn);
        if (currentIn == null) {
            throw new NullPointerException();
        }
        if (markSupported() && false == currentIn.markSupported()) {
            throw new IllegalStateException("Component input stream must support mark");
        }
    }

}
