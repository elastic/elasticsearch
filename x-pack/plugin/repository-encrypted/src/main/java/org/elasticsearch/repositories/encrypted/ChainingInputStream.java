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
 * A {@code ChainingInputStream} concatenates multiple component input streams into a
 * single input stream.
 * It starts reading from the first input stream until it's exhausted, whereupon
 * it closes it and starts reading from the next one, until the last component input
 * stream is exhausted.
 * <p>
 * The implementing subclass provides the component input streams by implementing the
 * {@link #nextComponent(InputStream)} method. This method receives the instance of the
 * current input stream, which has been exhausted, and must return the next input stream.
 * The {@code ChainingInputStream} assumes ownership of the newly generated component input
 * stream, i.e. they should not be used by other callers and they will be closed when they
 * are exhausted or when the {@code ChainingInputStream} is closed.
 * <p>
 * This stream does support {@code mark} and {@code reset} but it expects that the component
 * streams also support it. If the component input streams do not support {@code mark} and
 * {@code reset}, the implementing subclass must override {@code markSupported} to return
 * {@code false}.
 * <p>
 * The {@code close} call will close the current component input stream and any subsequent {@code read},
 * {@code skip}, {@code available} and {@code reset} calls will throw {@code IOException}s.
 * <p>
 * The {@code ChainingInputStream} is similar in purpose to the {@link java.io.SequenceInputStream}.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 */
public abstract class ChainingInputStream extends InputStream {

    /**
     * value for the current input stream when there are no subsequent streams left, i.e. when
     * {@link #nextComponent(InputStream)} returns {@code null}
     */
    private static final InputStream EXHAUSTED_MARKER = InputStream.nullInputStream();

    /**
     * The instance of the currently in use component input stream,
     * i.e. the instance currently servicing the read and skip calls on the {@code ChainingInputStream}
     */
    protected InputStream currentIn; // protected for tests
    /**
     * The instance of the component input stream at the time of the last {@code mark} call.
     */
    private InputStream markIn;
    /**
     * {@code true} if {@link #close()} has been called; any subsequent {@code read}, {@code skip}
     * {@code available} and {@code reset} calls will throw {@code IOException}s
     */
    private boolean closed;

    /**
     * This method is responsible for generating the component input streams.
     * It is passed the current input stream and must return the successive one,
     * or {@code null} if the current component is the last one. It is passed the {@code null} value
     * at the very start, when no component input stream has yet been obtained.
     */
    abstract @Nullable InputStream nextComponent(@Nullable InputStream currentComponentIn) throws IOException;

    @Override
    public int read() throws IOException {
        ensureOpen();
        do {
            int byteVal = currentIn == null ? -1 : currentIn.read();
            if (byteVal != -1) {
                return byteVal;
            }
        } while (nextIn());
        return -1;
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
        } while (nextIn());
        return -1;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        if (currentIn == null) {
            nextIn();
        }
        long bytesRemaining = n;
        while (bytesRemaining > 0) {
            long bytesSkipped = currentIn.skip(bytesRemaining);
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
        if (currentIn == null) {
            nextIn();
        }
        return currentIn.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        if (markSupported() && false == closed) {
            // closes any previously stored mark input stream
            if (markIn != null && markIn != EXHAUSTED_MARKER && currentIn != markIn) {
                try {
                    markIn.close();
                } catch (IOException e) {
                    // an IOException on a component input stream close is not important
                }
            }
            // stores the current input stream to be reused in case of a reset
            markIn = currentIn;
            if (markIn != null && markIn != EXHAUSTED_MARKER) {
                markIn.mark(readlimit);
            }
        }
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        currentIn = markIn;
        if (currentIn != null && currentIn != EXHAUSTED_MARKER) {
            currentIn.reset();
        }
    }

    @Override
    public void close() throws IOException {
        if (false == closed) {
            closed = true;
            if (currentIn != null && currentIn != EXHAUSTED_MARKER) {
                currentIn.close();
            }
            if (markIn != null && markIn != currentIn && markIn != EXHAUSTED_MARKER) {
                markIn.close();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private boolean nextIn() throws IOException {
        if (currentIn == EXHAUSTED_MARKER) {
            return false;
        }
        // close the current component, but only if it is not saved because of mark
        if (currentIn != null && currentIn != markIn) {
            currentIn.close();
        }
        currentIn = nextComponent(currentIn);
        if (markSupported() && currentIn != null && false == currentIn.markSupported()) {
            throw new IllegalStateException("Component input stream must support mark");
        }
        if (currentIn == null) {
            currentIn = EXHAUSTED_MARKER;
            return false;
        }
        return true;
    }

}
