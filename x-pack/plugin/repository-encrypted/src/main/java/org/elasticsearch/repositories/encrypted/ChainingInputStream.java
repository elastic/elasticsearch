/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Objects;

/**
 * A {@code ChainingInputStream} concatenates multiple component input streams into a
 * single input stream.
 * It starts reading from the first input stream until it's exhausted, whereupon
 * it closes it and starts reading from the next one, until the last component input
 * stream is exhausted.
 * <p>
 * The implementing subclass provides the component input streams by implementing the
 * {@link #nextComponent(InputStream)} method. This method receives the instance of the
 * current input stream, which has been exhausted, and must return the next input stream,
 * or {@code null} if there are no more component streams.
 * The {@code ChainingInputStream} assumes ownership of the newly generated component input
 * stream, i.e. components should not be used by other callers and they will be closed
 * when they are exhausted or when the {@code ChainingInputStream} is closed.
 * <p>
 * This stream does support {@code mark} and {@code reset} but it expects that the component
 * streams also support it. When {@code mark} is invoked on the chaining input stream, the
 * call is forwarded to the current input stream component and a reference to that component
 * is stored internally. A {@code reset} invocation on the chaining input stream will then make the
 * stored component the current component and will then call the {@code reset} on it.
 * The {@link #nextComponent(InputStream)} method must be able to generate the same components
 * anew, starting from the component of the {@code reset} call.
 * If the component input streams do not support {@code mark}/{@code reset} or
 * {@link #nextComponent(InputStream)} cannot generate the same component multiple times,
 * the implementing subclass must override {@link #markSupported()} to return {@code false}.
 * <p>
 * The {@code close} call will close the current component input stream and any subsequent {@code read},
 * {@code skip}, {@code available} and {@code reset} calls will throw {@code IOException}s.
 * <p>
 * The {@code ChainingInputStream} is similar in purpose to the {@link java.io.SequenceInputStream},
 * with the addition of {@code mark}/{@code reset} support.
 * <p>
 * This is NOT thread-safe, multiple threads sharing a single instance must synchronize access.
 */
public abstract class ChainingInputStream extends InputStream {

    private static final Logger LOGGER = LogManager.getLogger(ChainingInputStream.class);

    /**
     * value for the current input stream when there are no subsequent streams remaining, i.e. when
     * {@link #nextComponent(InputStream)} returns {@code null}
     */
    protected static final InputStream EXHAUSTED_MARKER = InputStream.nullInputStream(); // protected for tests

    /**
     * The instance of the currently in use component input stream,
     * i.e. the instance currently servicing the read and skip calls on the {@code ChainingInputStream}
     */
    protected InputStream currentIn; // protected for tests
    /**
     * The instance of the component input stream at the time of the last {@code mark} call.
     */
    protected InputStream markIn; // protected for tests
    /**
     * {@code true} if {@link #close()} has been called; any subsequent {@code read}, {@code skip}
     * {@code available} and {@code reset} calls will throw {@code IOException}s
     */
    private boolean closed;

    /**
     * Returns a new {@link ChainingInputStream} that concatenates the bytes to be read from the first
     * input stream with the bytes from the second input stream. The stream arguments must support
     * the {@code mark} and {@code reset} operations; otherwise use {@link SequenceInputStream}.
     *
     * @param first the input stream supplying the first bytes of the returned {@link ChainingInputStream}
     * @param second the input stream supplying the bytes after the {@code first} input stream has been exhausted
     */
    public static ChainingInputStream chain(InputStream first, InputStream second) {
        if (false == Objects.requireNonNull(first).markSupported()) {
            throw new IllegalArgumentException("The first component input stream does not support mark");
        }
        if (false == Objects.requireNonNull(second).markSupported()) {
            throw new IllegalArgumentException("The second component input stream does not support mark");
        }
        // components can be reused, and the {@code ChainingInputStream} eagerly closes components after every use
        // "first" and "second" are closed when the returned {@code ChainingInputStream} is closed
        final InputStream firstComponent = Streams.noCloseStream(first);
        final InputStream secondComponent = Streams.noCloseStream(second);
        // be sure to remember the start of components because they might be reused
        firstComponent.mark(Integer.MAX_VALUE);
        secondComponent.mark(Integer.MAX_VALUE);

        return new ChainingInputStream() {

            @Override
            InputStream nextComponent(InputStream currentComponentIn) throws IOException {
                if (currentComponentIn == null) {
                    // when returning the next component, start from its beginning
                    firstComponent.reset();
                    return firstComponent;
                } else if (currentComponentIn == firstComponent) {
                    // when returning the next component, start from its beginning
                    secondComponent.reset();
                    return secondComponent;
                } else if (currentComponentIn == secondComponent) {
                    return null;
                } else {
                    throw new IllegalStateException("Unexpected component input stream");
                }
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(super::close, first, second);
            }
        };
    }

    /**
     * This method is responsible for generating the component input streams.
     * It is passed the current input stream and must return the successive one,
     * or {@code null} if the current component is the last one.
     * It is passed the {@code null} value at the very start, when no component
     * input stream has yet been generated.
     * The successive input stream returns the bytes (during reading) that should
     * logically follow the bytes that have been previously returned by the passed-in
     * {@code currentComponentIn}; i.e. the first {@code read} call on the next
     * component returns the byte logically following the last byte of the previous
     * component.
     * In order to support {@code mark}/{@code reset} this method must be able
     * to generate the successive input stream given any of the previously generated
     * ones, i.e. implementors must not assume that the passed-in argument is the
     * instance last returned by this method. Therefore, implementors must identify
     * the bytes that the passed-in component generated and must return a new
     * {@code InputStream} which returns the bytes that logically follow, even if
     * the same sequence has been previously returned by another component.
     * If this is not possible, and the implementation
     * can only generate the component input streams once, it must override
     * {@link #nextComponent(InputStream)} to return {@code false}.
     */
    abstract @Nullable InputStream nextComponent(@Nullable InputStream currentComponentIn) throws IOException;

    /**
     * Reads the next byte of data from this chaining input stream.
     * The value byte is returned as an {@code int} in the range
     * {@code 0} to {@code 255}. If no byte is available
     * because the end of the stream has been reached, the value
     * {@code -1} is returned. The end of the chaining input stream
     * is reached when the end of the last component stream is reached.
     * This method blocks until input data is available (possibly
     * asking for the next input stream component), the end of
     * the stream is detected, or an exception is thrown.
     *
     * @return     the next byte of data, or {@code -1} if the end of the
     *             stream is reached.
     * @exception  IOException  if this stream has been closed or
     *             an I/O error occurs on the current component stream.
     * @see        ChainingInputStream#read(byte[], int, int)
     */
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

    /**
     * Reads up to {@code len} bytes of data into an array of bytes from this
     * chaining input stream. If {@code len} is zero, then no bytes are read
     * and {@code 0} is returned; otherwise, there is an attempt to read at least one byte.
     * The {@code read} call is forwarded to the current component input stream.
     * If the current component input stream is exhausted, the next one is obtained
     * by invoking {@link #nextComponent(InputStream)} and the {@code read} call is
     * forwarded to that. If the current component is exhausted
     * and there is no subsequent component the value {@code -1} is returned;
     * otherwise, at least one byte is read and stored into {@code b}, starting at
     * offset {@code off}.
     *
     * @param   b     the buffer into which the data is read.
     * @param   off   the start offset in the destination array {@code b}
     * @param   len   the maximum number of bytes read.
     * @return  the total number of bytes read into the buffer, or
     *          {@code -1} if there is no more data because the current
     *          input stream component is exhausted and there is no next one
     *          {@link #nextComponent(InputStream)} retuns {@code null}.
     * @throws  NullPointerException If {@code b} is {@code null}.
     * @throws  IndexOutOfBoundsException If {@code off} is negative,
     *          {@code len} is negative, or {@code len} is greater than
     *          {@code b.length - off}
     * @throws  IOException if this stream has been closed or an I/O error
     *          occurs on the current component input stream.
     * @see     java.io.InputStream#read(byte[], int, int)
     */
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

    /**
     * Skips over and discards {@code n} bytes of data from the
     * chaining input stream. If {@code n} is negative or {@code 0},
     * the value {@code 0} is returned and no bytes are skipped.
     * The {@code skip} method will skip exactly {@code n} bytes,
     * possibly generating the next component input streams and
     * recurring to {@code read} if {@code skip} on the current
     * component does not make progress (returns 0).
     * The actual number of bytes skipped, which can be smaller than
     * {@code n}, is returned.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @throws     IOException if this stream is closed, or if
     *             {@code currentComponentIn.skip(n)} throws an IOException
     */
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

    /**
     * Returns an estimate of the number of bytes that can be read (or
     * skipped over) from this chaining input stream without blocking by the next
     * caller of a method for this stream. The next caller might be
     * the same thread or another thread. A single read or skip of this
     * many bytes will not block, but may read or skip fewer bytes.
     * <p>
     * This simply forwards the {@code available} call to the current
     * component input stream, so the returned value is a conservative
     * lower bound of the available byte count; i.e. it's possible that
     * subsequent component streams have available bytes but this method
     * only returns the available bytes of the current component.
     *
     * @return     an estimate of the number of bytes that can be read (or skipped
     *             over) from this input stream without blocking.
     * @exception  IOException  if this stream is closed or if
     *             {@code currentIn.available()} throws an IOException
     */
    @Override
    public int available() throws IOException {
        ensureOpen();
        if (currentIn == null) {
            nextIn();
        }
        return currentIn.available();
    }

    /**
     * Tests if this chaining input stream supports the {@code mark} and
     * {@code reset} methods. By default this returns {@code true} but there
     * are some requirements for how components are generated (see
     * {@link #nextComponent(InputStream)}), in which case, if the implementer
     * cannot satisfy them, it should override this to return {@code false}.
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Marks the current position in this input stream. A subsequent call to
     * the {@code reset} method repositions this stream at the last marked
     * position so that subsequent reads re-read the same bytes.
     * <p>
     * The {@code readlimit} arguments tells this input stream to
     * allow that many bytes to be read before the mark position can be
     * invalidated.
     * <p>
     * The {@code mark} call is forwarded to the current component input
     * stream and a reference to it is stored internally.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position can be invalidated.
     * @see     BufferOnMarkInputStream#reset()
     * @see     java.io.InputStream#mark(int)
     */
    @Override
    public void mark(int readlimit) {
        if (markSupported() && false == closed) {
            // closes any previously stored mark input stream
            if (markIn != null && markIn != EXHAUSTED_MARKER && currentIn != markIn) {
                try {
                    markIn.close();
                } catch (IOException e) {
                    // an IOException on a component input stream close is not important
                    LOGGER.info("IOException while closing a marked component input stream during a mark", e);
                }
            }
            // stores the current input stream to be reused in case of a reset
            markIn = currentIn;
            if (markIn != null && markIn != EXHAUSTED_MARKER) {
                markIn.mark(readlimit);
            }
        }
    }

    /**
     * Repositions this stream to the position at the time the
     * {@code mark} method was last called on this chaining input stream,
     * or at the beginning if the {@code mark} method was never called.
     * Subsequent read calls will return the same bytes in the same
     * order since the point of the {@code mark} call. Naturally,
     * {@code mark} can be invoked at any moment, even after a
     * {@code reset}.
     * <p>
     * The previously stored reference to the current component during the
     * {@code mark} invocation is made the new current component and then
     * the {@code reset} call is forwarded to it. The next internal call to
     * {@link #nextComponent(InputStream)} will use this component, so
     * the {@link #nextComponent(InputStream)} must not assume monotonous
     * arguments.
     *
     * @throws IOException  if the stream has been closed or the number of bytes
     *                      read since the last mark call exceeded the
     *                      {@code readLimit} parameter
     * @see     java.io.InputStream#mark(int)
     */
    @Override
    public void reset() throws IOException {
        ensureOpen();
        if (false == markSupported()) {
            throw new IOException("Mark/reset not supported");
        }
        if (currentIn != null && currentIn != EXHAUSTED_MARKER && currentIn != markIn) {
            try {
                currentIn.close();
            } catch (IOException e) {
                // an IOException on a component input stream close is not important
                LOGGER.info("IOException while closing the current component input stream during a reset", e);
            }
        }
        currentIn = markIn;
        if (currentIn != null && currentIn != EXHAUSTED_MARKER) {
            currentIn.reset();
        }
    }

    /**
     * Closes this chaining input stream, closing the current component stream as well
     * as any internally stored reference of a component during a {@code mark} call.
     *
     * @exception  IOException  if an I/O error occurs while closing the current or the marked stream.
     */
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
        if (currentIn == null) {
            currentIn = EXHAUSTED_MARKER;
            return false;
        }
        if (markSupported() && false == currentIn.markSupported()) {
            throw new IllegalStateException("Component input stream must support mark");
        }
        return true;
    }

}
