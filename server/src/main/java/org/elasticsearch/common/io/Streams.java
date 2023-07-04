/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Simple utility methods for file and stream copying.
 * All copy methods use a block size of 4096 bytes,
 * and close all affected streams when done.
 * <p>
 * Mainly for use within the framework,
 * but also useful for application code.
 */
public abstract class Streams {

    public static final int BUFFER_SIZE = 1024 * 8;

    /**
     * OutputStream that just throws all the bytes away
     */
    public static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {
        @Override
        public void write(int b) {
            // no-op
        }

        @Override
        public void write(byte[] b, int off, int len) {
            // no-op
        }
    };

    /**
     * Copy the contents of the given byte array to the given OutputStream.
     * Closes the stream when done.
     *
     * @param in  the byte array to copy from
     * @param out the OutputStream to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(byte[] in, OutputStream out) throws IOException {
        Objects.requireNonNull(in, "No input byte array specified");
        Objects.requireNonNull(out, "No OutputStream specified");
        try (OutputStream out2 = out) {
            out2.write(in);
        }
    }

    // ---------------------------------------------------------------------
    // Copy methods for java.io.Reader / java.io.Writer
    // ---------------------------------------------------------------------

    /**
     * Copy the contents of the given Reader to the given Writer.
     * Closes both when done.
     *
     * @param in  the Reader to copy from
     * @param out the Writer to copy to
     * @return the number of characters copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(Reader in, Writer out) throws IOException {
        Objects.requireNonNull(in, "No Reader specified");
        Objects.requireNonNull(out, "No Writer specified");
        // Leverage try-with-resources to close in and out so that exceptions in close() are either propagated or added as suppressed
        // exceptions to the main exception
        try (Reader in2 = in; Writer out2 = out) {
            return doCopy(in2, out2);
        }
    }

    private static int doCopy(Reader in, Writer out) throws IOException {
        int byteCount = 0;
        char[] buffer = new char[BUFFER_SIZE];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);
            byteCount += bytesRead;
        }
        out.flush();
        return byteCount;
    }

    /**
     * Copy the contents of the given String to the given output Writer.
     * Closes the write when done.
     *
     * @param in  the String to copy from
     * @param out the Writer to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(String in, Writer out) throws IOException {
        Objects.requireNonNull(in, "No input String specified");
        Objects.requireNonNull(out, "No Writer specified");
        try (Writer out2 = out) {
            out2.write(in);
        }
    }

    /**
     * Copy the contents of the given Reader into a String.
     * Closes the reader when done.
     *
     * @param in the reader to copy from
     * @return the String that has been copied to
     * @throws IOException in case of I/O errors
     */
    public static String copyToString(Reader in) throws IOException {
        StringWriter out = new StringWriter();
        copy(in, out);
        return out.toString();
    }

    /**
     * Fully consumes the input stream, throwing the bytes away. Returns the number of bytes consumed.
     */
    public static long consumeFully(InputStream inputStream) throws IOException {
        return org.elasticsearch.core.Streams.copy(inputStream, NULL_OUTPUT_STREAM);
    }

    public static List<String> readAllLines(InputStream input) throws IOException {
        final List<String> lines = new ArrayList<>();
        readAllLines(input, lines::add);
        return lines;
    }

    public static void readAllLines(InputStream input, Consumer<String> consumer) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                consumer.accept(line);
            }
        }
    }

    /**
     * Wraps an {@link InputStream} such that it's {@code close} method becomes a noop
     *
     * @param stream {@code InputStream} to wrap
     * @return wrapped {@code InputStream}
     */
    public static InputStream noCloseStream(InputStream stream) {
        return new FilterInputStream(stream) {
            @Override
            public void close() {
                // noop
            }
        };
    }

    /**
     * Wraps the given {@link BytesStream} in a {@link StreamOutput} that simply flushes when
     * close is called.
     */
    public static BytesStream flushOnCloseStream(BytesStream os) {
        return new FlushOnCloseOutputStream(os);
    }

    /**
     * Reads all bytes from the given {@link InputStream} and closes it afterwards.
     */
    public static BytesReference readFully(InputStream in) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        org.elasticsearch.core.Streams.copy(in, out);
        return out.bytes();
    }

    /**
     * Limits the given input stream to the provided number of bytes
     */
    public static InputStream limitStream(InputStream in, long limit) {
        return new LimitedInputStream(in, limit);
    }

    /**
     * A wrapper around a {@link BytesStream} that makes the close operation a flush. This is
     * needed as sometimes a stream will be closed but the bytes that the stream holds still need
     * to be used and the stream cannot be closed until the bytes have been consumed.
     */
    private static class FlushOnCloseOutputStream extends BytesStream {

        private final BytesStream delegate;

        private FlushOnCloseOutputStream(BytesStream bytesStreamOutput) {
            this.delegate = bytesStreamOutput;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            delegate.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            delegate.writeBytes(b, offset, length);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
        }

        @Override
        public BytesReference bytes() {
            return delegate.bytes();
        }
    }

    /**
     * A wrapper around an {@link InputStream} that limits the number of bytes that can be read from the stream.
     */
    static class LimitedInputStream extends FilterInputStream {

        private static final long NO_MARK = -1L;

        private long currentLimit; // is always non-negative
        private long limitOnLastMark;

        LimitedInputStream(InputStream in, long limit) {
            super(in);
            if (limit < 0L) {
                throw new IllegalArgumentException("limit must be non-negative");
            }
            this.currentLimit = limit;
            this.limitOnLastMark = NO_MARK;
        }

        @Override
        public int read() throws IOException {
            final int result;
            if (currentLimit == 0 || (result = in.read()) == -1) {
                return -1;
            } else {
                currentLimit--;
                return result;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int result;
            if (currentLimit == 0 || (result = in.read(b, off, Math.toIntExact(Math.min(len, currentLimit)))) == -1) {
                return -1;
            } else {
                currentLimit -= result;
                return result;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            final long skipped = in.skip(Math.min(n, currentLimit));
            currentLimit -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return Math.toIntExact(Math.min(in.available(), currentLimit));
        }

        @Override
        public synchronized void mark(int readlimit) {
            in.mark(readlimit);
            limitOnLastMark = currentLimit;
        }

        @Override
        public synchronized void reset() throws IOException {
            in.reset();
            if (limitOnLastMark != NO_MARK) {
                currentLimit = limitOnLastMark;
            }
        }
    }
}
