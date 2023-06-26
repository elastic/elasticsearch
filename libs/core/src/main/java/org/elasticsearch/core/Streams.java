/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Simple utility methods for file and stream copying. All copy methods close all affected streams when done.
 * <p>
 * Mainly for use within the framework, but also useful for application code.
 */
public class Streams {

    private static final ThreadLocal<byte[]> LOCAL_BUFFER = ThreadLocal.withInitial(() -> new byte[8 * 1024]);

    private Streams() {

    }

    /**
     * Copy the contents of the given InputStream to the given OutputStream. Optionally, closes both streams when done.
     *
     * @param in     the stream to copy from
     * @param out    the stream to copy to
     * @param close  whether to close both streams after copying
     * @param buffer buffer to use for copying
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static long copy(final InputStream in, final OutputStream out, byte[] buffer, boolean close) throws IOException {
        Exception err = null;
        try {
            long byteCount = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            return byteCount;
        } catch (IOException | RuntimeException e) {
            err = e;
            throw e;
        } finally {
            if (close) {
                IOUtils.close(err, in, out);
            }
        }
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out, boolean close) throws IOException {
        return copy(in, out, LOCAL_BUFFER.get(), close);
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out, byte[] buffer) throws IOException {
        return copy(in, out, buffer, true);
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out) throws IOException {
        return copy(in, out, LOCAL_BUFFER.get(), true);
    }

    /**
     * Read up to {code count} bytes from {@code input} and store them into {@code buffer}.
     * The buffers position will be incremented by the number of bytes read from the stream.
     * @param input stream to read from
     * @param buffer buffer to read into
     * @param count maximum number of bytes to read
     * @return number of bytes read from the stream
     * @throws IOException in case of I/O errors
     */
    public static int read(InputStream input, ByteBuffer buffer, int count) throws IOException {
        if (buffer.hasArray()) {
            return readToHeapBuffer(input, buffer, count);
        }
        return readToDirectBuffer(input, buffer, count);
    }

    private static int readToHeapBuffer(InputStream input, ByteBuffer buffer, int count) throws IOException {
        final int pos = buffer.position();
        int read = readFully(input, buffer.array(), buffer.arrayOffset() + pos, count);
        if (read > 0) {
            buffer.position(pos + read);
        }
        return read;
    }

    private static int readToDirectBuffer(InputStream input, ByteBuffer b, int count) throws IOException {
        int totalRead = 0;
        final byte[] buffer = LOCAL_BUFFER.get();
        while (totalRead < count) {
            final int len = Math.min(count - totalRead, buffer.length);
            final int read = input.read(buffer, 0, len);
            if (read == -1) {
                break;
            }
            b.put(buffer, 0, read);
            totalRead += read;
        }
        return totalRead;
    }

    public static int readFully(InputStream reader, byte[] dest) throws IOException {
        return readFully(reader, dest, 0, dest.length);
    }

    public static int readFully(InputStream reader, byte[] dest, int offset, int len) throws IOException {
        int read = 0;
        while (read < len) {
            final int r = reader.read(dest, offset + read, len - read);
            if (r == -1) {
                break;
            }
            read += r;
        }
        return read;
    }

    /**
     * Wraps an {@link OutputStream} such that it's {@code close} method becomes a noop
     *
     * @param stream {@code OutputStream} to wrap
     * @return wrapped {@code OutputStream}
     */
    public static OutputStream noCloseStream(OutputStream stream) {
        return new FilterOutputStream(stream) {

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() {
                // noop
            }
        };
    }
}
