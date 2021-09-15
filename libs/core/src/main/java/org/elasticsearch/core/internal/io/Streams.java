/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
}
