/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

/**
 * {@link Compressor} implementation based on the DEFLATE compression algorithm.
 */
public class DeflateCompressor implements Compressor {

    // An arbitrary header that we use to identify compressed streams
    // It needs to be different from other compressors and to not be specific
    // enough so that no stream starting with these bytes could be detected as
    // a XContent
    private static final byte[] HEADER = new byte[] { 'D', 'F', 'L', '\0' };

    public static final int HEADER_SIZE = HEADER.length;

    // 3 is a good trade-off between speed and compression ratio
    private static final int LEVEL = 3;
    // We use buffering on the input and output of in/def-laters in order to
    // limit the number of JNI calls
    public static final int BUFFER_SIZE = 4096;

    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; ++i) {
            if (bytes.get(i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    // Reusable inflater reference for streaming decompression
    private static final ThreadLocal<ReleasableReference<Inflater>> inflaterForStreamRef = ThreadLocal.withInitial(() -> {
        final Inflater inflater = new Inflater(true);
        return new ReleasableReference<>(inflater, inflater::reset);
    });

    // Reusable deflater reference for streaming compression
    private static final ThreadLocal<ReleasableReference<Deflater>> deflaterForStreamRef = ThreadLocal.withInitial(() -> {
        final Deflater deflater = new Deflater(LEVEL, true);
        return new ReleasableReference<>(deflater, deflater::reset);
    });

    // Reference to a deflater or inflater that is used to make sure we do not use the same stream twice when nesting streams.
    private static final class ReleasableReference<T> implements Releasable {

        protected final T resource;

        private final Releasable releasable;

        // Thread that is currently using this reference. Only used for assertions and only assigned if assertions are enabled.
        private Thread thread = null;

        // true if this reference is currently in use and is not available for re-use
        boolean inUse;

        protected ReleasableReference(T resource, Releasable releasable) {
            this.resource = resource;
            this.releasable = releasable;
        }

        T get() {
            if (Assertions.ENABLED) {
                assert thread == null;
                thread = Thread.currentThread();
            }
            assert inUse == false;
            inUse = true;
            return resource;
        }

        @Override
        public void close() {
            if (Assertions.ENABLED) {
                assert thread == Thread.currentThread()
                    : "Opened on [" + thread.getName() + "] but closed on [" + Thread.currentThread().getName() + "]";
                thread = null;
            }
            assert inUse;
            inUse = false;
            releasable.close();
        }
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        return inputStream(in, true);
    }

    /**
     * Creates a new input stream that decompresses the contents read from the provided input stream.
     * Closing the returned stream will close the provided input stream.
     * Optionally uses thread-local, pooled resources to save off-heap allocations if the stream is guaranteed to not escape the current
     * thread.
     *
     * @param in           input stream to wrap
     * @param threadLocal  whether this stream will only be used on the current thread or not
     * @return             decompressing stream
     */
    public static InputStream inputStream(InputStream in, boolean threadLocal) throws IOException {
        final byte[] headerBytes = new byte[HEADER.length];
        final int len = Streams.readFully(in, headerBytes);
        if (len != HEADER.length || Arrays.equals(headerBytes, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with DEFLATE!");
        }

        final Releasable releasable;
        final Inflater inflater;
        if (threadLocal) {
            final ReleasableReference<Inflater> current = inflaterForStreamRef.get();
            if (current.inUse) {
                // Nested de-compression streams should not happen but we still handle them safely by using a fresh Inflater
                inflater = new Inflater(true);
                releasable = inflater::end;
            } else {
                inflater = current.get();
                releasable = current;
            }
        } else {
            inflater = new Inflater(true);
            releasable = inflater::end;
        }
        return new InflaterInputStream(in, inflater, BUFFER_SIZE) {

            private Releasable release = releasable;

            @Override
            public void close() throws IOException {
                if (release == null) {
                    return;
                }
                try {
                    super.close();
                } finally {
                    // We need to ensure that we only call this once
                    release.close();
                    release = null;
                }
            }
        };
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        out.write(HEADER);
        final ReleasableReference<Deflater> current = deflaterForStreamRef.get();
        final Releasable releasable;
        final Deflater deflater;
        if (current.inUse) {
            // Nested compression streams should not happen but we still handle them safely by using a fresh Deflater
            deflater = new Deflater(LEVEL, true);
            releasable = deflater::end;
        } else {
            deflater = current.get();
            releasable = current;
        }
        final boolean syncFlush = true;
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(out, deflater, BUFFER_SIZE, syncFlush) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    // We are ensured to only call this once since we wrap this stream in a BufferedOutputStream that will only close
                    // its delegate once below
                    releasable.close();
                }
            }
        };
        return new BufferedOutputStream(deflaterOutputStream, BUFFER_SIZE);
    }

    private static final ThreadLocal<BytesStreamOutput> baos = ThreadLocal.withInitial(BytesStreamOutput::new);

    // Reusable Inflater reference. Note: This is a separate instance from the one used for the decompressing stream wrapper because we
    // want to be able to deal with decompressing bytes references that were read from a decompressing stream.
    private static final ThreadLocal<Inflater> inflaterRef = ThreadLocal.withInitial(() -> new Inflater(true));

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        if (bytesReference.length() < HEADER.length) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Input bytes length %d is less than DEFLATE header size %d",
                    bytesReference.length(),
                    HEADER.length
                )
            );
        }
        final BytesStreamOutput buffer = baos.get();
        try {
            final Inflater inflater = inflaterRef.get();
            try (InflaterOutputStream ios = new InflaterOutputStream(buffer, inflater)) {
                bytesReference.slice(HEADER.length, bytesReference.length() - HEADER.length).writeTo(ios);
            } finally {
                inflater.reset();
            }
            return buffer.copyBytes();
        } finally {
            buffer.reset();
        }
    }

    // Reusable Deflater reference. Note: This is a separate instance from the one used for the compressing stream wrapper because we
    // want to be able to deal with compressing bytes references to a decompressing stream.
    private static final ThreadLocal<Deflater> deflaterRef = ThreadLocal.withInitial(() -> new Deflater(LEVEL, true));

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        final BytesStreamOutput buffer = baos.get();
        try {
            buffer.write(HEADER);
            final Deflater deflater = deflaterRef.get();
            try (DeflaterOutputStream dos = new DeflaterOutputStream(buffer, deflater, true)) {
                bytesReference.writeTo(dos);
            } finally {
                deflater.reset();
            }
            return buffer.copyBytes();
        } finally {
            buffer.reset();
        }
    }
}
