/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
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
    private static final byte[] HEADER = new byte[]{'D', 'F', 'L', '\0'};
    // 3 is a good trade-off between speed and compression ratio
    private static final int LEVEL = 3;
    // We use buffering on the input and output of in/def-laters in order to
    // limit the number of JNI calls
    private static final int BUFFER_SIZE = 4096;

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

    @Override
    public int headerLength() {
        return HEADER.length;
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

        // Thread that is currently using this reference
        private Thread user = null;

        // true if this reference is currently in use and is not available for re-use
        boolean inUse;

        protected ReleasableReference(T resource, Releasable releasable) {
            this.resource = resource;
            this.releasable = releasable;
        }

        T get() {
            if (Assertions.ENABLED) {
                assert user == null;
                user = Thread.currentThread();
            }
            assert inUse == false;
            inUse = true;
            return resource;
        }

        @Override
        public void close() {
            if (Assertions.ENABLED) {
                assert user == Thread.currentThread() :
                        "Opened on [" + user.getName() + "] but closed on [" + Thread.currentThread().getName() + "]";
                user = null;
            }
            assert inUse;
            inUse = false;
            releasable.close();
        }
    }

    @Override
    public StreamInput streamInput(StreamInput in) throws IOException {
        final byte[] headerBytes = new byte[HEADER.length];
        int len = 0;
        while (len < headerBytes.length) {
            final int read = in.read(headerBytes, len, headerBytes.length - len);
            if (read == -1) {
                break;
            }
            len += read;
        }
        if (len != HEADER.length || Arrays.equals(headerBytes, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with DEFLATE!");
        }

        final ReleasableReference<Inflater> current = inflaterForStreamRef.get();
        final Releasable releasable;
        final Inflater inflater;
        if (current.inUse) {
            // Nested de-compression streams should not happen but we still handle them safely by using a fresh Inflater
            inflater = new Inflater(true);
            releasable = inflater::end;
        } else {
            inflater = current.get();
            releasable = current;
        }
        InputStream decompressedIn = new InflaterInputStream(in, inflater, BUFFER_SIZE) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    releasable.close();
                }
            }
        };
        return new InputStreamStreamInput(new BufferedInputStream(decompressedIn, BUFFER_SIZE));
    }

    @Override
    public StreamOutput streamOutput(OutputStream out) throws IOException {
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
                    releasable.close();
                }
            }
        };
        return new OutputStreamStreamOutput(new BufferedOutputStream(deflaterOutputStream, BUFFER_SIZE));
    }

    private static final ThreadLocal<BytesStreamOutput> baos = ThreadLocal.withInitial(BytesStreamOutput::new);

    // Reusable Inflater reference. Note: This is a separate instance from the one used for the decompressing stream wrapper because we
    // want to be able to deal with decompressing bytes references that were read from a decompressing stream.
    private static final ThreadLocal<Inflater> inflaterRef = ThreadLocal.withInitial(() -> new Inflater(true));

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        final BytesStreamOutput buffer = baos.get();
        final Inflater inflater = inflaterRef.get();
        inflater.reset();
        try (InflaterOutputStream ios = new InflaterOutputStream(buffer, inflater)) {
            bytesReference.slice(HEADER.length, bytesReference.length() - HEADER.length).writeTo(ios);
        }
        final BytesReference res = buffer.copyBytes();
        buffer.reset();
        return res;
    }

    // Reusable Deflater reference. Note: This is a separate instance from the one used for the compressing stream wrapper because we
    // want to be able to deal with compressing bytes references to a decompressing stream.
    private static final ThreadLocal<Deflater> deflaterRef = ThreadLocal.withInitial(() -> new Deflater(LEVEL, true));

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        final BytesStreamOutput buffer = baos.get();
        final Deflater deflater = deflaterRef.get();
        deflater.reset();
        buffer.write(HEADER);
        try (DeflaterOutputStream dos = new DeflaterOutputStream(buffer, deflater, true)) {
            bytesReference.writeTo(dos);
        }
        final BytesReference res = buffer.copyBytes();
        buffer.reset();
        return res;
    }
}
