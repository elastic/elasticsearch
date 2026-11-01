/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link RangeStorageObject} over the front of a file that records whether the reader ran off its end.
 * <p>
 * A header read from a prefix is only an answer if the reader stopped before the prefix did. A reader that reached the
 * prefix's end may have been cut mid-record — a header whose last name straddles the boundary — and would return a
 * shortened list that looks complete. The caller falls back to reading the whole file when {@link #reachedEnd()}.
 */
final class HeaderPrefixProbe extends RangeStorageObject {

    private volatile boolean reachedEnd;

    HeaderPrefixProbe(StorageObject delegate, long offset, long length) {
        super(delegate, offset, length);
    }

    /**
     * Whether any stream opened from this object was read to its end.
     */
    boolean reachedEnd() {
        return reachedEnd;
    }

    @Override
    public InputStream newStream() throws IOException {
        return new EndTrackingStream(super.newStream());
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        // The delegate recognises only its own streams, so hand it the one this object wrapped.
        super.abortStream(stream instanceof EndTrackingStream tracking ? tracking.wrapped : stream);
    }

    private final class EndTrackingStream extends FilterInputStream {
        private final InputStream wrapped;

        EndTrackingStream(InputStream wrapped) {
            super(wrapped);
            this.wrapped = wrapped;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b < 0) {
                reachedEnd = true;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n < 0) {
                reachedEnd = true;
            }
            return n;
        }
    }
}
