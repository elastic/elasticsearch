/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Wraps a {@link StorageObject} and decompresses its stream via a {@link DecompressionCodec}.
 * Used for compound extensions like .csv.gz or .ndjson.gz.
 *
 * <p>Stream-only codecs (gzip, zstd) do not support random access. {@link #newStream(long, long)}
 * and {@link #length()} throw {@link UnsupportedOperationException}.
 *
 * <p>When the codec is a {@link SplittableDecompressionCodec}, {@link #newStream(long, long)}
 * delegates to {@link SplittableDecompressionCodec#decompressRange} to support block-aligned
 * split decompression.
 */
final class DecompressingStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final DecompressionCodec codec;

    DecompressingStorageObject(StorageObject delegate, DecompressionCodec codec) {
        Check.notNull(delegate, "delegate cannot be null");
        Check.notNull(codec, "codec cannot be null");
        this.delegate = delegate;
        this.codec = codec;
    }

    @Override
    public InputStream newStream() throws IOException {
        if (codec instanceof SplittableDecompressionCodec splittable && delegate instanceof RangeStorageObject range) {
            return splittable.decompressRange(range.rawDelegate(), range.offset(), range.offset() + range.length());
        }
        InputStream raw = delegate.newStream();
        try {
            // Wrap raw in an uncloseable filter before handing it to the codec. The decompressor
            // (e.g. GZIPInputStream) cascades close() to the underlying stream; on providers like
            // S3 that drains the full response body to recycle the connection. Hiding close() from
            // the codec lets us release its inflate buffers separately from the raw stream, so
            // abortStream() below can route the abort to the raw stream without a drain.
            InputStream decompressed = codec.decompress(new UncloseableInputStream(raw));
            return new DecompressedStream(decompressed, raw);
        } catch (IOException | RuntimeException e) {
            try {
                raw.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        if (codec instanceof IndexedDecompressionCodec indexed) {
            return indexed.decompressFrame(delegate, position, length);
        }
        if (codec instanceof SplittableDecompressionCodec splittable) {
            return splittable.decompressRange(delegate, position, position + length);
        }
        throw new UnsupportedOperationException(
            "Stream-only compression ("
                + codec.name()
                + ") does not support random access; use formats that read sequentially (e.g. CSV, NDJSON)"
        );
    }

    @Override
    public long length() throws IOException {
        throw new UnsupportedOperationException("Decompressed length is unknown for stream-only compression (" + codec.name() + ")");
    }

    @Override
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics();
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {
        delegate.attachMetrics(metrics, scheme);
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        if (stream instanceof DecompressedStream ds) {
            // Close the decompressor first to release its small in-memory buffers (Inflater
            // for gzip, ZstdInputStream native handle, etc.). Because the codec was given an
            // UncloseableInputStream over raw, this close does not propagate to raw — so the
            // subsequent delegate.abortStream(raw) can take the abort path (e.g. S3
            // ResponseInputStream.abort()) instead of a draining close().
            IOException primary = null;
            try {
                ds.decompressed().close();
            } catch (IOException e) {
                primary = e;
            }
            try {
                delegate.abortStream(ds.raw());
            } catch (IOException e) {
                if (primary == null) {
                    throw e;
                }
                primary.addSuppressed(e);
                throw primary;
            }
            if (primary != null) {
                throw primary;
            }
            return;
        }
        // Streams produced by splittable/indexed codecs (via decompressRange/decompressFrame)
        // are opaque to us; their close() already releases whatever the codec opened internally.
        // Splittable reads are bounded ranges, so drain-on-close is naturally bounded.
        stream.close();
    }

    /**
     * Bundles the decompressed stream with the raw delegate stream it was built from so
     * {@link #abortStream(InputStream)} can route the abort to the raw stream — which is where
     * providers like S3 perform the connection-discard via {@code Abortable.abort()}.
     * <p>
     * The {@link #close()} override is required because the codec wraps {@code raw} in an
     * {@link UncloseableInputStream}: without it, closing the decompressor would no-op on
     * the raw stream, leaking the underlying connection. After a full read {@code raw} is at
     * EOF, so {@code raw.close()} is just connection release; partial-read callers that need
     * to skip the close-time drain on providers like S3 must use
     * {@link DecompressingStorageObject#abortStream(InputStream)} instead.
     */
    private static final class DecompressedStream extends FilterInputStream {
        private final InputStream raw;

        DecompressedStream(InputStream decompressed, InputStream raw) {
            super(decompressed);
            this.raw = raw;
        }

        InputStream decompressed() {
            return in;
        }

        InputStream raw() {
            return raw;
        }

        @Override
        public void close() throws IOException {
            IOException primary = null;
            try {
                in.close();
            } catch (IOException e) {
                primary = e;
            }
            try {
                raw.close();
            } catch (IOException e) {
                if (primary == null) {
                    throw e;
                }
                primary.addSuppressed(e);
                throw primary;
            }
            if (primary != null) {
                throw primary;
            }
        }
    }

    /**
     * Hides {@link InputStream#close()} from the wrapped stream so callers (here, the
     * decompressor) cannot cascade their close into the underlying connection. The owner of
     * the wrapped stream is responsible for closing or aborting it explicitly.
     * <p>
     * JDK stream codecs call {@code Inflater.end()} (or equivalent native-handle cleanup)
     * on their own {@code close()} <em>before</em> delegating to the wrapped stream — e.g.
     * {@code InflaterInputStream.close()} ends the {@code Inflater} and only then calls
     * {@code in.close()}. Because the underlying {@code close()} here is a no-op, codec
     * cleanup still runs; only connection release is deferred to the owner via
     * {@link DecompressingStorageObject#abortStream(InputStream)} or {@link DecompressedStream#close()}.
     */
    private static final class UncloseableInputStream extends FilterInputStream {
        UncloseableInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {
            // intentionally a no-op; outer code closes or aborts the wrapped stream directly
        }
    }
}
