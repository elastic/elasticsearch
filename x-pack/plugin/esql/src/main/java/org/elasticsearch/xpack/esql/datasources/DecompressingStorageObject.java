/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
    @Nullable
    private final CircuitBreaker breaker;

    DecompressingStorageObject(StorageObject delegate, DecompressionCodec codec) {
        this(delegate, codec, null);
    }

    DecompressingStorageObject(StorageObject delegate, DecompressionCodec codec, @Nullable CircuitBreaker breaker) {
        Check.notNull(delegate, "delegate cannot be null");
        Check.notNull(codec, "codec cannot be null");
        this.delegate = delegate;
        this.codec = codec;
        this.breaker = breaker;
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
            InputStream decompressed = codec.decompress(new UncloseableInputStream(raw), breaker);
            return new DecompressedStream(decompressed, raw, delegate);
        } catch (IOException | RuntimeException e) {
            try {
                // Abort rather than close so providers like S3 skip the draining connection teardown.
                delegate.abortStream(raw);
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
    public long lengthForFooterCacheKey() throws IOException {
        return delegate.lengthForFooterCacheKey();
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
            ds.releaseRaw(delegate);
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
     * {@link #close()} always aborts the raw GET. Gzip/zstd/lz4/brotli text files are one
     * whole-object GET; discarding the connection is cheaper than draining unread compressed
     * bytes. Idempotent with {@link DecompressingStorageObject#abortStream(InputStream)}.
     */
    private static final class DecompressedStream extends FilterInputStream {
        private final InputStream raw;
        private final StorageObject rawOwner;
        private final AtomicBoolean closed = new AtomicBoolean();

        DecompressedStream(InputStream decompressed, InputStream raw, StorageObject rawOwner) {
            super(decompressed);
            this.raw = raw;
            this.rawOwner = rawOwner;
        }

        InputStream decompressed() {
            return in;
        }

        InputStream raw() {
            return raw;
        }

        @Override
        public void close() throws IOException {
            releaseRaw(rawOwner);
        }

        /**
         * Closes the codec then aborts {@code raw} through {@code owner}. No-op after the first
         * successful call so {@code close()} and {@code abortStream} are interchangeable.
         */
        void releaseRaw(StorageObject owner) throws IOException {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            Exception primary = null;
            try {
                in.close();
            } catch (Exception e) {
                // RuntimeException from the codec must not skip abort: closed is already true, so a
                // later close/abortStream cannot recover the raw GET.
                primary = e;
            }
            try {
                owner.abortStream(raw);
            } catch (Exception e) {
                if (primary == null) {
                    if (e instanceof IOException ioe) {
                        throw ioe;
                    }
                    throw e;
                }
                primary.addSuppressed(e);
            }
            if (primary == null) {
                return;
            }
            if (primary instanceof IOException ioe) {
                throw ioe;
            }
            if (primary instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException(primary);
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
