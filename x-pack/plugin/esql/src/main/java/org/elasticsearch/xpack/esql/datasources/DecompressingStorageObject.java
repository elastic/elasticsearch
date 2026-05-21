/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

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
        // raw is bound to a local before being handed to the codec so that, when the codec's
        // wrapping-stream constructor throws on header inspection (e.g. SnappyFramedInputStream
        // rejects a non-snappy file with "invalid stream header"), we still own a reference to
        // the underlying stream and can close it. Closing matters because, when the delegate is
        // a ConcurrencyLimitedStorageObject, the underlying stream is a PermitReleasingInputStream
        // whose close() releases the cloud-API concurrency permit. Leaking it pins the permit
        // until JVM shutdown — and the limiter pool only holds 50.
        InputStream raw = delegate.newStream();
        try {
            return codec.decompress(raw);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(raw);
            throw t;
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
}
