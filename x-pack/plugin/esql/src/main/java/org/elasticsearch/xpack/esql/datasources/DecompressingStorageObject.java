/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
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
        return codec.decompress(delegate.newStream());
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
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
