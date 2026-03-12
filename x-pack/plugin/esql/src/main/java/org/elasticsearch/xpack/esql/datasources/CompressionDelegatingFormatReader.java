/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Delegating {@link FormatReader} that wraps the raw {@link StorageObject} in a
 * {@link DecompressingStorageObject} before delegating to the inner reader.
 * Used for compound extensions like .csv.gz or .ndjson.gz.
 */
final class CompressionDelegatingFormatReader implements FormatReader {

    private final FormatReader inner;
    private final DecompressionCodec codec;

    CompressionDelegatingFormatReader(FormatReader inner, DecompressionCodec codec) {
        Check.notNull(inner, "inner reader cannot be null");
        Check.notNull(codec, "codec cannot be null");
        this.inner = inner;
        this.codec = codec;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        return inner.metadata(new DecompressingStorageObject(object, codec));
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return inner.read(new DecompressingStorageObject(object, codec), projectedColumns, batchSize);
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, ErrorPolicy errorPolicy)
        throws IOException {
        return inner.read(new DecompressingStorageObject(object, codec), projectedColumns, batchSize, errorPolicy);
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, int rowLimit)
        throws IOException {
        return inner.read(new DecompressingStorageObject(object, codec), projectedColumns, batchSize, rowLimit);
    }

    @Override
    public CloseableIterator<Page> read(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        int rowLimit,
        ErrorPolicy errorPolicy
    ) throws IOException {
        return inner.read(new DecompressingStorageObject(object, codec), projectedColumns, batchSize, rowLimit, errorPolicy);
    }

    @Override
    public CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        return inner.readSplit(
            new DecompressingStorageObject(object, codec),
            projectedColumns,
            batchSize,
            skipFirstLine,
            resolvedAttributes
        );
    }

    @Override
    public CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        boolean lastSplit,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        return inner.readSplit(
            new DecompressingStorageObject(object, codec),
            projectedColumns,
            batchSize,
            skipFirstLine,
            lastSplit,
            resolvedAttributes
        );
    }

    @Override
    public CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        boolean lastSplit,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException {
        return inner.readSplit(
            new DecompressingStorageObject(object, codec),
            projectedColumns,
            batchSize,
            skipFirstLine,
            lastSplit,
            resolvedAttributes,
            errorPolicy
        );
    }

    @Override
    public ErrorPolicy defaultErrorPolicy() {
        return inner.defaultErrorPolicy();
    }

    @Override
    public String formatName() {
        return inner.formatName();
    }

    @Override
    public List<String> fileExtensions() {
        return inner.fileExtensions();
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        FormatReader configured = inner.withConfig(config);
        return configured == inner ? this : new CompressionDelegatingFormatReader(configured, codec);
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
