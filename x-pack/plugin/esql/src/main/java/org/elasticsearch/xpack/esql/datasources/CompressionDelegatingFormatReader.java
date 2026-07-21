/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        return inner.read(new DecompressingStorageObject(object, codec), context);
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(object, FormatReadContext.of(projectedColumns, batchSize));
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
    public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        Configured<FormatReader> configured = inner.withConfigTrackingConsumedKeys(config);
        FormatReader wrapped = configured.value() == inner ? this : new CompressionDelegatingFormatReader(configured.value(), codec);
        return new Configured<>(wrapped, configured.consumedKeys());
    }

    @Override
    public FormatReader withPushedFilter(Object pushedFilter) {
        FormatReader filtered = inner.withPushedFilter(pushedFilter);
        return filtered == inner ? this : new CompressionDelegatingFormatReader(filtered, codec);
    }

    @Override
    public FormatReader withSchema(List<Attribute> schema) {
        FormatReader configured = inner.withSchema(schema);
        return configured == inner ? this : new CompressionDelegatingFormatReader(configured, codec);
    }

    @Override
    public FormatReader withDeclaredDateFormats(Map<String, String> physicalNameToPattern) {
        // Delegate to the wrapped text reader (a compressed .csv.gz / .ndjson.gz still text-parses); without this the
        // interface default would return the wrapper and the declared per-column formats would be silently dropped.
        FormatReader configured = inner.withDeclaredDateFormats(physicalNameToPattern);
        return configured == inner ? this : new CompressionDelegatingFormatReader(configured, codec);
    }

    @Override
    public FormatReader withDeclaredTypeColumns(Set<String> physicalDeclaredColumns) {
        // Forward for symmetry with the other declared withers; the wrapped text readers no-op on it (they gate per-field
        // via ErrorPolicy, not on a whole-column type check), so this is inert today but keeps the wrapper transparent.
        FormatReader configured = inner.withDeclaredTypeColumns(physicalDeclaredColumns);
        return configured == inner ? this : new CompressionDelegatingFormatReader(configured, codec);
    }

    @Override
    public FormatReader withDeclaredPathBinding(boolean declaredPathBinding) {
        // Delegate to the wrapped text reader: a compressed .csv.gz binds its declared paths exactly like the plain
        // file. Without this the interface default would return the wrapper and every compressed read would silently
        // fall back to positional binding — the very bug this flag exists to fix.
        FormatReader configured = inner.withDeclaredPathBinding(declaredPathBinding);
        return configured == inner ? this : new CompressionDelegatingFormatReader(configured, codec);
    }

    @Override
    public boolean declaredNameBindingNeedsFileStart() {
        return inner.declaredNameBindingNeedsFileStart();
    }

    @Override
    public RowPositionStrategy rowPositionStrategy() {
        return inner.rowPositionStrategy();
    }

    FormatReader unwrap() {
        return inner;
    }

    DecompressionCodec codec() {
        return codec;
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
