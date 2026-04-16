/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * FormatReader implementation for NDJSON files.
 * Implements {@link SegmentableFormatReader} for intra-file parallel parsing.
 */
public class NdJsonFormatReader implements SegmentableFormatReader {

    public static final String SCHEMA_SAMPLE_SIZE_SETTING = "esql.datasource.ndjson.schema_sample_size";
    public static final int DEFAULT_SCHEMA_SAMPLE_SIZE = 20_000;

    private final BlockFactory blockFactory;
    private final Settings settings;
    private final List<Attribute> resolvedSchema;
    private final int schemaSampleSize;

    public NdJsonFormatReader(Settings settings, BlockFactory blockFactory, List<Attribute> resolvedSchema) {
        this(settings, blockFactory, resolvedSchema, schemaSampleSize(settings));
    }

    NdJsonFormatReader(Settings settings, BlockFactory blockFactory) {
        this(settings, blockFactory, null);
    }

    private NdJsonFormatReader(Settings settings, BlockFactory blockFactory, List<Attribute> resolvedSchema, int schemaSampleSize) {
        this.blockFactory = blockFactory;
        this.settings = settings == null ? Settings.EMPTY : settings;
        this.resolvedSchema = resolvedSchema;
        this.schemaSampleSize = schemaSampleSize;
    }

    @Override
    public NdJsonFormatReader withSchema(List<Attribute> schema) {
        return new NdJsonFormatReader(settings, blockFactory, schema, schemaSampleSize);
    }

    @Override
    public FormatReader withConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return this;
        }
        int newSampleSize = parseInt(config.get("schema_sample_size"), schemaSampleSize);
        Check.isTrue(newSampleSize > 0, "schema_sample_size must be positive, got: {}", newSampleSize);
        if (newSampleSize == schemaSampleSize) {
            return this;
        }
        return new NdJsonFormatReader(settings, blockFactory, resolvedSchema, newSampleSize);
    }

    private List<Attribute> inferSchemaIfNeeded(List<Attribute> attributes, StorageObject object) throws IOException {
        if (attributes != null && attributes.isEmpty() == false) {
            return attributes;
        }

        try (var stream = object.newStream()) {
            return NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize);
        }
    }

    private static int schemaSampleSize(Settings settings) {
        Settings resolved = settings == null ? Settings.EMPTY : settings;
        return resolved.getAsInt(SCHEMA_SAMPLE_SIZE_SETTING, DEFAULT_SCHEMA_SAMPLE_SIZE);
    }

    private static int parseInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value [" + value + "]", e);
        }
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema;
        try (var stream = object.newStream()) {
            schema = NdJsonSchemaInferrer.inferSchema(stream, schemaSampleSize);
        }
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(
            object,
            FormatReadContext.builder().projectedColumns(projectedColumns).batchSize(batchSize).errorPolicy(defaultErrorPolicy()).build()
        );
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        boolean skipFirstLine = context.firstSplit() == false;
        boolean trimLastPartialLine = context.lastSplit() == false;
        ErrorPolicy errorPolicy = context.errorPolicy() != null ? context.errorPolicy() : defaultErrorPolicy();
        return new NdJsonPageIterator(
            object,
            context.projectedColumns(),
            context.batchSize(),
            context.rowLimit(),
            blockFactory,
            skipFirstLine,
            trimLastPartialLine,
            inferSchemaIfNeeded(resolvedSchema, object),
            errorPolicy
        );
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        long consumed = 0;
        byte[] buf = new byte[8192];
        int bytesRead;
        while ((bytesRead = stream.read(buf, 0, buf.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                consumed++;
                if (buf[i] == '\n') {
                    return consumed;
                }
                if (buf[i] == '\r') {
                    if (i + 1 < bytesRead) {
                        if (buf[i + 1] == '\n') {
                            i++;
                            consumed++;
                        }
                    } else {
                        int next = stream.read();
                        if (next == '\n') {
                            consumed++;
                        }
                    }
                    return consumed;
                }
            }
        }
        return -1;
    }

    @Override
    public String formatName() {
        return "ndjson";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".ndjson", ".jsonl", ".json");
    }

    @Override
    public void close() {
        // Nothing to close at reader level
    }
}
