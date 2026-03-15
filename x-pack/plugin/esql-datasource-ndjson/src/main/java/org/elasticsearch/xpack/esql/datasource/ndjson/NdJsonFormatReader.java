/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * FormatReader implementation for NDJSON files.
 * Implements {@link SegmentableFormatReader} for intra-file parallel parsing.
 */
public class NdJsonFormatReader implements SegmentableFormatReader {

    private final BlockFactory blockFactory;
    private final List<Attribute> resolvedSchema;

    public NdJsonFormatReader(BlockFactory blockFactory) {
        this(blockFactory, null);
    }

    private NdJsonFormatReader(BlockFactory blockFactory, List<Attribute> resolvedSchema) {
        this.blockFactory = blockFactory;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public NdJsonFormatReader withSchema(List<Attribute> schema) {
        return new NdJsonFormatReader(blockFactory, schema);
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema;
        try (var stream = object.newStream()) {
            schema = NdJsonSchemaInferrer.inferSchema(stream);
        }
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        boolean skipFirstLine = context.firstSplit() == false;
        boolean trimLastPartialLine = context.lastSplit() == false;
        return new NdJsonPageIterator(
            object,
            context.projectedColumns(),
            context.batchSize(),
            blockFactory,
            skipFirstLine,
            trimLastPartialLine,
            resolvedSchema
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
