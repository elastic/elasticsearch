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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.List;

/**
 * FormatReader implementation for NDJSON files.
 */
public class NdJsonFormatReader implements FormatReader {

    private final BlockFactory blockFactory;

    public NdJsonFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
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
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return new NdJsonPageIterator(object, projectedColumns, batchSize, blockFactory);
    }

    @Override
    public String formatName() {
        return "ndjson";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".ndjson", ".jsonl");
    }

    @Override
    public void close() {
        // Nothing to close at reader level
    }
}
