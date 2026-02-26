/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that reads NDJSON lines and produces ESQL Pages.
 */
final class NdJsonPageIterator implements CloseableIterator<Page> {

    private final NdJsonPageDecoder pageDecoder;
    private boolean endOfFile = false;
    private Page nextPage;

    NdJsonPageIterator(StorageObject object, List<String> projectedColumns, int batchSize, BlockFactory blockFactory) throws IOException {
        // FIXME: either read schema ahead of time, of buffer the stream and replay it to avoid opening it twice.
        List<Attribute> attributes;
        try (var stream = object.newStream()) {
            attributes = NdJsonSchemaInferrer.inferSchema(stream);
        }

        var inputStream = object.newStream();
        this.pageDecoder = new NdJsonPageDecoder(inputStream, attributes, projectedColumns, batchSize, blockFactory);
    }

    @Override
    public boolean hasNext() {
        if (nextPage != null) {
            return true;
        }
        if (endOfFile) {
            return false;
        }
        try {
            nextPage = pageDecoder.decodePage();
            endOfFile = nextPage == null;
            return nextPage != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page result = nextPage;
        nextPage = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(pageDecoder);
    }
}
