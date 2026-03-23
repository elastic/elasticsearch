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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that reads NDJSON lines and produces ESQL Pages.
 *
 * <p>When {@code skipFirstLine} is true (for non-first splits), discards bytes up to
 * and including the first newline before starting to parse. This implements the standard
 * line-alignment protocol for split boundary handling.
 *
 * <p>When {@code resolvedAttributes} is provided, uses those instead of inferring schema
 * from the split data, avoiding the risk of schema divergence across splits.
 */
final class NdJsonPageIterator implements CloseableIterator<Page> {

    private final NdJsonPageDecoder pageDecoder;
    private boolean endOfFile = false;
    private Page nextPage;

    NdJsonPageIterator(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        BlockFactory blockFactory,
        boolean skipFirstLine,
        boolean trimLastPartialLine,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        InputStream inputStream = object.newStream();
        if (skipFirstLine) {
            skipToNextLine(inputStream);
        }
        if (trimLastPartialLine) {
            inputStream = trimLastPartialLine(inputStream);
        }
        this.pageDecoder = new NdJsonPageDecoder(inputStream, resolvedAttributes, projectedColumns, batchSize, blockFactory);
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

    static void skipToNextLine(InputStream stream) throws IOException {
        int b;
        while ((b = stream.read()) != -1) {
            if (b == '\n') {
                return;
            }
        }
    }

    private static final int TRIM_CHUNK_SIZE = 8192;

    static InputStream trimLastPartialLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[TRIM_CHUNK_SIZE];
        int lastNewline = -1;
        int totalRead = 0;
        int bytesRead;
        try {
            while ((bytesRead = in.read(chunk)) != -1) {
                int baseOffset = totalRead;
                buffer.write(chunk, 0, bytesRead);
                for (int i = bytesRead - 1; i >= 0; i--) {
                    if (chunk[i] == '\n') {
                        lastNewline = baseOffset + i;
                        break;
                    }
                }
                totalRead += bytesRead;
            }
        } finally {
            IOUtils.close(in);
        }
        if (lastNewline == -1) {
            return new ByteArrayInputStream(new byte[0]);
        }
        return new ByteArrayInputStream(buffer.toByteArray(), 0, lastNewline + 1);
    }
}
