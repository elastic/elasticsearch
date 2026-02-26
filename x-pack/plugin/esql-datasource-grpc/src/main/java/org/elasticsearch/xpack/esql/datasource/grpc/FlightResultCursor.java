/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.util.List;

/**
 * Wraps an Arrow {@link FlightStream} as a {@link ResultCursor}.
 * Each call to {@link #next()} converts the current {@link VectorSchemaRoot} batch
 * into an ESQL {@link Page} using {@link FlightTypeMapping}.
 */
class FlightResultCursor implements ResultCursor {

    private final FlightStream stream;
    private final List<Attribute> attributes;
    private final BlockFactory blockFactory;
    private boolean hasNextBatch;

    FlightResultCursor(FlightStream stream, List<Attribute> attributes, BlockFactory blockFactory) {
        this.stream = stream;
        this.attributes = attributes;
        this.blockFactory = blockFactory;
        this.hasNextBatch = advance();
    }

    @Override
    public boolean hasNext() {
        return hasNextBatch;
    }

    @Override
    public Page next() {
        VectorSchemaRoot root = stream.getRoot();
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[attributes.size()];
        for (int col = 0; col < attributes.size(); col++) {
            blocks[col] = FlightTypeMapping.toBlock(root.getVector(col), rowCount, blockFactory);
        }
        hasNextBatch = advance();
        return new Page(rowCount, blocks);
    }

    private boolean advance() {
        return stream.next();
    }

    @Override
    public void cancel() {
        stream.cancel("Query cancelled", null);
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (Exception e) {
            if (e instanceof IOException ioEx) {
                throw ioEx;
            }
            throw new IOException("Failed to close FlightStream", e);
        }
    }
}
