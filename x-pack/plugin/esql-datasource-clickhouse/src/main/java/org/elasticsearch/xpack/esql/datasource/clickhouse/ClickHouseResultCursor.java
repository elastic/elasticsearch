/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;

/** Streams rows into breaker-accounted blocks and closes the HTTP response on cancellation. */
final class ClickHouseResultCursor implements ResultCursor {
    private final ClickHouseClient.Response response;
    private final QueryRequest request;
    private final int columnCount;
    private List<Object> nextRow;

    ClickHouseResultCursor(ClickHouseClient.Response response, QueryRequest request) throws IOException {
        this.response = response;
        this.request = request;
        columnCount = Math.max(1, request.attributes().size());
        try {
            List<Object> names = response.readRow();
            List<Object> types = response.readRow();
            if (names == null || types == null || names.size() != columnCount || types.size() != columnCount) {
                throw new IOException("ClickHouse result schema changed during the query");
            }
            for (int column = 0; column < request.attributes().size(); column++) {
                if (request.attributes().get(column).name().equals(names.get(column)) == false) {
                    throw new IOException("ClickHouse result columns do not match the query");
                }
            }
            nextRow = response.readRow();
        } catch (IOException | RuntimeException e) {
            response.close();
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return nextRow != null;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        int batchSize = Math.max(1, Math.min(request.batchSize(), 1024));
        Block.Builder[] builders = new Block.Builder[request.attributes().size()];
        Block[] blocks = new Block[builders.length];
        boolean success = false;
        try {
            for (int column = 0; column < builders.length; column++) {
                builders[column] = ClickHouseTypes.builder(request.attributes().get(column).dataType(), request.blockFactory(), batchSize);
            }
            int rows = 0;
            while (nextRow != null && rows < batchSize) {
                if (nextRow.size() != columnCount) {
                    throw new IOException("ClickHouse returned an unexpected number of column values");
                }
                for (int column = 0; column < builders.length; column++) {
                    ClickHouseTypes.append(builders[column], request.attributes().get(column).dataType(), nextRow.get(column));
                }
                rows++;
                nextRow = response.readRow();
            }
            for (int column = 0; column < builders.length; column++) {
                blocks[column] = builders[column].build();
            }
            Page page = new Page(rows, blocks);
            success = true;
            return page;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the ClickHouse result", e);
        } finally {
            Releasables.close(builders);
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    public void cancel() {
        response.connection().disconnect();
    }

    @Override
    public void close() throws IOException {
        response.close();
    }
}
