/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Single-split source operator factory that executes a connector query on a background thread
 * and feeds pages into an {@link AsyncExternalSourceBuffer} for the driver to consume.
 */
public class AsyncConnectorSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final Connector connector;
    private final QueryRequest baseRequest;
    private final int maxBufferSize;
    private final Executor executor;

    public AsyncConnectorSourceOperatorFactory(Connector connector, QueryRequest baseRequest, int maxBufferSize, Executor executor) {
        if (connector == null) {
            throw new IllegalArgumentException("connector must not be null");
        }
        if (baseRequest == null) {
            throw new IllegalArgumentException("baseRequest must not be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor must not be null");
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got: " + maxBufferSize);
        }
        this.connector = connector;
        this.baseRequest = baseRequest;
        this.maxBufferSize = maxBufferSize;
        this.executor = executor;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferSize);
        driverContext.addAsyncAction();
        executor.execute(() -> {
            try (ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
                ExternalSourceDrainUtils.drainPages(cursor, buffer);
                buffer.finish(false);
            } catch (Exception e) {
                buffer.onFailure(e);
            } finally {
                try {
                    connector.close();
                } catch (IOException ignored) {} finally {
                    driverContext.removeAsyncAction();
                }
            }
        });
        return new AsyncExternalSourceOperator(buffer);
    }

    @Override
    public String describe() {
        return "AsyncConnectorSource[" + connector + "]";
    }
}
