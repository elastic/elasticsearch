/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Single-split source operator factory that executes a connector query on a background thread
 * and feeds pages into an {@link AsyncExternalSourceBuffer} for the driver to consume.
 * <p>
 * Uses non-blocking async drain: the producer thread is released when the buffer is full and
 * resumes via the executor when space is freed, with no timeout.
 */
public class AsyncConnectorSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final Connector connector;
    private final QueryRequest baseRequest;
    private final int maxBufferSize;
    private final int rowLimit;
    private final Executor executor;
    private final ExternalSliceQueue sliceQueue;

    public AsyncConnectorSourceOperatorFactory(
        Connector connector,
        QueryRequest baseRequest,
        int maxBufferSize,
        Executor executor,
        @Nullable ExternalSliceQueue sliceQueue
    ) {
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
        this.rowLimit = baseRequest.rowLimit();
        this.executor = executor;
        this.sliceQueue = sliceQueue;
    }

    public AsyncConnectorSourceOperatorFactory(Connector connector, QueryRequest baseRequest, int maxBufferSize, Executor executor) {
        this(connector, baseRequest, maxBufferSize, executor, null);
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
        long maxBufferBytes = (long) maxBufferSize * Operator.TARGET_PAGE_SIZE;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        driverContext.addAsyncAction();

        ActionListener<Void> failureListener = ActionListener.wrap(v -> {}, e -> {
            buffer.onFailure(e);
            closeConnectorQuietly();
            driverContext.removeAsyncAction();
        });
        if (sliceQueue != null) {
            executor.execute(
                ActionRunnable.run(failureListener, () -> processNextSplit(request, sliceQueue, buffer, driverContext, rowLimit))
            );
        } else {
            executor.execute(ActionRunnable.run(failureListener, () -> {
                ResultCursor cursor = connector.execute(request, Split.SINGLE);
                ExternalSourceDrainUtils.drainPagesWithBudgetAsync(
                    cursor,
                    buffer,
                    rowLimit,
                    executor,
                    ActionListener.runAfter(
                        ActionListener.<Integer>wrap(consumed -> buffer.finish(false), e -> buffer.onFailure(e)),
                        () -> {
                            closeQuietly(cursor);
                            closeConnectorQuietly();
                            driverContext.removeAsyncAction();
                        }
                    )
                );
            }));
        }
        return new AsyncExternalSourceOperator(buffer);
    }

    private void processNextSplit(
        QueryRequest request,
        ExternalSliceQueue queue,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        int rowsRemaining
    ) {
        if (buffer.noMoreInputs() || (rowLimit != FormatReader.NO_LIMIT && rowsRemaining <= 0)) {
            buffer.finish(false);
            closeConnectorQuietly();
            driverContext.removeAsyncAction();
            return;
        }
        ExternalSplit split = queue.nextSplit();
        if (split == null) {
            buffer.finish(false);
            closeConnectorQuietly();
            driverContext.removeAsyncAction();
            return;
        }

        ResultCursor cursor;
        try {
            cursor = connector.execute(request, split);
        } catch (Exception e) {
            buffer.onFailure(e);
            closeConnectorQuietly();
            driverContext.removeAsyncAction();
            return;
        }

        ActionListener<Void> failureListener = ActionListener.wrap(v -> {}, e -> {
            buffer.onFailure(e);
            closeConnectorQuietly();
            driverContext.removeAsyncAction();
        });
        ExternalSourceDrainUtils.drainPagesWithBudgetAsync(
            cursor,
            buffer,
            rowsRemaining,
            executor,
            ActionListener.runAfter(ActionListener.<Integer>wrap(consumed -> {
                int remaining = rowLimit != FormatReader.NO_LIMIT ? rowsRemaining - consumed : rowsRemaining;
                executor.execute(
                    ActionRunnable.run(failureListener, () -> processNextSplit(request, queue, buffer, driverContext, remaining))
                );
            }, failureListener::onFailure), () -> closeQuietly(cursor))
        );
    }

    private void closeConnectorQuietly() {
        try {
            connector.close();
        } catch (IOException ignored) {}
    }

    private static void closeQuietly(ResultCursor cursor) {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public String describe() {
        return "AsyncConnectorSource[" + connector + ", maxBufferBytes=" + ((long) maxBufferSize * Operator.TARGET_PAGE_SIZE) + "]";
    }
}
