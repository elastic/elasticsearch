/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.SizeBlockingQueue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCursor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared mutable state for incremental pagination. Created by {@link ComputeService} when
 * pagination is enabled, passed to {@link StorePageOperator.StorePageOperatorFactory}, and
 * read back after execution to build the {@link org.elasticsearch.xpack.esql.session.Result}.
 * <p>
 * The {@link #firstPageListener()} fires as soon as the first output page is captured,
 * allowing the query response to be sent before the remaining pages are stored.
 */
public class PaginationStoreProvider {

    private static final double QUEUE_PRESSURE_THRESHOLD = 0.5;
    private static final long MAX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final CursorStorageService cursorStorageService;
    private final PaginationContext paginationContext;
    private final ThreadPool threadPool;
    private volatile List<ColumnInfoImpl> columns;
    private volatile TaskId backgroundTaskId;
    private final AtomicReference<List<Page>> firstPage = new AtomicReference<>();
    private final AtomicInteger totalRows = new AtomicInteger();
    private final AtomicInteger totalPages = new AtomicInteger();

    private final SubscribableListener<List<Page>> firstPageListener = new SubscribableListener<>();
    private final AtomicBoolean firstPageResolved = new AtomicBoolean(false);

    public PaginationStoreProvider(CursorStorageService cursorStorageService, PaginationContext paginationContext, ThreadPool threadPool) {
        this.cursorStorageService = cursorStorageService;
        this.paginationContext = paginationContext;
        this.threadPool = threadPool;
    }

    public void setColumns(List<ColumnInfoImpl> columns) {
        this.columns = columns;
    }

    public List<ColumnInfoImpl> columns() {
        return columns;
    }

    public void setBackgroundTaskId(TaskId taskId) {
        this.backgroundTaskId = taskId;
    }

    public TaskId backgroundTaskId() {
        return backgroundTaskId;
    }

    public CursorStorageService cursorStorageService() {
        return cursorStorageService;
    }

    public PaginationContext paginationContext() {
        return paginationContext;
    }

    public void setFirstPage(List<Page> pages) {
        firstPage.set(pages);
        if (firstPageResolved.compareAndSet(false, true)) {
            waitForWriteCapacity(pages, System.nanoTime() + MAX_WAIT_NANOS);
        }
    }

    /**
     * For index-based storage, defers the first page response until the system_write thread pool
     * queue drops below {@link #QUEUE_PRESSURE_THRESHOLD}. This provides backpressure to clients:
     * the client cannot send the next paginated query until the current one's first page is returned,
     * so delaying the response naturally throttles the request rate when the server is overloaded.
     * Fails with a rejection error if the queue stays above threshold for 5 seconds.
     */
    private void waitForWriteCapacity(List<Page> pages, long deadlineNanos) {
        if (paginationContext.storageType() != EsqlCursor.StorageType.INDEX || hasWriteCapacity()) {
            firstPageListener.onResponse(pages);
            return;
        }
        if (System.nanoTime() >= deadlineNanos) {
            firstPageListener.onFailure(new EsRejectedExecutionException("server too busy for paginated query"));
            return;
        }
        threadPool.schedule(() -> waitForWriteCapacity(pages, deadlineNanos), TimeValue.timeValueMillis(10), threadPool.generic());
    }

    private boolean hasWriteCapacity() {
        ExecutorService exec = threadPool.executor(ThreadPool.Names.SYSTEM_WRITE);
        if (exec instanceof ThreadPoolExecutor tpe && tpe.getQueue() instanceof SizeBlockingQueue<?> q) {
            return (double) q.size() / q.capacity() < QUEUE_PRESSURE_THRESHOLD;
        }
        return true;
    }

    public List<Page> firstPage() {
        return firstPage.get();
    }

    /**
     * Returns the first page and clears the internal reference so the page blocks can be released.
     */
    public List<Page> consumeFirstPage() {
        return firstPage.getAndSet(null);
    }

    /**
     * Listener that resolves as soon as the first output page is captured by the operator.
     * Subscribers can use this to send the query response without waiting for all pages to be stored.
     */
    public SubscribableListener<List<Page>> firstPageListener() {
        return firstPageListener;
    }

    /**
     * Signals that the query produced no pages (e.g., empty result set).
     * Resolves the first page listener with {@code null}.
     */
    public void signalNoPagesProduced() {
        if (firstPageResolved.compareAndSet(false, true)) {
            firstPageListener.onResponse(null);
        }
    }

    public void addTotalRows(int rows) {
        totalRows.addAndGet(rows);
    }

    public int totalRows() {
        return totalRows.get();
    }

    public void setTotalPages(int pages) {
        totalPages.set(pages);
    }

    public int totalPages() {
        return totalPages.get();
    }
}
