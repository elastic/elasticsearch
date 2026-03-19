/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.util.List;
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

    private final EsqlCursorIndexService cursorIndexService;
    private final PaginationContext paginationContext;
    private volatile List<ColumnInfoImpl> columns;
    private volatile TaskId backgroundTaskId;
    private final AtomicReference<List<Page>> firstPage = new AtomicReference<>();
    private final AtomicInteger totalRows = new AtomicInteger();
    private final AtomicInteger totalPages = new AtomicInteger();

    private final SubscribableListener<List<Page>> firstPageListener = new SubscribableListener<>();
    private final AtomicBoolean firstPageResolved = new AtomicBoolean(false);

    public PaginationStoreProvider(EsqlCursorIndexService cursorIndexService, PaginationContext paginationContext) {
        this.cursorIndexService = cursorIndexService;
        this.paginationContext = paginationContext;
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

    public EsqlCursorIndexService cursorIndexService() {
        return cursorIndexService;
    }

    public PaginationContext paginationContext() {
        return paginationContext;
    }

    public void setFirstPage(List<Page> pages) {
        firstPage.set(pages);
        if (firstPageResolved.compareAndSet(false, true)) {
            firstPageListener.onResponse(pages);
        }
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
