/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.List;
import java.util.Map;

/**
 * A {@link StreamStorage} that delegates to a real one, so a test can override one method to record calls, delay them, fail them or change
 * what they return, and keep the real behaviour for the rest. A real {@link InMemoryStreamStore} is wrapped, not mocked.
 */
class DelegatingStreamStorage implements StreamStorage {

    private final StreamStorage delegate;

    DelegatingStreamStorage(StreamStorage delegate) {
        this.delegate = delegate;
    }

    @Override
    public void create(
        String streamId,
        TaskId taskId,
        Map<String, String> headers,
        List<ColumnInfo> columns,
        long expirationMillis,
        ActionListener<Void> listener
    ) {
        delegate.create(streamId, taskId, headers, columns, expirationMillis, listener);
    }

    @Override
    public void extendTtl(String streamId, long expirationMillis, ActionListener<Void> listener) {
        delegate.extendTtl(streamId, expirationMillis, listener);
    }

    @Override
    public void appendPage(String streamId, long offset, Page page, ActionListener<Void> listener) {
        delegate.appendPage(streamId, offset, page, listener);
    }

    @Override
    public void markComplete(String streamId, boolean isPartial, long totalRows, long totalPages, ActionListener<Void> listener) {
        delegate.markComplete(streamId, isPartial, totalRows, totalPages, listener);
    }

    @Override
    public void markFailed(String streamId, Exception exception, ActionListener<Void> listener) {
        delegate.markFailed(streamId, exception, listener);
    }

    @Override
    public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
        delegate.getMetadata(streamId, listener);
    }

    @Override
    public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
        delegate.getPages(streamId, offset, rows, listener);
    }

    @Override
    public void delete(String streamId, ActionListener<Void> listener) {
        delegate.delete(streamId, listener);
    }
}
