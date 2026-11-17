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

public interface StreamStorage {

    void create(
        String streamId,
        TaskId taskId,
        Map<String, String> headers,
        List<ColumnInfo> columns,
        long expirationMillis,
        ActionListener<Void> listener
    );

    void extendTtl(String streamId, long expirationMillis, ActionListener<Void> listener);

    void appendPage(String streamId, long offset, Page page, ActionListener<Void> listener);

    void markComplete(String streamId, boolean isPartial, long totalRows, long totalPages, ActionListener<Void> listener);

    void markFailed(String streamId, Exception exception, ActionListener<Void> listener);

    void getMetadata(String streamId, ActionListener<StreamMetadata> listener);

    void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener);

    void delete(String streamId, ActionListener<Void> listener);
}
