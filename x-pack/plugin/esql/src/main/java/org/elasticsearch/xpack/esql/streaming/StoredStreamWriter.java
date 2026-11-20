/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface StoredStreamWriter {

    String createStarted(
        TaskId taskId,
        @Nullable Instant expiration,
        List<ColumnInfo> columns,
        Map<String, String> headers,
        List<String> warnings
    );

    void appendPage(int seq, long oggset, Page page);

    void markComplete(boolean partial, long tookMillis, long pages, long rows);

    void markFailed(boolean partial, long tookMillis, Exception e);

    void markCanceled();
}
