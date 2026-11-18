/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record StreamMetadata(
    String streamId,
    Instant ttl,
    TaskId taskId,
    Map<String, String> headers,
    List<ColumnInfo> columns,
    Status status,
    List<String> warnings,
    @Nullable StreamCompletionInfo completionInfo
) {

    enum Status {
        RUNNING,
        COMPLETE,
        FAILED,
        CANCELED
    }

    record StreamCompletionInfo(boolean partial, long tookMillis, long pages, long rows, @Nullable Exception exception) {}
}
