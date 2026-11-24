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

/**
 * Stores the pages and the status of one stream so that they can be read later.
 * <p>
 * Call {@link #createStarted} once, then {@link #appendPage} for each page to store, then exactly one of
 * {@link #markComplete}, {@link #markFailed} or {@link #markCanceled}. Once a final status is set, later calls do not change it.
 */
public interface StoredStreamWriter {

    /** Registers the new stream by its id. */
    String createStarted(
        TaskId taskId,
        @Nullable Instant expiration,
        List<ColumnInfo> columns,
        Map<String, String> headers,
        List<String> warnings
    );

    /**
     * Stores a page of the stream. This does not take over the ownership of the page: the page is serialized into the writer's
     * internal representation right away, or copied if needed. The caller still owns the page and may release it as soon as this
     * method returns.
     *
     * @param seq    the position of the page in the stream
     * @param offset the number of rows in the stream before this page
     */
    void appendPage(int seq, long offset, Page page);

    /** Ends the stream after all its pages were produced. */
    void markComplete(boolean partial, long tookMillis, long pages, long rows);

    /** Ends the stream because the query failed. */
    void markFailed(boolean partial, long tookMillis, Exception e);

    /** Ends the stream because the query was cancelled. */
    void markCanceled();
}
