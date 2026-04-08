/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Abstraction for storing and retrieving paginated ES|QL cursor state.
 * Implementations may back this with a system index, local files, or other storage.
 */
public interface CursorStorageService {

    // ---- Write path ----

    /**
     * Stores incomplete metadata to indicate that the query is still producing pages.
     */
    void storeMetadataIncomplete(
        String cursorId,
        List<ColumnInfoImpl> columns,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        TaskId taskId,
        Map<String, String> securityHeaders,
        ActionListener<String> listener
    );

    /**
     * Stores a single page of data.
     */
    void storePage(String cursorId, int pageIndex, List<Page> pageData, boolean needsDeepCopy, ActionListener<Void> listener);

    /**
     * Updates the metadata document to mark the cursor as complete with final row/page counts.
     */
    void updateMetadataComplete(String cursorId, int totalRows, int totalPages, ActionListener<Void> listener);

    /**
     * Updates the metadata document to mark the cursor as failed.
     */
    void updateMetadataFailed(String cursorId, ActionListener<Void> listener);

    // ---- Read path ----

    /**
     * Retrieves cursor metadata.
     */
    void getMetadata(String cursorId, ActionListener<CursorMetadata> listener);

    /**
     * Retrieves a single page of data by page index.
     */
    void getPage(String cursorId, int pageIndex, ActionListener<List<Page>> listener);

    /**
     * Extends the keep-alive expiration for a cursor.
     */
    void updateKeepAlive(String cursorId, TimeValue keepAlive, ActionListener<Void> listener);

    // ---- Delete path ----

    /**
     * Deletes all stored state for a cursor.
     */
    void delete(String cursorId, ActionListener<Boolean> listener);

    // ---- Data classes ----

    /** Sentinel value for {@code totalPages} indicating the query is still producing pages. */
    int TOTAL_PAGES_IN_PROGRESS = -1;

    /** Sentinel value for {@code totalPages} indicating the query failed after partial storage. */
    int TOTAL_PAGES_FAILED = -2;

    /**
     * Cursor metadata describing the schema and state of a paginated result set.
     */
    record CursorMetadata(
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationTimeMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> headers
    ) {
        /** The query is still running and producing pages. */
        public boolean isInProgress() {
            return totalPages == TOTAL_PAGES_IN_PROGRESS;
        }

        /** The query failed after partial storage. */
        public boolean isFailed() {
            return totalPages == TOTAL_PAGES_FAILED;
        }

        public boolean isComplete() {
            return totalPages >= 0;
        }
    }
}
