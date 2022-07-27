/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

/**
 * Implemented by {@link WorkerBulkByScrollTaskState} and {@link BulkByScrollTask.Status} to consistently implement
 * {@link #getSuccessfullyProcessed()}.
 */
public interface SuccessfullyProcessed {
    /**
     * Total number of successfully processed documents.
     */
    default long getSuccessfullyProcessed() {
        return getUpdated() + getCreated() + getDeleted();
    }

    /**
     * Count of documents updated.
     */
    long getUpdated();

    /**
     * Count of documents created.
     */
    long getCreated();

    /**
     * Count of successful delete operations.
     */
    long getDeleted();
}
