/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchException;

// Wrapper for indexing failures thrown internally in the transform indexer
class BulkIndexingException extends ElasticsearchException {
    private final boolean irrecoverable;

    /**
     * Create a BulkIndexingException
     *
     * @param msg The message
     * @param cause The most important cause of the bulk indexing failure
     * @param irrecoverable whether this is a permanent or irrecoverable error (controls retry)
     * @param args arguments for formating the message
     */
    BulkIndexingException(String msg, Throwable cause, boolean irrecoverable, Object... args) {
        super(msg, cause, args);
        this.irrecoverable = irrecoverable;
    }

    public boolean isIrrecoverable() {
        return irrecoverable;
    }
}
