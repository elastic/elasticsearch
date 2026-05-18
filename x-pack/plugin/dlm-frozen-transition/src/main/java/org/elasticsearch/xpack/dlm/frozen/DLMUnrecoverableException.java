/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ElasticsearchException;

/**
 * Exception thrown when a DLM operation encounters an unrecoverable error that should not be retried.
 * For example, if a required snapshot repository has been removed while a convert-to-frozen operation
 * is in progress, there is no point retrying the operation.
 */
public class DLMUnrecoverableException extends ElasticsearchException {
    public DLMUnrecoverableException(String indexName, String msg, Object... args) {
        super(msg, args);
        setIndex(indexName);
    }
}
