/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.exception.ElasticsearchWrapperException;

public class TransformStatePersistenceException extends RuntimeException implements ElasticsearchWrapperException {
    public TransformStatePersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
