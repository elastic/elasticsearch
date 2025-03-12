/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;

public class InferenceException extends ElasticsearchException implements ElasticsearchWrapperException {
    public InferenceException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

}
