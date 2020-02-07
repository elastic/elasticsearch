/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;

/**
 * Exception thrown when a problem is encountered while initialising an ILM policy for an index.
 */
public class InitializePolicyException extends ElasticsearchException {

    public InitializePolicyException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
