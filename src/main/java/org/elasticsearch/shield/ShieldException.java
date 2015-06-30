/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;

/**
 *
 */
public class ShieldException extends ElasticsearchException.WithRestHeadersException {

    public ShieldException(String msg, Tuple... headers) {
        super(msg, headers);
    }

    public ShieldException(String msg, Throwable cause, Tuple... headers) {
        super(msg, headers);
        initCause(cause);
    }
}
