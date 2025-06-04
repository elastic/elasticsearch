/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.exception.ElasticsearchException;

/**
 * An exception indicates that a compute should be terminated early as the downstream pipeline has enough or no long requires more data.
 */
public final class DriverEarlyTerminationException extends ElasticsearchException {
    public DriverEarlyTerminationException(String message) {
        super(message);
    }
}
