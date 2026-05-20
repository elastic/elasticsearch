/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

/**
 * Exception thrown when an unsupported temporality is encountered for a given aggregation.
 * <p>
 * Unlike {@link InvalidTemporalityException} (which signals an unrecognized temporality string
 * and is treated as a warning), this exception is intended to fail the query.
 * </p>
 */
public class UnsupportedTemporalityException extends RuntimeException {

    public UnsupportedTemporalityException(String message) {
        super(message);
    }
}
