/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;

/**
 * Exception thrown when an invalid temporality value is encountered during resolution.
 * This exception is used by {@link TemporalityAccessor#create(org.elasticsearch.compute.data.BytesRefBlock, Temporality)}
 * to signal that a temporality value is neither "cumulative" nor "delta".
 */
public class InvalidTemporalityException extends RuntimeException {

    public InvalidTemporalityException(BytesRef invalidValue) {
        super("Invalid temporality value: [" + invalidValue.utf8ToString() + "], expected [cumulative] or [delta]");
    }
}
