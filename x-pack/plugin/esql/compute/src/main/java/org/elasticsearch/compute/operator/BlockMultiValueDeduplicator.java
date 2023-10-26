/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

/**
 * The detail implementation is specified in {@code X-MultivalueDedupe.java.st}
 */
public interface BlockMultiValueDeduplicator {
    /**
     * Move the deduplicator to the given position
     */
    void moveToPosition(int position);

    /**
     * Returns the number of values in the current position
     */
    int valueCount();
}
