/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * The type of elements in {@link Block} and {@link Vector}
 */
public enum ElementType {
    INT,
    LONG,
    DOUBLE,
    NULL, // Blocks contain only null values
    BYTES_REF,
    UNKNOWN // Intermediate blocks, which doesn't support retrieving elements
}
