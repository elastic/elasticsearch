/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Builds a {@link FieldContext} for a given field at encode time. Implementations
 * bridge the mapper layer (which knows the field's data type and time-series
 * metric role) and the codec layer (which decides the pipeline). The codec layer
 * receives this function and does not import mapper classes directly.
 */
@FunctionalInterface
public interface FieldContextResolver {

    /**
     * Resolves the field context for the given field name and block size.
     *
     * @param fieldName the name of the field being encoded
     * @param blockSize the number of values per numeric block
     * @return the field context for pipeline selection
     */
    FieldContext resolve(String fieldName, int blockSize);
}
