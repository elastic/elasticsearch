/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * This is a unmapped-fields strategy discriminator.
 */
public enum UnmappedResolution {
    /**
     * Don't attempt to patch the plan: in case the query uses such a field not present in the index mapping, fail the query.
     */
    FAIL,

    /**
     * In case the query references a field that's not present in the index mapping, alias this field to value {@code null} of type
     * {@link DataType}.{@code NULL}
     */
    NULLIFY,

    /**
     * Just like {@code NULLIFY}, but instead of null-aliasing, insert extractors in the data source.
     */
    LOAD
}
