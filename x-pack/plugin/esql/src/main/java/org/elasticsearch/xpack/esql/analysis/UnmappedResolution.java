/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * This is an unmapped-fields strategy discriminator.
 */
public enum UnmappedResolution {
    /**
     * Use the default behavior for the query type: standard ESQL queries fail when referencing unmapped fields, while other query types
     * (e.g. PROMQL) may treat them differently.
     */
    DEFAULT,

    /**
     * In case the query references a field that's not present in the index mapping, alias this field to value {@code null} of type
     * {@link DataType#NULL}
     */
    NULLIFY,

    /**
     * In case the query references a field that's not present in the index mapping, attempt to load it from {@code _source}.
     */
    LOAD,

    /**
     * Load all source fields that are not present in the index mapping into a synthetic {@code _unmapped_fields} column,
     * without requiring individual unmapped fields to be referenced in the query.
     */
    LOAD_ALL
}
