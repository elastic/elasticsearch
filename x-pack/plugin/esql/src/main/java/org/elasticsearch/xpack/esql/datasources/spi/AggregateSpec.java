/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Specification for a single aggregate to be computed from file-level statistics.
 *
 * @param type       the aggregate function type
 * @param columnName the column to aggregate (null for COUNT_STAR)
 * @param resultType the expected ESQL data type of the result
 */
public record AggregateSpec(AggType type, String columnName, DataType resultType) {

    public enum AggType {
        COUNT_STAR,
        MIN,
        MAX
    }
}
