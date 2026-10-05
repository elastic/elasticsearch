/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * One within-file schema-inference widen worth reporting: a column whose inferred type moved
 * because a later sampled value no longer fit the type earlier values had committed it to.
 * <p>
 * Carried on {@link SourceMetadata#widenedColumns()} so {@code schema_resolution: strict} can refuse
 * the widen even for a single-file dataset, where cross-file reconciliation
 * ({@code SchemaReconciliation.reconcileStrict}) has no second file to compare against and so
 * otherwise sees nothing to validate.
 *
 * @param columnName the widened column's name
 * @param fromType   the type the column held before this value
 * @param toType     the type the column widened to
 * @param value      the sampled value that forced the move, truncated to {@link #MAX_VALUE_LENGTH}
 * @param sampleRow  1-based row number, within the inference sample, that carried {@code value}
 */
public record WidenedColumn(String columnName, DataType fromType, DataType toType, String value, long sampleRow) {

    /**
     * Cap on the retained value. {@code value} is unbounded user data (a raw cell or field) reproduced
     * verbatim in warnings, a {@code schema_resolution: strict} exception message, and the schema cache
     * ({@code SchemaCacheEntry}) — truncating once here, at construction, is the one place that bounds
     * it for every consumer instead of relying on each to do so.
     */
    public static final int MAX_VALUE_LENGTH = 256;

    public WidenedColumn {
        value = Strings.cleanTruncate(value, MAX_VALUE_LENGTH);
    }
}
