/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Statistics about a data source for query planning and optimization.
 * Implementations should provide as much information as available from
 * the underlying data source metadata.
 */
public interface SourceStatistics {

    /**
     * Returns the total number of rows in the source, if known.
     *
     * @return optional row count
     */
    OptionalLong rowCount();

    /**
     * Returns the total size in bytes, if known.
     *
     * @return optional size in bytes
     */
    OptionalLong sizeInBytes();

    /**
     * Returns per-column statistics, if available.
     * The map keys are column names.
     *
     * @return optional map of column name to column statistics
     */
    default Optional<Map<String, ColumnStatistics>> columnStatistics() {
        return Optional.empty();
    }

    /**
     * Statistics for an individual column.
     */
    interface ColumnStatistics {
        /**
         * Returns the number of null values in this column, if known.
         */
        OptionalLong nullCount();

        /**
         * Returns the number of distinct values in this column, if known.
         */
        OptionalLong distinctCount();

        /**
         * Returns the minimum value as a comparable object, if known.
         * The type depends on the column data type.
         */
        Optional<Object> minValue();

        /**
         * Returns the maximum value as a comparable object, if known.
         * The type depends on the column data type.
         */
        Optional<Object> maxValue();
    }
}
