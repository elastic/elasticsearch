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
     * <p>
     * Contract for {@link #minValue()}/{@link #maxValue()}: the values must already be in ESQL's
     * in-memory representation for the column's data type — i.e. exactly what a scan of the same
     * column would produce. In particular temporal columns must be normalized (e.g. {@code DATETIME}
     * as epoch-milliseconds, not raw parquet days/micros), because {@code PushStatsToExternalSource}
     * substitutes these values directly into MIN/MAX aggregations without any further conversion.
     * A format reader that cannot produce a scan-equivalent value for a column (e.g. statistics that
     * are not ordered by the column's logical type) must omit min/max for that column
     * ({@code Optional.empty()}) so the query falls back to a scan.
     */
    interface ColumnStatistics {
        /**
         * Returns the number of null values in this column, if known.
         */
        OptionalLong nullCount();

        /**
         * Returns the number of non-null values in this column, if known. Multivalue-aware:
         * a multivalued cell contributes one per value, so this is what {@code COUNT(col)}
         * returns and is served directly instead of being derived from {@code rowCount - nullCount}
         * (which under-counts multivalued columns).
         */
        default OptionalLong valueCount() {
            return OptionalLong.empty();
        }

        /**
         * Returns the number of distinct values in this column, if known.
         */
        OptionalLong distinctCount();

        /**
         * Returns the minimum value as a comparable object, if known. The type and representation
         * must match ESQL's in-memory form for the column's data type (see the interface contract).
         */
        Optional<Object> minValue();

        /**
         * Returns the maximum value as a comparable object, if known. The type and representation
         * must match ESQL's in-memory form for the column's data type (see the interface contract).
         */
        Optional<Object> maxValue();

        /**
         * Returns the uncompressed size of this column in bytes, if known.
         * Useful for cost-based filter evaluation ordering.
         */
        default OptionalLong sizeInBytes() {
            return OptionalLong.empty();
        }
    }
}
