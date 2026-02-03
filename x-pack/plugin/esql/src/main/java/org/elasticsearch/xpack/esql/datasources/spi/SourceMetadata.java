/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unified metadata output type returned by all schema discovery mechanisms.
 * This interface provides a consistent way to access metadata regardless of
 * whether it comes from a FormatReader (Parquet, CSV) or a TableCatalog
 * (Iceberg, Delta Lake).
 * <p>
 * For file-based sources (Parquet, CSV), the schema is embedded in the file itself,
 * so no additional metadata needs to flow through to execution.
 * <p>
 * For table-based sources (Iceberg, Delta Lake), the native schema and other
 * source-specific data must be preserved in {@link #sourceMetadata()} to avoid
 * re-resolving the table during execution. Core passes this through without
 * interpreting it; only the source-specific operator factory understands it.
 * <p>
 * Implementations should be immutable and thread-safe.
 */
public interface SourceMetadata {

    /**
     * Returns the resolved schema as ESQL attributes.
     * The attributes represent the columns available for querying.
     *
     * @return list of attributes representing the schema, never null
     */
    List<Attribute> schema();

    /**
     * Returns the source type identifier.
     * Examples: "parquet", "iceberg", "csv", "delta"
     *
     * @return the source type string, never null
     */
    String sourceType();

    /**
     * Returns the original path or location of the source.
     * This is the URI or path used to access the data.
     *
     * @return the location string, never null
     */
    String location();

    /**
     * Returns optional statistics for query planning.
     * Statistics can include row counts, column statistics, etc.
     *
     * @return optional statistics, empty if not available
     */
    default Optional<SourceStatistics> statistics() {
        return Optional.empty();
    }

    /**
     * Returns optional partition column names.
     * For partitioned data sources, this indicates which columns
     * are used for partitioning.
     *
     * @return optional list of partition column names, empty if not partitioned
     */
    default Optional<List<String>> partitionColumns() {
        return Optional.empty();
    }

    /**
     * Returns opaque source-specific metadata.
     * <p>
     * This is used by table-based sources (Iceberg, Delta Lake) to pass native
     * schema and other source-specific data through to the operator factory
     * without core needing to understand it.
     * <p>
     * For example, Iceberg stores its native {@code Schema} object here under
     * a well-known key. The Iceberg operator factory retrieves it when creating
     * operators, avoiding the need to re-resolve the table.
     * <p>
     * File-based sources typically return an empty map since the schema is
     * embedded in the file itself.
     *
     * @return map of source-specific metadata, never null
     */
    default Map<String, Object> sourceMetadata() {
        return Map.of();
    }

    /**
     * Returns configuration for operator creation.
     * <p>
     * This replaces source-specific configuration classes (like S3Configuration)
     * leaking into core. Configuration is stored as a generic map that the
     * source-specific operator factory interprets.
     * <p>
     * Common keys include:
     * <ul>
     *   <li>"access_key" - S3 access key</li>
     *   <li>"secret_key" - S3 secret key</li>
     *   <li>"endpoint" - S3 endpoint URL</li>
     *   <li>"region" - AWS region</li>
     * </ul>
     *
     * @return configuration map, never null
     */
    default Map<String, Object> config() {
        return Map.of();
    }
}
