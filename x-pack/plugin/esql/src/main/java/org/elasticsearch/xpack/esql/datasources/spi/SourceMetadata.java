/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.HashMap;
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
     * Client-facing notices about this one file, raised while resolving its metadata: hints derived from the schema
     * sample, such as a {@code \N} the current mode will keep as literal text. Notices about the dataset's options are
     * not a file's and ride {@link FormatReader#configWarnings()} instead. They live on the metadata rather than being
     * emitted where raised because
     * resolution runs on an executor thread whose response headers never reach the client, and because a cached
     * resolution must replay them too or the same query would warn on its first run and not its second. Every run of a
     * query gets these notices; within one run, identical texts are collapsed to a single line.
     */
    default List<String> warnings() {
        return List.of();
    }

    /**
     * Columns whose inferred type widened while reading this one file's schema sample, e.g. a column
     * that committed to {@code integer} and then moved to {@code keyword} on a later non-numeric
     * value. Reported separately from {@link #warnings()} (a structured record rather than text)
     * because {@code schema_resolution: strict} needs to refuse these programmatically, including for
     * a single-file dataset where {@code SchemaReconciliation.reconcileStrict} otherwise has nothing
     * to compare the lone file's schema against.
     *
     * @return widened columns for this file, empty if inference found none (or the source type, such
     *         as Parquet or Iceberg, doesn't sample-infer at all)
     */
    default List<WidenedColumn> widenedColumns() {
        return List.of();
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

    /**
     * Keys for the schema-sample width, stored in {@link #sourceMetadata()} so they travel with
     * the coordinator metadata map and are not serialized onto {@code FileSplit}.
     */
    String SAMPLE_BYTES_KEY = "_sample.bytes";
    String SAMPLE_ROWS_KEY = "_sample.rows";

    /**
     * Bytes covered by the schema sample, or 0 when the schema was not inferred from a sample.
     * Split discovery uses {@code sampleBytes / sampleRows} as the row width under LIMIT.
     */
    default long sampleBytes() {
        return sampleNumber(sourceMetadata(), SAMPLE_BYTES_KEY);
    }

    /**
     * Rows in the schema sample, or 0 when the schema was not inferred from a sample.
     */
    default int sampleRows() {
        return (int) sampleNumber(sourceMetadata(), SAMPLE_ROWS_KEY);
    }

    private static long sampleNumber(Map<String, Object> metadata, String key) {
        Object value = metadata == null ? null : metadata.get(key);
        return value instanceof Number number ? number.longValue() : 0L;
    }

    /** Copies {@code base} with sample width keys when both counts are positive. */
    static Map<String, Object> withSample(Map<String, Object> base, long sampleBytes, int sampleRows) {
        if (sampleBytes <= 0 || sampleRows <= 0) {
            return base == null ? Map.of() : base;
        }
        Map<String, Object> copy = base == null || base.isEmpty() ? new HashMap<>() : new HashMap<>(base);
        copy.put(SAMPLE_BYTES_KEY, sampleBytes);
        copy.put(SAMPLE_ROWS_KEY, sampleRows);
        return copy;
    }
}
