/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse.spi;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Connects to table catalog systems like Iceberg, Delta Lake, or Hudi.
 * Provides metadata resolution and scan planning for table-based data sources.
 * <p>
 * Unlike FormatReader which reads individual files, TableCatalog
 * understands table structure including partitioning, snapshots, and
 * metadata management. It returns the same {@link SourceMetadata} type as
 * FormatReader for consistency in schema discovery.
 * <p>
 * Table-based sources differ from file-based sources in that the schema is
 * defined at the TABLE level, separate from data files. The native schema
 * (e.g., Iceberg Schema) must be preserved in {@link SourceMetadata#sourceMetadata()}
 * to avoid re-resolving the table during execution.
 * <p>
 * Implementations typically reuse a FormatReader (e.g., ParquetFormatReader)
 * for actual data reading after planning which files to read.
 *
 */
public interface TableCatalog extends Closeable {

    /**
     * Returns the catalog type identifier (e.g., "iceberg", "delta", "hudi").
     */
    String catalogType();

    /**
     * Checks if this catalog can handle the given path.
     *
     * @param path the path to check
     * @return true if this catalog can resolve and read the path
     */
    boolean canHandle(String path);

    /**
     * Resolves metadata for a table at the given path.
     *
     * @param tablePath the table path
     * @param config configuration for accessing the table
     * @return source metadata including schema
     * @throws IOException if metadata cannot be read
     */
    SourceMetadata metadata(String tablePath, Map<String, Object> config) throws IOException;

    /**
     * Plans a scan of the table, returning the list of data files to read.
     *
     * @param tablePath the table path
     * @param config configuration for accessing the table
     * @param predicates filter predicates for partition pruning
     * @return list of data files to read
     * @throws IOException if scan planning fails
     */
    List<DataFile> planScan(String tablePath, Map<String, Object> config, List<Object> predicates) throws IOException;

    /**
     * Returns filter pushdown support for this catalog, if available.
     *
     * @return filter pushdown support, or null if not supported
     */
    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    /**
     * Returns an operator factory provider for custom operator creation.
     *
     * @return operator factory provider, or null to use default
     */
    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    /**
     * Represents a data file within a table that should be read during a scan.
     */
    interface DataFile {
        /**
         * Path to the data file.
         */
        String path();

        /**
         * Format of the data file (e.g., "parquet", "orc").
         */
        String format();

        /**
         * Size of the data file in bytes.
         */
        long sizeInBytes();

        /**
         * Number of records in the data file.
         */
        long recordCount();

        /**
         * Partition values for this data file.
         */
        Map<String, Object> partitionValues();
    }
}
