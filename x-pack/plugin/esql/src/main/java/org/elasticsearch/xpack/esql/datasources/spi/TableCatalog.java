/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

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
 */
public interface TableCatalog extends Closeable {

    String catalogType();

    boolean canHandle(String path);

    SourceMetadata metadata(String tablePath, Map<String, Object> config) throws IOException;

    List<DataFile> planScan(String tablePath, Map<String, Object> config, List<Object> predicates) throws IOException;

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    interface DataFile {
        String path();

        String format();

        long sizeInBytes();

        long recordCount();

        Map<String, Object> partitionValues();
    }
}
