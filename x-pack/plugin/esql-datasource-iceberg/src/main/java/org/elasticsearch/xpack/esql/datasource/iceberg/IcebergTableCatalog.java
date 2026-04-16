/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Iceberg table catalog implementation.
 * Provides metadata resolution and scan planning for Iceberg tables stored in S3.
 */
public class IcebergTableCatalog implements TableCatalog {

    private static final String CATALOG_TYPE = "iceberg";

    @Override
    public String catalogType() {
        return CATALOG_TYPE;
    }

    @Override
    public boolean canHandle(String path) {
        // Check if the path looks like an S3 path and could be an Iceberg table
        // A more robust implementation would check for the presence of metadata directory
        return path != null && (path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://"));
    }

    @Override
    public SourceMetadata metadata(String tablePath, Map<String, Object> config) throws IOException {
        S3Configuration s3Config = extractS3Config(config);
        try {
            IcebergTableMetadata metadata = IcebergCatalogAdapter.resolveTable(tablePath, s3Config);
            return new IcebergSourceMetadata(metadata);
        } catch (Exception e) {
            throw new IOException("Failed to resolve Iceberg table metadata: " + tablePath, e);
        }
    }

    @Override
    public List<DataFile> planScan(String tablePath, Map<String, Object> config, List<Object> predicates) throws IOException {
        S3Configuration s3Config = extractS3Config(config);
        S3FileIO fileIO = null;

        try {
            // Resolve the table metadata first
            IcebergTableMetadata metadata = IcebergCatalogAdapter.resolveTable(tablePath, s3Config);

            // Create FileIO and table for scanning
            fileIO = S3FileIOFactory.create(s3Config);
            StaticTableOperations ops = new StaticTableOperations(metadata.metadataLocation(), fileIO);
            Table table = new BaseTable(ops, tablePath);

            // Create a table scan
            TableScan scan = table.newScan();

            // Apply predicates if any (convert from generic predicates to Iceberg expressions)
            // For now, we don't apply predicates at the scan planning level
            // Predicate pushdown happens during actual reading via IcebergSourceOperatorFactory

            // Plan the files to read
            List<DataFile> dataFiles = new ArrayList<>();
            try (CloseableIterable<FileScanTask> fileTasks = scan.planFiles()) {
                for (FileScanTask task : fileTasks) {
                    dataFiles.add(new IcebergDataFile(task));
                }
            }

            return dataFiles;
        } catch (Exception e) {
            throw new IOException("Failed to plan Iceberg table scan: " + tablePath, e);
        } finally {
            IOUtils.closeWhileHandlingException(fileIO);
        }
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the catalog level
    }

    /**
     * Extract S3 configuration from the config map.
     */
    private S3Configuration extractS3Config(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return null;
        }

        String accessKey = (String) config.get("access_key");
        String secretKey = (String) config.get("secret_key");
        String endpoint = (String) config.get("endpoint");
        String region = (String) config.get("region");

        return S3Configuration.fromFields(accessKey, secretKey, endpoint, region);
    }

    /**
     * Implementation of DataFile for Iceberg file scan tasks.
     */
    private static class IcebergDataFile implements DataFile {
        private final FileScanTask task;

        IcebergDataFile(FileScanTask task) {
            this.task = task;
        }

        @Override
        public String path() {
            return task.file().path().toString();
        }

        @Override
        public String format() {
            return task.file().format().name().toLowerCase(java.util.Locale.ROOT);
        }

        @Override
        public long sizeInBytes() {
            return task.file().fileSizeInBytes();
        }

        @Override
        public long recordCount() {
            return task.file().recordCount();
        }

        @Override
        public Map<String, Object> partitionValues() {
            // For now, return empty map - partition values would require schema context
            return Collections.emptyMap();
        }
    }

    /**
     * Adapter that wraps IcebergTableMetadata to implement SourceMetadata.
     */
    private static class IcebergSourceMetadata implements SourceMetadata {
        private final IcebergTableMetadata metadata;

        IcebergSourceMetadata(IcebergTableMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public List<org.elasticsearch.xpack.esql.core.expression.Attribute> schema() {
            return metadata.attributes();
        }

        @Override
        public String sourceType() {
            return metadata.sourceType();
        }

        @Override
        public String location() {
            return metadata.tablePath();
        }
    }
}
