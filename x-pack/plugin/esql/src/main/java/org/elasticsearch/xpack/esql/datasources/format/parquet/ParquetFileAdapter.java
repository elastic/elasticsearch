/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.format.parquet;

import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.xpack.esql.datasources.StorageProviderRegistry;
import org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasources.spi.StorageManager;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Locale;

/**
 * Adapter for reading Parquet file schemas directly (without Iceberg metadata).
 * This allows reading standalone Parquet files from various storage backends.
 * <p>
 * <b>Hadoop-free approach:</b> By using explicit {@link ParquetReadOptions} with a non-Hadoop
 * {@link org.apache.parquet.io.InputFile} implementation, this adapter avoids triggering
 * Hadoop Configuration initialization. When {@code ParquetFileReader.open(InputFile, ParquetReadOptions)}
 * is called with a non-HadoopInputFile, the reader uses {@code ParquetReadOptions} directly
 * instead of creating {@code HadoopReadOptions}, bypassing Hadoop class loading.
 * <p>
 * <b>Performance optimization:</b> Uses {@link ParquetMetadataConverter#SKIP_ROW_GROUPS} filter
 * to read only the file-level schema metadata, skipping row group details which are not needed
 * for schema resolution. This reduces I/O, parsing overhead, and memory usage.
 * <p>
 * <b>Storage abstraction:</b> Uses {@link StorageManager} to obtain {@link StorageObject} instances,
 * which are then adapted to Parquet's InputFile interface via {@link ParquetStorageObjectAdapter}.
 * This provides a clean separation between storage routing (handled by StorageManager) and
 * format-specific schema resolution (handled by this class).
 */
public class ParquetFileAdapter {

    private static final String SOURCE_TYPE_PARQUET = "parquet";

    /**
     * Resolve schema from a standalone Parquet file using the provided StorageManager.
     * <p>
     * This method uses StorageManager to obtain a StorageObject for the given path,
     * then adapts it to Parquet's InputFile interface for schema extraction.
     *
     * @param filePath the path to the Parquet file (s3://, file://, http://, or https://)
     * @param config configuration object (S3Configuration for S3 schemes, null for HTTP/file schemes)
     * @param storageManager the StorageManager to use for obtaining StorageObject instances
     * @return IcebergTableMetadata with schema extracted from Parquet file
     * @throws Exception if file cannot be read
     */
    public static IcebergTableMetadata resolveParquetFile(String filePath, Object config, StorageManager storageManager) throws Exception {
        // Detect scheme to determine how to get the StorageObject
        StoragePath storagePath = StoragePath.of(filePath);
        String scheme = storagePath.scheme().toLowerCase(Locale.ROOT);

        StorageObject storageObject;
        if ((scheme.equals("http") || scheme.equals("https")) && config == null) {
            // For HTTP/HTTPS with null config, use registry-based approach
            // This allows HttpStorageProvider to be registered with an executor
            storageObject = storageManager.newStorageObject(filePath);
        } else {
            // For S3 and file schemes, or HTTP with config, use config-based approach
            storageObject = storageManager.newStorageObject(filePath, config);
        }

        // Create format-specific adapter from StorageObject
        org.apache.parquet.io.InputFile parquetInputFile = new ParquetStorageObjectAdapter(storageObject);

        // Build ParquetReadOptions explicitly to avoid Hadoop initialization.
        // Key points:
        // 1. Using ParquetReadOptions (not HadoopReadOptions) avoids HadoopParquetConfiguration creation
        // 2. SKIP_ROW_GROUPS filter reads only file-level metadata (schema), skipping row group details
        // 3. When ParquetFileReader.open() receives a non-HadoopInputFile with explicit options,
        // it uses the provided options directly without creating Hadoop-specific configurations
        ParquetReadOptions options = ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.SKIP_ROW_GROUPS).build();

        try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
            MessageType parquetSchema = reader.getFileMetaData().getSchema();

            // Convert Parquet schema to Iceberg schema
            Schema icebergSchema = ParquetSchemaUtil.convert(parquetSchema);

            // Extract S3Configuration for metadata (only if config is S3Configuration)
            S3Configuration s3Config = config instanceof S3Configuration s3 ? s3 : null;
            return new IcebergTableMetadata(filePath, icebergSchema, s3Config, SOURCE_TYPE_PARQUET);
        }
    }

    /**
     * Resolve schema from a standalone Parquet file.
     * <p>
     * This is a convenience method that creates a default StorageManager internally.
     * For better control over storage configuration, use
     * {@link #resolveParquetFile(String, Object, StorageManager)} instead.
     * <p>
     * Supports multiple URL schemes:
     * <ul>
     *   <li>s3:// - Uses S3StorageProvider for AWS S3 access</li>
     *   <li>file:// - Uses LocalStorageProvider for local file system access</li>
     *   <li>http:// and https:// - Uses HttpStorageProvider (requires registry with executor)</li>
     * </ul>
     *
     * @param filePath the path to the Parquet file (s3://, file://, http://, or https://)
     * @param config configuration object (S3Configuration for S3 schemes, null for HTTP/file schemes)
     * @return IcebergTableMetadata with schema extracted from Parquet file
     * @throws Exception if file cannot be read
     * @deprecated Use {@link #resolveParquetFile(String, Object, StorageManager)} instead.
     *             This method creates a default StorageManager internally and will be removed in a future version.
     */
    @Deprecated
    public static IcebergTableMetadata resolveParquetFile(String filePath, Object config) throws Exception {
        // Create a default StorageManager with an empty registry.
        // The StorageManager will create providers directly via createProviderWithConfig()
        // when newStorageObject(String, Object) is called.
        StorageProviderRegistry registry = new StorageProviderRegistry();
        try (StorageManager storageManager = new StorageManager(registry)) {
            return resolveParquetFile(filePath, config, storageManager);
        }
    }
}
