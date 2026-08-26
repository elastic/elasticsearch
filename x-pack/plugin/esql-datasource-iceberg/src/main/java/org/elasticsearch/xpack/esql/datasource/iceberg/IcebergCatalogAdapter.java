/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

/**
 * Adapter for accessing Iceberg catalog and table metadata.
 * Provides a simplified interface for resolving Iceberg tables.
 * <p>
 * This implementation uses Iceberg's StaticTableOperations with S3FileIO,
 * avoiding Hadoop dependencies and security manager issues.
 */
public class IcebergCatalogAdapter {

    private static final String SOURCE_TYPE_ICEBERG = "iceberg";
    private static final String METADATA_DIR = "metadata";
    private static final String METADATA_FILE_EXTENSION = ".metadata.json";

    /**
     * Resolve Iceberg table metadata from a table path.
     * Uses StaticTableOperations with S3FileIO instead of HadoopCatalog.
     *
     * @param tablePath the S3 path to the Iceberg table
     * @param s3Config S3 configuration (credentials, endpoint, etc.)
     * @return IcebergTableMetadata with resolved schema
     * @throws Exception if table cannot be resolved
     */
    public static IcebergTableMetadata resolveTable(String tablePath, S3Configuration s3Config) throws Exception {
        // Create S3FileIO for accessing table metadata
        S3FileIO fileIO = S3FileIOFactory.create(s3Config);

        try {
            // Find the latest metadata file
            String metadataLocation = findLatestMetadataFile(tablePath, fileIO);

            // Load table using StaticTableOperations
            StaticTableOperations ops = new StaticTableOperations(metadataLocation, fileIO);
            Table table = new BaseTable(ops, tablePath);
            Schema schema = table.schema();

            // Pass the metadata location so we can recreate the table later if needed
            return new IcebergTableMetadata(tablePath, schema, s3Config, SOURCE_TYPE_ICEBERG, metadataLocation);
        } finally {
            // Close FileIO to release resources - use IOUtils which logs suppressed exceptions
            IOUtils.closeWhileHandlingException(fileIO);
        }
    }

    /**
     * Find the latest metadata file in the table's metadata directory.
     * Iceberg tables store metadata in versioned JSON files like v1.metadata.json, v2.metadata.json, etc.
     *
     * Since FileIO doesn't have a listPrefix method, we try common version numbers.
     * This is a simplified approach that works for test fixtures and small tables.
     * For production, consider using a catalog that tracks the current metadata location.
     *
     * @param tablePath the base path to the Iceberg table
     * @param fileIO the FileIO to use for checking file existence
     * @return the full path to the latest metadata file
     * @throws IOException if no metadata files found
     */
    private static String findLatestMetadataFile(String tablePath, FileIO fileIO) throws IOException {
        // Ensure tablePath ends with /
        String normalizedPath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
        String metadataDir = normalizedPath + METADATA_DIR + "/";

        // First, try to read version-hint.text which points to the current metadata version
        // This is the most reliable approach as it's maintained by Iceberg
        String versionHintPath = metadataDir + "version-hint.text";
        try {
            org.apache.iceberg.io.InputFile versionHintFile = fileIO.newInputFile(versionHintPath);
            if (versionHintFile.exists()) {
                // Read the version number from the hint file
                try (java.io.InputStream is = versionHintFile.newStream()) {
                    String versionStr = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
                    int version = Integer.parseInt(versionStr);
                    String metadataPath = metadataDir + "v" + version + METADATA_FILE_EXTENSION;
                    // Verify the metadata file exists
                    org.apache.iceberg.io.InputFile metadataFile = fileIO.newInputFile(metadataPath);
                    if (metadataFile.exists()) {
                        return metadataPath;
                    }
                }
            }
        } catch (Exception e) {
            // Version hint doesn't exist or couldn't be read, fall through to scan
        }

        // Fallback: Try to find metadata files by checking common version numbers
        // Start from a reasonable max version and work backwards
        for (int version = 100; version >= 1; version--) {
            String metadataPath = metadataDir + "v" + version + METADATA_FILE_EXTENSION;
            try {
                org.apache.iceberg.io.InputFile inputFile = fileIO.newInputFile(metadataPath);
                // Actually check if the file exists - newInputFile() alone doesn't verify existence
                if (inputFile.exists()) {
                    return metadataPath;
                }
            } catch (Exception e) {
                // Error checking this version, try next
            }
        }

        throw new IOException("No metadata files found in " + metadataDir + ". Tried version-hint.text and versions 1-100");
    }

    /**
     * Extract version number from a metadata filename.
     * For example: "s3://bucket/table/metadata/v123.metadata.json" -> 123
     *
     * @param path the full path to the metadata file
     * @return the version number, or 0 if it cannot be parsed
     */
    static int extractVersionNumber(String path) {
        try {
            // Get filename from path
            int lastSlash = path.lastIndexOf('/');
            String filename = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;

            // Remove "v" prefix and ".metadata.json" suffix
            if (filename.startsWith("v") && filename.endsWith(METADATA_FILE_EXTENSION)) {
                String versionStr = filename.substring(1, filename.length() - METADATA_FILE_EXTENSION.length());
                return Integer.parseInt(versionStr);
            }
        } catch (NumberFormatException e) {
            // If parsing fails, return 0
        }
        return 0;
    }
}
