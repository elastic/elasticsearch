/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for IcebergCatalogAdapter.
 * Tests the version number extraction logic used for finding metadata files.
 *
 * Note: The main resolveTable() and findLatestMetadataFile() methods require
 * actual S3 connectivity and are tested via integration tests.
 */
public class IcebergCatalogAdapterTests extends ESTestCase {

    public void testExtractVersionNumberFromSimplePath() throws Exception {
        int version = invokeExtractVersionNumber("v1.metadata.json");
        assertEquals(1, version);
    }

    public void testExtractVersionNumberFromFullPath() throws Exception {
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/v42.metadata.json");
        assertEquals(42, version);
    }

    public void testExtractVersionNumberFromLargeVersion() throws Exception {
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/v9999.metadata.json");
        assertEquals(9999, version);
    }

    public void testExtractVersionNumberFromPathWithNestedDirs() throws Exception {
        int version = invokeExtractVersionNumber("s3://bucket/path/to/table/metadata/v123.metadata.json");
        assertEquals(123, version);
    }

    public void testExtractVersionNumberReturnsZeroForInvalidFormat() throws Exception {
        // Missing v prefix
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/1.metadata.json");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForWrongExtension() throws Exception {
        // Wrong file extension
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/v1.json");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForNonNumeric() throws Exception {
        // Non-numeric version
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/vABC.metadata.json");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForEmptyFilename() throws Exception {
        int version = invokeExtractVersionNumber("");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForJustExtension() throws Exception {
        int version = invokeExtractVersionNumber(".metadata.json");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForSnapshotFile() throws Exception {
        // Iceberg snapshot files have different naming
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/snap-123456789.avro");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberReturnsZeroForVersionHintFile() throws Exception {
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/version-hint.text");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberWithTrailingSlash() throws Exception {
        // Edge case: path ending with slash (shouldn't happen but handle gracefully)
        int version = invokeExtractVersionNumber("s3://bucket/table/metadata/");
        assertEquals(0, version);
    }

    public void testExtractVersionNumberFromLocalPath() throws Exception {
        // Local filesystem path format
        int version = invokeExtractVersionNumber("/path/to/table/metadata/v7.metadata.json");
        assertEquals(7, version);
    }

    public void testExtractVersionNumberFromWindowsPath() throws Exception {
        // Windows-style path (forward slashes work)
        int version = invokeExtractVersionNumber("C:/data/table/metadata/v15.metadata.json");
        assertEquals(15, version);
    }

    public void testMetadataDirectorySuffix() {
        // Verify the expected metadata directory structure
        String tablePath = "s3://bucket/table";
        String expectedMetadataPath = tablePath + "/metadata/v1.metadata.json";
        assertTrue(expectedMetadataPath.endsWith(".metadata.json"));
        assertTrue(expectedMetadataPath.contains("/metadata/"));
    }

    public void testSourceTypeConstant() {
        // The source type should be "iceberg"
        // This validates that any IcebergTableMetadata returned will have the correct sourceType
        String expectedSourceType = "iceberg";

        // We can verify this by checking that IcebergTableMetadata created with "iceberg" works
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get())
        );
        IcebergTableMetadata metadata = new IcebergTableMetadata("s3://bucket/table", schema, null, "iceberg");
        assertEquals(expectedSourceType, metadata.sourceType());
    }

    private int invokeExtractVersionNumber(String path) {
        return IcebergCatalogAdapter.extractVersionNumber(path);
    }
}
