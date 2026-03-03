/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class HivePartitionDetectorTests extends ESTestCase {

    public void testStandardHivePaths() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/month=01/file1.parquet"),
            entry("s3://bucket/data/year=2024/month=02/file2.parquet"),
            entry("s3://bucket/data/year=2023/month=12/file3.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.detect(files);

        assertFalse(result.isEmpty());
        assertEquals(2, result.partitionColumns().size());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("month"));

        Map<String, Object> file1Partitions = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/year=2024/month=01/file1.parquet"));
        assertEquals(2024, file1Partitions.get("year"));
        assertEquals(1, file1Partitions.get("month"));
    }

    public void testTypeInferenceInteger() {
        assertEquals(DataType.INTEGER, HivePartitionDetector.inferType(List.of("1", "2", "42")));
    }

    public void testTypeInferenceLong() {
        assertEquals(DataType.LONG, HivePartitionDetector.inferType(List.of("1", "9999999999")));
    }

    public void testTypeInferenceDouble() {
        assertEquals(DataType.DOUBLE, HivePartitionDetector.inferType(List.of("1.5", "2.7")));
    }

    public void testTypeInferenceBoolean() {
        assertEquals(DataType.BOOLEAN, HivePartitionDetector.inferType(List.of("true", "false", "TRUE")));
    }

    public void testTypeInferenceKeywordFallback() {
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of("us-east", "eu-west")));
    }

    public void testMixedTypesInferKeyword() {
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of("2024", "hello")));
    }

    public void testInconsistentKeysReturnsEmpty() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet"),
            entry("s3://bucket/data/month=01/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.detect(files);
        assertTrue(result.isEmpty());
    }

    public void testNonHivePathsReturnEmpty() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet"),
            entry("s3://bucket/data/2024/02/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.detect(files);
        assertTrue(result.isEmpty());
    }

    public void testUrlEncodedValues() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/city=S%C3%A3o%20Paulo/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.detect(files);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("city"));

        Map<String, Object> partitions = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/city=S%C3%A3o%20Paulo/file.parquet"));
        assertEquals("São Paulo", partitions.get("city"));
    }

    public void testSingleFile() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.detect(files);

        assertFalse(result.isEmpty());
        assertEquals(1, result.partitionColumns().size());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
    }

    public void testEmptyFileList() {
        PartitionMetadata result = HivePartitionDetector.detect(List.of());
        assertTrue(result.isEmpty());
    }

    public void testNullFileList() {
        PartitionMetadata result = HivePartitionDetector.detect(null);
        assertTrue(result.isEmpty());
    }

    public void testEmptyValues() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/file.parquet"));
        PartitionMetadata result = HivePartitionDetector.detect(files);
        assertTrue(result.isEmpty());
    }

    public void testMultiplePartitionLevels() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/country=US/state=CA/city=LA/file.parquet"),
            entry("s3://bucket/data/country=UK/state=London/city=Westminster/file.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.detect(files);

        assertFalse(result.isEmpty());
        assertEquals(3, result.partitionColumns().size());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("country"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("state"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("city"));
    }

    public void testCastValueInteger() {
        assertEquals(42, HivePartitionDetector.castValue("42", DataType.INTEGER));
    }

    public void testCastValueLong() {
        assertEquals(9999999999L, HivePartitionDetector.castValue("9999999999", DataType.LONG));
    }

    public void testCastValueDouble() {
        assertEquals(3.14, HivePartitionDetector.castValue("3.14", DataType.DOUBLE));
    }

    public void testCastValueBoolean() {
        assertEquals(true, HivePartitionDetector.castValue("true", DataType.BOOLEAN));
        assertEquals(false, HivePartitionDetector.castValue("false", DataType.BOOLEAN));
    }

    public void testCastValueKeyword() {
        assertEquals("hello", HivePartitionDetector.castValue("hello", DataType.KEYWORD));
    }

    public void testFileWithEqualsInFilename() {
        // file.parquet contains '=' but is not a partition segment (has a dot)
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/a=b.parquet"));

        PartitionMetadata result = HivePartitionDetector.detect(files);

        assertFalse(result.isEmpty());
        assertEquals(1, result.partitionColumns().size());
        assertTrue(result.partitionColumns().containsKey("year"));
    }

    public void testImplementsPartitionDetector() {
        HivePartitionDetector detector = HivePartitionDetector.INSTANCE;
        assertEquals("hive", detector.name());

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet"),
            entry("s3://bucket/data/year=2023/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());
        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
    }

    public void testDetectViaInterfaceIgnoresConfig() {
        PartitionDetector detector = HivePartitionDetector.INSTANCE;
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/file.parquet"));

        PartitionMetadata result = detector.detect(files, Map.of("irrelevant", "value"));
        assertFalse(result.isEmpty());
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }
}
