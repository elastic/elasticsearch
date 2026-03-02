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

public class TemplatePartitionDetectorTests extends ESTestCase {

    public void testStandardTemplate() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}/{month}/{day}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/15/file1.parquet"),
            entry("s3://bucket/data/2024/02/20/file2.parquet"),
            entry("s3://bucket/data/2023/12/31/file3.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(3, result.partitionColumns().size());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("month"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("day"));

        Map<String, Object> file1 = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/2024/01/15/file1.parquet"));
        assertEquals(2024, file1.get("year"));
        assertEquals(1, file1.get("month"));
        assertEquals(15, file1.get("day"));
    }

    public void testTypeInferenceKeyword() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{region}/{city}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/americas/sao_paulo/file.parquet"),
            entry("s3://bucket/data/europe/london/file.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("region"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("city"));

        Map<String, Object> file1 = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/americas/sao_paulo/file.parquet"));
        assertEquals("americas", file1.get("region"));
        assertEquals("sao_paulo", file1.get("city"));
    }

    public void testInconsistentSegmentCountReturnsEmpty() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}/{month}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet"),
            entry("s3://bucket/data/file2.parquet")  // not enough segments
        );

        PartitionMetadata result = detector.detect(files, Map.of());
        assertTrue(result.isEmpty());
    }

    public void testSingleColumnTemplate() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{date}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024-01-15/file.parquet"),
            entry("s3://bucket/data/2024-02-20/file.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(1, result.partitionColumns().size());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("date"));
    }

    public void testKinesisFirehoseLayout() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}/{month}/{day}/{hour}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/15/12/file.json"),
            entry("s3://bucket/data/2024/01/15/13/file.json")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(4, result.partitionColumns().size());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("hour"));
    }

    public void testHudiLayout() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{region}/{city}");

        List<StorageEntry> files = List.of(entry("s3://bucket/data/americas/sao_paulo/file.parquet"));

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(
            "americas",
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/americas/sao_paulo/file.parquet")).get("region")
        );
        assertEquals(
            "sao_paulo",
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/americas/sao_paulo/file.parquet")).get("city")
        );
    }

    public void testUrlEncodedValues() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{city}");

        List<StorageEntry> files = List.of(entry("s3://bucket/data/S%C3%A3o%20Paulo/file.parquet"));

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        Map<String, Object> values = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/S%C3%A3o%20Paulo/file.parquet"));
        assertEquals("São Paulo", values.get("city"));
    }

    public void testEmptyFilesReturnsEmpty() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}");
        PartitionMetadata result = detector.detect(List.of(), Map.of());
        assertTrue(result.isEmpty());
    }

    public void testNullFilesReturnsEmpty() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}");
        PartitionMetadata result = detector.detect(null, Map.of());
        assertTrue(result.isEmpty());
    }

    public void testParseTemplateColumns() {
        assertEquals(List.of("year", "month", "day"), TemplatePartitionDetector.parseTemplateColumns("{year}/{month}/{day}"));
        assertEquals(List.of("region"), TemplatePartitionDetector.parseTemplateColumns("{region}"));
        assertEquals(List.of(), TemplatePartitionDetector.parseTemplateColumns("no_placeholders"));
    }

    public void testNullTemplateThrows() {
        expectThrows(IllegalArgumentException.class, () -> new TemplatePartitionDetector(null));
    }

    public void testEmptyTemplateThrows() {
        expectThrows(IllegalArgumentException.class, () -> new TemplatePartitionDetector(""));
    }

    public void testTemplateWithNoPlaceholdersThrows() {
        expectThrows(IllegalArgumentException.class, () -> new TemplatePartitionDetector("no/placeholders"));
    }

    public void testNameReturnsTemplate() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}");
        assertEquals("template", detector.name());
    }

    public void testMixedIntegerAndKeyword() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{year}/{region}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/us-east/file.parquet"),
            entry("s3://bucket/data/2023/eu-west/file.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("region"));
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }
}
