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

    /**
     * Standard metadata names are dedicated: a template placeholder claiming one (here
     * {@code {_index}}) surfaces under the {@code _partition.} prefix — the same contract the
     * Hive detector enforces — so {@code METADATA _index} keeps its spec meaning while the
     * layout's value stays queryable. Each rename is disclosed via a {@code Warning} header at
     * detection time.
     */
    public void testReservedPlaceholderSurfacesUnderPartitionPrefix() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{_index}/{year}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/alpha/2024/file1.parquet"),
            entry("s3://bucket/data/beta/2023/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertFalse("reserved name must not surface as-is", result.partitionColumns().containsKey("_index"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._index"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));

        Map<String, Object> file1 = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/alpha/2024/file1.parquet"));
        assertEquals("alpha", file1.get("_partition._index"));
        assertEquals(2024, file1.get("year"));

        assertWarnings(
            "Partition columns shadowing reserved metadata names were renamed; reference them by the _partition.* name.",
            "partition column [_index] surfaced as [_partition._index]"
        );
    }

    /**
     * The per-row composed pair is reserved too: {@code {_id}} and {@code {_source}} placeholders
     * must not reach {@code VirtualColumnIterator}'s name-keyed role dispatch (a bare {@code _id}
     * partition column crashes on the missing {@code _rowPosition} channel; a bare {@code _source}
     * silently substitutes synthesized JSON for the layout's value).
     */
    public void testReservedPerRowNamesAreRenamedToo() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{_id}/{_source}");

        List<StorageEntry> files = List.of(entry("s3://bucket/data/k1/v1/file1.parquet"));

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._id"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._source"));
        assertFalse(result.partitionColumns().containsKey("_id"));
        assertFalse(result.partitionColumns().containsKey("_source"));

        assertWarnings(
            "Partition columns shadowing reserved metadata names were renamed; reference them by the _partition.* name.",
            "partition column [_id] surfaced as [_partition._id]",
            "partition column [_source] surfaced as [_partition._source]"
        );
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

    /**
     * The template detector delegates type inference and casting to {@link HivePartitionDetector}, so it shares
     * the capitalized-boolean handling: a {@code {flag}} template over {@code True/} and {@code False/} segments
     * infers {@code BOOLEAN} and casts each to its boolean value.
     */
    public void testCapitalizedBooleanTemplatePartition() {
        TemplatePartitionDetector detector = new TemplatePartitionDetector("{flag}");

        List<StorageEntry> files = List.of(entry("s3://bucket/data/True/file1.parquet"), entry("s3://bucket/data/False/file2.parquet"));

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.BOOLEAN, result.partitionColumns().get("flag"));
        assertEquals(true, result.filePartitionValues().get(StoragePath.of("s3://bucket/data/True/file1.parquet")).get("flag"));
        assertEquals(false, result.filePartitionValues().get(StoragePath.of("s3://bucket/data/False/file2.parquet")).get("flag"));
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
