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

public class AutoPartitionDetectorTests extends ESTestCase {

    public void testHivePathsAutoDetected() {
        PartitionDetector detector = AutoPartitionDetector.fromConfig(PartitionConfig.DEFAULT);

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet"),
            entry("s3://bucket/data/year=2023/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
    }

    public void testBarePathsWithTemplateConfig() {
        PartitionConfig config = new PartitionConfig(PartitionConfig.AUTO, "{year}/{month}");
        PartitionDetector detector = AutoPartitionDetector.fromConfig(config);

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet"),
            entry("s3://bucket/data/2023/12/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("month"));
    }

    public void testBarePathsWithoutTemplateReturnsEmpty() {
        PartitionDetector detector = AutoPartitionDetector.fromConfig(PartitionConfig.DEFAULT);

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet"),
            entry("s3://bucket/data/2023/12/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());
        assertTrue(result.isEmpty());
    }

    public void testHivePathsWithTemplateStrategyUsesTemplate() {
        PartitionConfig config = new PartitionConfig(PartitionConfig.TEMPLATE, "{year}");
        PartitionDetector detector = new TemplatePartitionDetector("{year}");

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file.parquet"),
            entry("s3://bucket/data/year=2023/file.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());
        assertFalse(result.isEmpty());
        // Template detector extracts the last segment before filename positionally
        assertEquals("year=2024", result.filePartitionValues().get(StoragePath.of("s3://bucket/data/year=2024/file.parquet")).get("year"));
    }

    public void testNoneStrategyReturnsEmpty() {
        PartitionConfig config = new PartitionConfig(PartitionConfig.NONE, null);
        PartitionDetector detector = GlobExpander.resolveDetector(config);

        assertNull(detector);
    }

    public void testNullConfigDefaultsToHive() {
        PartitionDetector detector = AutoPartitionDetector.fromConfig(null);
        assertEquals("hive", ((HivePartitionDetector) detector).name());
    }

    public void testAutoDetectorName() {
        AutoPartitionDetector detector = new AutoPartitionDetector(PartitionConfig.DEFAULT);
        assertEquals("auto", detector.name());
    }

    public void testHiveDetectedBeforeTemplate() {
        PartitionConfig config = new PartitionConfig(PartitionConfig.AUTO, "{col}");
        PartitionDetector detector = AutoPartitionDetector.fromConfig(config);

        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet"),
            entry("s3://bucket/data/year=2023/file2.parquet")
        );

        PartitionMetadata result = detector.detect(files, Map.of());
        assertFalse(result.isEmpty());
        // Hive should be detected first, so the column should be "year" not "col"
        assertTrue(result.partitionColumns().containsKey("year"));
        assertFalse(result.partitionColumns().containsKey("col"));
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }
}
