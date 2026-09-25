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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionMetadataTests extends ESTestCase {

    public void testEmptyMetadataHasNoNullableColumns() {
        assertEquals(Set.of(), PartitionMetadata.EMPTY.nullablePartitionColumns());
    }

    public void testNoNullValuesYieldsEmptyNullableSet() {
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("year", DataType.INTEGER);
        cols.put("region", DataType.KEYWORD);

        Map<StoragePath, Map<String, Object>> files = new LinkedHashMap<>();
        files.put(StoragePath.of("s3://b/year=2024/region=us/f1.parquet"), Map.of("year", 2024, "region", "us"));
        files.put(StoragePath.of("s3://b/year=2023/region=eu/f2.parquet"), Map.of("year", 2023, "region", "eu"));

        PartitionMetadata pm = new PartitionMetadata(cols, files);
        assertEquals(Set.of(), pm.nullablePartitionColumns());
    }

    public void testSingleNullValueMarksColumnNullable() {
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("year", DataType.INTEGER);
        cols.put("month", DataType.INTEGER);

        Map<StoragePath, Map<String, Object>> files = new LinkedHashMap<>();
        files.put(StoragePath.of("s3://b/year=2024/month=01/f1.parquet"), Map.of("year", 2024, "month", 1));
        // Map.of() rejects nulls — use HashMap for the sentinel-decoded entry.
        Map<String, Object> withNullMonth = new HashMap<>();
        withNullMonth.put("year", 2024);
        withNullMonth.put("month", null);
        files.put(StoragePath.of("s3://b/year=2024/month=__HIVE_DEFAULT_PARTITION__/f2.parquet"), withNullMonth);

        PartitionMetadata pm = new PartitionMetadata(cols, files);
        assertEquals(Set.of("month"), pm.nullablePartitionColumns());
    }

    public void testMultipleNullableColumns() {
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("a", DataType.KEYWORD);
        cols.put("b", DataType.KEYWORD);
        cols.put("c", DataType.KEYWORD);

        Map<StoragePath, Map<String, Object>> files = new LinkedHashMap<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("a", "x");
        row1.put("b", null);
        row1.put("c", "z");
        Map<String, Object> row2 = new HashMap<>();
        row2.put("a", null);
        row2.put("b", "y");
        row2.put("c", "z");
        files.put(StoragePath.of("s3://b/a=x/b=__HIVE_DEFAULT_PARTITION__/c=z/f1.parquet"), row1);
        files.put(StoragePath.of("s3://b/a=__HIVE_DEFAULT_PARTITION__/b=y/c=z/f2.parquet"), row2);

        PartitionMetadata pm = new PartitionMetadata(cols, files);
        assertEquals(Set.of("a", "b"), pm.nullablePartitionColumns());
    }

    public void testMissingEntryTreatedAsNullable() {
        // A defensive case: if a file's value map is missing a declared column, treat that
        // column as nullable rather than asserting non-null based on incomplete data.
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("year", DataType.INTEGER);
        cols.put("region", DataType.KEYWORD);

        Map<StoragePath, Map<String, Object>> files = new LinkedHashMap<>();
        files.put(StoragePath.of("s3://b/year=2024/region=us/f1.parquet"), Map.of("year", 2024, "region", "us"));
        files.put(StoragePath.of("s3://b/year=2023/f2.parquet"), Map.of("year", 2023));

        PartitionMetadata pm = new PartitionMetadata(cols, files);
        assertEquals(Set.of("region"), pm.nullablePartitionColumns());
    }

    public void testEmptyFileValuesReturnsAllColumns() {
        // Defensive: when partition columns are declared but no per-file values are tracked,
        // we have no basis to prove non-null — every column must remain nullable.
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("year", DataType.INTEGER);
        cols.put("region", DataType.KEYWORD);

        PartitionMetadata pm = new PartitionMetadata(cols, Map.of());
        assertEquals(Set.of("year", "region"), pm.nullablePartitionColumns());
    }

    public void testNullInLastFileStillDetected() {
        // Regression guard against any future short-circuit-after-first-file mistake:
        // the helper must inspect every file, not stop once it has seen one non-null value.
        LinkedHashMap<String, DataType> cols = new LinkedHashMap<>();
        cols.put("year", DataType.INTEGER);
        cols.put("region", DataType.KEYWORD);

        Map<StoragePath, Map<String, Object>> files = new LinkedHashMap<>();
        files.put(StoragePath.of("s3://b/year=2024/region=us/f1.parquet"), Map.of("year", 2024, "region", "us"));
        files.put(StoragePath.of("s3://b/year=2024/region=eu/f2.parquet"), Map.of("year", 2024, "region", "eu"));
        Map<String, Object> lateNull = new HashMap<>();
        lateNull.put("year", 2024);
        lateNull.put("region", null);
        files.put(StoragePath.of("s3://b/year=2024/region=__HIVE_DEFAULT_PARTITION__/f3.parquet"), lateNull);

        PartitionMetadata pm = new PartitionMetadata(cols, files);
        assertEquals(Set.of("region"), pm.nullablePartitionColumns());
    }

    public void testFromHivePartitionDetectorWithSentinel() {
        // End-to-end bridge with HivePartitionDetector: confirms the sentinel decoding from
        // #149353 surfaces here as a null entry that nullablePartitionColumns() picks up.
        List<StorageEntry> files = List.of(
            new StorageEntry(StoragePath.of("s3://b/year=2024/month=01/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/year=2024/month=__HIVE_DEFAULT_PARTITION__/f2.parquet"), 100, Instant.EPOCH)
        );

        PartitionMetadata pm = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);
        assertFalse(pm.isEmpty());
        assertEquals(Set.of("month"), pm.nullablePartitionColumns());
    }

    public void testFromHivePartitionDetectorWithoutSentinel() {
        List<StorageEntry> files = List.of(
            new StorageEntry(StoragePath.of("s3://b/year=2024/month=01/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/year=2024/month=02/f2.parquet"), 100, Instant.EPOCH)
        );

        PartitionMetadata pm = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);
        assertFalse(pm.isEmpty());
        assertEquals(Set.of(), pm.nullablePartitionColumns());
    }

    /**
     * The schema's listing and the scan's are two different sets of files. The columns are the schema's answer; the
     * per-file values have to be the scan's, or a file the schema never listed reads null for every partition column.
     */
    public void testValuesComeFromTheListingThatNamedTheFilesBeingRead() {
        StoragePath resolved = StoragePath.of("s3://b/hour=00/a.parquet");
        StoragePath discovered = StoragePath.of("s3://b/hour=07/b.parquet");
        PartitionMetadata overThePrefix = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(resolved, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        PartitionMetadata overTheScan = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(resolved, 1, Instant.EPOCH), new StorageEntry(discovered, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );

        PartitionMetadata valued = overThePrefix.valuedOver(overTheScan);

        assertEquals("the columns stay the schema's", overThePrefix.partitionColumns(), valued.partitionColumns());
        assertEquals(2, valued.filePartitionValues().size());
        assertEquals(0, valued.filePartitionValues().get(resolved).get("hour"));
        assertEquals("the file nobody resolved knows its own value", 7, valued.filePartitionValues().get(discovered).get("hour"));
    }

    /**
     * The two listings can type a column differently, because a wider set of paths can carry a value a narrower one
     * could not. The type the plan was built on wins, and a value it cannot hold has none rather than being handed
     * over as the wrong class.
     */
    public void testAValueTheDeclaredTypeCannotHoldHasNoValue() {
        StoragePath numeric = StoragePath.of("s3://b/hour=07/a.parquet");
        StoragePath notNumeric = StoragePath.of("s3://b/hour=unknown/b.parquet");
        PartitionMetadata declaredInteger = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(numeric, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        PartitionMetadata scanned = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(numeric, 1, Instant.EPOCH), new StorageEntry(notNumeric, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        assertEquals("the wider listing typed it as text", DataType.KEYWORD, scanned.partitionColumns().get("hour"));

        PartitionMetadata valued = declaredInteger.valuedOver(scanned);

        assertEquals(DataType.INTEGER, valued.partitionColumns().get("hour"));
        assertEquals("a token the declared type holds is re-cast to it", 7, valued.filePartitionValues().get(numeric).get("hour"));
        assertNull("and one it cannot hold has no value", valued.filePartitionValues().get(notNumeric).get("hour"));
    }

    /** Nothing to value over: the schema's own answer stands. */
    public void testValuingOverNothingChangesNothing() {
        PartitionMetadata metadata = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(StoragePath.of("s3://b/hour=00/a.parquet"), 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        assertSame(metadata, metadata.valuedOver(null));
        assertSame(metadata, metadata.valuedOver(PartitionMetadata.EMPTY));
    }

    /**
     * Per-file values are evidence about the files they came from, and a listing that stopped at a bound covers part
     * of a dataset. Dropped, the column is unprovable rather than provably non-null over a fraction of the data.
     */
    public void testWithoutPerFileEvidenceNoColumnIsProvenNonNull() {
        PartitionMetadata overAPrefix = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(StoragePath.of("s3://b/year=2024/month=01/f.parquet"), 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        assertEquals("with evidence, both columns are provably non-null here", Set.of(), overAPrefix.nullablePartitionColumns());

        PartitionMetadata unproven = overAPrefix.withoutPerFileEvidence();

        assertEquals("the columns are still the dataset's", overAPrefix.partitionColumns(), unproven.partitionColumns());
        assertEquals(Set.of("year", "month"), unproven.nullablePartitionColumns());
        assertSame("and nothing to drop leaves it alone", unproven, unproven.withoutPerFileEvidence());
    }
}
