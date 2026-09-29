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
import java.util.ArrayList;
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

    /**
     * The re-cast runs through the value's text, which recovers the path's token only when the scan's listing kept
     * it as text. A listing that typed the column numerically has parsed the token away, so under a declared
     * keyword — where the spelling is the value — there is nothing to recover and no value is invented.
     */
    public void testASpellingTheScansListingParsedAwayIsNotInvented() {
        StoragePath text = StoragePath.of("s3://b/hour=morning/a.parquet");
        StoragePath padded = StoragePath.of("s3://b/hour=00/b.parquet");
        PartitionMetadata declaredKeyword = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(text, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        // A filtered scan can see a narrower set of folders than the schema's listing did, and type them tighter.
        PartitionMetadata scanned = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(padded, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        assertEquals(DataType.KEYWORD, declaredKeyword.partitionColumns().get("hour"));
        assertEquals(DataType.INTEGER, scanned.partitionColumns().get("hour"));

        PartitionMetadata valued = declaredKeyword.valuedOver(scanned);

        assertEquals(DataType.KEYWORD, valued.partitionColumns().get("hour"));
        assertNull(
            "[hour=00] must not come back as the string [0], which is what re-casting a parsed value would give",
            valued.filePartitionValues().get(padded).get("hour")
        );
    }

    /**
     * The reviewer's case, at the unit the defect lived in. A sample of two integral folders types the column
     * {@code INTEGER}; the full listing also holds {@code x=5e1}, which is not an integral token, so detection over
     * it answers {@code DOUBLE} and every value arrives as a {@code Double}. Conforming those through their text
     * asked {@code Integer.parseInt("1.0")} and nulled the whole column — every row of every folder, including the
     * six that fit perfectly well.
     */
    public void testANumericColumnSurvivesAWiderScanType() {
        List<StorageEntry> sample = List.of(
            new StorageEntry(StoragePath.of("s3://b/x=1/a.parquet"), 1, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/x=2/b.parquet"), 1, Instant.EPOCH)
        );
        List<StorageEntry> everything = new ArrayList<>(sample);
        everything.add(new StorageEntry(StoragePath.of("s3://b/x=5e1/c.parquet"), 1, Instant.EPOCH));

        PartitionMetadata declared = HivePartitionDetector.INSTANCE.detect(sample, WarningSinks.FAILING);
        PartitionMetadata scanned = HivePartitionDetector.INSTANCE.detect(everything, WarningSinks.FAILING);
        assertEquals("the sample types it integral", DataType.INTEGER, declared.partitionColumns().get("x"));
        assertEquals("the whole listing does not", DataType.DOUBLE, scanned.partitionColumns().get("x"));

        PartitionMetadata valued = declared.valuedOver(scanned);

        assertEquals(1, valued.filePartitionValues().get(sample.get(0).path()).get("x"));
        assertEquals(2, valued.filePartitionValues().get(sample.get(1).path()).get("x"));
        assertEquals(
            "and a folder the sample missed keeps the value its number holds exactly",
            50,
            valued.filePartitionValues().get(everything.get(2).path()).get("x")
        );
    }

    /**
     * An {@code unsigned_long} partition column is held sign-flip-encoded, so the boxed {@code Long} a detection
     * produces is not the value — {@code ExternalScalarRenderer} decodes it before anyone reads it. Conforming one to
     * a narrower declared type must decode first; treating the encoding as the number puts every value in the column
     * out by 2^63, and does it silently, because the partition warning only fires where a value went null.
     */
    public void testAnUnsignedLongScanTypeIsDecodedBeforeItIsNarrowed() {
        StoragePath small = StoragePath.of("s3://b/id=00000000003000000000/a.parquet");
        StoragePath huge = StoragePath.of("s3://b/id=18446744073709551615/b.parquet");
        PartitionMetadata declaredLong = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(small, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        PartitionMetadata scanned = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(small, 1, Instant.EPOCH), new StorageEntry(huge, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        assertEquals("the sample alone types it signed", DataType.LONG, declaredLong.partitionColumns().get("id"));
        assertEquals("the whole listing does not", DataType.UNSIGNED_LONG, scanned.partitionColumns().get("id"));

        PartitionMetadata valued = declaredLong.valuedOver(scanned);

        assertEquals("the value the folder names, not its encoding", 3000000000L, valued.filePartitionValues().get(small).get("id"));
        assertNull("and a value beyond the signed range has none under it", valued.filePartitionValues().get(huge).get("id"));
    }

    /** A number the declared type cannot hold exactly still has no value under it. */
    public void testANumberTheDeclaredTypeCannotHoldExactlyHasNoValue() {
        // Dotless on purpose: HivePartitionDetector#segmentKey rejects a segment containing a dot, so x=1.5
        // is not a partition folder at all. 5e-1 is the same value spelled in a way the detector accepts.
        StoragePath fraction = StoragePath.of("s3://b/x=5e-1/a.parquet");
        StoragePath huge = StoragePath.of("s3://b/x=99999999999/b.parquet");
        PartitionMetadata declaredInteger = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(StoragePath.of("s3://b/x=1/z.parquet"), 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        PartitionMetadata scannedFraction = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(fraction, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );
        PartitionMetadata scannedHuge = HivePartitionDetector.INSTANCE.detect(
            List.of(new StorageEntry(huge, 1, Instant.EPOCH)),
            WarningSinks.FAILING
        );

        assertNull(
            "a fraction is not an integer",
            declaredInteger.valuedOver(scannedFraction).filePartitionValues().get(fraction).get("x")
        );
        assertNull(
            "and neither is a magnitude outside the type",
            declaredInteger.valuedOver(scannedHuge).filePartitionValues().get(huge).get("x")
        );
    }
}
