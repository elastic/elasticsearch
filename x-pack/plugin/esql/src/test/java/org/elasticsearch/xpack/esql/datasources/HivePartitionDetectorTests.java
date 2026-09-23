/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HivePartitionDetectorTests extends ESTestCase {

    public void testStandardHivePaths() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/month=01/file1.parquet"),
            entry("s3://bucket/data/year=2024/month=02/file2.parquet"),
            entry("s3://bucket/data/year=2023/month=12/file3.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

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
        assertEquals(DataType.LONG, HivePartitionDetector.inferType(List.of(Long.toString(Long.MAX_VALUE))));
    }

    public void testTypeInferenceUnsignedLong() {
        assertEquals(DataType.UNSIGNED_LONG, HivePartitionDetector.inferType(List.of("9223372036854775808", "18446744073709551615")));
        assertEquals(DataType.UNSIGNED_LONG, HivePartitionDetector.inferType(List.of("1", "9999999999", "9223372036854775808")));
    }

    public void testTypeInferenceMixedNegativeAndUnsignedLongFallsBackToKeyword() {
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of("-1", "9223372036854775808")));
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of(Long.toString(Long.MIN_VALUE), "18446744073709551615")));
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

    /**
     * The other half of the inference/cast symmetry: a token that is not {@code true}/{@code false} in any
     * case (here {@code yes}, and a whitespace-padded {@code " true"}) infers {@code KEYWORD}, so it never
     * routes to the BOOLEAN cast branch. Inference admits exactly the token set the cast accepts, so a
     * KEYWORD-typed value cannot reach {@code strictParseBoolean}.
     */
    public void testTypeInferenceRejectsNonBooleanTokens() {
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of("true", "yes")));
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of(" true", "false")));
    }

    /**
     * A hive tree with capitalized boolean folders ({@code flag=True/}, {@code flag=False/}) types the column
     * {@code BOOLEAN} and casts each folder to its boolean value.
     */
    public void testCapitalizedBooleanPartitionFoldersInferAndCast() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/flag=True/file1.parquet"),
            entry("s3://bucket/data/flag=False/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.BOOLEAN, result.partitionColumns().get("flag"));

        assertEquals(true, result.filePartitionValues().get(StoragePath.of("s3://bucket/data/flag=True/file1.parquet")).get("flag"));
        assertEquals(false, result.filePartitionValues().get(StoragePath.of("s3://bucket/data/flag=False/file2.parquet")).get("flag"));
    }

    /**
     * A capitalized boolean folder mixed with the Hive null sentinel: the column still infers {@code BOOLEAN},
     * the sentinel casts to {@code null} (the null short-circuit precedes the boolean branch), and {@code True}
     * casts to {@code true}.
     */
    public void testCapitalizedBooleanPartitionWithNullSentinel() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/flag=True/file1.parquet"),
            entry("s3://bucket/data/flag=__HIVE_DEFAULT_PARTITION__/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.BOOLEAN, result.partitionColumns().get("flag"));
        assertEquals(true, result.filePartitionValues().get(StoragePath.of("s3://bucket/data/flag=True/file1.parquet")).get("flag"));
        assertNull(
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/flag=__HIVE_DEFAULT_PARTITION__/file2.parquet")).get("flag")
        );
    }

    public void testUnsignedLongPartitionFoldersInferAndCast() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/id=1/file1.parquet"),
            entry("s3://bucket/data/id=9223372036854775808/file2.parquet"),
            entry("s3://bucket/data/id=18446744073709551615/file3.parquet"),
            entry("s3://bucket/data/id=__HIVE_DEFAULT_PARTITION__/file4.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.UNSIGNED_LONG, result.partitionColumns().get("id"));
        assertUnsignedLongPartitionValue(result, "s3://bucket/data/id=1/file1.parquet", "1");
        assertUnsignedLongPartitionValue(result, "s3://bucket/data/id=9223372036854775808/file2.parquet", "9223372036854775808");
        assertUnsignedLongPartitionValue(result, "s3://bucket/data/id=18446744073709551615/file3.parquet", "18446744073709551615");
        assertNull(
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/id=__HIVE_DEFAULT_PARTITION__/file4.parquet")).get("id")
        );
    }

    public void testMixedNegativeAndUnsignedLongPartitionFoldersInferKeyword() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/id=-1/file1.parquet"),
            entry("s3://bucket/data/id=9223372036854775808/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("id"));
        assertEquals("-1", result.filePartitionValues().get(StoragePath.of("s3://bucket/data/id=-1/file1.parquet")).get("id"));
        assertEquals(
            "9223372036854775808",
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/id=9223372036854775808/file2.parquet")).get("id")
        );
    }

    public void testMixedTypesInferKeyword() {
        assertEquals(DataType.KEYWORD, HivePartitionDetector.inferType(List.of("2024", "hello")));
    }

    /**
     * Standard metadata names are dedicated: a partition directory claiming one (here
     * {@code /_index=…/}) surfaces under the {@code _partition.} prefix so {@code METADATA _index}
     * keeps its spec meaning (dataset name) while the layout's value stays queryable. Values and
     * types follow the rename; non-colliding keys are untouched.
     */
    public void testReservedMetadataNameSurfacesUnderPartitionPrefix() {
        List<String> warnings = new ArrayList<>();
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/_index=alpha/year=2024/file1.parquet"),
            entry("s3://bucket/data/_index=beta/year=2023/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, warnings::add);

        assertFalse(result.isEmpty());
        assertEquals(2, result.partitionColumns().size());
        assertFalse("reserved name must not surface as-is", result.partitionColumns().containsKey("_index"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._index"));
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));

        Map<String, Object> file1 = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/_index=alpha/year=2024/file1.parquet"));
        assertEquals("alpha", file1.get("_partition._index"));
        assertEquals(2024, file1.get("year"));

        assertEquals(
            List.of(
                "Partition keys named like a metadata column are renamed to [_partition.<key>]",
                "partition key [_index] is named [_partition._index]"
            ),
            warnings
        );
    }

    /**
     * {@code _tier} is a snapshot-gated standard metadata name, but reservation is build-mode
     * independent: a {@code /_tier=…/} layout is renamed to {@code _partition._tier} in release
     * builds too, so the same dataset surfaces the same column names regardless of build.
     */
    public void testSnapshotGatedReservedNameStillRenamedInReleaseBuilds() {
        List<String> warnings = new ArrayList<>();
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/_tier=hot/file1.parquet"),
            entry("s3://bucket/data/_tier=cold/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, warnings::add);

        assertFalse(result.isEmpty());
        assertFalse("snapshot-gated reserved name must not surface as-is", result.partitionColumns().containsKey("_tier"));
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._tier"));
        assertEquals(
            "hot",
            result.filePartitionValues().get(StoragePath.of("s3://bucket/data/_tier=hot/file1.parquet")).get("_partition._tier")
        );

        assertEquals(
            List.of(
                "Partition keys named like a metadata column are renamed to [_partition.<key>]",
                "partition key [_tier] is named [_partition._tier]"
            ),
            warnings
        );
    }

    public void testInconsistentKeysReturnsEmpty() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet"),
            entry("s3://bucket/data/month=01/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);
        assertTrue(result.isEmpty());
    }

    public void testNonHivePathsReturnEmpty() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet"),
            entry("s3://bucket/data/2024/02/file2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);
        assertTrue(result.isEmpty());
    }

    public void testUrlEncodedValues() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/city=S%C3%A3o%20Paulo/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("city"));

        Map<String, Object> partitions = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/city=S%C3%A3o%20Paulo/file.parquet"));
        assertEquals("São Paulo", partitions.get("city"));
    }

    /**
     * A literal {@code +} in a partition folder must survive as {@code +}, not be turned into a space. Hive
     * partition folders are {@code %XX}-escaped only and write {@code +} literally, so decoding a value the
     * {@code application/x-www-form-urlencoded} way corrupts {@code tag=a+b/} to {@code "a b"} and a filter on the
     * true value drops every row of that folder.
     */
    public void testLiteralPlusIsNotDecodedToSpace() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=a+b/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("tag"));

        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=a+b/file.parquet"));
        assertEquals("a+b", partitions.get("tag"));
    }

    /** An escaped {@code +} ({@code %2B}) decodes to a literal {@code +}, round-tripping the {@code %XX} escape. */
    public void testPercentEncodedPlusDecodesToPlus() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=a%2Bb/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=a%2Bb/file.parquet"));
        assertEquals("a+b", partitions.get("tag"));
    }

    /** An escaped {@code :} ({@code %3A}) decodes to a literal colon (the everyday shape for namespace/timestamp values). */
    public void testPercentEncodedColonDecodesToColon() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=ns%3Aclick/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=ns%3Aclick/file.parquet"));
        assertEquals("ns:click", partitions.get("tag"));
    }

    /**
     * Within one value a literal {@code +} stays {@code +} while an escaped space ({@code %20}) decodes to a space.
     * Form-urlencoded decoding would collapse both to spaces.
     */
    public void testLiteralPlusAndEscapedSpaceInOneValue() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=a+b%20c/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=a+b%20c/file.parquet"));
        assertEquals("a+b c", partitions.get("tag"));
    }

    /** A malformed escape ({@code %} not followed by two hex digits) is left as the raw value, never throwing. */
    public void testMalformedPercentEscapeFallsBackToRawValue() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=a%2/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=a%2/file.parquet"));
        assertEquals("a%2", partitions.get("tag"));
    }

    /**
     * A literal {@code +} next to a malformed escape falls back to the raw value, keeping the {@code +} literal rather
     * than surfacing the {@code %2B} form the decoder would use for a well-formed value.
     */
    public void testLiteralPlusWithMalformedEscapeFallsBackToRawValue() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/tag=a+%2/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        Map<String, Object> partitions = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/tag=a+%2/file.parquet"));
        assertEquals("a+%2", partitions.get("tag"));
    }

    public void testSingleFile() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(1, result.partitionColumns().size());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
    }

    public void testEmptyFileList() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(List.of(), WarningSinks.FAILING);
        assertTrue(result.isEmpty());
    }

    public void testNullFileList() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(null, WarningSinks.FAILING);
        assertTrue(result.isEmpty());
    }

    public void testEmptyValues() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/file.parquet"));
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);
        assertTrue(result.isEmpty());
    }

    public void testMultiplePartitionLevels() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/country=US/state=CA/city=LA/file.parquet"),
            entry("s3://bucket/data/country=UK/state=London/city=Westminster/file.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

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

    public void testCastValueUnsignedLong() {
        String value = "9999999999999999999";
        assertEquals(NumericUtils.asLongUnsigned(new BigInteger(value)), HivePartitionDetector.castValue(value, DataType.UNSIGNED_LONG));
    }

    public void testCastValueDouble() {
        assertEquals(3.14, HivePartitionDetector.castValue("3.14", DataType.DOUBLE));
    }

    public void testCastValueBoolean() {
        assertEquals(true, HivePartitionDetector.castValue("true", DataType.BOOLEAN));
        assertEquals(false, HivePartitionDetector.castValue("false", DataType.BOOLEAN));
    }

    /**
     * Capitalized boolean partition folders ({@code flag=True/}, {@code flag=False/}) are accepted by
     * {@code tryAllBoolean} case-insensitively, so the cast agrees: any casing of {@code true}/{@code false}
     * casts to the boolean value, keeping inference and cast symmetric.
     */
    public void testCastValueBooleanCaseInsensitive() {
        assertEquals(true, HivePartitionDetector.castValue("True", DataType.BOOLEAN));
        assertEquals(false, HivePartitionDetector.castValue("FALSE", DataType.BOOLEAN));
        assertEquals(true, HivePartitionDetector.castValue("TrUe", DataType.BOOLEAN));
        assertEquals(false, HivePartitionDetector.castValue("False", DataType.BOOLEAN));
        assertEquals(true, HivePartitionDetector.castValue("TRUE", DataType.BOOLEAN));
    }

    /**
     * The cast side of the symmetry: a token outside the case-insensitive {@code true}/{@code false} set
     * ({@code yes}, or a whitespace-padded {@code " true"}) throws when cast to {@code BOOLEAN}. Such tokens
     * infer {@code KEYWORD} (see {@code testTypeInferenceRejectsNonBooleanTokens}), so they never reach this
     * branch in practice, but the cast pins the same accepted set.
     */
    public void testCastValueBooleanRejectsNonBooleanTokens() {
        expectThrows(InvalidArgumentException.class, () -> HivePartitionDetector.castValue("yes", DataType.BOOLEAN));
        expectThrows(InvalidArgumentException.class, () -> HivePartitionDetector.castValue(" true", DataType.BOOLEAN));
    }

    public void testCastValueKeyword() {
        assertEquals("hello", HivePartitionDetector.castValue("hello", DataType.KEYWORD));
    }

    public void testFileWithEqualsInFilename() {
        // The object-name cut drops a=b.parquet. year is the only partition; the dot in b.parquet is not what excludes a.
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/a=b.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

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

        PartitionMetadata result = detector.detect(files, WarningSinks.FAILING);
        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
    }

    /** Renamed from testDetectViaInterfaceIgnoresConfig: detect() no longer takes a config map to ignore. */
    public void testDetectViaInterface() {
        PartitionDetector detector = HivePartitionDetector.INSTANCE;
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/file.parquet"));

        PartitionMetadata result = detector.detect(files, WarningSinks.FAILING);
        assertFalse(result.isEmpty());
    }

    public void testHiveDefaultPartitionAlone() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/year=2024/month=__HIVE_DEFAULT_PARTITION__/file.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        // month has only the Hive null sentinel; with no concrete values it should still be parseable as INTEGER
        // and the per-file value should be null, not the literal sentinel string.
        assertEquals(DataType.INTEGER, result.partitionColumns().get("month"));

        Map<String, Object> partitions = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/year=2024/month=__HIVE_DEFAULT_PARTITION__/file.parquet"));
        assertEquals(2024, partitions.get("year"));
        assertNull(partitions.get("month"));
    }

    public void testHiveDefaultPartitionMixedWithIntegers() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/month=06/f1.parquet"),
            entry("s3://bucket/data/year=2024/month=__HIVE_DEFAULT_PARTITION__/f2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("month"));

        Map<String, Object> p1 = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/year=2024/month=06/f1.parquet"));
        assertEquals(6, p1.get("month"));

        Map<String, Object> p2 = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/year=2024/month=__HIVE_DEFAULT_PARTITION__/f2.parquet"));
        assertNull(p2.get("month"));
    }

    public void testHiveDefaultPartitionMixedWithStrings() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/region=us-east/f1.parquet"),
            entry("s3://bucket/data/region=__HIVE_DEFAULT_PARTITION__/f2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("region"));

        Map<String, Object> p1 = result.filePartitionValues().get(StoragePath.of("s3://bucket/data/region=us-east/f1.parquet"));
        assertEquals("us-east", p1.get("region"));

        Map<String, Object> p2 = result.filePartitionValues()
            .get(StoragePath.of("s3://bucket/data/region=__HIVE_DEFAULT_PARTITION__/f2.parquet"));
        assertNull(p2.get("region"));
    }

    public void testInferTypeSkipsNullsInteger() {
        assertEquals(DataType.INTEGER, HivePartitionDetector.inferType(Arrays.asList("1", null, "2")));
    }

    public void testInferTypeSkipsNullsDouble() {
        assertEquals(DataType.DOUBLE, HivePartitionDetector.inferType(Arrays.asList("1.5", null, "2.7")));
    }

    public void testInferTypeSkipsNullsBoolean() {
        assertEquals(DataType.BOOLEAN, HivePartitionDetector.inferType(Arrays.asList("true", null, "false")));
    }

    public void testCastValueNullReturnsNull() {
        assertNull(HivePartitionDetector.castValue(null, DataType.INTEGER));
        assertNull(HivePartitionDetector.castValue(null, DataType.LONG));
        assertNull(HivePartitionDetector.castValue(null, DataType.UNSIGNED_LONG));
        assertNull(HivePartitionDetector.castValue(null, DataType.DOUBLE));
        assertNull(HivePartitionDetector.castValue(null, DataType.BOOLEAN));
        assertNull(HivePartitionDetector.castValue(null, DataType.KEYWORD));
    }

    /**
     * Mixed partition depth — older data partitioned by year only, newer by year/month/day — yields no partition
     * columns at all, not even the common {@code year}. The detector requires an identical key set across every
     * file. Pinned as the current contract: changing it to an intersection, to per-file nulls, or to a loud error
     * would change results for stored datasets and needs its own design pass.
     */
    public void testMixedPartitionDepthReturnsEmpty() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/data/year=2024/f1.parquet"),
            entry("s3://bucket/data/year=2024/month=01/f2.parquet"),
            entry("s3://bucket/data/year=2024/month=01/day=15/f3.parquet")
        );

        assertTrue(
            "inconsistent key sets across files yield no partition columns",
            HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING).isEmpty()
        );
    }

    private static void assertUnsignedLongPartitionValue(PartitionMetadata result, String path, String expected) {
        Object value = result.filePartitionValues().get(StoragePath.of(path)).get("id");
        assertTrue(value instanceof Long);
        Number decoded = NumericUtils.unsignedLongAsNumber((Long) value);
        assertEquals(new BigInteger(expected), new BigInteger(decoded.toString()));
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }

    /** With a sink the rename notice goes there, not to the response headers of whatever thread ran detection. */
    public void testReservedMetadataNameRenameWarningGoesToSink() {
        List<StorageEntry> files = List.of(entry("s3://bucket/data/_index=alpha/year=2024/file1.parquet"));
        List<String> sink = new ArrayList<>();

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, sink::add);

        assertEquals(DataType.KEYWORD, result.partitionColumns().get("_partition._index"));
        assertEquals(
            List.of(
                "Partition keys named like a metadata column are renamed to [_partition.<key>]",
                "partition key [_index] is named [_partition._index]"
            ),
            sink
        );
    }

    /** {@code price=1.5} beside an integral sibling types the column double and keeps both values. */
    public void testDecimalPartitionValueInfersDouble() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/year=2024/price=1.5/f1.parquet"),
            entry("s3://bucket/year=2024/price=2/f2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.INTEGER, result.partitionColumns().get("year"));
        assertEquals(DataType.DOUBLE, result.partitionColumns().get("price"));
        assertEquals(1.5, result.filePartitionValues().get(StoragePath.of("s3://bucket/year=2024/price=1.5/f1.parquet")).get("price"));
        assertEquals(2.0, result.filePartitionValues().get(StoragePath.of("s3://bucket/year=2024/price=2/f2.parquet")).get("price"));
    }

    /** An empty folder {@code k=} is the value {@code ""}, not a rejected segment. */
    public void testEmptyPartitionValue() {
        List<StorageEntry> files = List.of(entry("s3://bucket/k=/f.csv"), entry("s3://bucket/k=x/f.csv"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(Set.of("k"), result.partitionColumns().keySet());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("k"));
        assertEquals("", result.filePartitionValues().get(StoragePath.of("s3://bucket/k=/f.csv")).get("k"));
        assertEquals("x", result.filePartitionValues().get(StoragePath.of("s3://bucket/k=x/f.csv")).get("k"));
    }

    /** {@code k=} under {@code year=} is a second column, not a reason to drop the listing. */
    public void testEmptyPartitionValueBesideYear() {
        List<StorageEntry> files = List.of(entry("s3://bucket/year=2024/k=/f.parquet"), entry("s3://bucket/year=2024/k=x/f.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(Set.of("year", "k"), result.partitionColumns().keySet());
        assertEquals("", result.filePartitionValues().get(StoragePath.of("s3://bucket/year=2024/k=/f.parquet")).get("k"));
        assertEquals("x", result.filePartitionValues().get(StoragePath.of("s3://bucket/year=2024/k=x/f.parquet")).get("k"));
    }

    /** Dotted values that are not doubles stay keyword and keep the column. */
    public void testDottedNonNumericValuesStayKeyword() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/v=1.2.3/f.parquet"),
            entry("s3://bucket/v=a..b/f.parquet"),
            entry("s3://bucket/v=../f.parquet"),
            entry("s3://bucket/v=.../f.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertFalse(result.isEmpty());
        assertEquals(DataType.KEYWORD, result.partitionColumns().get("v"));
        assertEquals("1.2.3", value(result, "s3://bucket/v=1.2.3/f.parquet", "v"));
        assertEquals("a..b", value(result, "s3://bucket/v=a..b/f.parquet", "v"));
        assertEquals("..", value(result, "s3://bucket/v=../f.parquet", "v"));
        assertEquals("...", value(result, "s3://bucket/v=.../f.parquet", "v"));
    }

    /** {@code 1.} and {@code .5} are doubles, not rejected for the trailing or leading dot. */
    public void testLeadingOrTrailingDotInfersDouble() {
        List<StorageEntry> files = List.of(entry("s3://bucket/n=1./f1.parquet"), entry("s3://bucket/n=.5/f2.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertEquals(DataType.DOUBLE, result.partitionColumns().get("n"));
        assertEquals(1.0, value(result, "s3://bucket/n=1./f1.parquet", "n"));
        assertEquals(0.5, value(result, "s3://bucket/n=.5/f2.parquet", "n"));
    }

    /** A dot in the key binds nothing. The value may contain dots; the key may not. */
    public void testDottedKeysBindNothing() {
        for (String folder : List.of("a.b=c", "a..b=c", ".hidden=x", "a.=x")) {
            PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(
                List.of(entry("s3://bucket/" + folder + "/f.parquet")),
                WarningSinks.FAILING
            );
            assertTrue(folder, result.isEmpty());
        }
    }

    /**
     * {@code _partition._index=bar} is a dotted key and is skipped. {@code _index=foo} still surfaces, renamed.
     * Allowing any dot would detect both keys and bail to empty.
     */
    public void testDottedKeySkippedBesideReservedRename() {
        List<StorageEntry> files = List.of(entry("s3://bucket/_index=foo/_partition._index=bar/f.parquet"));
        List<String> sink = new ArrayList<>();

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, sink::add);

        assertFalse(result.isEmpty());
        assertEquals(Set.of("_partition._index"), result.partitionColumns().keySet());
        assertEquals("foo", value(result, "s3://bucket/_index=foo/_partition._index=bar/f.parquet", "_partition._index"));
    }

    /** {@code k=} is {@code ""}. The Hive null sentinel stays null. They are not the same value. */
    public void testEmptyValueBesideHiveDefaultPartition() {
        List<StorageEntry> files = List.of(
            entry("s3://bucket/k=/f1.parquet"),
            entry("s3://bucket/k=__HIVE_DEFAULT_PARTITION__/f2.parquet")
        );

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertEquals("", value(result, "s3://bucket/k=/f1.parquet", "k"));
        assertNull(value(result, "s3://bucket/k=__HIVE_DEFAULT_PARTITION__/f2.parquet", "k"));
    }

    /** An empty string is not skipped by integral inference, so {@code k=} beside {@code k=2024} is keyword. */
    public void testEmptyValueBesideIntegerInfersKeyword() {
        List<StorageEntry> files = List.of(entry("s3://bucket/k=/f1.parquet"), entry("s3://bucket/k=2024/f2.parquet"));

        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(files, WarningSinks.FAILING);

        assertEquals(DataType.KEYWORD, result.partitionColumns().get("k"));
        assertEquals("", value(result, "s3://bucket/k=/f1.parquet", "k"));
        assertEquals("2024", value(result, "s3://bucket/k=2024/f2.parquet", "k"));
    }

    /** A raw second {@code =} is not a value. Writers percent-escape it. */
    public void testSecondEqualsBindsNothing() {
        assertTrue(HivePartitionDetector.INSTANCE.detect(List.of(entry("s3://bucket/tag=a=b/f.parquet")), WarningSinks.FAILING).isEmpty());
        assertTrue(HivePartitionDetector.INSTANCE.detect(List.of(entry("s3://bucket/tag==b/f.parquet")), WarningSinks.FAILING).isEmpty());
    }

    /** {@code %3D} decodes to {@code =}. {@code %2E} decodes to {@code .} and types as double. */
    public void testPercentEncodedEqualsAndDot() {
        PartitionMetadata equals = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/tag=a%3Db/f.parquet")),
            WarningSinks.FAILING
        );
        assertEquals("a=b", value(equals, "s3://bucket/tag=a%3Db/f.parquet", "tag"));

        PartitionMetadata dot = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/price=1%2E5/f.parquet")),
            WarningSinks.FAILING
        );
        assertEquals(DataType.DOUBLE, dot.partitionColumns().get("price"));
        assertEquals(1.5, value(dot, "s3://bucket/price=1%2E5/f.parquet", "price"));
    }

    /** Keys are not percent-decoded. {@code a%2Eb} is the column name, not {@code a.b}. */
    public void testPercentEncodedDotInKeyStaysRaw() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/a%2Eb=c/f.parquet")),
            WarningSinks.FAILING
        );

        assertEquals(Set.of("a%2Eb"), result.partitionColumns().keySet());
        assertEquals("c", value(result, "s3://bucket/a%2Eb=c/f.parquet", "a%2Eb"));
    }

    /** A trailing {@code %} is a malformed escape and stays the raw keyword. */
    public void testTrailingPercentStaysRaw() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/tag=100%/f.parquet")),
            WarningSinks.FAILING
        );

        assertEquals(DataType.KEYWORD, result.partitionColumns().get("tag"));
        assertEquals("100%", value(result, "s3://bucket/tag=100%/f.parquet", "tag"));
    }

    /** {@code month=06} is the object name, so only {@code year} binds. */
    public void testExtensionlessObjectNameIsNotAPartition() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/year=2024/month=06")),
            WarningSinks.FAILING
        );

        assertFalse(result.isEmpty());
        assertEquals(Set.of("year"), result.partitionColumns().keySet());
        assertEquals(2024, value(result, "s3://bucket/year=2024/month=06", "year"));
    }

    /** The first {@code year=} wins. The later folder is the same key and is ignored. */
    public void testDuplicateKeyFirstWins() {
        PartitionMetadata result = HivePartitionDetector.INSTANCE.detect(
            List.of(entry("s3://bucket/year=2024/year=2025/f.parquet")),
            WarningSinks.FAILING
        );

        assertEquals(Set.of("year"), result.partitionColumns().keySet());
        assertEquals(2024, value(result, "s3://bucket/year=2024/year=2025/f.parquet", "year"));
    }

    /** Empty pieces from {@code //} drop. The object name drops, including when a trailing slash follows it. */
    public void testDirectorySegmentsDropsObjectNameAndEmptyPieces() {
        assertEquals(List.of("data", "year=2024"), HivePartitionDetector.directorySegments("/data/year=2024/file.parquet"));
        assertEquals(List.of("data", "year=2024"), HivePartitionDetector.directorySegments("/data//year=2024/file.parquet"));
        assertEquals(List.of("data", "year=2024"), HivePartitionDetector.directorySegments("/data/year=2024/file.parquet/"));
    }

    private static Object value(PartitionMetadata result, String path, String column) {
        return result.filePartitionValues().get(StoragePath.of(path)).get(column);
    }

}
