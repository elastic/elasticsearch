/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ReadConfigFingerprint;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * The one-file-at-a-time fold must match {@link ExternalSourceResolver#aggregateFileStatistics} on the same
 * harvests, including when completions arrive out of listing order. The batch fold is the oracle.
 */
public class RunningFileStatsFoldTests extends ESTestCase {

    public void testHomogeneousFilesMatchBatchFold() {
        assertReconciliationMatches(
            List.of(file("file:///a.parquet", DataType.LONG, 1L, 4L, 3L), file("file:///b.parquet", DataType.LONG, 2L, 9L, 5L)),
            true,
            Map.of(),
            false
        );
    }

    public void testDatetimeWidensToNanosInEitherCompletionOrder() {
        assertReconciliationMatches(List.of(datetimeFile("file:///a.parquet"), nanosFile("file:///b.parquet")), true, Map.of(), false);
        assertReconciliationMatches(List.of(nanosFile("file:///a.parquet"), datetimeFile("file:///b.parquet")), true, Map.of(), false);
    }

    public void testIntegerWidenedToDoubleMatchesBatchFold() {
        assertReconciliationMatches(
            List.of(file("file:///a.parquet", DataType.INTEGER, 1L, 2L, 2L), doubleFile("file:///b.parquet", 0.5d, 4.5d)),
            true,
            Map.of(),
            false
        );
    }

    public void testAbsentColumnMatchesBatchFoldForBothNullPolicies() {
        SourceMetadata withBoth = file(
            "file:///a.parquet",
            List.of(attr("x", DataType.LONG), attr("y", DataType.LONG)),
            stats(3L, Map.of("x", col(1L, 3L, 3L, 0L), "y", col(10L, 12L, 3L, 0L)))
        );
        SourceMetadata onlyX = file("file:///b.parquet", List.of(attr("x", DataType.LONG)), stats(2L, Map.of("x", col(4L, 8L, 2L, 0L))));
        assertReconciliationMatches(List.of(withBoth, onlyX), true, Map.of(), false);
        assertReconciliationMatches(List.of(withBoth, onlyX), false, Map.of(), false);
    }

    public void testDuplicateListingPathFoldsOncePerPosition() {
        SourceMetadata file = file("file:///a.parquet", DataType.LONG, 1L, 4L, 10L);
        assertReconciliationMatches(List.of(file, file), true, Map.of(), false);
    }

    public void testOneFileMissingStatsNullsTheAggregate() {
        SourceMetadata present = file("file:///a.parquet", DataType.LONG, 1L, 4L, 3L);
        SourceMetadata missing = new SimpleSourceMetadata(
            List.of(attr("x", DataType.LONG)),
            "parquet",
            "file:///b.parquet",
            null,
            null,
            null,
            null
        );
        assertReconciliationMatches(List.of(present, missing), true, Map.of(), false);
        assertReconciliationMatches(List.of(missing, present), true, Map.of(), false);
    }

    public void testPinnedColumnMatchesOverlayThenMerge() {
        StoragePath pathA = StoragePath.of("file:///a.csv");
        StoragePath pathB = StoragePath.of("file:///b.csv");
        SourceMetadata a = file(
            pathA.toString(),
            List.of(attr("val", DataType.LONG), attr("id", DataType.LONG)),
            stats(3L, Map.of("val", col(1L, 20L, 3L, 0L), "id", col(1L, 3L, 3L, 0L)))
        );
        SourceMetadata b = file(
            pathB.toString(),
            List.of(attr("val", DataType.LONG), attr("id", DataType.LONG)),
            stats(2L, Map.of("val", col(-5L, 100L, 2L, 0L), "id", col(4L, 5L, 2L, 0L)))
        );
        assertReconciliationMatches(List.of(a, b), false, Map.of(pathA, Set.of("val")), false);
        assertReconciliationMatches(List.of(a, b), false, Map.of(pathA, Set.of("val")), true);
    }

    public void testReadConfigIdentityMatchesBatchFold() {
        SourceMetadata a = withIdentity(file("file:///a.parquet", DataType.LONG, 1L, 2L, 2L), "fp-1", true);
        SourceMetadata b = withIdentity(file("file:///b.parquet", DataType.LONG, 3L, 4L, 2L), "fp-1", true);
        SourceMetadata mixed = withIdentity(file("file:///c.parquet", DataType.LONG, 5L, 6L, 2L), "fp-2", false);
        assertReconciliationMatches(List.of(a, b), true, Map.of(), false);
        assertReconciliationMatches(List.of(a, mixed), true, Map.of(), false);
    }

    public void testFirstFileWinsAnchorRewriteMatchesBatchFold() {
        assertFirstFileWinsMatches(
            List.of(
                typed("file:///1.parquet", DataType.DATETIME, DataType.LONG, datetimeAndId(2L, 1000L, 5000L, 1L, 9L)),
                typed("file:///2.parquet", DataType.DATE_NANOS, DataType.LONG, datetimeAndId(2L, 2_000_000L, 9_000_000L, 3L, 7L))
            ),
            true,
            Set.of()
        );
        assertFirstFileWinsMatches(
            List.of(
                file("file:///part-a.parquet", DataType.INTEGER, 1L, 2L, 2L),
                file("file:///part-b.parquet", DataType.LONG, -10L, 20L, 2L)
            ),
            true,
            Set.of()
        );
        assertFirstFileWinsMatches(
            List.of(
                file("file:///part-a.parquet", DataType.INTEGER, 1L, 2L, 2L),
                file("file:///part-b.parquet", DataType.LONG, -10L, 20L, 2L)
            ),
            true,
            Set.of("x")
        );
        assertFirstFileWinsMatches(
            List.of(file("file:///part-a.csv", DataType.INTEGER, 1L, 2L, 2L), file("file:///part-b.csv", DataType.LONG, -10L, 20L, 2L)),
            false,
            Set.of()
        );
        long encoded0 = DeclaredTypeCoercions.coerceToUnsignedLong(0L);
        long encoded200 = DeclaredTypeCoercions.coerceToUnsignedLong(200L);
        assertFirstFileWinsMatches(
            List.of(
                file("file:///part-a.parquet", DataType.UNSIGNED_LONG, encoded0, encoded200, 2L),
                file("file:///part-b.parquet", DataType.LONG, 1L, 50L, 2L)
            ),
            true,
            Set.of()
        );
    }

    public void testSlimDropsColumnStatisticsAndKeepsFileLevelCounts() {
        Map<String, Object> harvest = stats(4L, Map.of("x", col(1L, 9L, 4L, 0L)));
        harvest.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 80L);
        harvest.put(SourceStatisticsSerializer.STATS_READABLE_UNIT_COUNT, 1L);
        harvest.put(ExternalStats.READ_CONFIG_FINGERPRINT_KEY, "fp");
        harvest.put("custom", "kept");
        SourceMetadata meta = file("file:///a.parquet", List.of(attr("x", DataType.LONG)), harvest).withWarnings(List.of("notice"));

        SourceMetadata slim = RunningFileStatsFold.slim(meta);

        assertEquals(meta.schema(), slim.schema());
        assertEquals(meta.location(), slim.location());
        assertEquals(List.of("notice"), slim.warnings());
        Map<String, Object> kept = slim.sourceMetadata();
        assertEquals(4L, kept.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(80L, kept.get(SourceStatisticsSerializer.STATS_SIZE_BYTES));
        assertEquals(1L, kept.get(SourceStatisticsSerializer.STATS_READABLE_UNIT_COUNT));
        assertEquals("fp", kept.get(ExternalStats.READ_CONFIG_FINGERPRINT_KEY));
        assertEquals("kept", kept.get("custom"));
        assertThat(kept, not(hasKey(SourceStatisticsSerializer.columnMinKey("x"))));
        assertThat(kept, not(hasKey(SourceStatisticsSerializer.columnMaxKey("x"))));
        assertThat(kept, not(hasKey(SourceStatisticsSerializer.columnValueCountKey("x"))));
        assertThat(kept, not(hasKey(SourceStatisticsSerializer.columnNullCountKey("x"))));
    }

    public void testAgreedFingerprintSurvivesTheFold() {
        SourceMetadata a = withIdentity(file("file:///a.parquet", DataType.LONG, 1L, 2L, 2L), "fp-1", true);
        SourceMetadata b = withIdentity(file("file:///b.parquet", DataType.LONG, 3L, 4L, 2L), "fp-1", true);
        Map<String, Object> folded = foldReconciliation(List.of(a, b), true, identity(2));
        assertEquals("fp-1", folded.get(ExternalStats.READ_CONFIG_FINGERPRINT_KEY));
        assertEquals(Boolean.TRUE, folded.get(ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY));

        SourceMetadata mixed = withIdentity(file("file:///c.parquet", DataType.LONG, 5L, 6L, 2L), "fp-2", false);
        Map<String, Object> disagreed = foldReconciliation(List.of(a, mixed), true, identity(2));
        assertEquals(ReadConfigFingerprint.MIXED, disagreed.get(ExternalStats.READ_CONFIG_FINGERPRINT_KEY));
        assertNull(disagreed.get(ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY));
    }

    private void assertReconciliationMatches(
        List<SourceMetadata> files,
        boolean implicitNulls,
        Map<StoragePath, Set<String>> pins,
        boolean dropPinnedRowCount
    ) {
        Map<String, Object> expected = batchReconciliation(files, implicitNulls, pins, dropPinnedRowCount);
        for (int[] order : orders(files.size())) {
            Map<String, Object> actual = RunningFileStatsFold.applyPinnedColumns(
                foldReconciliation(files, implicitNulls, order),
                pins,
                dropPinnedRowCount
            );
            assertEquals("completion order " + java.util.Arrays.toString(order), expected, actual);
        }
    }

    private void assertFirstFileWinsMatches(List<SourceMetadata> files, boolean implicitNulls, Set<String> declared) {
        Map<String, Object> expected = ExternalSourceResolver.aggregateFileStatistics(files, implicitNulls, declared);
        Map<String, DataType> anchorTypes = ExternalSourceResolver.attributesToTypeMap(files.get(0).schema());
        for (int[] order : orders(files.size())) {
            RunningFileStatsFold fold = RunningFileStatsFold.firstFileWins(anchorTypes, implicitNulls, declared);
            for (int index : order) {
                fold.accept(index, files.get(index));
            }
            assertEquals("completion order " + java.util.Arrays.toString(order), expected, fold.finish());
        }
    }

    private static Map<String, Object> foldReconciliation(List<SourceMetadata> files, boolean implicitNulls, int[] order) {
        RunningFileStatsFold fold = RunningFileStatsFold.reconciliation(implicitNulls);
        for (int index : order) {
            fold.accept(index, files.get(index));
        }
        return fold.finish();
    }

    private static Map<String, Object> batchReconciliation(
        List<SourceMetadata> files,
        boolean implicitNulls,
        Map<StoragePath, Set<String>> pins,
        boolean dropPinnedRowCount
    ) {
        Map<StoragePath, SourceMetadata> byPath = new LinkedHashMap<>();
        Map<StoragePath, Map<String, DataType>> perFileTypes = new LinkedHashMap<>();
        Map<String, DataType> reconciled = new HashMap<>();
        StoragePath[] paths = new StoragePath[files.size()];
        for (int i = 0; i < files.size(); i++) {
            SourceMetadata meta = files.get(i);
            StoragePath path = StoragePath.of(meta.location());
            paths[i] = path;
            byPath.put(path, meta);
            Map<String, DataType> fileTypes = ExternalSourceResolver.attributesToTypeMap(meta.schema());
            perFileTypes.put(path, fileTypes);
            for (Map.Entry<String, DataType> entry : fileTypes.entrySet()) {
                DataType prior = reconciled.get(entry.getKey());
                reconciled.put(entry.getKey(), prior == null ? entry.getValue() : TypeWidening.join(prior, entry.getValue()));
            }
        }
        return ExternalSourceResolver.aggregateFileStatistics(
            listingOf(paths),
            byPath,
            perFileTypes,
            reconciled,
            pins,
            dropPinnedRowCount,
            implicitNulls
        );
    }

    /** Listing order, reverse, and shuffled completions. Index 0 stays the FIRST_FILE_WINS anchor. */
    private List<int[]> orders(int size) {
        int[] identity = identity(size);
        int[] reverse = new int[size];
        for (int i = 0; i < size; i++) {
            reverse[i] = size - 1 - i;
        }
        List<int[]> orders = new ArrayList<>();
        orders.add(identity);
        orders.add(reverse);
        for (int n = 0; n < 3; n++) {
            int[] shuffled = identity.clone();
            for (int i = size - 1; i > 0; i--) {
                int j = randomIntBetween(0, i);
                int tmp = shuffled[i];
                shuffled[i] = shuffled[j];
                shuffled[j] = tmp;
            }
            orders.add(shuffled);
        }
        return orders;
    }

    private static int[] identity(int size) {
        int[] idx = new int[size];
        for (int i = 0; i < size; i++) {
            idx[i] = i;
        }
        return idx;
    }

    private static SimpleSourceMetadata file(String location, DataType type, long min, long max, long rows) {
        return file(location, List.of(attr("x", type)), stats(rows, Map.of("x", col(min, max, rows, 0L))));
    }

    private static SourceMetadata doubleFile(String location, double min, double max) {
        Map<String, Object> harvest = new HashMap<>();
        harvest.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 2L);
        harvest.put(SourceStatisticsSerializer.columnMinKey("x"), min);
        harvest.put(SourceStatisticsSerializer.columnMaxKey("x"), max);
        harvest.put(SourceStatisticsSerializer.columnValueCountKey("x"), 2L);
        harvest.put(SourceStatisticsSerializer.columnNullCountKey("x"), 0L);
        return file(location, List.of(attr("x", DataType.DOUBLE)), harvest);
    }

    private static SourceMetadata datetimeFile(String location) {
        return file(location, List.of(attr("ts", DataType.DATETIME)), stats(2L, Map.of("ts", col(1_000L, 5_000L, 2L, 0L))));
    }

    private static SourceMetadata nanosFile(String location) {
        return file(
            location,
            List.of(attr("ts", DataType.DATE_NANOS)),
            stats(2L, Map.of("ts", col(2_000_000_000L, 9_000_000_000L, 2L, 0L)))
        );
    }

    private static SourceMetadata typed(String location, DataType ts, DataType id, Map<String, Object> harvest) {
        return file(location, List.of(attr("ts", ts), attr("id", id)), harvest);
    }

    private static Map<String, Object> datetimeAndId(long rows, long tsMin, long tsMax, long idMin, long idMax) {
        return stats(rows, Map.of("ts", col(tsMin, tsMax, rows, 0L), "id", col(idMin, idMax, rows, 0L)));
    }

    private static SimpleSourceMetadata file(String location, List<Attribute> schema, Map<String, Object> harvest) {
        int dot = location.lastIndexOf('.');
        String sourceType = location.substring(dot + 1);
        return new SimpleSourceMetadata(schema, sourceType, location, null, null, harvest, null);
    }

    private static SourceMetadata withIdentity(SourceMetadata meta, String fingerprint, boolean licensed) {
        Map<String, Object> harvest = new HashMap<>(meta.sourceMetadata());
        harvest.put(ExternalStats.READ_CONFIG_FINGERPRINT_KEY, fingerprint);
        if (licensed) {
            harvest.put(ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY, Boolean.TRUE);
        }
        return new SimpleSourceMetadata(meta.schema(), meta.sourceType(), meta.location(), null, null, harvest, null);
    }

    private static Map<String, Object> stats(long rows, Map<String, long[]> columns) {
        Map<String, Object> harvest = new HashMap<>();
        harvest.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rows);
        for (Map.Entry<String, long[]> entry : columns.entrySet()) {
            long[] c = entry.getValue();
            harvest.put(SourceStatisticsSerializer.columnMinKey(entry.getKey()), c[0]);
            harvest.put(SourceStatisticsSerializer.columnMaxKey(entry.getKey()), c[1]);
            harvest.put(SourceStatisticsSerializer.columnValueCountKey(entry.getKey()), c[2]);
            harvest.put(SourceStatisticsSerializer.columnNullCountKey(entry.getKey()), c[3]);
        }
        return harvest;
    }

    /** min, max, value count, null count. */
    private static long[] col(long min, long max, long valueCount, long nullCount) {
        return new long[] { min, max, valueCount, nullCount };
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static FileList listingOf(StoragePath... paths) {
        return new FileList() {
            @Override
            public int fileCount() {
                return paths.length;
            }

            @Override
            public StoragePath path(int i) {
                return paths[i];
            }

            @Override
            public long size(int i) {
                return 0;
            }

            @Override
            public long lastModifiedMillis(int i) {
                return 0;
            }

            @Override
            public String originalPattern() {
                return "test-listing";
            }

            @Override
            public PartitionMetadata partitionMetadata() {
                return null;
            }

            @Override
            public boolean isResolved() {
                return true;
            }

            @Override
            public boolean isEmpty() {
                return paths.length == 0;
            }

            @Override
            public long estimatedBytes() {
                return 0;
            }
        };
    }
}
