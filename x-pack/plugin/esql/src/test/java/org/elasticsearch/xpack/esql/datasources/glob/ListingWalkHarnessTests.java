/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.DAY_SPEC;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.FILES_PER_DAY;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.HIVE_ON;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.LIST_PAGE_SIZE;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.LOGS_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.MIXED_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.NOW;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.PADDED_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.expand;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.fieldShapedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.folderRange;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.listingHints;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.mixedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.paddedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.pathsOf;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.reference;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.temporalHints;

/**
 * Named-tree harness: listing must match a flat prefix listing plus {@code PartitionValueMatcher} and
 * {@code PartitionSpec.overlaps}. LIST pins document today's request counts; do not "fix" them here.
 *
 * <p>Documented holes (flipped by later PRs):
 * <ul>
 *   <li>Mixed-tree {@code month == 6} returns only {@code month=6} (PR 4). {@code 006} is another spelling of 6;
 *       {@code 007} is integer 7. Boolean/encoded-string splice holes stay out of scope for D.</li>
 * </ul>
 */
public class ListingWalkHarnessTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final int MAX = Integer.MAX_VALUE;
    /**
     * Between unpruned 61×8=488 and one-sided Oct 14–31 18×8=144. Cross-month 7 days is 56.
     */
    private static final int LOWERED_CAP = 200;

    private static final Instant LAST_DAY = Instant.parse("2026-10-14T00:00:00Z");
    private static final Instant DAYS_3 = Instant.parse("2026-10-12T00:00:00Z");
    private static final Instant DAYS_6 = Instant.parse("2026-10-09T00:00:00Z");
    private static final Instant DAYS_7_IN_MONTH = Instant.parse("2026-10-08T00:00:00Z");
    private static final Instant DAYS_14 = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant OCT_START = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant NOV_START = Instant.parse("2026-11-01T00:00:00Z");
    private static final Instant CROSS_START = Instant.parse("2026-09-28T00:00:00Z");
    private static final Instant CROSS_END = Instant.parse("2026-10-05T00:00:00Z");

    public void testListObjectsPagesAtOneThousand() throws IOException {
        List<StorageEntry> files = new ArrayList<>(2500);
        for (int i = 0; i < 2500; i++) {
            files.add(ListingWalkHarness.entry(String.format(Locale.ROOT, "s3://bucket/wide/part-%04d.parquet", i)));
        }
        ListingWalkHarness provider = new ListingWalkHarness(files);
        provider.childrenUnsupported = true;
        FileList result = expand(provider, "s3://bucket/wide/*.parquet", null, MAX);
        assertEquals(2500, result.fileCount());
        assertEquals(3, provider.listObjectPages.get());
        assertEquals(0, provider.listChildrenRequests.get());
        assertEquals(3, provider.requestCount());
        assertEquals(LIST_PAGE_SIZE, 1000);
    }

    public void testChildrenUnsupportedForcesOnePrefixList() throws IOException {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        provider.childrenUnsupported = true;
        FileList result = expand(provider, LOGS_GLOB, listingHints(List.of(window(LAST_DAY, NOW)), DAY_SPEC), MAX);
        assertEquals(FILES_PER_DAY, result.fileCount());
        assertEquals(0, provider.listChildrenRequests.get());
        assertEquals(1, provider.listObjectPages.get());
    }

    public void testPathBindingsFirstWinsOnRepeatedKey() {
        StoragePath path = StoragePath.of("s3://bucket/logs/year=2026/year=1999/f.parquet");
        Map<String, String> bindings = ListingWalkHarness.pathBindings(path);
        assertEquals("2026", bindings.get("year"));
        assertEquals(Set.of("year"), bindings.keySet());
    }

    public void testUnfilteredFieldTreeMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(), DAY_SPEC);
    }

    public void testLastDayMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(LAST_DAY, NOW)), DAY_SPEC);
    }

    public void testThreeDaysMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(DAYS_3, NOW)), DAY_SPEC);
    }

    public void testSixDaysMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(DAYS_6, NOW)), DAY_SPEC);
    }

    public void testInMonthSevenDaysMatchesReference() throws IOException {
        // Grain INs already pick the 7 October days; the 4/0.2 finish rule may still flatten (PR 2).
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(DAYS_7_IN_MONTH, NOW)), DAY_SPEC);
    }

    public void testFourteenDaysMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(DAYS_14, NOW)), DAY_SPEC);
    }

    public void testCalendarMonthMatchesReference() throws IOException {
        assertWalkAndFlatMatch(fieldShapedTree(), LOGS_GLOB, List.of(window(OCT_START, NOV_START)), DAY_SPEC);
    }

    /**
     * Grain INs are a cross-product (14 day folders); {@link FolderRangePredicate} keeps the 7 that overlap.
     */
    public void testCrossMonthSevenDaysKeepsSevenFolders() throws IOException {
        List<StorageEntry> tree = fieldShapedTree();
        List<Expression> filters = List.of(window(CROSS_START, CROSS_END));
        assertWalkAndFlatMatch(tree, LOGS_GLOB, filters, DAY_SPEC);
        ListingWalkHarness.Reference ref = reference(
            tree,
            LOGS_GLOB,
            listingHints(filters, DAY_SPEC),
            DAY_SPEC,
            temporalHints(filters, DAY_SPEC),
            HIVE_ON
        );
        assertEquals("overlaps keeps 7 days × " + FILES_PER_DAY, 7 * FILES_PER_DAY, ref.paths().size());
        assertFalse(containsPath(ref.paths(), "/month=9/day=1/"));
        assertFalse(containsPath(ref.paths(), "/month=10/day=5/"));

        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.childrenUnsupported = true;
        FileList flattened = expand(flat, LOGS_GLOB, listingHints(filters, DAY_SPEC), LOWERED_CAP, folderRange(filters, DAY_SPEC));
        assertEquals(7 * FILES_PER_DAY, flattened.fileCount());
        assertFalse(containsPath(pathsOf(flattened), "/month=9/day=1/"));
        assertFalse(containsPath(pathsOf(flattened), "/month=10/day=5/"));
    }

    public void testPaddedMonthEqualsSixMatchesIsolatedMatcher() throws IOException {
        List<Expression> filters = List.of(monthEquals(6));
        assertWalkAndFlatMatch(paddedTree(), PADDED_GLOB, filters, PartitionSpec.EMPTY);
    }

    /**
     * Unpadded {@code month=6} exists, so the splice hits and padded siblings are not listed. PR 4 leaves
     * {@code month=*} and the matcher keeps {@code 6}/{@code 06}/{@code 006}.
     */
    public void testMixedTreeMonthEqualsSixReturnsOnlyUnpaddedSpelling() throws IOException {
        List<StorageEntry> tree = mixedTree();
        List<PartitionFilterHint> hints = List.of(new PartitionFilterHint("month", Operator.EQUALS, List.of(6)));
        ListingWalkHarness.Reference ref = reference(tree, MIXED_GLOB, hints, PartitionSpec.EMPTY, List.of(), HIVE_ON);
        assertEquals("matcher keeps 6, 06, and 006", 3 * FILES_PER_DAY, ref.paths().size());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        assertSplicedUnpaddedOnly(expand(walk, MIXED_GLOB, hints, MAX));
        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.childrenUnsupported = true;
        assertSplicedUnpaddedOnly(expand(flat, MIXED_GLOB, hints, MAX));
    }

    private static void assertSplicedUnpaddedOnly(FileList result) {
        assertEquals(FILES_PER_DAY, result.fileCount());
        assertFalse(result.isInferenceAnchor());
        for (String path : pathsOf(result)) {
            assertTrue(path, path.contains("/month=6/"));
            assertFalse(path, path.contains("/month=06/"));
            assertFalse(path, path.contains("/month=006/"));
        }
    }

    /**
     * One-sided {@code @timestamp >= X} emits no grain IN. {@link FolderRangePredicate} prunes before the cap
     * on the walk and the forced-flat path.
     */
    public void testOneSidedTimestampStaysUnderLoweredCapOnWalkAndFlat() throws IOException {
        List<StorageEntry> tree = fieldShapedTree();
        List<Expression> filters = List.of(gte(LAST_DAY));
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        assertTrue("one-sided bound must not join listing hints", hints.isEmpty());
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        assertNotNull(pred);
        ListingWalkHarness.Reference ref = reference(tree, LOGS_GLOB, hints, DAY_SPEC, temporalHints(filters, DAY_SPEC), HIVE_ON);
        assertEquals("overlaps keeps Oct 14–31", 18 * FILES_PER_DAY, ref.paths().size());
        assertTrue(containsPath(ref.paths(), "/month=10/day=14/"));
        assertFalse(containsPath(ref.paths(), "/month=9/"));

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, LOGS_GLOB, hints, LOWERED_CAP, pred);
        assertObserved(walked, ref);

        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.childrenUnsupported = true;
        FileList flattened = expand(flat, LOGS_GLOB, hints, LOWERED_CAP, pred);
        assertObserved(flattened, ref);
        assertEquals(pathsOf(walked), pathsOf(flattened));
    }

    public void testYearEquals2099IsInferenceAnchor() throws IOException {
        List<PartitionFilterHint> hints = List.of(new PartitionFilterHint("year", Operator.EQUALS, List.of(2099)));
        ListingWalkHarness.Reference ref = reference(fieldShapedTree(), LOGS_GLOB, hints, PartitionSpec.EMPTY, List.of(), HIVE_ON);
        assertTrue(ref.inferenceAnchor());
        assertTrue(ref.paths().isEmpty());

        ListingWalkHarness walk = new ListingWalkHarness(fieldShapedTree());
        FileList walked = expand(walk, LOGS_GLOB, hints, MAX);
        assertTrue(walked.isInferenceAnchor());
        assertEquals(1, walked.fileCount());
        assertEquals(List.of(), walked.listingWarnings());

        ListingWalkHarness flat = new ListingWalkHarness(fieldShapedTree());
        flat.childrenUnsupported = true;
        FileList flattened = expand(flat, LOGS_GLOB, hints, MAX);
        assertTrue(flattened.isInferenceAnchor());
        assertEquals(1, flattened.fileCount());
        assertEquals(List.of(), flattened.listingWarnings());
        assertEquals(0, ref.excludedByNameFilter());
    }

    // --- LIST pins: today's request counts. Do not "fix" them in this PR. ---

    public void testListPinLastDay() throws IOException {
        assertListPin(window(LAST_DAY, NOW), 5, FILES_PER_DAY);
    }

    public void testListPinThreeDays() throws IOException {
        assertListPin(window(DAYS_3, NOW), 7, 3 * FILES_PER_DAY);
    }

    public void testListPinSixDays() throws IOException {
        assertListPin(window(DAYS_6, NOW), 10, 6 * FILES_PER_DAY);
    }

    public void testListPinSevenDaysInMonth() throws IOException {
        assertListPin(window(DAYS_7_IN_MONTH, NOW), 5, 7 * FILES_PER_DAY);
    }

    public void testListPinFourteenDays() throws IOException {
        assertListPin(window(DAYS_14, NOW), 5, 14 * FILES_PER_DAY);
    }

    public void testListPinCalendarMonth() throws IOException {
        assertListPin(window(OCT_START, NOV_START), 5, 31 * FILES_PER_DAY);
    }

    public void testListPinSevenDayCrossMonth() throws IOException {
        assertListPin(window(CROSS_START, CROSS_END), 5, 7 * FILES_PER_DAY);
    }

    private static void assertListPin(Expression filter, int expectedRequests, int expectedFiles) throws IOException {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        List<Expression> filters = List.of(filter);
        FileList result = expand(provider, LOGS_GLOB, listingHints(filters, DAY_SPEC), MAX, folderRange(filters, DAY_SPEC));
        assertEquals("LIST pin (today's number; do not 'fix' here)", expectedRequests, provider.requestCount());
        assertEquals(expectedFiles, result.fileCount());
    }

    private static void assertWalkAndFlatMatch(List<StorageEntry> tree, String glob, List<Expression> filters, PartitionSpec spec)
        throws IOException {
        List<PartitionFilterHint> hints = listingHints(filters, spec);
        List<PartitionFilterHint> temporal = temporalHints(filters, spec);
        FolderRangePredicate pred = folderRange(filters, spec);
        ListingWalkHarness.Reference ref = reference(tree, glob, hints, spec, temporal, HIVE_ON);

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, glob, hints, MAX, pred);
        assertObserved(walked, ref);

        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.childrenUnsupported = true;
        FileList flattened = expand(flat, glob, hints, MAX, pred);
        assertObserved(flattened, ref);
        assertEquals("walk and forced-flat must keep the same files", pathsOf(walked), pathsOf(flattened));
    }

    private static boolean containsPath(List<String> paths, String fragment) {
        for (String path : paths) {
            if (path.contains(fragment)) {
                return true;
            }
        }
        return false;
    }

    private static void assertObserved(FileList result, ListingWalkHarness.Reference ref) {
        assertEquals(ref.inferenceAnchor(), result.isInferenceAnchor());
        if (result.isInferenceAnchor()) {
            assertEquals(1, result.fileCount());
        } else {
            assertEquals(ref.paths(), pathsOf(result));
            Set<String> columns = result.partitionMetadata() == null ? Set.of() : result.partitionMetadata().partitionColumns().keySet();
            assertEquals(ref.partitionColumns(), columns);
        }
        assertEquals(ref.notices(), result.listingWarnings());
        assertEquals(0, ref.excludedByNameFilter());
        assertFalse(result.isTruncated());
    }

    private static Expression window(Instant start, Instant end) {
        return new And(SRC, gte(start), lt(end));
    }

    private static Expression gte(Instant start) {
        return new GreaterThanOrEqual(SRC, timestamp(), new Literal(SRC, start.toEpochMilli(), DataType.DATETIME));
    }

    private static Expression lt(Instant end) {
        return new LessThan(SRC, timestamp(), new Literal(SRC, end.toEpochMilli(), DataType.DATETIME));
    }

    private static Expression monthEquals(int month) {
        return new Equals(
            SRC,
            new FieldAttribute(SRC, "month", new EsField("month", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new Literal(SRC, month, DataType.INTEGER)
        );
    }

    private static FieldAttribute timestamp() {
        return new FieldAttribute(
            SRC,
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
