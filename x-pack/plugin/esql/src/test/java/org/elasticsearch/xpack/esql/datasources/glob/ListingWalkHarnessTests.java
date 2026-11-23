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
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
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
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.DAY_SPEC;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.FILES_PER_DAY;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.HIVE_ON;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.HOUR_SPEC;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.LIST_PAGE_SIZE;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.LOGS_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.MIXED_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.NOW;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.PADDED_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_DAY_END;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_DAY_START;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_GLOB;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_ONE_DAY_FILES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_ONE_DAY_FINISH_PAGES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_ONE_DAY_WALK_ABORT_CHILDREN;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_PREFIX;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_PREFIXES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_END;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_HOUR_FILES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.expand;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.fieldShapedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.folderRange;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.listingHints;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.mixedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.objectPages;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.paddedTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.pathsOf;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.reference;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.temporalHints;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.wideHiveSevenDayTree;
import static org.elasticsearch.xpack.esql.datasources.glob.ListingWalkHarness.wideHiveTree;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Named-tree harness: listing must match a flat prefix listing plus {@code PartitionValueMatcher},
 * {@code PartitionSpec.overlaps}, and {@code _file.*} hints. LIST pins record request counts under the
 * request-count finish rule. Key-dump replay and the 154-prefix AWS VPC Hive tree pin listing cost.
 */
public class ListingWalkHarnessTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final int MAX = Integer.MAX_VALUE;
    /**
     * Between unpruned 61×8=488 and one-sided Oct 14–31 18×8=144. Cross-month 7 days is 56.
     */
    private static final int LOWERED_CAP = 200;
    /**
     * Enough objects per day that 7 survivors beat one prefix listing: {@code 7 <= ceil(62 * 200 / 1000)}.
     */
    private static final int DENSE_FILES_PER_DAY = 200;
    private static final String LOGS_LIST_PREFIX = "s3://bucket/logs/";
    /** Fixed-depth Hive glob so a day hint can walk; {@link ListingWalkHarness#LOGS_GLOB} is globstar and skips the walk. */
    private static final String HIVE_DAY_GLOB = "s3://bucket/logs/year=*/month=*/day=*/*.parquet";

    private static final Instant LAST_DAY = Instant.parse("2026-10-14T00:00:00Z");
    private static final Instant DAYS_3 = Instant.parse("2026-10-12T00:00:00Z");
    private static final Instant DAYS_6 = Instant.parse("2026-10-09T00:00:00Z");
    private static final Instant DAYS_7_IN_MONTH = Instant.parse("2026-10-08T00:00:00Z");
    private static final Instant DAYS_14 = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant OCT_START = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant NOV_START = Instant.parse("2026-11-01T00:00:00Z");
    private static final Instant CROSS_START = Instant.parse("2026-09-28T00:00:00Z");
    private static final Instant CROSS_END = Instant.parse("2026-10-05T00:00:00Z");

    @After
    public void clearFinishMode() {
        PartitionPruningWalk.clearFinishMode();
    }

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
        assertEquals(PartitionPruningWalk.LIST_PAGE_SIZE, LIST_PAGE_SIZE);
        assertEquals(3, objectPages(2500));
        assertEquals(1, objectPages(0));
        assertEquals(1, objectPages(1));
        assertEquals(1, objectPages(1000));
        assertEquals(2, objectPages(1001));
    }

    public void testLoadKeyDumpResourceRoundTrip() throws IOException {
        List<StorageEntry> loaded = ListingWalkHarness.loadKeyDumpResource("key-dump-sample.keys");
        ListingWalkHarness provider = new ListingWalkHarness(loaded);
        assertEquals(4, provider.entries().size());
        List<String> paths = ListingWalkHarness.pathsOf(provider.entries());
        assertEquals(
            List.of(
                "s3://bucket/AWSLogs/aws-account-id=000000000001/aws-service=vpcflowlogs/"
                    + "aws-region=us-east-1/year=2026/month=06/day=01/hour=00/f-0.parquet",
                "s3://bucket/AWSLogs/aws-account-id=000000000001/aws-service=vpcflowlogs/"
                    + "aws-region=us-east-1/year=2026/month=07/day=01/hour=00/f-0.parquet",
                "s3://bucket/AWSLogs/aws-account-id=000000000002/aws-service=vpcflowlogs/"
                    + "aws-region=us-west-2/year=2026/month=06/day=01/hour=00/f-0.parquet",
                "s3://bucket/AWSLogs/aws-account-id=000000000003/aws-service=vpcflowlogs/"
                    + "aws-region=eu-west-1/year=2026/month=06/day=01/hour=00/f-0.parquet"
            ),
            paths
        );
        StorageEntry tsv = provider.entries().get(0);
        assertEquals(50L, tsv.length());
        assertEquals(Instant.parse("2026-06-01T00:00:00Z"), tsv.lastModified());
        assertEquals(100L, provider.entries().get(2).length());
        StorageEntry millis = provider.entries().get(3);
        assertEquals(10L, millis.length());
        assertEquals(Instant.ofEpochMilli(-1000L), millis.lastModified());
        expectThrows(IllegalArgumentException.class, () -> ListingWalkHarness.loadKeyDumpResource("missing.keys"));
    }

    public void testLoadKeyDumpEmptyAndCommentsOnly() throws IOException {
        assertEquals(List.of(), ListingWalkHarness.loadKeyDump(new BufferedReader(new StringReader(""))));
        assertEquals(List.of(), ListingWalkHarness.loadKeyDump(new BufferedReader(new StringReader("# comment\n\n  \n"))));
    }

    public void testLoadKeyDumpPreservesPathSpacesAndParsesMillis() throws IOException {
        List<StorageEntry> loaded = ListingWalkHarness.loadKeyDump(
            new BufferedReader(
                new StringReader("s3://bucket/a.parquet \n" + "s3://bucket/a.parquet|1|1\n" + "s3://bucket/b.parquet|2|-1000\n")
            )
        );
        ListingWalkHarness provider = new ListingWalkHarness(loaded);
        assertEquals(
            List.of("s3://bucket/a.parquet", "s3://bucket/a.parquet ", "s3://bucket/b.parquet"),
            ListingWalkHarness.pathsOf(provider.entries())
        );
        assertEquals(1L, provider.entries().get(0).length());
        assertEquals(Instant.ofEpochMilli(1L), provider.entries().get(0).lastModified());
        assertEquals(100L, provider.entries().get(1).length());
        assertEquals(Instant.ofEpochMilli(-1000L), provider.entries().get(2).lastModified());
    }

    public void testReferenceAppliesFileNameHint() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=9/day=14/keep.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=9/day=14/drop.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=9/day=15/keep.parquet")
        );
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("day", Operator.EQUALS, List.of(14)),
            new PartitionFilterHint(FileMetadataColumns.NAME, Operator.EQUALS, List.of("keep.parquet"))
        );
        ListingWalkHarness.Reference ref = reference(tree, HIVE_DAY_GLOB, hints, PartitionSpec.EMPTY, List.of(), HIVE_ON);
        assertEquals(List.of("s3://bucket/logs/year=2026/month=9/day=14/keep.parquet"), ref.paths());
        assertEquals(1, ref.excludedByFileHint());
        assertFalse(ref.inferenceAnchor());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, HIVE_DAY_GLOB, hints, MAX);
        assertObserved(walked, ref);
        assertTrue("walk lists children", walk.listChildrenRequests() > 0);
        assertFalse("walk does not fall back to the dataset root", walk.listedPrefixes().contains(LOGS_LIST_PREFIX));

        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.setChildrenUnsupported(true);
        FileList flattened = expand(flat, HIVE_DAY_GLOB, hints, MAX);
        assertObserved(flattened, ref);
        assertEquals(pathsOf(walked), pathsOf(flattened));
    }

    public void testReferenceAllFilteredFileHintStashesOneFile() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=9/day=14/f.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/year=2026/month=9/day=15/f.parquet")
        );
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("day", Operator.EQUALS, List.of(14)),
            new PartitionFilterHint(FileMetadataColumns.NAME, Operator.EQUALS, List.of("missing.parquet"))
        );
        ListingWalkHarness.Reference ref = reference(tree, HIVE_DAY_GLOB, hints, PartitionSpec.EMPTY, List.of(), HIVE_ON);
        assertEquals(List.of("s3://bucket/logs/year=2026/month=9/day=14/f.parquet"), ref.paths());
        assertFalse("file-hint stash is not a partition inference anchor", ref.inferenceAnchor());
        assertEquals(1, ref.excludedByFileHint());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, HIVE_DAY_GLOB, hints, MAX);
        assertObserved(walked, ref);
        assertFalse(walked.isInferenceAnchor());
        assertEquals(1, walked.fileCount());
        assertTrue("walk lists children", walk.listChildrenRequests() > 0);
        assertFalse("walk does not fall back to the dataset root", walk.listedPrefixes().contains(LOGS_LIST_PREFIX));

        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.setChildrenUnsupported(true);
        FileList flattened = expand(flat, HIVE_DAY_GLOB, hints, MAX);
        assertObserved(flattened, ref);
        assertEquals(pathsOf(walked), pathsOf(flattened));
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
     * Integer {@code month == 6} leaves {@code month=*}. Walk and forced-flat keep every typed spelling
     * ({@code 6}/{@code 06}/{@code 006}), matching the matcher.
     */
    public void testMixedTreeMonthEqualsSixKeepsEveryTypedSpelling() throws IOException {
        List<StorageEntry> tree = mixedTree();
        List<PartitionFilterHint> hints = List.of(new PartitionFilterHint("month", Operator.EQUALS, List.of(6)));
        ListingWalkHarness.Reference ref = reference(tree, MIXED_GLOB, hints, PartitionSpec.EMPTY, List.of(), HIVE_ON);
        assertEquals("matcher keeps 6, 06, and 006", 3 * FILES_PER_DAY, ref.paths().size());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, MIXED_GLOB, hints, MAX);
        assertObserved(walked, ref);
        ListingWalkHarness flat = new ListingWalkHarness(tree);
        flat.childrenUnsupported = true;
        FileList flattened = expand(flat, MIXED_GLOB, hints, MAX);
        assertObserved(flattened, ref);
        assertEquals(pathsOf(walked), pathsOf(flattened));
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

    // --- LIST pins: request-count finish vs flat. Sparse tree (k=8) flattens multi-day windows. ---

    public void testListPinLastDay() throws IOException {
        // S=1 always finishes. Same 5 as the 4/0.2 rule.
        assertListPin(window(LAST_DAY, NOW), 5, FILES_PER_DAY, false);
    }

    public void testListPinThreeDays() throws IOException {
        // k=8: remaining 2 pages > 1 prefix page, so flatten after measuring the first day.
        assertListPin(window(DAYS_3, NOW), 6, 3 * FILES_PER_DAY, true);
    }

    public void testListPinSixDays() throws IOException {
        assertListPin(window(DAYS_6, NOW), 6, 6 * FILES_PER_DAY, true);
    }

    public void testListPinSevenDaysInMonth() throws IOException {
        // Sparse k=8 still flattens; one wasted LIST of the first day then the prefix.
        assertListPin(window(DAYS_7_IN_MONTH, NOW), 6, 7 * FILES_PER_DAY, true);
    }

    public void testListPinFourteenDays() throws IOException {
        assertListPin(window(DAYS_14, NOW), 6, 14 * FILES_PER_DAY, true);
    }

    public void testListPinCalendarMonth() throws IOException {
        assertListPin(window(OCT_START, NOV_START), 6, 31 * FILES_PER_DAY, true);
    }

    public void testListPinSevenDayCrossMonth() throws IOException {
        assertListPin(window(CROSS_START, CROSS_END), 6, 7 * FILES_PER_DAY, true);
    }

    /**
     * 7 of 31 October days with {@link #DENSE_FILES_PER_DAY} objects: finish is cheaper than flattening
     * ~62 day-folders, so the prefix is never listObjects'd.
     */
    public void testListPinSevenDaysInMonthDoesNotFlatListDenseTree() throws IOException {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree(DENSE_FILES_PER_DAY));
        List<Expression> filters = List.of(window(DAYS_7_IN_MONTH, NOW));
        FileList result = expand(provider, LOGS_GLOB, listingHints(filters, DAY_SPEC), MAX, folderRange(filters, DAY_SPEC));
        assertEquals(7 * DENSE_FILES_PER_DAY, result.fileCount());
        assertFalse("7-day must not flat-list the prefix", provider.listedPrefixes.contains(LOGS_LIST_PREFIX));
        assertEquals("4 listChildren (root, year, peek Sep, Oct) + 7 day listings", 11, provider.requestCount());
        int dayListings = 0;
        for (String prefix : provider.listedPrefixes) {
            if (prefix.contains("/day=")) {
                dayListings++;
            }
        }
        assertEquals(7, dayListings);
    }

    public void testAlwaysFlatAlwaysFinishAndRuleKeepTheSameFiles() throws IOException {
        List<Expression> windows = List.of(window(LAST_DAY, NOW), window(DAYS_7_IN_MONTH, NOW), window(OCT_START, NOV_START));
        for (Expression filter : windows) {
            List<StorageEntry> tree = fieldShapedTree();
            List<Expression> filters = List.of(filter);
            List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
            FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
            ListingWalkHarness.Reference ref = reference(tree, LOGS_GLOB, hints, DAY_SPEC, temporalHints(filters, DAY_SPEC), HIVE_ON);

            ListingWalkHarness ruleProvider = new ListingWalkHarness(tree);
            FileList rule = expand(ruleProvider, LOGS_GLOB, hints, MAX, pred);
            assertObserved(rule, ref);

            ListingWalkHarness flatProvider = new ListingWalkHarness(tree);
            FileList flat = PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FLAT,
                () -> expand(flatProvider, LOGS_GLOB, hints, MAX, pred)
            );
            assertObserved(flat, ref);
            assertEquals(pathsOf(rule), pathsOf(flat));

            ListingWalkHarness childrenOff = new ListingWalkHarness(tree);
            childrenOff.childrenUnsupported = true;
            FileList forcedFlat = expand(childrenOff, LOGS_GLOB, hints, MAX, pred);
            assertObserved(forcedFlat, ref);
            assertEquals(pathsOf(rule), pathsOf(forcedFlat));

            ListingWalkHarness finishProvider = new ListingWalkHarness(tree);
            FileList finish = PartitionPruningWalk.withFinishMode(
                PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
                () -> expand(finishProvider, LOGS_GLOB, hints, MAX, pred)
            );
            assertObserved(finish, ref);
            assertEquals(pathsOf(rule), pathsOf(finish));
        }
        assertFinishModeChangesListShape();
    }

    /**
     * 154-prefix Hive tree, time-only 1 June filter. Folder-range only so grain-IN splice does not retry.
     * Cap 512 still aborts at {@link ListingWalkHarness#WIDE_HIVE_ONE_DAY_WALK_ABORT_CHILDREN} children, then
     * a prefix listing. Walk and forced-flat keep the same files as the oracle.
     */
    public void testWideHiveTreeOneDayWalkAbortsAt512() throws IOException {
        List<StorageEntry> tree = wideHiveTree();
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        ListingWalkHarness.Reference ref = reference(tree, WIDE_HIVE_GLOB, hints, DAY_SPEC, temporalHints(filters, DAY_SPEC), HIVE_ON);
        assertEquals(WIDE_HIVE_ONE_DAY_FILES, ref.paths().size());
        assertFalse(ref.inferenceAnchor());
        Set<String> accountRegions = new HashSet<>();
        for (String path : ref.paths()) {
            int yearAt = path.indexOf("/year=");
            assertTrue(path, yearAt > 0);
            accountRegions.add(path.substring(0, yearAt));
        }
        assertEquals(WIDE_HIVE_PREFIXES, accountRegions.size());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = expand(walk, WIDE_HIVE_GLOB, List.of(), MAX, pred, 512);
        assertObserved(walked, ref);

        ListingWalkHarness flatExpand = new ListingWalkHarness(tree);
        flatExpand.setChildrenUnsupported(true);
        FileList flattened = expand(flatExpand, WIDE_HIVE_GLOB, List.of(), MAX, pred);
        assertObserved(flattened, ref);
        assertEquals(pathsOf(walked), pathsOf(flattened));

        ListingWalkHarness rawFlat = new ListingWalkHarness(tree);
        int drained = 0;
        try (var it = rawFlat.listObjects(StoragePath.of(WIDE_HIVE_PREFIX), true)) {
            while (it.hasNext()) {
                it.next();
                drained++;
            }
        }
        assertEquals(tree.size(), drained);
        assertEquals(0, rawFlat.listChildrenRequests());
        assertEquals(objectPages(tree.size()), rawFlat.listObjectPages());
        assertEquals(WIDE_HIVE_ONE_DAY_WALK_ABORT_CHILDREN, walk.listChildrenRequests());
        assertEquals(objectPages(tree.size()), walk.listObjectPages());
        assertEquals(objectPages(tree.size()), flatExpand.listObjectPages());
        assertTrue(walk.listedPrefixes().contains(WIDE_HIVE_PREFIX + "/"));
    }

    /**
     * Same 1-day tree under the default ceiling. {@code ALWAYS_FINISH} pins complete-walk LIST: sparse {@code k=4}
     * still makes RULE flatten. Observed {@code listChildren} must match the derived day-grain formula.
     */
    public void testWideHiveTreeOneDayWalkCompletesUnderDefaultCeiling() throws IOException {
        assertEquals(879, WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES);
        assertTrue(
            "default ceiling must cover the derived 1-day walk",
            WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES < PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
        );
        List<StorageEntry> tree = wideHiveTree();
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        ListingWalkHarness.Reference ref = reference(tree, WIDE_HIVE_GLOB, hints, DAY_SPEC, temporalHints(filters, DAY_SPEC), HIVE_ON);
        assertEquals(WIDE_HIVE_ONE_DAY_FILES, ref.paths().size());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
            () -> expand(walk, WIDE_HIVE_GLOB, List.of(), MAX, pred)
        );
        assertEquals(WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES, walk.listChildrenRequests());
        // ALWAYS_FINISH lists matching day dirs; June 2 siblings can survive the folder-range grain,
        // so file count is a superset of the oracle, not an exact match.
        assertThat(walked.fileCount(), greaterThanOrEqualTo(WIDE_HIVE_ONE_DAY_FILES));
        assertThat(walk.listObjectPages(), greaterThanOrEqualTo(WIDE_HIVE_ONE_DAY_FINISH_PAGES));
        assertFalse("complete walk must not prefix-list the dataset", walk.listedPrefixes().contains(WIDE_HIVE_PREFIX + "/"));
        assertEquals(walk.listChildrenRequests() + walk.listObjectPages(), walk.requestCount());
    }

    /**
     * 7-day window at hour grain on the same 154 prefixes. Observed {@code listChildren} is 2111; the
     * default ceiling is 3000 so this walk completes. Raise the setting if a wider layout exceeds it.
     */
    public void testWideHiveTreeSevenDayWalkCompletesUnderDefaultCeiling() throws IOException {
        int sevenDayFiles = WIDE_HIVE_SEVEN_DAY_HOUR_FILES;
        assertEquals(2111, WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES);
        assertTrue(
            "hour-grain derived 7-day count must still fit the default ceiling",
            WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES < PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
        );
        List<StorageEntry> tree = wideHiveSevenDayTree();
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_SEVEN_DAY_END));
        List<PartitionFilterHint> hints = listingHints(filters, HOUR_SPEC);
        FolderRangePredicate pred = folderRange(filters, HOUR_SPEC);
        ListingWalkHarness.Reference ref = reference(tree, WIDE_HIVE_GLOB, hints, HOUR_SPEC, temporalHints(filters, HOUR_SPEC), HIVE_ON);
        assertEquals(sevenDayFiles, ref.paths().size());

        ListingWalkHarness walk = new ListingWalkHarness(tree);
        FileList walked = PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
            () -> expand(walk, WIDE_HIVE_GLOB, List.of(), MAX, pred)
        );
        assertEquals(WIDE_HIVE_SEVEN_DAY_WALK_DIRECTORIES, walk.listChildrenRequests());
        assertThat(walked.fileCount(), greaterThanOrEqualTo(sevenDayFiles));
        assertTrue(
            "observed 7-day hour-grain walk must fit the default ceiling",
            walk.listChildrenRequests() <= PartitionPruningWalk.DEFAULT_MAX_DIRECTORY_LISTINGS
        );
        assertFalse(walk.listedPrefixes().contains(WIDE_HIVE_PREFIX + "/"));
    }

    /**
     * Sparse {@code k=4} on 154 matching day dirs: RULE still flattens after a complete walk. Raising the
     * ceiling is what avoids the 46M-object prefix listing, not a new finish-vs-flat formula.
     */
    public void testWideHiveTreeOneDayRuleStillFlattensSparseLeaves() throws IOException {
        List<StorageEntry> tree = wideHiveTree();
        List<Expression> filters = List.of(window(WIDE_HIVE_DAY_START, WIDE_HIVE_DAY_END));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);
        ListingWalkHarness walk = new ListingWalkHarness(tree);
        expand(walk, WIDE_HIVE_GLOB, List.of(), MAX, pred);
        assertTrue("RULE finish-vs-flat still prefix-lists this sparse tree", walk.listedPrefixes().contains(WIDE_HIVE_PREFIX + "/"));
        assertEquals(WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES, walk.listChildrenRequests());
    }

    /** RULE flattens 7-day (k=8); ALWAYS_FINISH lists the 7 days; ALWAYS_FLAT prefix-lists without measuring. */
    private static void assertFinishModeChangesListShape() throws IOException {
        List<StorageEntry> tree = fieldShapedTree();
        List<Expression> filters = List.of(window(DAYS_7_IN_MONTH, NOW));
        List<PartitionFilterHint> hints = listingHints(filters, DAY_SPEC);
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);

        ListingWalkHarness rule = new ListingWalkHarness(tree);
        expand(rule, LOGS_GLOB, hints, MAX, pred);
        assertTrue("RULE measures then flattens", rule.listedPrefixes.contains(LOGS_LIST_PREFIX));

        ListingWalkHarness alwaysFlat = new ListingWalkHarness(tree);
        PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FLAT,
            () -> expand(alwaysFlat, LOGS_GLOB, hints, MAX, pred)
        );
        assertEquals(List.of(LOGS_LIST_PREFIX), alwaysFlat.listedPrefixes);

        ListingWalkHarness alwaysFinish = new ListingWalkHarness(tree);
        PartitionPruningWalk.withFinishMode(
            PartitionPruningWalk.FinishMode.ALWAYS_FINISH,
            () -> expand(alwaysFinish, LOGS_GLOB, hints, MAX, pred)
        );
        assertFalse(alwaysFinish.listedPrefixes.contains(LOGS_LIST_PREFIX));
        int days = 0;
        for (String prefix : alwaysFinish.listedPrefixes) {
            if (prefix.contains("/day=")) {
                days++;
            }
        }
        assertEquals(7, days);
    }

    private static void assertListPin(Expression filter, int expectedRequests, int expectedFiles, boolean flatListedPrefix)
        throws IOException {
        ListingWalkHarness provider = new ListingWalkHarness(fieldShapedTree());
        List<Expression> filters = List.of(filter);
        FileList result = expand(provider, LOGS_GLOB, listingHints(filters, DAY_SPEC), MAX, folderRange(filters, DAY_SPEC));
        assertEquals("LIST pin", expectedRequests, provider.requestCount());
        assertEquals(expectedFiles, result.fileCount());
        assertEquals(flatListedPrefix, provider.listedPrefixes.contains(LOGS_LIST_PREFIX));
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

    private static final String AWS_TEMPLATE_GLOB = "s3://bucket/AWSLogs/*/vpcflowlogs/*/*/*/*/*.log.gz";
    /** Account+region splice; withdraw-flat lists this prefix, a completed walk does not. */
    private static final String AWS_SPLICED_PREFIX = "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/";
    private static final String AWS_NOV_PREFIX = "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/";
    private static final Map<String, Object> AWS_TEMPLATE = Map.of(
        PartitionConfig.CONFIG_PARTITIONING_DETECTION,
        "template",
        PartitionConfig.CONFIG_PARTITIONING_PATH,
        "AWSLogs/{account}/vpcflowlogs/{region}/{year}/{month}/{day}"
    );

    /** Small-prefix day IN walks year/month/day after splicing account/region; LIST stays in the tens. */
    public void testTemplateAwsDayInWalksFewListings() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/19/b.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/17/c.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/12/01/d.log.gz")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        List<PartitionFilterHint> hints = List.of(
            hint("account", Operator.EQUALS, "012345678901"),
            hint("region", Operator.EQUALS, "us-east-1"),
            hint("year", Operator.EQUALS, 2026),
            hint("month", Operator.EQUALS, 11),
            hint("day", Operator.IN, 18, 19)
        );

        FileList result = expand(provider, AWS_TEMPLATE_GLOB, hints, MAX, AWS_TEMPLATE);

        assertEquals(
            List.of(
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz",
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/19/b.log.gz"
            ),
            pathsOf(result)
        );
        assertThat(provider.listChildrenRequests(), lessThanOrEqualTo(10));
        assertThat(provider.childListedPrefixes(), hasItem(AWS_NOV_PREFIX));
        assertThat(provider.listedPrefixes(), not(hasItem(AWS_SPLICED_PREFIX)));
        assertFalse(containsPath(pathsOf(result), "/11/17/"));
        assertFalse(containsPath(pathsOf(result), "/12/01/"));
    }

    /**
     * Everyday {@code WHERE day IN (…) AND srcaddr=…}: leftover pending is a data column. After the last
     * placeholder the walk finishes survivors instead of listing past the layout and withdrawing.
     */
    public void testTemplatePartitionAndDataColumnStillWalks() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/19/b.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/17/c.log.gz")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        List<PartitionFilterHint> hints = List.of(
            hint("account", Operator.EQUALS, "012345678901"),
            hint("region", Operator.EQUALS, "us-east-1"),
            hint("day", Operator.IN, 18, 19),
            hint("srcaddr", Operator.EQUALS, "10.0.0.1")
        );

        FileList result = expand(provider, AWS_TEMPLATE_GLOB, hints, MAX, AWS_TEMPLATE);

        assertEquals(
            List.of(
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz",
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/19/b.log.gz"
            ),
            pathsOf(result)
        );
        assertThat(provider.childListedPrefixes(), hasItem(AWS_NOV_PREFIX));
        assertThat(provider.listedPrefixes(), not(hasItem(AWS_SPLICED_PREFIX)));
        assertFalse(containsPath(pathsOf(result), "/11/17/"));
    }

    /**
     * Trailing template literal after a hinted placeholder: leftover pending must not skip {@code data/}
     * or withdraw once that identity level is walked.
     */
    public void testTemplateTrailingLiteralWithDataColumnStillWalks() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/logs/2024/data/a.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/2025/data/b.parquet")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        Map<String, Object> config = Map.of(
            PartitionConfig.CONFIG_PARTITIONING_DETECTION,
            "template",
            PartitionConfig.CONFIG_PARTITIONING_PATH,
            "{year}/data"
        );
        List<PartitionFilterHint> hints = List.of(hint("year", Operator.EQUALS, 2024), hint("srcaddr", Operator.EQUALS, "10.0.0.1"));

        FileList result = expand(provider, "s3://bucket/logs/*/data/*.parquet", hints, MAX, config);

        assertEquals(List.of("s3://bucket/logs/2024/data/a.parquet"), pathsOf(result));
        assertThat(provider.listedPrefixes(), not(hasItem("s3://bucket/logs/")));
        assertThat(provider.childListedPrefixes(), hasItem("s3://bucket/logs/"));
        assertThat(provider.childListedPrefixes(), hasItem("s3://bucket/logs/2024/"));
        assertThat(
            "trailing identity must not list pruned years just to advance unused peeks",
            provider.childListedPrefixes(),
            not(hasItem("s3://bucket/logs/2025/"))
        );
    }

    /**
     * Independent grain INs would keep 11/01 and 12/30. Per-path folder range keeps the two lag folders.
     */
    public void testTemplateJointMonthDayKeepsBoundaryFolders() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/30/a.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/12/01/b.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/01/c.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/12/30/d.log.gz")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        List<Expression> filters = List.of(window(Instant.parse("2026-11-30T00:00:00Z"), Instant.parse("2026-12-02T00:00:00Z")));
        List<PartitionFilterHint> hints = new ArrayList<>();
        hints.add(hint("account", Operator.EQUALS, "012345678901"));
        hints.add(hint("region", Operator.EQUALS, "us-east-1"));
        hints.addAll(listingHints(filters, DAY_SPEC));
        FolderRangePredicate pred = folderRange(filters, DAY_SPEC);

        FileList result = expand(provider, AWS_TEMPLATE_GLOB, hints, MAX, AWS_TEMPLATE, pred);

        assertEquals(
            List.of(
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/30/a.log.gz",
                "s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/12/01/b.log.gz"
            ),
            pathsOf(result)
        );
        assertFalse(containsPath(pathsOf(result), "/11/01/"));
        assertFalse(containsPath(pathsOf(result), "/12/30/"));
        assertThat(provider.listedPrefixes(), not(hasItem(AWS_SPLICED_PREFIX)));
        assertThat(provider.childListedPrefixes(), hasItem(AWS_NOV_PREFIX));
    }

    /**
     * Glob already spells {@code vpcflowlogs}: a CloudTrail sibling cannot match, so skip it and keep walking.
     * Day 17 exists so a completed walk prunes; falling back to a flat {@code AWSLogs/} list would fail this.
     */
    public void testTemplateGlobSpelledLiteralSkipsServiceSibling() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/17/c.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/cloudtrail/us-east-1/2026/11/18/b.json")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        List<PartitionFilterHint> hints = List.of(hint("day", Operator.EQUALS, 18));

        FileList result = expand(provider, AWS_TEMPLATE_GLOB, hints, MAX, AWS_TEMPLATE);

        assertEquals(List.of("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz"), pathsOf(result));
        assertThat(provider.listedPrefixes(), not(hasItem("s3://bucket/AWSLogs/")));
        assertFalse(containsPath(pathsOf(result), "/11/17/"));
    }

    /**
     * A {@code *} glob slot filled by a template literal must withdraw on a sibling. Bypass rewrite, which
     * would pin {@code vpcflowlogs} and skip instead. Day 17 is here so a silent skip would prune and return
     * a non-null walk.
     */
    public void testTemplateStrayLiteralDirWithdraws() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/18/a.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/vpcflowlogs/us-east-1/2026/11/17/c.log.gz"),
            ListingWalkHarness.entry("s3://bucket/AWSLogs/012345678901/cloudtrail/us-east-1/2026/11/18/b.json")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        List<PartitionFilterHint> hints = List.of(hint("day", Operator.EQUALS, 18));
        String glob = "*/*/*/*/*/*/*.log.gz";
        PartitionPruningWalk.WalkResult walk = PartitionPruningWalk.tryWalk(
            provider,
            StoragePath.of("s3://bucket/AWSLogs/"),
            new GlobMatcher(glob),
            ExclusionConfig.fromConfig(null).compile(),
            hints,
            MAX,
            glob,
            null,
            null,
            Integer.MAX_VALUE,
            MAX,
            () -> false,
            "AWSLogs/{account}/vpcflowlogs/{region}/{year}/{month}/{day}"
        );
        assertNull(walk);
        assertThat(provider.childListedPrefixes(), hasItem("s3://bucket/AWSLogs/012345678901/"));
        assertThat(provider.listedPrefixes(), not(hasItem("s3://bucket/AWSLogs/")));
    }

    /** {@code **} is variable depth: TEMPLATE does not walk. */
    public void testTemplateGlobstarDoesNotWalk() throws IOException {
        List<StorageEntry> tree = List.of(
            ListingWalkHarness.entry("s3://bucket/logs/2024/a.parquet"),
            ListingWalkHarness.entry("s3://bucket/logs/2025/b.parquet")
        );
        ListingWalkHarness provider = new ListingWalkHarness(tree);
        Map<String, Object> config = Map.of(
            PartitionConfig.CONFIG_PARTITIONING_DETECTION,
            "template",
            PartitionConfig.CONFIG_PARTITIONING_PATH,
            "{year}"
        );
        FileList result = expand(
            provider,
            "s3://bucket/logs/" + "**/*.parquet",
            List.of(hint("year", Operator.IN, 2024, 2030)),
            MAX,
            config
        );
        assertEquals(List.of("s3://bucket/logs/2024/a.parquet"), pathsOf(result));
        assertTrue(provider.childListedPrefixes().isEmpty());
    }

    private static PartitionFilterHint hint(String column, Operator op, Object... values) {
        return new PartitionFilterHint(column, op, List.of(values));
    }
}
