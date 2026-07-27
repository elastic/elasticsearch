/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ChunkedToXContent.wrapAsToXContent;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that filtering a Hive-partitioned external dataset on a partition column reads only the matching partition
 * folders, and returns exactly the matching rows.
 *
 * <p>A {@code FROM <dataset>} is wrapped in a {@code FragmentExec}, so the coordinator physical-plan walk never reaches
 * a top-level {@code ExternalSourceExec}; discovery runs on the <em>fragment</em> path
 * ({@code discoverSplitsFromFragments}), which lowers each {@code ExternalRelation} in isolation and would drop the
 * {@code Filter} ancestor. {@code SplitDiscoveryPhase.guardedRelations} recovers the partition conjunct before that
 * lowering so {@code FileSplitProvider.matchesPartitionFilters} can prune. Most tests here force distribution
 * ({@code external_distribution=round_robin} across {@code >=2} data nodes) and assert the external scan ran on a data
 * node, so the fragment path is genuinely exercised rather than a coordinator-local warm-fold; a control
 * ({@link #testCsvYearPrunesUndistributed}) checks pruning without forcing distribution.
 *
 * <p>Routing the conjunct to the matcher is necessary but not sufficient — the matcher itself has to compare partition
 * values correctly, and two cases return <em>wrong rows</em> rather than merely slow ones:
 * a keyword partition value (a Java {@code String}) compared against an ES|QL literal (a Lucene {@code BytesRef}) must
 * normalize through {@code BytesRefs.toString}, or {@code region == "US"} prunes every file and returns nothing; and a
 * filter over a downstream-generated column that merely shares a partition column's name (an {@code EVAL year = ...}
 * shadowing the path's {@code year}) must be bound by {@code NameId}, or it prunes on the path value and silently drops
 * matching rows. A numeric {@code year/month/day} matrix exercises neither, which is why
 * {@link #testCsvKeywordPartitionEqualityPrunes} and {@link #testCsvEvalShadowsPartitionColumnNoWrongPrune} carry the
 * weight here.
 *
 * <p><b>Fixture.</b> A three-dimension Hive tree {@code year=YYYY/month=MM/day=DD/f.<ext>}, over
 * years {@code {2024,2025}} × months {@code {01,06}} × days {@code {01,15}} = <b>8 single-row files</b>. Each file
 * carries one row whose {@code id} data column is {@code YYYYMMDD} (see {@link #idFor}), so every row is uniquely
 * identifiable and a partial filter prunes to a <em>subset of several</em> files, not always one:
 * {@code year==2025} spans 4 files, {@code month==06} spans 4, {@code year==2025 AND month==06} spans 2, the full
 * 3-part spec spans 1.
 *
 * <p><b>Every case is asserted in both directions</b> (see {@link #assertPrune}): the exact surviving rows, against a
 * Java oracle over the fixture — which catches a file wrongly pruned away — <em>and</em> {@code files_scanned} from the
 * profile, which catches a filter that does not prune when it should. Neither alone is enough: a row count cannot tell a
 * dropped row from a never-matching one, and a file count cannot tell a correct answer from a lucky one.
 *
 * <p>Cases that expect <em>fewer</em> files than the fixture holds assert that pruning fired. Cases that expect the full
 * count are the opposite guard — they pin that a data-column filter, an un-evaluable {@code OR} arm, or a row-derived
 * column does <em>not</em> prune, so they fail if pruning is ever drawn too wide.
 */
public class ExternalHivePartitionPruningIT extends AbstractExternalDataSourceIT {

    private static final int[] YEARS = { 2024, 2025 };
    private static final int[] MONTHS = { 1, 6 };
    private static final int[] DAYS = { 1, 15 };
    private static final int TOTAL_FILES = 8; // 2 x 2 x 2, one single-row file each

    /** The region fixture's three single-row files, each id fixed to its folder so the expected row is unambiguous. */
    private static final String[] REGIONS = { "US", "EU", "AP" };
    private static final long US_ID = 0L;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    // -- Single-dimension partial pushdown: leading, middle, and deepest dimension --

    public void testCsvYearOnlyPrunesToFourFiles() throws Exception {
        // Leading dimension. year=2025 spans months {01,06} x days {01,15} = 4 files.
        assertPrune("csv_year", "csv", "WHERE year == 2025", 4, idsWhere((y, m, d) -> y == 2025));
    }

    public void testParquetYearOnlyPrunesToFourFiles() throws Exception {
        assertPrune("pq_year", "parquet", "WHERE year == 2025", 4, idsWhere((y, m, d) -> y == 2025));
    }

    public void testCsvMonthOnlyPrunesNonLeadingDimension() throws Exception {
        // Non-leading dimension. month=06 spans years {2024,2025} x days {01,15} = 4 files.
        assertPrune("csv_month", "csv", "WHERE month == 6", 4, idsWhere((y, m, d) -> m == 6));
    }

    public void testParquetMonthOnlyPrunesNonLeadingDimension() throws Exception {
        assertPrune("pq_month", "parquet", "WHERE month == 6", 4, idsWhere((y, m, d) -> m == 6));
    }

    public void testCsvDayOnlyPrunesDeepestDimension() throws Exception {
        // Deepest dimension. day=15 spans years {2024,2025} x months {01,06} = 4 files.
        assertPrune("csv_day", "csv", "WHERE day == 15", 4, idsWhere((y, m, d) -> d == 15));
    }

    public void testParquetDayOnlyPrunesDeepestDimension() throws Exception {
        assertPrune("pq_day", "parquet", "WHERE day == 15", 4, idsWhere((y, m, d) -> d == 15));
    }

    // -- Multi-dimension conjunctive pushdown: 2-of-3 and full spec --

    public void testCsvYearAndMonthPrunesToTwoFiles() throws Exception {
        // 2-of-3 subset. year=2025 AND month=06 spans days {01,15} = 2 files.
        assertPrune("csv_ym", "csv", "WHERE year == 2025 AND month == 6", 2, idsWhere((y, m, d) -> y == 2025 && m == 6));
    }

    public void testParquetYearAndMonthPrunesToTwoFiles() throws Exception {
        assertPrune("pq_ym", "parquet", "WHERE year == 2025 AND month == 6", 2, idsWhere((y, m, d) -> y == 2025 && m == 6));
    }

    public void testCsvFullSpecPrunesToOneFile() throws Exception {
        // Full 3-part spec pins exactly one file.
        assertPrune(
            "csv_ymd",
            "csv",
            "WHERE year == 2025 AND month == 6 AND day == 15",
            1,
            idsWhere((y, m, d) -> y == 2025 && m == 6 && d == 15)
        );
    }

    public void testParquetFullSpecPrunesToOneFile() throws Exception {
        assertPrune(
            "pq_ymd",
            "parquet",
            "WHERE year == 2025 AND month == 6 AND day == 15",
            1,
            idsWhere((y, m, d) -> y == 2025 && m == 6 && d == 15)
        );
    }

    // -- Range + IN on a partition column --

    public void testCsvYearInMatchingBothYearsScansAll() throws Exception {
        // IN over both existing years matches every folder -> no prune (8 files), rows still correct.
        assertPrune("csv_in2", "csv", "WHERE year IN (2024, 2025)", TOTAL_FILES, idsWhere((y, m, d) -> y == 2024 || y == 2025));
    }

    public void testCsvYearInSingleYearPrunesToFour() throws Exception {
        // IN over a single existing year prunes to that year's 4 files.
        assertPrune("csv_in1", "csv", "WHERE year IN (2025)", 4, idsWhere((y, m, d) -> y == 2025));
    }

    public void testCsvMonthRangePrunesToSubset() throws Exception {
        // month > 1 keeps only month=06 -> 4 files.
        assertPrune("csv_mrange", "csv", "WHERE month > 1", 4, idsWhere((y, m, d) -> m > 1));
    }

    public void testParquetMonthRangePrunesToSubset() throws Exception {
        assertPrune("pq_mrange", "parquet", "WHERE month > 1", 4, idsWhere((y, m, d) -> m > 1));
    }

    // -- Mixed partition + data column: partition prunes folders, data column stays a row filter --

    public void testCsvYearPlusDataColumnDoesNotOverPrune() throws Exception {
        // year=2025 prunes to its 4 files; the id>N conjunct must NOT further prune files (id is a data column,
        // not path-derived) — it only filters rows. So files_scanned stays 4 while the surviving rows reflect both
        // conjuncts. 2025 file ids are 20250101, 20250115, 20250601, 20250615; id > 20250200 keeps the two June rows.
        assertPrune(
            "csv_mixed",
            "csv",
            "WHERE year == 2025 AND id > 20250200",
            4,
            idsWhere((y, m, d) -> y == 2025 && idFor(y, m, d) > 20250200)
        );
    }

    public void testParquetYearPlusDataColumnDoesNotOverPrune() throws Exception {
        assertPrune(
            "pq_mixed",
            "parquet",
            "WHERE year == 2025 AND id > 20250200",
            4,
            idsWhere((y, m, d) -> y == 2025 && idFor(y, m, d) > 20250200)
        );
    }

    // -- Negative / no-prune sanity: filter on a data-only column must never prune a partition file --

    public void testCsvDataColumnOnlyFilterScansAllFiles() throws Exception {
        // id is a data column present in every file; filtering on it must scan ALL 8 files (never prune on a
        // non-partition column -> no wrong results). Exactly one row matches.
        assertPrune("csv_data", "csv", "WHERE id == 20250615", TOTAL_FILES, idsWhere((y, m, d) -> idFor(y, m, d) == 20250615));
    }

    public void testParquetDataColumnOnlyFilterScansAllFiles() throws Exception {
        assertPrune("pq_data", "parquet", "WHERE id == 20250615", TOTAL_FILES, idsWhere((y, m, d) -> idFor(y, m, d) == 20250615));
    }

    // -- Zero-match: a partition filter matching no folder prunes to the empty set (not an error, not a full scan) --

    public void testCsvZeroMatchPrunesToNoFiles() throws Exception {
        // year=2099 matches no folder, so every file is pruned: nothing scanned, no rows.
        assertPrune("csv_zero", "csv", "WHERE year == 2099", 0, idsWhere((y, m, d) -> y == 2099));
    }

    public void testParquetZeroMatchPrunesToNoFiles() throws Exception {
        assertPrune("pq_zero", "parquet", "WHERE year == 2099", 0, idsWhere((y, m, d) -> y == 2099));
    }

    /**
     * Perf guard for elastic/elasticsearch#153873: a partition filter that prunes <em>every</em> file must make the
     * read path scan nothing, not read the whole dataset only to drop every row in a downstream row filter. The
     * {@code files_scanned == 0} the tests above assert does not prove this on its own — that counter reports
     * <em>survivors</em>, so it was already {@code 0} before the fix while the scan operator still opened all
     * {@link #TOTAL_FILES} files. This pins the real I/O the operator performed: no split total and no bytes read,
     * asserted on both the {@code round_robin} and {@code coordinator_only} distribution strategies (a fully-pruned
     * query short-circuits to coordinator-local under either, so the swap must reach both paths).
     */
    public void testZeroMatchPruneReadsNothing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        for (String distribution : List.of("round_robin", "coordinator_only")) {
            String dataset = registerTree("csv_prune_io_" + distribution, "csv");

            QueryPragmas pragmas = new QueryPragmas(
                Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distribution).build()
            );
            var request = syncEsqlQueryRequest("FROM " + dataset + " | WHERE year == 1999 | KEEP id");
            request.pragmas(pragmas);
            request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
            request.profile(true);

            try (var response = run(request)) {
                assertThat("[" + distribution + "] a zero-match partition filter must return no rows", getValuesList(response), empty());
                assertThat(
                    "[" + distribution + "] the pruned-away files must not be scanned",
                    response.getExecutionInfo().queryProfile().filesScanned(),
                    equalTo(0)
                );
                List<AsyncExternalSourceOperator.Status> statuses = externalScanStatuses(response);
                assertThat(
                    "[" + distribution + "] the scan operator must run so its zero-I/O can be asserted (not vacuously skipped)",
                    statuses,
                    not(empty())
                );
                for (AsyncExternalSourceOperator.Status status : statuses) {
                    assertEquals("[" + distribution + "] exhaustively-pruned read must total no splits", 0, status.splitsTotal());
                    assertEquals("[" + distribution + "] exhaustively-pruned read must read no bytes", 0L, status.bytesRead());
                }
            }
        }
    }

    // -- NOT-EQUALS on a partition column: prunes the excluded folder, keeps the rest --

    public void testCsvNotEqualsPrunesExcludedYear() throws Exception {
        // year != 2024 drops 2024's 4 files, keeps 2025's 4 files.
        assertPrune("csv_ne", "csv", "WHERE year != 2024", 4, idsWhere((y, m, d) -> y != 2024));
    }

    public void testParquetNotEqualsPrunesExcludedYear() throws Exception {
        assertPrune("pq_ne", "parquet", "WHERE year != 2024", 4, idsWhere((y, m, d) -> y != 2024));
    }

    // -- OR over partition-only arms: prunes to exactly the union of the named folders (nullableOr TRUE/FALSE path) --

    public void testCsvOrOfPartitionArmsPrunesToUnion() throws Exception {
        // (year=2024 AND month=01) OR (year=2025 AND month=06): each named (year,month) folder spans days {01,15},
        // so the union is 4 files across the two folders.
        assertPrune(
            "csv_or",
            "csv",
            "WHERE (year == 2024 AND month == 1) OR (year == 2025 AND month == 6)",
            4,
            idsWhere((y, m, d) -> (y == 2024 && m == 1) || (y == 2025 && m == 6))
        );
    }

    public void testParquetOrOfPartitionArmsPrunesToUnion() throws Exception {
        assertPrune(
            "pq_or",
            "parquet",
            "WHERE (year == 2024 AND month == 1) OR (year == 2025 AND month == 6)",
            4,
            idsWhere((y, m, d) -> (y == 2024 && m == 1) || (y == 2025 && m == 6))
        );
    }

    // -- OR with a data-column arm: the un-evaluable arm forces keep-on-null -> no file may be pruned --

    public void testCsvOrWithDataColumnDoesNotPrune() throws Exception {
        // year=2025 OR id==20240101 : the id arm cannot be evaluated at partition time, so nullableOr(FALSE, null)
        // == null for the 2024 files -> keep. No file may be pruned -> files_scanned == 8. The surviving rows are
        // year 2025's four, plus the single 2024 row whose id matches.
        assertPrune(
            "csv_or_data",
            "csv",
            "WHERE year == 2025 OR id == 20240101",
            TOTAL_FILES,
            idsWhere((y, m, d) -> y == 2025 || idFor(y, m, d) == 20240101)
        );
    }

    public void testParquetOrWithDataColumnDoesNotPrune() throws Exception {
        assertPrune(
            "pq_or_data",
            "parquet",
            "WHERE year == 2025 OR id == 20240101",
            TOTAL_FILES,
            idsWhere((y, m, d) -> y == 2025 || idFor(y, m, d) == 20240101)
        );
    }

    // -- Keyword/string partition column: equality + range must prune correctly (BytesRef literal vs String value) --

    public void testCsvKeywordPartitionEqualityPrunes() throws Exception {
        // region partitions {US, EU, AP}. region == "US" must prune to exactly the US file. The literal is a BytesRef;
        // the partition value is a Java String — a raw toString() compare would see hex and never match.
        String dataset = registerRegionTree("csv_kw_eq", "csv");
        assertPruneRegion(dataset, "WHERE region == \"US\"", 1, List.of(US_ID));
    }

    public void testParquetKeywordPartitionEqualityPrunes() throws Exception {
        String dataset = registerRegionTree("pq_kw_eq", "parquet");
        assertPruneRegion(dataset, "WHERE region == \"US\"", 1, List.of(US_ID));
    }

    public void testCsvKeywordPartitionEqualityIsCaseSensitive() throws Exception {
        // Lowercase "us" must NOT match the uppercase US folder: keyword equality is byte-exact. Every folder pruned
        // -> 0 files, 0 rows.
        String dataset = registerRegionTree("csv_kw_case", "csv");
        assertPruneRegion(dataset, "WHERE region == \"us\"", 0, List.of());
    }

    public void testCsvKeywordPartitionRangePrunes() throws Exception {
        // region >= "M" keeps only US ('U'=0x55 >= 'M'=0x4D); EU ('E'=0x45) and AP ('A'=0x41) are < "M" -> dropped.
        String dataset = registerRegionTree("csv_kw_range", "csv");
        assertPruneRegion(dataset, "WHERE region >= \"M\"", 1, List.of(US_ID));
    }

    public void testParquetKeywordPartitionRangePrunes() throws Exception {
        String dataset = registerRegionTree("pq_kw_range", "parquet");
        assertPruneRegion(dataset, "WHERE region >= \"M\"", 1, List.of(US_ID));
    }

    // -- Shadowed/generated column named like a partition: EVAL redefines `year`, filter must use the ROW value --

    public void testCsvEvalShadowsPartitionColumnNoWrongPrune() throws Exception {
        // EVAL year = id % 10000 redefines `year` to the row-derived MMDD (0615 for both June-15 files, across BOTH
        // path years 2024 and 2025). WHERE year == 615 must therefore return BOTH June-15 rows. If the seed prunes by
        // the PATH partition `year` (a name-only match), no folder equals 615, everything is pruned, and the query
        // silently returns nothing. The generated `year` is not a partition column, so NO file may be pruned either.
        String dataset = registerTree("csv_shadow", "csv");
        assertPrune(
            dataset,
            "EVAL year = id % 10000 | WHERE year == 615",
            TOTAL_FILES,
            TOTAL_FILES,
            idsWhere((y, m, d) -> idFor(y, m, d) % 10000 == 615)
        );
    }

    public void testParquetEvalShadowsPartitionColumnNoWrongPrune() throws Exception {
        String dataset = registerTree("pq_shadow", "parquet");
        assertPrune(
            dataset,
            "EVAL year = id % 10000 | WHERE year == 615",
            TOTAL_FILES,
            TOTAL_FILES,
            idsWhere((y, m, d) -> idFor(y, m, d) % 10000 == 615)
        );
    }

    // -- A filter below a row-selecting node must NOT prune: LIMIT does not commute with WHERE --

    /**
     * {@code SORT id | LIMIT 4 | WHERE year == 2025} means: take the four lowest-id rows — 2024's, since every 2024 id
     * sorts below every 2025 id — and filter them <em>afterwards</em>. None is from 2025, so the correct answer is
     * <b>no rows at all</b>. Were the {@code WHERE} used to prune first, the 2024 files would be skipped, the
     * {@code LIMIT} window would refill from the 2025 files, and the query would return four rows nobody asked for.
     *
     * <p><b>What this test does and does not prove.</b> It pins the user-visible answer; it does <em>not</em>
     * discriminate {@code SplitDiscoveryPhase}'s seed guard, and passes with that guard removed. {@code LIMIT} is a
     * pipeline breaker, so the fragment ends beneath it and the filter never enters the fragment body that split
     * discovery walks — the seed cannot leak here today. The guard is defence in depth against that boundary moving;
     * the unit test {@code testGuardedRelationsDoesNotSeedPastLimit} is what actually holds it in place. This test is
     * here so that if the boundary ever does move, the wrong answer is caught at the query level rather than shipped.
     */
    public void testFilterAboveLimitMustNotPrune() throws Exception {
        String dataset = registerTree("csv_limit", "csv");
        assertPrune(dataset, "SORT id ASC | LIMIT 4 | WHERE year == 2025", TOTAL_FILES, TOTAL_FILES, List.of());
    }

    /**
     * The complement: with the filter <em>below</em> the limit, {@code WHERE} genuinely runs first, so it sits inside
     * the fragment and must still prune. Pins that keeping the seed away from cardinality-sensitive nodes has not
     * simply switched pruning off whenever a {@code LIMIT} appears in the query.
     */
    public void testFilterBelowLimitStillPrunes() throws Exception {
        String dataset = registerTree("csv_limit_below", "csv");
        assertPrune(dataset, "WHERE year == 2025 | SORT id ASC | LIMIT 4", TOTAL_FILES, 4, idsWhere((y, m, d) -> y == 2025));
    }

    // -- Keyed glob: the folder-LISTING layer. These are the only tests here where the glob rewrite is live. --

    /**
     * Positive control. On a glob that names its partition keys, a partition filter rewrites the glob itself, so the
     * non-matching folders are never enumerated. This must keep working — the guards below restrict when a hint may be
     * trusted, and a guard drawn too wide would silently turn every keyed dataset back into a full listing.
     */
    public void testKeyedGlobPrunesTheListing() throws Exception {
        String dataset = registerKeyedTree("csv_keyed", "csv");
        assertPrune(dataset, "WHERE year == 2025", TOTAL_FILES, 4, idsWhere((y, m, d) -> y == 2025));
    }

    /** Same, across all three keys, so every segment of the glob is rewritten. */
    public void testKeyedGlobPrunesOnAllThreeKeys() throws Exception {
        String dataset = registerKeyedTree("csv_keyed_full", "csv");
        assertPrune(
            dataset,
            "WHERE year == 2025 AND month == 6 AND day == 15",
            TOTAL_FILES,
            1,
            idsWhere((y, m, d) -> y == 2025 && m == 6 && d == 15)
        );
    }

    /**
     * The listing layer's cardinality guard, end to end. {@code SORT id | LIMIT 4 | WHERE year == 2025} takes the four
     * lowest ids — all from 2024 — and filters them afterwards, so the answer is <b>no rows</b>. If the hint were
     * allowed to rewrite the glob, only 2025's folders would be listed, the limit window would refill from them, and
     * the query would return four rows nobody asked for.
     *
     * <p>Unlike its {@code **}-glob twin, this one genuinely discriminates the guard: remove it and the query returns
     * four rows.
     */
    public void testKeyedGlobMustNotPruneListingAboveLimit() throws Exception {
        String dataset = registerKeyedTree("csv_keyed_limit", "csv");
        assertPrune(dataset, "SORT id ASC | LIMIT 4 | WHERE year == 2025", TOTAL_FILES, TOTAL_FILES, List.of());
    }

    /**
     * The listing layer's shadow guard, end to end. {@code EVAL year = id % 10000} redefines {@code year} as a row
     * value; {@code WHERE year == 615} must match the two June-15 rows. If the hint were trusted, the glob would be
     * rewritten to {@code year=615/}, which matches no folder — historically an outright "matched no files" error.
     */
    public void testKeyedGlobMustNotPruneListingOnShadowedColumn() throws Exception {
        String dataset = registerKeyedTree("csv_keyed_shadow", "csv");
        assertPrune(
            dataset,
            "EVAL year = id % 10000 | WHERE year == 615",
            TOTAL_FILES,
            TOTAL_FILES,
            idsWhere((y, m, d) -> idFor(y, m, d) % 10000 == 615)
        );
    }

    // -- End-to-end: pruning must be visible to a user, in the rendered profile JSON --

    /**
     * The full stack in one assertion: a real Hive-partitioned query runs distributed, the partition filter prunes
     * files, and the counters survive all the way out to the {@code profile} block of the response body a client
     * actually receives. Every other test here reads the profile as an in-process object, which never exercises the
     * XContent rendering — so a counter that is collected correctly but never serialized would pass all of them.
     * {@code files_scanned} in the rendered body is the number a client sees, and it is the number asserted here.
     */
    public void testPruningIsVisibleInRenderedProfileJson() throws Exception {
        String dataset = registerTree("csv_profile_e2e", "csv");
        internalCluster().ensureAtLeastNumDataNodes(2);

        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest("FROM " + dataset + " | WHERE year == 2025 | KEEP id | SORT id ASC");
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);

        try (var response = run(request)) {
            // The rows a client sees: exactly year 2025's four, none dropped by pruning.
            List<Long> ids = getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
            assertThat(ids, equalTo(idsWhere((y, m, d) -> y == 2025)));

            // The profile a client sees. 4 of the 8 files opened, the other 4 skipped without being read.
            String json = Strings.toString(wrapAsToXContent(response), true, false);
            assertThat(
                "the rendered profile must show only the 4 matching files were opened",
                json,
                containsString("\"files_scanned\" : 4")
            );
        }
    }

    // -- Control: the SAME filter must prune without the round_robin pragma forcing distribution --

    /**
     * Not a different code path — every {@code FROM <dataset>} goes through the fragment path. This just drops the
     * {@code round_robin} pragma the other tests use, pinning that pruning does not depend on it.
     */
    public void testCsvYearPrunesUndistributed() throws Exception {
        String dataset = registerTree("csv_coord", "csv");
        String query = "FROM " + dataset + " | WHERE year == 2025 | KEEP id | SORT id ASC";
        try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
            List<Long> ids = getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
            assertThat("the same rows must come back without the distribution pragma", ids, equalTo(idsWhere((y, m, d) -> y == 2025)));
            assertThat(
                "the partition filter must prune to the 4 matching files here too",
                response.getExecutionInfo().queryProfile().filesScanned(),
                equalTo(4)
            );
        }
    }

    /** Registers the 8-file {@code year/month/day} fixture and asserts the filter's pruning + rows. */
    private void assertPrune(String name, String format, String filterClause, int expectedFilesScanned, List<Long> expectedIds)
        throws IOException {
        assertPrune(registerTree(name, format), filterClause, TOTAL_FILES, expectedFilesScanned, expectedIds);
    }

    /**
     * The bidirectional pruning assertion, run against a pre-registered dataset. Pruning is only correct if it is right
     * in <em>both</em> directions, so every case is checked both ways:
     *
     * <ul>
     *   <li><b>Rows (correctness).</b> The query must return exactly {@code expectedIds} — the ids computed by
     *       {@link #idsWhere}, an oracle that evaluates the same predicate in plain Java over the known fixture,
     *       independently of ES|QL. This is what catches pruning that drops a file it should have kept: a row-count
     *       check cannot, because a lost row and a never-matching row produce the same number.</li>
     *   <li><b>Files (the optimization).</b> Exactly {@code expectedFilesScanned} of the fixture's {@code totalFiles}
     *       may be opened — the filter must narrow the scan to the matching partitions.</li>
     * </ul>
     *
     * <p>Both are asserted on <b>both plan shapes</b>: the projection path ({@code KEEP id | SORT id}) and the aggregate
     * path ({@code STATS COUNT(*)}), which lower differently and — for aggregates — can short-circuit the scan entirely.
     * A filter that pruned correctly under one and wrongly under the other would otherwise slip through.
     */
    private void assertPrune(String dataset, String filterClause, int totalFiles, int expectedFilesScanned, List<Long> expectedIds) {
        internalCluster().ensureAtLeastNumDataNodes(2);

        // Projection path: assert the exact surviving rows, not merely how many.
        List<List<Object>> rows = runPruned(dataset, filterClause + " | KEEP id | SORT id ASC", totalFiles, expectedFilesScanned);
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat(
            "[" + filterClause + "] must return exactly the matching rows, dropping none and inventing none",
            actualIds,
            equalTo(expectedIds)
        );

        // Aggregate path: same filter, different lowering (and the shape the warm short-circuit lives on).
        List<List<Object>> counted = runPruned(dataset, filterClause + " | STATS c = COUNT(*)", totalFiles, expectedFilesScanned);
        assertThat("expect a single count row", counted.size(), equalTo(1));
        assertThat(
            "[" + filterClause + "] the aggregate path must agree with the projection path",
            ((Number) counted.get(0).get(0)).longValue(),
            equalTo((long) expectedIds.size())
        );
    }

    /**
     * Runs {@code FROM <dataset> | <tail>} distributed across {@code >=2} data nodes and asserts the file accounting:
     * the scan ran on a data node (so the fragment path is genuinely exercised), exactly {@code expectedFilesScanned}
     * files were opened, and every candidate file is accounted for as either scanned or pruned. Returns the rows.
     */
    private List<List<Object>> runPruned(String dataset, String tail, int totalFiles, int expectedFilesScanned) {
        // round_robin distributes every surviving split to a data node regardless of plan shape, so the read lowers
        // through a FragmentExec and split discovery runs on the fragment path (discoverSplitsFromFragments).
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());

        String query = "FROM " + dataset + " | " + tail;
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);
        try (var response = run(request)) {
            // The node set cannot be required to be >= 2: once pruning succeeds the surviving split set may fit one
            // node. A non-empty scan-node set proves the external scan ran on a data node (distributed fragment
            // operator), not a coordinator-local warm-fold. A fully-pruned query scans nowhere at all, so there is
            // no node to assert on.
            if (expectedFilesScanned > 0) {
                assertThat(
                    "external scan must run on a data node via the distributed fragment path",
                    externalScanNodeNames(response).size(),
                    greaterThanOrEqualTo(1)
                );
            }

            var profile = response.getExecutionInfo().queryProfile();
            assertThat(
                "[" + tail + "] must scan exactly " + expectedFilesScanned + " of " + totalFiles + " files",
                profile.filesScanned(),
                equalTo(expectedFilesScanned)
            );
            return getValuesList(response);
        }
    }

    /** Region-fixture variant of {@link #assertPrune}: 3 single-row files ({@code region} in {@code US, EU, AP}). */
    private void assertPruneRegion(String dataset, String filterClause, int expectedFilesScanned, List<Long> expectedIds) {
        assertPrune(dataset, filterClause, 3, expectedFilesScanned, expectedIds);
    }

    /**
     * The ids the fixture must yield for a predicate, evaluated in plain Java over the known {@code year/month/day}
     * tree. An oracle independent of the engine under test: the expected rows are derived from the fixture's
     * definition, not from what ES|QL happens to return.
     */
    private static List<Long> idsWhere(PartitionPredicate predicate) {
        List<Long> ids = new ArrayList<>();
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    if (predicate.test(year, month, day)) {
                        ids.add((long) idFor(year, month, day));
                    }
                }
            }
        }
        Collections.sort(ids);
        return ids;
    }

    /** A predicate over one fixture file's partition triple, used to derive the expected rows. */
    @FunctionalInterface
    private interface PartitionPredicate {
        boolean test(int year, int month, int day);
    }

    /**
     * The same 8-file tree, registered with a glob that <em>names</em> its partition keys — {@code year=*},
     * {@code month=*}, {@code day=*} — instead of hiding them behind a bare {@code **}.
     *
     * <p>This is the shape that reaches the listing layer. {@code GlobExpander.rewriteSegment} only rewrites a segment
     * spelled exactly {@code key=*}, so on the {@code **} glob every other test here uses, the rewrite is inert and the
     * folders are always listed — only the read layer prunes. With the keys named, a partition hint rewrites the glob
     * and the non-matching folders are never <em>enumerated</em>, which is a pruning decision taken before a
     * {@code FileList} exists and which nothing downstream can undo. That is why the guards have to hold here too.
     *
     * <p>Two fixture details are load-bearing. The glob keeps a recursive wildcard and a concrete extension — the local
     * file provider only recurses for globs containing one, and the format is inferred from the extension. And the
     * month/day segments are written <em>unpadded</em>
     * ({@code month=6}, not {@code month=06}) because the rewrite spells a hint's value with {@code String.valueOf} —
     * {@code month == 6} would rewrite to {@code month=6} and match no zero-padded folder at all.
     */
    private String registerKeyedTree(String name, String format) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    Path dir = root.resolve("year=" + year).resolve("month=" + month).resolve("day=" + day);
                    Files.createDirectories(dir);
                    writeRow(dir, idFor(year, month, day), format);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's trailing '/**' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/year=*/month=*/day=*/**/*." + format;
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    /**
     * Registers the 8-file {@code year/month/day} Hive tree for the given format ({@code csv} or {@code parquet}),
     * one single-row file per {@code (year, month, day)} triple, each row's {@code id} = {@link #idFor}. Returns the
     * registered dataset name.
     */
    private String registerTree(String name, String format) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeFile(root, year, month, day, format);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*." + format;
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    /**
     * Registers a single-dimension {@code region=<STR>} Hive tree with three single-row files ({@code US}, {@code EU},
     * {@code AP}). The names are case-distinct so they don't collapse on a case-insensitive filesystem (macOS APFS),
     * while a separate equality test ({@link #testCsvKeywordPartitionEqualityIsCaseSensitive}) pins byte-exact keyword
     * matching. Each file's {@code id} is a small int so {@code COUNT(*)} is well-defined. The partition column type is
     * KEYWORD, so its value rides as a Java String while the query literal is a BytesRef — the exact mismatch the
     * keyword-comparison fix addresses.
     */
    private String registerRegionTree(String name, String format) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int i = 0; i < REGIONS.length; i++) {
            // id == index into REGIONS, so US is 0 (US_ID), EU is 1, AP is 2. Pinning the value to the folder is what
            // lets the keyword tests assert *which* row came back, not merely how many.
            final int rowId = i;
            Path dir = root.resolve("region=" + REGIONS[i]);
            Files.createDirectories(dir);
            if (format.equals("csv")) {
                Files.writeString(dir.resolve("f.csv"), "id\n" + rowId + "\n", StandardCharsets.UTF_8);
            } else {
                writeParquet(dir.resolve("f.parquet"), "message test { required int32 id; }", 1, 1024, (g, i2) -> g.add("id", rowId));
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*." + format;
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    /**
     * The single {@code id} data value for the file at {@code (year, month, day)}: {@code YYYYMMDD} as an int, so it
     * is unique per file, orderable, and decodes the partition triple by eye (e.g. {@code 20250615}).
     */
    private static int idFor(int year, int month, int day) {
        return year * 10000 + month * 100 + day;
    }

    /** Zero-pads a one- or two-digit partition segment value to two digits (e.g. {@code 1 -> "01"}, {@code 15 -> "15"}). */
    private static String pad2(int v) {
        return v < 10 ? "0" + v : Integer.toString(v);
    }

    /** Writes {@code root/year=YYYY/month=MM/day=DD/f.<format>} carrying one row with a single {@code id} column. */
    private static void writeFile(Path root, int year, int month, int day, String format) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        writeRow(dir, idFor(year, month, day), format);
    }

    /** Writes {@code dir/f.<format>} carrying one row with a single {@code id} column. */
    private static void writeRow(Path dir, int id, String format) throws IOException {
        if (format.equals("csv")) {
            Files.writeString(dir.resolve("f.csv"), "id\n" + id + "\n", StandardCharsets.UTF_8);
        } else {
            // One row, one int32 id column. rowGroupSize 1024 keeps it a single row group -> one split.
            writeParquet(dir.resolve("f.parquet"), "message test { required int32 id; }", 1, 1024, (g, i) -> g.add("id", id));
        }
    }
}
