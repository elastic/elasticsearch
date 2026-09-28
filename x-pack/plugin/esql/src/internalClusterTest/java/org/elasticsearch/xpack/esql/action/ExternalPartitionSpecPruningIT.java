/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * A {@code partition_spec} overlay projects a file-column filter onto Hive path keys.
 * Rows and {@code files_scanned} are both asserted: a row count cannot tell a dropped
 * file from a never-matching one, and a file count cannot tell a correct answer from
 * a lucky one.
 */
public class ExternalPartitionSpecPruningIT extends AbstractExternalDataSourceIT {

    private static final int[] YEARS = { 2024, 2025 };
    private static final int[] MONTHS = { 1, 6 };
    private static final int[] DAYS = { 1, 15 };
    private static final int TOTAL_FILES = 8;

    private static final Instant MARCH_15_2024 = Instant.parse("2024-03-15T00:00:00Z");
    private static final Instant JAN_1_2025 = Instant.parse("2025-01-01T00:00:00Z");

    private static final String[] REGIONS = { "US", "EU", "AP" };

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    // docs example 4
    public void testMillisTsRangeCrossingYearKeepsLaterMonths() throws Exception {
        // 2024-01 folders sit entirely before March 15; 2024-06 and all of 2025 overlap.
        String dataset = registerMillisTree("spec_ms_cross");
        assertPrune(
            dataset,
            "WHERE ts > \"" + MARCH_15_2024 + "\"::datetime",
            6,
            idsWhere((y, m, d) -> folderStart(y, m, d).isAfter(MARCH_15_2024))
        );
    }

    // docs example 4, open lower bound only
    public void testMillisTsRangeFromNextYearDropsPriorYear() throws Exception {
        String dataset = registerMillisTree("spec_ms_year");
        assertPrune(dataset, "WHERE ts >= \"" + JAN_1_2025 + "\"::datetime", 4, idsWhere((y, m, d) -> y == 2025));
    }

    // docs example 5
    public void testSecondsStartRangeCrossingYear() throws Exception {
        String dataset = registerSecondsTree("spec_sec_cross");
        assertPrune(
            dataset,
            "WHERE start > " + MARCH_15_2024.getEpochSecond(),
            6,
            idsWhere((y, m, d) -> folderStart(y, m, d).isAfter(MARCH_15_2024))
        );
    }

    // docs example 3
    public void testIdentityRemapPrunesRegion() throws Exception {
        String dataset = registerRegionTree("spec_remap");
        assertPrune(dataset, "WHERE region == \"EU\"", 3, 1, List.of(1L));
    }

    // docs example 2
    public void testIdentityYearStillPrunesWithSpecPresent() throws Exception {
        String dataset = registerMillisTree("spec_ident_year");
        assertPrune(dataset, "WHERE year == 2024", 4, idsWhere((y, m, d) -> y == 2024));
    }

    // docs example 6, DATE_EXTRACT literal form
    public void testDateExtractLiteralOnPathKeyStillPrunes() throws Exception {
        String dataset = registerMillisTree("spec_extract_year");
        assertPrune(dataset, "WHERE year == DATE_EXTRACT(\"year\", \"" + JAN_1_2025 + "\"::datetime)", 4, idsWhere((y, m, d) -> y == 2025));
    }

    // docs example 6
    public void testYearLiteralFoldPrunes() throws Exception {
        String dataset = registerMillisTree("spec_year_fold");
        assertPrune(dataset, "WHERE year == YEAR(\"2024-01-01\")", 4, idsWhere((y, m, d) -> y == 2024));
    }

    // Same listing fold as docs example 6, on month.
    public void testMonthLiteralFoldPrunes() throws Exception {
        String dataset = registerMillisTree("spec_month_fold");
        assertPrune(dataset, "WHERE month == MONTH(\"2024-06-15\")", 4, idsWhere((y, m, d) -> m == 6));
    }

    // docs example 7
    public void testYearOfTsInvertsThroughSpec() throws Exception {
        String dataset = registerMillisTree("spec_year_invert");
        assertPrune(dataset, "WHERE YEAR(ts) > 2024", 4, idsWhere((y, m, d) -> y == 2025));
    }

    // docs example 7, no spec. Invert does not skip folders.
    public void testYearOfTsWithoutSpecScansAllFiles() throws Exception {
        String dataset = registerMillisTreeWithoutSpec("spec_year_nospec");
        assertPrune(dataset, "WHERE YEAR(ts) > 2024", TOTAL_FILES, idsWhere((y, m, d) -> y == 2025));
    }

    // docs example 11
    public void testTemplateRenamedKeysRangePrunes() throws Exception {
        String dataset = registerTemplateYyyTree("spec_yyy_mo");
        List<Long> ids = new ArrayList<>();
        for (int year : YEARS) {
            for (int month : MONTHS) {
                if (year == 2024 && month == 1) {
                    continue;
                }
                ids.add((long) (year * 100 + month));
            }
        }
        Collections.sort(ids);
        assertPrune(dataset, "WHERE ts > \"" + MARCH_15_2024 + "\"::datetime", 4, 3, ids);
    }

    // docs example 8
    public void testMonthOfTsIsCyclicFullScan() throws Exception {
        String dataset = registerMillisTree("spec_month_cyclic");
        assertPrune(dataset, "WHERE MONTH(ts) == 6", TOTAL_FILES, idsWhere((y, m, d) -> m == 6));
    }

    // Not a shadow proof. WHERE ts > 0 after EVAL ts = id is true for every id and for every
    // datetime after 1970, so 8 files either way. Do not cite this as a docs example.
    public void testEvalShadowsTsDoesNotPrune() throws Exception {
        String dataset = registerMillisTree("spec_shadow");
        assertPrune(dataset, "EVAL ts = id | WHERE ts > 0", TOTAL_FILES, idsWhere((y, m, d) -> true));
    }

    // docs example 9
    public void testUnmatchedBindDoesNotPrune() throws Exception {
        String dataset = registerYyyTree("spec_unmatched");
        assertPrune(
            dataset,
            "WHERE ts > \"" + MARCH_15_2024 + "\"::datetime",
            TOTAL_FILES,
            idsWhere((y, m, d) -> folderStart(y, m, d).isAfter(MARCH_15_2024))
        );
    }

    // docs example 10
    public void testWrongUnitDoesNotFalsePrune() throws Exception {
        // start is unix seconds; spec default millis reads 1.71e9 as 1970. Warning, full scan, rows still correct.
        String dataset = registerSecondsTreeWrongUnit("spec_wrong_unit");
        assertPrune(
            dataset,
            "WHERE start > " + MARCH_15_2024.getEpochSecond(),
            TOTAL_FILES,
            idsWhere((y, m, d) -> folderStart(y, m, d).isAfter(MARCH_15_2024))
        );
    }

    // docs example 10, exclusive upper bound must not empty the scan
    public void testWrongUnitLessThanDoesNotEmptyScan() throws Exception {
        String dataset = registerSecondsTreeWrongUnit("spec_wrong_unit_lt");
        assertPrune(
            dataset,
            "WHERE start < " + MARCH_15_2024.getEpochSecond(),
            TOTAL_FILES,
            idsWhere((y, m, d) -> folderStart(y, m, d).getEpochSecond() < MARCH_15_2024.getEpochSecond())
        );
    }

    private void assertPrune(String dataset, String filterClause, int expectedFilesScanned, List<Long> expectedIds) {
        assertPrune(dataset, filterClause, TOTAL_FILES, expectedFilesScanned, expectedIds);
    }

    private void assertPrune(String dataset, String filterClause, int totalFiles, int expectedFilesScanned, List<Long> expectedIds) {
        internalCluster().ensureAtLeastNumDataNodes(2);

        List<List<Object>> rows = runPruned(dataset, filterClause + " | KEEP id | SORT id ASC", totalFiles, expectedFilesScanned);
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat(
            "[" + filterClause + "] must return exactly the matching rows, dropping none and inventing none",
            actualIds,
            equalTo(expectedIds)
        );

        List<List<Object>> counted = runPruned(dataset, filterClause + " | STATS c = COUNT(*)", totalFiles, expectedFilesScanned);
        assertThat("expect a single count row", counted.size(), equalTo(1));
        assertThat(
            "[" + filterClause + "] the aggregate path must agree with the projection path",
            ((Number) counted.get(0).get(0)).longValue(),
            equalTo((long) expectedIds.size())
        );
    }

    private List<List<Object>> runPruned(String dataset, String tail, int totalFiles, int expectedFilesScanned) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        String query = "FROM " + dataset + " | " + tail;
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        try (var response = run(request)) {
            if (expectedFilesScanned > 0) {
                assertThat(
                    "external scan must run on a data node via the distributed fragment path",
                    externalScanNodeNames(response).size(),
                    greaterThanOrEqualTo(1)
                );
            }
            assertThat(
                "[" + tail + "] must scan exactly " + expectedFilesScanned + " of " + totalFiles + " files",
                response.getExecutionInfo().queryProfile().filesScanned(),
                equalTo(expectedFilesScanned)
            );
            return getValuesList(response);
        }
    }

    private String registerMillisTree(String name) throws IOException {
        return registerMillisLayout(name, Map.of("partition_detection", "hive", "partition_spec", "year(ts), month(ts), day(ts)"));
    }

    private String registerMillisTreeWithoutSpec(String name) throws IOException {
        return registerMillisLayout(name, Map.of("partition_detection", "hive"));
    }

    private String registerMillisLayout(String name, Map<String, Object> settings) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeMillisFile(root, year, month, day);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(name, glob, settings);
    }

    private String registerTemplateYyyTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                Path dir = root.resolve(Integer.toString(year)).resolve(pad2(month));
                Files.createDirectories(dir);
                String ts = LocalDate.of(year, month, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
                Files.writeString(
                    dir.resolve("f.csv"),
                    "id:integer,ts:datetime\n" + (year * 100 + month) + "," + ts + "\n",
                    StandardCharsets.UTF_8
                );
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(
            name,
            glob,
            Map.of("partition_detection", "template", "partition_path", "{yyy}/{mo}", "partition_spec", "yyy=year(ts), mo=month(ts)")
        );
    }

    private String registerSecondsTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeSecondsFile(root, year, month, day);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(
            name,
            glob,
            Map.of("partition_detection", "hive", "partition_spec", "year(start, second), month(start, second), day(start, second)")
        );
    }

    private String registerSecondsTreeWrongUnit(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeSecondsFile(root, year, month, day);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(
            name,
            glob,
            Map.of("partition_detection", "hive", "partition_spec", "year(start), month(start), day(start)")
        );
    }

    private String registerYyyTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    Path dir = root.resolve("yyy=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
                    Files.createDirectories(dir);
                    writeMillisRow(dir, year, month, day);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(name, glob, Map.of("partition_detection", "hive", "partition_spec", "year(ts)"));
    }

    private String registerRegionTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int i = 0; i < REGIONS.length; i++) {
            Path dir = root.resolve("aws-region=" + REGIONS[i]);
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("f.csv"), "id:integer,region:keyword\n" + i + "," + REGIONS[i] + "\n", StandardCharsets.UTF_8);
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(name, glob, Map.of("partition_detection", "hive", "partition_spec", "aws-region=region"));
    }

    private static void writeMillisFile(Path root, int year, int month, int day) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        writeMillisRow(dir, year, month, day);
    }

    private static void writeMillisRow(Path dir, int year, int month, int day) throws IOException {
        String ts = folderStart(year, month, day).toString();
        Files.writeString(
            dir.resolve("f.csv"),
            "id:integer,ts:datetime\n" + idFor(year, month, day) + "," + ts + "\n",
            StandardCharsets.UTF_8
        );
    }

    private static void writeSecondsFile(Path root, int year, int month, int day) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        long start = folderStart(year, month, day).getEpochSecond();
        Files.writeString(
            dir.resolve("f.csv"),
            "id:integer,start:long\n" + idFor(year, month, day) + "," + start + "\n",
            StandardCharsets.UTF_8
        );
    }

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

    @FunctionalInterface
    private interface PartitionPredicate {
        boolean test(int year, int month, int day);
    }

    private static Instant folderStart(int year, int month, int day) {
        return LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static int idFor(int year, int month, int day) {
        return year * 10000 + month * 100 + day;
    }

    private static String pad2(int v) {
        return v < 10 ? "0" + v : Integer.toString(v);
    }
}
