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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Hive left + ES-index right: pins that an inlined {@code WHERE year IN (FROM index)} subquery
 * prunes Hive partition folders the same way a literal {@code year IN (2025)} does.
 *
 * <p>Not dataset × dataset subquery. The right side is an ES index of integer {@code year} values —
 * the same producer shape as {@code FromDatasetSubqueryIT.testInSubqueryMainDatasetSubqueryIndex}.
 * The static twin {@link ExternalHivePartitionPruningIT} already owns literal {@code year IN (2025)}.
 *
 * <p>Fixture matches that static IT: eight single-row CSV files under
 * {@code year=YYYY/month=MM/day=DD}, years {@code {2024,2025}} × months {@code {01,06}} × days
 * {@code {01,15}}. Each file's {@code id} is {@code YYYYMMDD}.
 */
public class ExternalHiveDynamicPartitionPruningIT extends AbstractExternalDataSourceIT {

    private static final int[] YEARS = { 2024, 2025 };
    private static final int[] MONTHS = { 1, 6 };
    private static final int[] DAYS = { 1, 15 };
    private static final int TOTAL_FILES = 8;

    private final List<String> createdIndices = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    @After
    public void deleteCreatedIndices() {
        for (String index : createdIndices) {
            try {
                client().admin().indices().prepareDelete(index).get();
            } catch (Exception e) {
                logger.warn("index cleanup [{}] failed", index, e);
            }
        }
        createdIndices.clear();
    }

    public void testSemiPrune() throws Exception {
        String dataset = registerTree("csv_semi");
        createYearIndex("wanted_years", 2025);

        assertPrune(dataset, "WHERE year IN (FROM wanted_years | KEEP year)", 4, idsWhere(y -> y == 2025), true);
    }

    public void testSemiBothYearsScansAll() throws Exception {
        String dataset = registerTree("csv_semi_both");
        createYearIndex("wanted_years", 2024, 2025);

        assertPrune(dataset, "WHERE year IN (FROM wanted_years | KEEP year)", TOTAL_FILES, idsWhere(y -> y == 2024 || y == 2025), false);
    }

    public void testAntiPrune() throws Exception {
        String dataset = registerTree("csv_anti");
        createYearIndex("wanted_years", 2025);

        assertPrune(dataset, "WHERE year NOT IN (FROM wanted_years | KEEP year)", 4, idsWhere(y -> y == 2024), false);
    }

    /**
     * Empty IN is not L1. {@code SemiJoin.buildEmptyRightSidePlan} replaces the left with an empty
     * {@code LocalRelation}, so the Hive relation never runs. Pin {@code filesScanned() == 0}.
     */
    public void testEmptySemiIsLocalRelationNotL1() throws Exception {
        String dataset = registerTree("csv_empty_semi");
        createYearIndex("wanted_years");

        assertPrune(dataset, "WHERE year IN (FROM wanted_years | KEEP year)", 0, List.of(), true);
    }

    /**
     * Today's contract, not a goal: forcing the hash-join path via
     * {@code in_subquery_hash_join_threshold=0} resets the pruning seed at {@code Join} (not
     * row-preserving), so the query full-scans all 8 files and still returns the four 2025 ids.
     */
    public void testHashJoinFailOpen() throws Exception {
        String dataset = registerTree("csv_hash_join");
        createYearIndex("wanted_years", 2025);

        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin")
                .put(QueryPragmas.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getKey(), 0)
                .build()
        );
        List<Long> expectedIds = idsWhere(y -> y == 2025);
        List<List<Object>> rows = runPruned(
            dataset,
            "WHERE year IN (FROM wanted_years | KEEP year) | KEEP id | SORT id ASC",
            TOTAL_FILES,
            pragmas
        );
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat(actualIds, equalTo(expectedIds));
    }

    public void testDataColumnFailOpen() throws Exception {
        String dataset = registerTree("csv_data_col");
        createIdIndex("wanted_ids", 20250615);

        assertPrune(
            dataset,
            "WHERE id IN (FROM wanted_ids | KEEP id)",
            TOTAL_FILES,
            idsWhere((y, m, d) -> idFor(y, m, d) == 20250615),
            false
        );
    }

    /**
     * Projection path always; STATS path when {@code alsoStats} is true (different lowering; empty IN
     * + STATS has crashed before).
     */
    private void assertPrune(String dataset, String filterClause, int expectedFilesScanned, List<Long> expectedIds, boolean alsoStats) {
        List<List<Object>> rows = runPruned(dataset, filterClause + " | KEEP id | SORT id ASC", expectedFilesScanned, roundRobin());
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat(
            "[" + filterClause + "] must return exactly the matching rows, dropping none and inventing none",
            actualIds,
            equalTo(expectedIds)
        );

        if (alsoStats) {
            List<List<Object>> counted = runPruned(dataset, filterClause + " | STATS c = COUNT(*)", expectedFilesScanned, roundRobin());
            assertThat("expect a single count row", counted.size(), equalTo(1));
            assertThat(
                "[" + filterClause + "] the aggregate path must agree with the projection path",
                ((Number) counted.get(0).get(0)).longValue(),
                equalTo((long) expectedIds.size())
            );
        }
    }

    private List<List<Object>> runPruned(String dataset, String tail, int expectedFilesScanned, QueryPragmas pragmas) {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String query = "FROM " + dataset + " | " + tail;
        var request = syncEsqlQueryRequest(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        try (var response = run(request, TIMEOUT)) {
            if (expectedFilesScanned > 0) {
                assertThat(
                    "external scan must run on a data node via the distributed fragment path",
                    externalScanNodeNames(response).size(),
                    greaterThanOrEqualTo(1)
                );
            } else {
                // Inverse of ExternalHivePartitionPruningIT.testZeroMatchPruneReadsNothing: L1 exhaustive
                // prune still starts a scan operator with 0 splits. Empty SEMI replaces the left with
                // LocalRelation, so no Hive scan runs at all.
                assertThat("empty SEMI replaces the left with LocalRelation; no Hive scan node", externalScanNodeNames(response), empty());
                assertThat(
                    "empty SEMI must not start an external scan (that would be L1 exhaustive prune)",
                    externalScanStatuses(response),
                    empty()
                );
            }

            var profile = response.getExecutionInfo().queryProfile();
            assertThat(
                "[" + tail + "] must scan exactly " + expectedFilesScanned + " of " + TOTAL_FILES + " files",
                profile.filesScanned(),
                equalTo(expectedFilesScanned)
            );
            return getValuesList(response);
        }
    }

    private static QueryPragmas roundRobin() {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
    }

    private void createYearIndex(String name, int... years) {
        assertAcked(client().admin().indices().prepareCreate(name).setMapping("year", "type=integer"));
        createdIndices.add(name);
        for (int year : years) {
            client().prepareIndex(name).setSource("year", year).get();
        }
        client().admin().indices().prepareRefresh(name).get();
        ensureGreen(name);
    }

    private void createIdIndex(String name, int id) {
        assertAcked(client().admin().indices().prepareCreate(name).setMapping("id", "type=integer"));
        createdIndices.add(name);
        client().prepareIndex(name).setSource("id", id).get();
        client().admin().indices().prepareRefresh(name).get();
        ensureGreen(name);
    }

    private static List<Long> idsWhere(YearPredicate predicate) {
        return idsWhere((y, m, d) -> predicate.test(y));
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
    private interface YearPredicate {
        boolean test(int year);
    }

    @FunctionalInterface
    private interface PartitionPredicate {
        boolean test(int year, int month, int day);
    }

    private String registerTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeFile(root, year, month, day);
                }
            }
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(name, glob, Map.of("partition_detection", "hive"));
    }

    private static int idFor(int year, int month, int day) {
        return year * 10000 + month * 100 + day;
    }

    private static String pad2(int v) {
        return v < 10 ? "0" + v : Integer.toString(v);
    }

    private static void writeFile(Path root, int year, int month, int day) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("f.csv"), "id\n" + idFor(year, month, day) + "\n", StandardCharsets.UTF_8);
    }
}
