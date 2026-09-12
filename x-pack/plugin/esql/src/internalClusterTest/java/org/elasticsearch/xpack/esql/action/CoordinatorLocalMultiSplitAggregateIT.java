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
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Aggregates over a read that stays on the coordinator while holding several splits.
 *
 * <p>A fan-in producer arrives at the placement decision unresolved, carrying its aggregate as a logical node inside a
 * fragment, so a scan of its physical nodes reports no reduction. That answer also feeds the separate decision of
 * whether a read kept in place keeps a gather boundary above it. Several splits on one node are read by parallel
 * drivers, so a plan judged not to need that boundary is free to be replicated across them: were the aggregate
 * replicated with no gather, each driver would emit its own partial and the query would return one row per driver
 * instead of one row overall.
 *
 * <p>Every assertion here is therefore about row <em>count</em> and value, not placement. {@code coordinator_only}
 * pins the read in place, and the files are separate so the producer holds more than one split, which is what makes
 * the drivers parallel. The remaining strategies run the same queries: they place the read differently, so agreement
 * between them is what distinguishes a placement-dependent fault from a wrong expectation.
 */
public class CoordinatorLocalMultiSplitAggregateIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 4;
    private static final int ROWS_PER_FILE = 3;
    private static final int TOTAL_ROWS = FILES * ROWS_PER_FILE;

    private static final List<String> STRATEGIES = List.of("coordinator_only", "round_robin", "adaptive");

    private String multiSplitDataset;
    private final List<String> fanInDatasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * One dataset globbing {@link #FILES} files, so it is a single producer holding that many splits, plus two such
     * datasets to put the same shape under a fan-in. Values are 1 per row, so a partial that escapes to the output is
     * visible as a short count rather than as a wrong sum alone.
     */
    @Before
    public void registerDatasets() throws Exception {
        fanInDatasets.clear();
        multiSplitDataset = registerGlobbedDataset("multi_split", createTempDir());
        for (int ds = 1; ds <= 2; ds++) {
            fanInDatasets.add(registerGlobbedDataset("fan_in_" + ds, createTempDir()));
        }
    }

    private String registerGlobbedDataset(String name, Path dir) throws Exception {
        for (int file = 1; file <= FILES; file++) {
            StringBuilder csv = new StringBuilder("v:integer,grp:keyword\n");
            for (int row = 1; row <= ROWS_PER_FILE; row++) {
                csv.append("1,g").append(file % 2).append('\n');
            }
            Files.writeString(dir.resolve("file" + file + ".csv"), csv.toString(), StandardCharsets.UTF_8);
        }
        return registerDataset(name, StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));
    }

    private List<List<Object>> runWith(String strategy, String query) {
        var request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), strategy).build()));
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            return getValuesList(response);
        }
    }

    /** An ungrouped aggregate must collapse to one row however the read is placed. */
    public void testUngroupedAggregateReturnsOneRow() {
        for (String strategy : STRATEGIES) {
            List<List<Object>> rows = runWith(strategy, "FROM " + multiSplitDataset + " | STATS c = COUNT(*), s = SUM(v)");
            assertThat(strategy, rows, hasSize(1));
            assertThat(strategy, ((Number) rows.getFirst().get(0)).longValue(), equalTo((long) TOTAL_ROWS));
            assertThat(strategy, ((Number) rows.getFirst().get(1)).longValue(), equalTo((long) TOTAL_ROWS));
        }
    }

    /**
     * The grouped counterpart, where an ungathered partial per driver would show up as a group repeated rather than as
     * a single short row. Files alternate between the two groups so every group spans several splits.
     */
    public void testGroupedAggregateReturnsOneRowPerGroup() {
        for (String strategy : STRATEGIES) {
            List<List<Object>> rows = runWith(strategy, "FROM " + multiSplitDataset + " | STATS c = COUNT(*) BY grp | SORT grp ASC");
            assertThat(strategy, rows, equalTo(List.of(List.of((long) TOTAL_ROWS / 2, "g0"), List.of((long) TOTAL_ROWS / 2, "g1"))));
        }
    }

    /** Distinctness is destroyed by a duplicated read, which a plain count would not always reveal. */
    public void testCountDistinctOverMultipleSplits() {
        for (String strategy : STRATEGIES) {
            List<List<Object>> rows = runWith(strategy, "FROM " + multiSplitDataset + " | STATS d = COUNT_DISTINCT(grp)");
            assertThat(strategy, rows, hasSize(1));
            assertThat(strategy, ((Number) rows.getFirst().getFirst()).longValue(), equalTo(2L));
        }
    }

    /** Several multi-split producers at once, so each one faces the placement decision separately. */
    public void testFanInOfMultiSplitProducersAggregatesToOneRow() {
        for (String strategy : STRATEGIES) {
            List<List<Object>> rows = runWith(strategy, "FROM " + String.join(", ", fanInDatasets) + " | STATS c = COUNT(*), s = SUM(v)");
            assertThat(strategy, rows, hasSize(1));
            assertThat(strategy, ((Number) rows.getFirst().get(0)).longValue(), equalTo((long) TOTAL_ROWS * fanInDatasets.size()));
            assertThat(strategy, ((Number) rows.getFirst().get(1)).longValue(), equalTo((long) TOTAL_ROWS * fanInDatasets.size()));
        }
    }

    /** TopN is the other plan shape the gather decision covers, and a per-driver limit would over-return. */
    public void testTopNOverMultipleSplitsRespectsGlobalLimit() {
        for (String strategy : STRATEGIES) {
            List<List<Object>> rows = runWith(strategy, "FROM " + multiSplitDataset + " | SORT v ASC | LIMIT 2 | KEEP v");
            assertThat(strategy, rows, equalTo(List.of(List.of(1), List.of(1))));
        }
    }
}
