/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Time-picker {@code request.filter} range on {@code @timestamp} skips Hive day folders
 * via {@code partition_spec}. Parquet only: {@code start} is INT64 epoch seconds, not an
 * annotated TIMESTAMP — the mapping supplies the date.
 */
public class ExternalPartitionSpecRequestFilterIT extends AbstractExternalDataSourceIT {

    private static final int[] YEARS = { 2024, 2025 };
    private static final int[] MONTHS = { 1, 6 };
    private static final int[] DAYS = { 1, 15 };
    private static final int GRID_FILES = 8;
    /** Grid plus the midnight extra under day D+1. */
    private static final int TOTAL_FILES = GRID_FILES + 1;

    private static final LocalDate DAY_D = LocalDate.of(2024, 6, 1);
    private static final Instant DAY_D_START = DAY_D.atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final Instant DAY_D_END = Instant.parse("2024-06-01T23:59:59.999Z");
    private static final Instant MIDNIGHT_EVENT = LocalDateTime.of(2024, 6, 1, 23, 59, 50).toInstant(ZoneOffset.UTC);
    private static final int MIDNIGHT_ID = 2024060199;

    private static final Instant JUNE_15_START = Instant.parse("2024-06-15T00:00:00Z");
    private static final Instant JUNE_15_END = Instant.parse("2024-06-15T23:59:59.999Z");
    private static final Instant JUNE_16_START = Instant.parse("2024-06-16T00:00:00Z");

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    public void testRequestFilterOneDaySkipsOtherDayFolders() throws Exception {
        String dataset = registerTree("spec_rf_day");
        assertPruneFilter(
            dataset,
            QueryBuilders.rangeQuery("@timestamp").gte(JUNE_15_START.toString()).lte(JUNE_15_END.toString()),
            1,
            List.of((long) idFor(2024, 6, 15))
        );
    }

    public void testWhereParamsPinTheSameOneDayWindow() throws Exception {
        String dataset = registerTree("spec_rf_params");
        QueryParams params = new QueryParams(
            List.of(paramAsConstant("_tstart", JUNE_15_START.toString()), paramAsConstant("_tend", JUNE_16_START.toString()))
        );
        assertPruneWhere(dataset, "WHERE @timestamp >= ?_tstart AND @timestamp < ?_tend", params, 1, List.of((long) idFor(2024, 6, 15)));
    }

    public void testMidnightRangeSkipsNextDayFolderHoldingLateEvent() throws Exception {
        // start=23:59:50 of day D lives under day D+1. Year listing still walks day=02;
        // overlapsExpressions at split drops that file. A lag PR flips this.
        String dataset = registerTree("spec_rf_midnight");
        assertPruneFilter(
            dataset,
            QueryBuilders.rangeQuery("@timestamp").gte(DAY_D_START.toString()).lte(DAY_D_END.toString()),
            1,
            List.of((long) idFor(2024, 6, 1))
        );
    }

    private void assertPruneFilter(String dataset, QueryBuilder requestFilter, int expectedFilesScanned, List<Long> expectedIds) {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<List<Object>> rows = runPruned(dataset, "KEEP id | SORT id ASC", requestFilter, null, expectedFilesScanned);
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat("[" + requestFilter + "] must return exactly the matching rows", actualIds, equalTo(expectedIds));

        List<List<Object>> counted = runPruned(dataset, "STATS c = COUNT(*)", requestFilter, null, expectedFilesScanned);
        assertThat("expect a single count row", counted.size(), equalTo(1));
        assertThat(
            "[" + requestFilter + "] the aggregate path must agree with the projection path",
            ((Number) counted.get(0).get(0)).longValue(),
            equalTo((long) expectedIds.size())
        );
    }

    private void assertPruneWhere(
        String dataset,
        String filterClause,
        QueryParams params,
        int expectedFilesScanned,
        List<Long> expectedIds
    ) {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<List<Object>> rows = runPruned(dataset, filterClause + " | KEEP id | SORT id ASC", null, params, expectedFilesScanned);
        List<Long> actualIds = rows.stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        assertThat("[" + filterClause + "] must return exactly the matching rows", actualIds, equalTo(expectedIds));

        List<List<Object>> counted = runPruned(dataset, filterClause + " | STATS c = COUNT(*)", null, params, expectedFilesScanned);
        assertThat("expect a single count row", counted.size(), equalTo(1));
        assertThat(
            "[" + filterClause + "] the aggregate path must agree with the projection path",
            ((Number) counted.get(0).get(0)).longValue(),
            equalTo((long) expectedIds.size())
        );
    }

    private List<List<Object>> runPruned(
        String dataset,
        String tail,
        QueryBuilder requestFilter,
        QueryParams params,
        int expectedFilesScanned
    ) {
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest("FROM " + dataset + " | " + tail);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        if (requestFilter != null) {
            request.filter(requestFilter);
        }
        if (params != null) {
            request.params(params);
        }
        try (var response = run(request)) {
            if (expectedFilesScanned > 0) {
                assertThat(
                    "external scan must run on a data node via the distributed fragment path",
                    externalScanNodeNames(response).size(),
                    greaterThanOrEqualTo(1)
                );
            }
            assertThat(
                "[" + tail + "] must scan exactly " + expectedFilesScanned + " of " + TOTAL_FILES + " files",
                response.getExecutionInfo().queryProfile().filesScanned(),
                equalTo(expectedFilesScanned)
            );
            return getValuesList(response);
        }
    }

    private String registerTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : YEARS) {
            for (int month : MONTHS) {
                for (int day : DAYS) {
                    writeFile(root, year, month, day, folderStart(year, month, day).getEpochSecond(), idFor(year, month, day));
                }
            }
        }
        writeFile(root, 2024, 6, 2, MIDNIGHT_EVENT.getEpochSecond(), MIDNIGHT_ID);
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("@timestamp", DatasetFieldMapping.withFormat("date", "start", "epoch_second"));
        return registerNonStrictDataset(
            name,
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", "year(@timestamp), month(@timestamp), day(@timestamp)")
        );
    }

    private static void writeFile(Path root, int year, int month, int day, long startEpochSeconds, int id) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        writeParquet(dir.resolve("f.parquet"), "message test { required int32 id; required int64 start; }", 1, 1024, (g, i) -> {
            g.add("id", id);
            g.add("start", startEpochSeconds);
        });
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
