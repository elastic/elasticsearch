/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Spec dataset versus a twin with the same files and mapping and no {@code partition_spec}.
 * Equal ids prove listing is not narrower than the row filter; {@code files_scanned} proves
 * the spec still skips folders. Parquet day-grid tests cover time-picker {@code request.filter}
 * skipping Hive day folders ({@code start} is INT64 epoch seconds).
 */
public class ExternalPartitionSpecRequestFilterIT extends AbstractExternalDataSourceIT {

    private static final Instant HOUR_10 = Instant.parse("2024-06-15T10:00:00Z");
    private static final Instant HOUR_11 = Instant.parse("2024-06-15T11:00:00Z");
    private static final Instant IN_HOUR_10 = Instant.parse("2024-06-15T10:10:00Z");
    /** Event in hour 10, delivered in the hour-11 folder. */
    private static final Instant LATE_HOUR = Instant.parse("2024-06-15T10:59:50Z");
    private static final Instant IN_HOUR_11 = Instant.parse("2024-06-15T11:10:00Z");
    private static final Instant NY_EVE = Instant.parse("2024-12-31T23:10:00Z");
    private static final Instant NY_LATE = Instant.parse("2024-12-31T23:59:50Z");
    private static final Instant NY_DAY = Instant.parse("2025-01-01T00:10:00Z");

    private static final int ID_IN_10 = 1;
    private static final int ID_LATE_HOUR = 10;
    private static final int ID_IN_11 = 2;
    private static final int ID_NY_EVE = 3;
    private static final int ID_NY_DAY = 4;
    private static final int ID_NY_LATE = 31;
    private static final int ID_LAG_END = 20;
    private static final int ID_LEAD_END = 21;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testRequestFilterOneDaySkipsOtherDayFolders() throws Exception {
        String dataset = registerParquetTree("spec_rf_day");
        assertPruneFilter(
            dataset,
            QueryBuilders.rangeQuery("@timestamp").gte(JUNE_15_START.toString()).lte(JUNE_15_END.toString()),
            1,
            List.of((long) parquetIdFor(2024, 6, 15))
        );
    }

    public void testWhereParamsPinTheSameOneDayWindow() throws Exception {
        String dataset = registerParquetTree("spec_rf_params");
        QueryParams params = new QueryParams(
            List.of(paramAsConstant("_tstart", JUNE_15_START.toString()), paramAsConstant("_tend", JUNE_16_START.toString()))
        );
        assertPruneWhere(
            dataset,
            "WHERE @timestamp >= ?_tstart AND @timestamp < ?_tend",
            params,
            1,
            List.of((long) parquetIdFor(2024, 6, 15))
        );
    }

    public void testWhereParamsWithExplicitDatetimeCast() throws Exception {
        String dataset = registerParquetTree("spec_rf_params_cast");
        QueryParams params = new QueryParams(
            List.of(paramAsConstant("_tstart", JUNE_15_START.toString()), paramAsConstant("_tend", JUNE_16_START.toString()))
        );
        assertPruneWhere(
            dataset,
            "WHERE @timestamp >= ?_tstart::datetime AND @timestamp < ?_tend::datetime",
            params,
            1,
            List.of((long) parquetIdFor(2024, 6, 15))
        );
    }

    public void testMidnightRangeSkipsNextDayFolderHoldingLateEvent() throws Exception {
        // start=23:59:50 of day D lives under day D+1. Without lag the split drops that file.
        String dataset = registerParquetTree("spec_rf_midnight");
        assertPruneFilter(
            dataset,
            QueryBuilders.rangeQuery("@timestamp").gte(DAY_D_START.toString()).lte(DAY_D_END.toString()),
            1,
            List.of((long) parquetIdFor(2024, 6, 1))
        );
    }

    public void testKibanaOneHourWindowMatchesTwin() throws Exception {
        for (Layout layout : Layout.values()) {
            Pair pair = registerPair(layout, specFor(layout), "kibana");
            // Hour 11: the late-delivered 10:59:50 row sits in that folder but is outside this window.
            var filter = kibanaHourFilter(HOUR_11, Instant.parse("2024-06-15T11:59:59.999Z"));
            Result spec = queryIds(pair.spec, filter, null);
            Result twin = queryIds(pair.twin, filter, null);
            assertThat(layout + " kibana filter ids", spec.ids, equalTo(twin.ids));
            assertThat(layout + " kibana filter ids", spec.ids, equalTo(List.of((long) ID_IN_11)));
            assertThat(layout + " spec scans fewer files than the twin", spec.filesScanned, lessThan(twin.filesScanned));
            assertThat(layout + " spec scans the in-hour file", spec.filesScanned, greaterThanOrEqualTo(1));

            Result whereSpec = queryIds(pair.spec, null, whereParams(HOUR_11, Instant.parse("2024-06-15T12:00:00Z")));
            Result whereTwin = queryIds(pair.twin, null, whereParams(HOUR_11, Instant.parse("2024-06-15T12:00:00Z")));
            assertThat(layout + " WHERE params ids", whereSpec.ids, equalTo(whereTwin.ids));
            assertThat(layout + " WHERE params ids", whereSpec.ids, equalTo(List.of((long) ID_IN_11)));
        }
    }

    public void testLateRowMissingWithoutLagPresentWithLag() throws Exception {
        for (Layout layout : List.of(Layout.HIVE_HOURLY_AWS, Layout.TEMPLATE_HOURLY)) {
            Pair noLag = registerPair(layout, specFor(layout), "nolag");
            Pair lag = registerPair(layout, specFor(layout) + ", lag(@timestamp, 15m)", "lag");
            QueryParams window = whereParams(HOUR_10, HOUR_11);
            Result twin = queryIds(noLag.twin, null, window);
            Result without = queryIds(noLag.spec, null, window);
            Result with = queryIds(lag.spec, null, window);
            assertThat(layout + " twin includes the late row", twin.ids, hasItem((long) ID_LATE_HOUR));
            assertThat(layout + " no lag drops the late row", without.ids, not(hasItem((long) ID_LATE_HOUR)));
            assertThat(layout + " no lag keeps the in-folder row", without.ids, equalTo(List.of((long) ID_IN_10)));
            assertThat(layout + " lag keeps the late row", with.ids, equalTo(twin.ids));
            assertThat(layout + " lag scans one more file", with.filesScanned, equalTo(without.filesScanned + 1));
        }
    }

    public void testNewYearLateRowWithLag() throws Exception {
        for (Layout layout : List.of(Layout.HIVE_HOURLY_AWS, Layout.TEMPLATE_HOURLY)) {
            Pair noLag = registerPair(layout, specFor(layout), "nynolag");
            Pair lag = registerPair(layout, specFor(layout) + ", lag(@timestamp, 15m)", "nylag");
            Instant start = Instant.parse("2024-12-31T23:50:00Z");
            Instant end = Instant.parse("2025-01-01T00:00:00Z");
            Result twin = queryIds(noLag.twin, null, whereParams(start, end));
            Result without = queryIds(noLag.spec, null, whereParams(start, end));
            Result with = queryIds(lag.spec, null, whereParams(start, end));
            assertThat(layout + " twin includes the New Year late row", twin.ids, hasItem((long) ID_NY_LATE));
            assertThat(layout + " no lag drops the New Year late row", without.ids, not(hasItem((long) ID_NY_LATE)));
            assertThat(layout + " lag keeps the New Year late row", with.ids, equalTo(twin.ids));
            assertThat(layout + " lag scans one more file", with.filesScanned, equalTo(without.filesScanned + 1));
        }
    }

    public void testZonedDateMathListingNotTighterThanTwin() throws Exception {
        Pair pair = registerPair(Layout.HIVE_HOURLY_AWS, hourlySpec(), "zoned");
        var filter = new RangeQueryBuilder("@timestamp").gte("2024-06-15||/d").lte("2024-06-15||/d").timeZone("America/New_York");
        Result spec = queryIds(pair.spec, filter, null);
        Result twin = queryIds(pair.twin, filter, null);
        assertThat("zoned listing must not drop rows the twin keeps", spec.ids, equalTo(twin.ids));
        assertThat("row filter dropped so 2025 folders stay", spec.ids, hasItem((long) ID_NY_DAY));
        assertThat("RANGE_QUERY listing injects no year IN", spec.filesScanned, equalTo(twin.filesScanned));
    }

    public void testIdentityOnTimestampMatchesTwinAndWarns() throws Exception {
        Pair pair = registerPair(Layout.HIVE_HOURLY_AWS, "dt=@timestamp, " + hourlySpec(), "ident");
        QueryParams window = whereParams(HOUR_11, Instant.parse("2024-06-15T12:00:00Z"));
        Result spec = queryIds(pair.spec, null, window);
        Result twin = queryIds(pair.twin, null, window);
        assertThat(spec.ids, equalTo(twin.ids));
        assertThat(httpWarnings("FROM " + pair.spec + " | KEEP id"), hasItem(containsString("identity to the date column")));
    }

    public void testPutRejectsStartWhenMappedAsTimestamp() throws Exception {
        Path root = writeHourlyHive(createTempDir().resolve("put_reject"));
        String glob = StoragePath.fileUri(root)
            + "/AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.csv";
        Exception e = expectThrows(
            Exception.class,
            () -> registerStrictDataset(
                "put_reject_spec",
                glob,
                mapping(),
                Map.of(
                    "partition_detection",
                    "hive",
                    "partition_spec",
                    "year(start, epoch_second), month(start, epoch_second), day(start, epoch_second), hour(start, epoch_second)"
                )
            )
        );
        assertThat(e.toString(), containsString("start"));
        assertThat(e.toString(), containsString("bind [@timestamp]"));
    }

    public void testMixedDepthExtraFileMatchesTwinAndWarns() throws Exception {
        Path root = createTempDir().resolve("mixed");
        Path hive = root.resolve("year=2024").resolve("month=06").resolve("day=15");
        writeCsv(hive, List.of(row(ID_IN_10, IN_HOUR_10)));
        Path other = root.resolve("other");
        writeCsv(other, List.of(row(99, IN_HOUR_11)));
        String glob = StoragePath.fileUri(root) + "/*" + "*/*.csv";
        LinkedHashMap<String, DatasetFieldMapping> properties = mapping();
        String spec = registerStrictDataset(
            "mixed_spec",
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", "year(@timestamp), month(@timestamp), day(@timestamp)")
        );
        String twin = registerStrictDataset("mixed_twin", glob, properties, Map.of("partition_detection", "hive"));
        Result specResult = queryIds(spec, null, whereParams(HOUR_10, Instant.parse("2024-06-16T00:00:00Z")));
        Result twinResult = queryIds(twin, null, whereParams(HOUR_10, Instant.parse("2024-06-16T00:00:00Z")));
        assertThat(specResult.ids, equalTo(twinResult.ids));
        assertThat(httpWarnings("FROM " + spec + " | KEEP id"), hasItem(containsString("the layout is mixed")));
    }

    public void testLagOnEndKeepsNextFolder() throws Exception {
        Path root = createTempDir().resolve("lag_end");
        Path hive = awsHive(root);
        writeCsv(hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=10"), List.of(row(ID_IN_10, IN_HOUR_10)));
        writeCsv(
            hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=11"),
            List.of(new FileRow(ID_LAG_END, Instant.parse("2024-06-15T10:50:00Z"), Instant.parse("2024-06-15T10:59:50Z")))
        );
        String glob = awsHourlyGlob(root);
        LinkedHashMap<String, DatasetFieldMapping> properties = mapping();
        String endSpec = "year(end), month(end), day(end), hour(end)";
        String noLag = registerStrictDataset(
            "spec_lag_end_nolag",
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", endSpec)
        );
        String lag = registerStrictDataset(
            "spec_lag_end_lag",
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", endSpec + ", lag(end, 15m)")
        );
        String twin = registerStrictDataset("twin_lag_end", glob, properties, Map.of("partition_detection", "hive"));
        QueryParams window = whereParams(HOUR_10, HOUR_11);
        Result twinResult = queryIds(twin, null, window, "end");
        Result without = queryIds(noLag, null, window, "end");
        Result with = queryIds(lag, null, window, "end");
        assertThat("twin includes the late-end row", twinResult.ids, hasItem((long) ID_LAG_END));
        assertThat("no lag drops the late-end row", without.ids, not(hasItem((long) ID_LAG_END)));
        assertThat("no lag keeps the in-folder row", without.ids, equalTo(List.of((long) ID_IN_10)));
        assertThat("lag keeps the late-end row", with.ids, equalTo(twinResult.ids));
        assertThat("lag scans one more file", with.filesScanned, equalTo(without.filesScanned + 1));
    }

    public void testLeadOnEndKeepsPreviousFolder() throws Exception {
        Path root = createTempDir().resolve("lead_end");
        Path hive = awsHive(root);
        writeCsv(
            hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=10"),
            List.of(new FileRow(ID_LEAD_END, Instant.parse("2024-06-15T10:50:00Z"), Instant.parse("2024-06-15T11:05:00Z")))
        );
        writeCsv(hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=11"), List.of(row(ID_IN_11, IN_HOUR_11)));
        String glob = awsHourlyGlob(root);
        LinkedHashMap<String, DatasetFieldMapping> properties = mapping();
        String endSpec = "year(end), month(end), day(end), hour(end)";
        String noLead = registerStrictDataset(
            "spec_lead_end_nolead",
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", endSpec)
        );
        String lead = registerStrictDataset(
            "spec_lead_end_lead",
            glob,
            properties,
            Map.of("partition_detection", "hive", "partition_spec", endSpec + ", lead(end, 15m)")
        );
        String twin = registerStrictDataset("twin_lead_end", glob, properties, Map.of("partition_detection", "hive"));
        QueryParams window = whereParams(HOUR_11, Instant.parse("2024-06-15T12:00:00Z"));
        Result twinResult = queryIds(twin, null, window, "end");
        Result without = queryIds(noLead, null, window, "end");
        Result with = queryIds(lead, null, window, "end");
        assertThat("twin includes the previous-folder row", twinResult.ids, hasItem((long) ID_LEAD_END));
        assertThat("no lead drops the previous-folder row", without.ids, not(hasItem((long) ID_LEAD_END)));
        assertThat("no lead keeps the in-folder row", without.ids, equalTo(List.of((long) ID_IN_11)));
        assertThat("lead keeps the previous-folder row", with.ids, equalTo(twinResult.ids));
        assertThat("lead scans one more file", with.filesScanned, equalTo(without.filesScanned + 1));
    }

    public void testInferredMappingIdentityOnEndWarns() throws Exception {
        Path root = writeHourlyHive(createTempDir().resolve("inferred_ident"));
        String spec = registerNonStrictDataset(
            "inferred_ident_end",
            awsHourlyGlob(root),
            mapping(),
            Map.of("partition_detection", "hive", "partition_spec", "dt=end, " + hourlySpec())
        );
        assertThat(httpWarnings("FROM " + spec + " | KEEP id"), hasItem(containsString("identity to the date column [end]")));
    }

    private static String hourlySpec() {
        return "year(@timestamp), month(@timestamp), day(@timestamp), hour(@timestamp)";
    }

    private static String specFor(Layout layout) {
        return layout == Layout.HIVE_DAILY ? "year(@timestamp), month(@timestamp), day(@timestamp)" : hourlySpec();
    }

    private Pair registerPair(Layout layout, String spec, String suffix) throws IOException {
        Path root = createTempDir().resolve(layout.name() + "_" + suffix);
        String glob = layout.write(root);
        LinkedHashMap<String, DatasetFieldMapping> properties = mapping();
        Map<String, Object> specSettings = new LinkedHashMap<>(layout.settings());
        specSettings.put("partition_spec", spec);
        String tag = layout.name().toLowerCase(Locale.ROOT) + "_" + suffix;
        String specName = registerStrictDataset("spec_" + tag, glob, properties, specSettings);
        String twinName = registerStrictDataset("twin_" + tag, glob, properties, layout.settings());
        return new Pair(specName, twinName);
    }

    private static LinkedHashMap<String, DatasetFieldMapping> mapping() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("@timestamp", DatasetFieldMapping.withFormat("date", "start", "epoch_second"));
        properties.put("end", DatasetFieldMapping.withFormat("date", null, "epoch_second"));
        return properties;
    }

    private Result queryIds(String dataset, Object filter, QueryParams params) {
        return queryIds(dataset, filter, params, "@timestamp");
    }

    private Result queryIds(String dataset, Object filter, QueryParams params, String timeColumn) {
        String query = "FROM " + dataset + " | KEEP id | SORT id";
        if (params != null) {
            query = "FROM "
                + dataset
                + " | WHERE `"
                + timeColumn
                + "` >= ?_tstart::datetime AND `"
                + timeColumn
                + "` < ?_tend::datetime | KEEP id | SORT id";
        }
        var request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        request.profile(true);
        if (filter instanceof RangeQueryBuilder range) {
            request.filter(new BoolQueryBuilder().must(new MatchAllQueryBuilder()).filter(range));
        } else if (filter != null) {
            request.filter((QueryBuilder) filter);
        }
        if (params != null) {
            request.params(params);
        }
        try (var response = run(request)) {
            List<Long> ids = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                ids.add(((Number) row.get(0)).longValue());
            }
            return new Result(ids, response.getExecutionInfo().queryProfile().filesScanned());
        }
    }

    private static QueryParams whereParams(Instant start, Instant end) {
        return new QueryParams(List.of(paramAsConstant("_tstart", start.toString()), paramAsConstant("_tend", end.toString())));
    }

    private static RangeQueryBuilder kibanaHourFilter(Instant gte, Instant lte) {
        return new RangeQueryBuilder("@timestamp").gte(gte.toString()).lte(lte.toString()).format("strict_date_optional_time");
    }

    private List<String> httpWarnings(String query) throws Exception {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query).endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return response.getWarnings();
    }

    private enum Layout {
        HIVE_DAILY {
            @Override
            String write(Path root) throws IOException {
                writeCsv(
                    root.resolve("year=2024").resolve("month=06").resolve("day=15"),
                    List.of(row(ID_IN_10, IN_HOUR_10), row(ID_IN_11, IN_HOUR_11))
                );
                writeCsv(root.resolve("year=2024").resolve("month=06").resolve("day=16"), List.of(row(ID_LATE_HOUR, LATE_HOUR)));
                writeCsv(root.resolve("year=2024").resolve("month=12").resolve("day=31"), List.of(row(ID_NY_EVE, NY_EVE)));
                writeCsv(
                    root.resolve("year=2025").resolve("month=01").resolve("day=01"),
                    List.of(row(ID_NY_DAY, NY_DAY), row(ID_NY_LATE, NY_LATE))
                );
                return StoragePath.fileUri(root) + "/year=*/month=*/day=*/*.csv";
            }

            @Override
            Map<String, Object> settings() {
                return Map.of("partition_detection", "hive");
            }
        },
        HIVE_HOURLY_AWS {
            @Override
            String write(Path root) throws IOException {
                writeHourlyHive(root);
                return StoragePath.fileUri(root)
                    + "/AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.csv";
            }

            @Override
            Map<String, Object> settings() {
                return Map.of("partition_detection", "hive");
            }
        },
        TEMPLATE_HOURLY {
            @Override
            String write(Path root) throws IOException {
                writeCsv(root.resolve("2024").resolve("06").resolve("15").resolve("10"), List.of(row(ID_IN_10, IN_HOUR_10)));
                writeCsv(
                    root.resolve("2024").resolve("06").resolve("15").resolve("11"),
                    List.of(row(ID_IN_11, IN_HOUR_11), row(ID_LATE_HOUR, LATE_HOUR))
                );
                writeCsv(root.resolve("2024").resolve("12").resolve("31").resolve("23"), List.of(row(ID_NY_EVE, NY_EVE)));
                writeCsv(
                    root.resolve("2025").resolve("01").resolve("01").resolve("00"),
                    List.of(row(ID_NY_DAY, NY_DAY), row(ID_NY_LATE, NY_LATE))
                );
                return StoragePath.fileUri(root) + "/*/*/*/*/*.csv";
            }

            @Override
            Map<String, Object> settings() {
                return Map.of("partition_detection", "template", "partition_path", "{year}/{month}/{day}/{hour}");
            }
        };

        abstract String write(Path root) throws IOException;

        abstract Map<String, Object> settings();
    }

    private static Path awsHive(Path root) {
        return root.resolve("AWSLogs")
            .resolve("aws-account-id=123456789012")
            .resolve("aws-service=vpcflowlogs")
            .resolve("aws-region=us-east-1");
    }

    private static String awsHourlyGlob(Path root) {
        return StoragePath.fileUri(root)
            + "/AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.csv";
    }

    private static Path writeHourlyHive(Path root) throws IOException {
        Path hive = awsHive(root);
        writeCsv(hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=10"), List.of(row(ID_IN_10, IN_HOUR_10)));
        writeCsv(
            hive.resolve("year=2024").resolve("month=06").resolve("day=15").resolve("hour=11"),
            List.of(row(ID_IN_11, IN_HOUR_11), row(ID_LATE_HOUR, LATE_HOUR))
        );
        writeCsv(hive.resolve("year=2024").resolve("month=12").resolve("day=31").resolve("hour=23"), List.of(row(ID_NY_EVE, NY_EVE)));
        writeCsv(
            hive.resolve("year=2025").resolve("month=01").resolve("day=01").resolve("hour=00"),
            List.of(row(ID_NY_DAY, NY_DAY), row(ID_NY_LATE, NY_LATE))
        );
        return root;
    }

    private static FileRow row(int id, Instant start) {
        return new FileRow(id, start, start.plusSeconds(60));
    }

    private static void writeCsv(Path dir, List<FileRow> rows) throws IOException {
        Files.createDirectories(dir);
        StringBuilder body = new StringBuilder("id:integer,start:long,end:long\n");
        for (FileRow row : rows) {
            body.append(row.id).append(',').append(row.start.getEpochSecond()).append(',').append(row.end.getEpochSecond()).append('\n');
        }
        Files.writeString(dir.resolve("f.csv"), body, StandardCharsets.UTF_8);
    }

    private static final int[] PARQUET_YEARS = { 2024, 2025 };
    private static final int[] PARQUET_MONTHS = { 1, 6 };
    private static final int[] PARQUET_DAYS = { 1, 15 };
    private static final int PARQUET_GRID_FILES = 8;
    private static final int PARQUET_TOTAL_FILES = PARQUET_GRID_FILES + 1;

    private static final LocalDate DAY_D = LocalDate.of(2024, 6, 1);
    private static final Instant DAY_D_START = DAY_D.atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final Instant DAY_D_END = Instant.parse("2024-06-01T23:59:59.999Z");
    private static final Instant MIDNIGHT_EVENT = LocalDateTime.of(2024, 6, 1, 23, 59, 50).toInstant(ZoneOffset.UTC);
    private static final int MIDNIGHT_ID = 2024060199;

    private static final Instant JUNE_15_START = Instant.parse("2024-06-15T00:00:00Z");
    private static final Instant JUNE_15_END = Instant.parse("2024-06-15T23:59:59.999Z");
    private static final Instant JUNE_16_START = Instant.parse("2024-06-16T00:00:00Z");

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
                "[" + tail + "] must scan exactly " + expectedFilesScanned + " of " + PARQUET_TOTAL_FILES + " files",
                response.getExecutionInfo().queryProfile().filesScanned(),
                equalTo(expectedFilesScanned)
            );
            return getValuesList(response);
        }
    }

    private String registerParquetTree(String name) throws IOException {
        Path root = createTempDir().resolve(name);
        for (int year : PARQUET_YEARS) {
            for (int month : PARQUET_MONTHS) {
                for (int day : PARQUET_DAYS) {
                    writeParquetFile(
                        root,
                        year,
                        month,
                        day,
                        parquetFolderStart(year, month, day).getEpochSecond(),
                        parquetIdFor(year, month, day)
                    );
                }
            }
        }
        writeParquetFile(root, 2024, 6, 2, MIDNIGHT_EVENT.getEpochSecond(), MIDNIGHT_ID);
        @SuppressWarnings("checkstyle:EmptyJavadoc")
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

    private static void writeParquetFile(Path root, int year, int month, int day, long startEpochSeconds, int id) throws IOException {
        Path dir = root.resolve("year=" + year).resolve("month=" + pad2(month)).resolve("day=" + pad2(day));
        Files.createDirectories(dir);
        writeParquet(dir.resolve("f.parquet"), "message test { required int32 id; required int64 start; }", 1, 1024, (g, i) -> {
            g.add("id", id);
            g.add("start", startEpochSeconds);
        });
    }

    private static Instant parquetFolderStart(int year, int month, int day) {
        return LocalDate.of(year, month, day).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static int parquetIdFor(int year, int month, int day) {
        return year * 10000 + month * 100 + day;
    }

    private static String pad2(int v) {
        return v < 10 ? "0" + v : Integer.toString(v);
    }

    private record FileRow(int id, Instant start, Instant end) {}

    private record Pair(String spec, String twin) {}

    private record Result(List<Long> ids, int filesScanned) {}
}
