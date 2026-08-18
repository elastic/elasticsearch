/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

/**
 * Regression test: {@code hive_partitioning} and {@code partition_path} * were not included in
 * {@code FileSourceFactory.COORDINATOR_KEYS}, causing the strict query-time validator
 * (added by elastic/elasticsearch#148327) to reject every external-source query that specified
 * either key with an "unknown option" error.
 *
 * <p>Each test here exercises a key that was previously blocked so that any regression in
 * the coordinator-key wiring fails with a clear "unknown option" exception rather than silently.
 */
public class ExternalCsvHivePartitionedIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /**
     * Writes a two-level Hive-style partition tree ({@code year=YYYY/month=MM/data.csv}),
     * queries it with {@code hive_partitioning: true}, and asserts that the query succeeds
     * (i.e., {@code hive_partitioning} is a known coordinator key) and that the partition
     * columns {@code year} and {@code month} are present in the result schema.
     *
     * <p>The glob uses {@code **} (double-star) rather than {@code year=*} because the local
     * filesystem storage provider only performs a shallow {@code DirectoryStream} listing for
     * non-recursive globs (single {@code *} has no {@code /} in its match); multi-level Hive
     * directories require recursive walking, which {@code **} triggers.
     */
    public void testHivePartitioningValidatesAndParses() throws Exception {
        Path root = createTempDir().resolve("hive_csv");
        writePartitionedCsvFiles(root);

        // The '**' pattern triggers recursive listing so LocalStorageProvider descends into year=/month= dirs.
        @SuppressWarnings("checkstyle:EmptyJavadoc") // checkstyle thinks this is Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_csv", glob, Map.of("hive_partitioning", true));
        String query = "FROM " + dataset + " | LIMIT 1";

        try (var response = run(syncEsqlQueryRequest(query))) {
            List<String> columnNames = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            assertThat("partition column 'year' must appear in result schema", columnNames, hasItem("year"));
            assertThat("partition column 'month' must appear in result schema", columnNames, hasItem("month"));
            assertThat("expect at least 1 row", response.columns().size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Same fixture, queries with explicit {@code partition_path} key, asserting that
     * {@code partition_path} is accepted by the coordinator-key validator (was also missing
     * from COORDINATOR_KEYS before this fix). The primary assertion is that the query does
     * not fail with "unknown option [partition_path]".
     */
    public void testPartitionPathValidatesAndParses() throws Exception {
        Path root = createTempDir().resolve("template_csv");
        writePartitionedCsvFiles(root);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // checkstyle thinks this is Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("template_csv", glob, Map.of("partition_path", "year={year}/month={month}/*.csv"));
        String query = "FROM " + dataset + " | LIMIT 1";

        // Primary assertion: query does not throw "unknown option [partition_path]".
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat("expect at least 1 column", response.columns().size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Collision regression for elastic/esql-planning#959: a Hive partition key ({@code year}) and a
     * physical CSV column share the name {@code year}. The data files live under
     * {@code year=2024/month=01/} but their bodies carry a different physical {@code year} value
     * (1999). Reads previously failed with "output schema size does not match mapping width" because
     * the data-node output schema over-counted the shadowed column.
     *
     * <p>Asserts the read now succeeds and that the partition (path-derived) value wins — {@code year}
     * resolves to {@code 2024}, not the physical {@code 1999} — matching Spark/DuckDB shadowing
     * semantics. A second query projecting only the partition column exercises the empty-data-schema
     * path end-to-end.
     */
    public void testHivePartitionColumnShadowsPhysicalColumn() throws Exception {
        Path root = createTempDir().resolve("hive_collision_csv");
        writeCollisionCsvFiles(root);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // checkstyle thinks this is Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_collision_csv", glob, Map.of("hive_partitioning", true));

        // KEEP id, year, value: the colliding 'year' must surface the path-derived 2024, not 1999.
        String query = "FROM " + dataset + " | KEEP id, year, value | LIMIT 5";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<String> columnNames = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            assertThat("colliding column 'year' must appear in result schema", columnNames, hasItem("year"));
            int yearIdx = columnNames.indexOf("year");

            List<List<Object>> rows = getValuesList(response);
            assertThat("expect both rows from the single data file", rows.size(), greaterThanOrEqualTo(2));
            for (List<Object> row : rows) {
                assertThat("partition value (2024) wins over the physical column value (1999)", row.get(yearIdx), is(2024));
            }
        }

        // Partition-only projection: empty data schema, the partition column must still resolve.
        String partitionOnlyQuery = "FROM " + dataset + " | KEEP year | LIMIT 5";
        try (var response = run(syncEsqlQueryRequest(partitionOnlyQuery))) {
            List<String> columnNames = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            int yearIdx = columnNames.indexOf("year");
            assertThat("partition column 'year' must be projectable on its own", yearIdx, greaterThanOrEqualTo(0));

            List<List<Object>> rows = getValuesList(response);
            assertThat("expect at least one row", rows.size(), greaterThanOrEqualTo(1));
            for (List<Object> row : rows) {
                assertThat("partition-only projection returns the path-derived 2024", row.get(yearIdx), is(2024));
            }
        }
    }

    private static void writeCollisionCsvFiles(Path root) throws Exception {
        Path month01 = root.resolve("year=2024").resolve("month=01");
        Files.createDirectories(month01);

        // The CSV body carries a physical 'year' column (1999) that collides with the path partition
        // key year=2024. After shadowing, reads must return the partition value 2024.
        String content = "id,value,year\n1,alpha,1999\n2,beta,1999\n";
        Files.writeString(month01.resolve("data.csv"), content, StandardCharsets.UTF_8);
    }

    /**
     * Multi-file variant of {@link #testHivePartitionColumnShadowsPhysicalColumn}: two partitions
     * (year=2024 and year=2025), each its own data.csv carrying a physical {@code year} column
     * (1999) that collides with the partition key. This drives the reconciliation-path per-file
     * mapping recompute ({@code shadowPartitionCollisions}) across multiple files through real
     * execution rather than mocked factory state — every row's {@code year} must surface its own
     * path value, never the physical 1999.
     */
    public void testHivePartitionColumnShadowAcrossMultipleFiles() throws Exception {
        Path root = createTempDir().resolve("hive_collision_multifile_csv");
        writeMultiFileCollisionCsvFiles(root);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_collision_multifile_csv", glob, Map.of("hive_partitioning", true));
        String query = "FROM " + dataset + " | KEEP id, year, value | LIMIT 10";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<String> columnNames = response.columns().stream().map(c -> c.name()).collect(Collectors.toList());
            int yearIdx = columnNames.indexOf("year");
            assertThat("colliding column 'year' must appear in result schema", yearIdx, greaterThanOrEqualTo(0));
            int idIdx = columnNames.indexOf("id");

            List<List<Object>> rows = getValuesList(response);
            assertThat("expect all four rows across both partitions", rows.size(), is(4));
            for (List<Object> row : rows) {
                // ids 1,2 live under year=2024; ids 3,4 under year=2025. The path value wins per file.
                int id = ((Number) row.get(idIdx)).intValue();
                int expectedYear = id <= 2 ? 2024 : 2025;
                assertThat("row " + id + " resolves the path-derived year, not the physical 1999", row.get(yearIdx), is(expectedYear));
            }
        }
    }

    /**
     * Locks the stated contract that a shadowed-column warning reaches the client. The warning is
     * emitted during coordinator-side resolution; this executes the collision query on a chosen
     * coordinator and reads that node's response {@code Warning} headers (mirroring
     * {@link WarningsIT}), proving the header actually propagates to the response rather than only
     * to a hand-bound test {@code ThreadContext}.
     */
    public void testHivePartitionShadowWarningReachesClient() throws Exception {
        Path root = createTempDir().resolve("hive_collision_warning_csv");
        writeMultiFileCollisionCsvFiles(root);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_collision_warning_csv", glob, Map.of("hive_partitioning", true));
        String query = "FROM " + dataset + " | KEEP id, year, value | LIMIT 10";

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        List<String> shadowWarnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        // ActionListener.running mirrors WarningsIT: the transport client owns the response ref-count
        // (closing it here would double-decRef), so we only read the coordinator's accumulated
        // response headers from the thread handling completion.
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.running(() -> {
            try {
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                threadContext.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .filter(w -> w.contains("physical column [year] is shadowed"))
                    .forEach(shadowWarnings::add);
            } finally {
                latch.countDown();
            }
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        assertThat(
            "the shadow warning must reach the client via the response Warning header",
            shadowWarnings.size(),
            greaterThanOrEqualTo(1)
        );
    }

    private static void writeMultiFileCollisionCsvFiles(Path root) throws Exception {
        Path p2024 = root.resolve("year=2024").resolve("month=01");
        Path p2025 = root.resolve("year=2025").resolve("month=02");
        Files.createDirectories(p2024);
        Files.createDirectories(p2025);

        // Both files share the [id, value, year] header; the physical 'year' (1999) collides with the
        // path partition key. Distinct partition values per file exercise the per-file mapping recompute.
        Files.writeString(p2024.resolve("data.csv"), "id,value,year\n1,alpha,1999\n2,beta,1999\n", StandardCharsets.UTF_8);
        Files.writeString(p2025.resolve("data.csv"), "id,value,year\n3,gamma,1999\n4,delta,1999\n", StandardCharsets.UTF_8);
    }

    /**
     * The standard AWS S3 log tree
     * {@code AWSLogs/aws-account-id=<id>/aws-service=vpcflowlogs/aws-region=<r>/year=<y>/month=<m>/day=<d>/…}
     * under {@code schema_resolution: first_file_wins}. The registered resource's pattern prefix ends at
     * {@code aws-service=vpcflowlogs/}, so {@code aws-account-id=} and {@code aws-service=} sit BOTH inside
     * the base path AND are claimed as partition columns — a compact listing that re-appends partition
     * columns to the base emits them twice and reads a key that does not exist. The zero-padded
     * {@code month=06} additionally pins value spelling ({@code month=06} must not come back as
     * {@code month=6}), so these queries cover both duplication and spelling.
     *
     * <p>The glob uses {@code **} so the local provider descends recursively and no {@code key=*} segment is
     * spelled, so {@code WHERE month == 6} must prune from the reconstructed listing's partition values —
     * which only line up when the reconstructed key equals the listed one.
     */
    public void testAwsVpcFlowLogLayoutFirstFileWinsRoundTrips() throws Exception {
        Path root = createTempDir().resolve("aws_vpcflow_csv");
        Path prefix = root.resolve("AWSLogs").resolve("aws-account-id=123456789012").resolve("aws-service=vpcflowlogs");
        Path region = prefix.resolve("aws-region=us-east-1").resolve("year=2024");
        writeCsv(region.resolve("month=06").resolve("day=01").resolve("flow-0001.csv"), "id,val\n1,a\n2,b\n");
        writeCsv(region.resolve("month=12").resolve("day=15").resolve("flow-0002.csv"), "id,val\n3,c\n");

        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(prefix) + "/**/*.csv";
        String dataset = registerDataset(
            "aws_vpcflow_csv",
            glob,
            Map.of("hive_partitioning", true, "schema_resolution", "first_file_wins")
        );

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, val"))) {
            assertThat(getValuesList(response).size(), is(3));
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | WHERE month == 6 | KEEP id, val"))) {
            assertThat(getValuesList(response).size(), is(2));
        }
    }

    /**
     * A comma-separated resource has no glob metacharacter, so its pattern prefix is the whole comma string
     * and no listed key starts with it. A compact encoding that prepends that base to each key reads objects
     * that do not exist; the round-trip guard discards such an encoding and keeps the raw listing, so the
     * reads succeed. {@code first_file_wins} routes through the compactor (the default
     * {@code union_by_name} never compacts).
     */
    public void testCommaSeparatedResourceFirstFileWinsRoundTrips() throws Exception {
        Path root = createTempDir().resolve("comma_ffw_csv");
        writeCsv(root.resolve("x.csv"), "id,val\n1,a\n");
        writeCsv(root.resolve("y.csv"), "id,val\n2,b\n");

        String uri = StoragePath.fileUri(root) + "/x.csv," + StoragePath.fileUri(root) + "/y.csv";
        String dataset = registerDataset("comma_ffw_csv", uri, Map.of("schema_resolution", "first_file_wins"));

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, val"))) {
            assertThat(getValuesList(response).size(), is(2));
        }
    }

    private static void writeCsv(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static void writePartitionedCsvFiles(Path root) throws Exception {
        Path month01 = root.resolve("year=2024").resolve("month=01");
        Path month02 = root.resolve("year=2024").resolve("month=02");
        Files.createDirectories(month01);
        Files.createDirectories(month02);

        String content01 = "id,value\n1,alpha\n2,beta\n";
        String content02 = "id,value\n3,gamma\n4,delta\n";
        Files.writeString(month01.resolve("data.csv"), content01, StandardCharsets.UTF_8);
        Files.writeString(month02.resolve("data.csv"), content02, StandardCharsets.UTF_8);
    }
}
