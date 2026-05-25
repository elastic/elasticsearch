/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Regression test: {@code hive_partitioning} and {@code partition_path} * were not included in
 * {@code FileSourceFactory.COORDINATOR_KEYS}, causing the strict query-time validator
 * (added by elastic/elasticsearch#148327) to reject every EXTERNAL query that specified
 * either key with an "unknown option" error.
 *
 * <p>Each test here exercises a key that was previously blocked so that any regression in
 * the coordinator-key wiring fails with a clear "unknown option" exception rather than silently.
 */
public class ExternalCsvHivePartitionedIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        return plugins;
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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path root = createTempDir().resolve("hive_csv");
        writePartitionedCsvFiles(root);

        // The '**' pattern triggers recursive listing so LocalStorageProvider descends into year=/month= dirs.
        @SuppressWarnings("checkstyle:EmptyJavadoc") // checkstyle thinks this is Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String query = "EXTERNAL \"" + glob + "\" WITH {\"hive_partitioning\": true} | LIMIT 1";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path root = createTempDir().resolve("template_csv");
        writePartitionedCsvFiles(root);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // checkstyle thinks this is Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String query = "EXTERNAL \"" + glob + "\" WITH {\"partition_path\": \"year={year}/month={month}/*.csv\"}" + " | LIMIT 1";

        // Primary assertion: query does not throw "unknown option [partition_path]".
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat("expect at least 1 column", response.columns().size(), greaterThanOrEqualTo(1));
        }
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
