/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests verifying that STRICT schema resolution rejects multi-file CSV queries
 * when file schemas are incompatible. These cases produce HTTP 400 errors that cannot be
 * asserted in the csv-spec runner (which has no error-assertion syntax).
 * <p>
 * Parquet STRICT-on-divergent is intentionally not covered here: writing Parquet files in
 * this internalClusterTest classpath fails with a {@code NoClassDefFoundError} on shaded
 * Woodstox classes (the Parquet writer pulls in {@code HadoopParquetConfiguration} which
 * needs {@code hadoop-client-runtime}, kept off the main classpath to avoid jar hell — see
 * {@code esql/qa/server/build.gradle parquetFixtureGenerator}). The Parquet schema-mismatch
 * surface is exercised end-to-end by {@code ParquetFormatSpecIT} via the
 * {@code parquet-multifile.csv-spec} FFW tests.
 */
public class ExternalSchemaResolutionErrorIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testStrictOnDivergentCsvTypedHeaders() throws Exception {
        Path dir = createTempDir().resolve("strict_csv_typed");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "name:keyword,age:integer\nalice,30\nbob,40\n", StandardCharsets.UTF_8);
        Files.writeString(
            dir.resolve("b.csv"),
            "age:long,name:keyword,city:keyword\n50,charlie,paris\n60,dave,london\n",
            StandardCharsets.UTF_8
        );

        assertStrictSchemaMismatch(dir, true);
    }

    public void testStrictOnDivergentCsvHeaderless() throws Exception {
        Path dir = createTempDir().resolve("strict_csv_headerless");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "abc,1\nxyz,2\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "10,10\n20,20\n30,30\n", StandardCharsets.UTF_8);

        assertStrictSchemaMismatch(dir, false);
    }

    public void testStrictOnCsvTypeDrift() throws Exception {
        Path dir = createTempDir().resolve("strict_csv_type_drift");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.csv"), "id,col,note\n1,123,alpha\n2,456,gamma\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,col,note\n4,abc,beta\n5,def,epsilon\n", StandardCharsets.UTF_8);

        assertStrictSchemaMismatch(dir, true);
    }

    /**
     * Registers a {@code *.csv} glob dataset with {@code {"schema_resolution":"strict", "header_row":<headerRow>}}
     * settings, runs {@code FROM <dataset> | STATS …}, and asserts the request fails with an
     * {@link IllegalArgumentException} from {@code SchemaReconciliation#validateStrictMatch} whose message
     * identifies a schema mismatch and points at the {@code union_by_name} escape hatch. This maps to HTTP 400
     * on the wire.
     */
    private void assertStrictSchemaMismatch(Path dir, boolean headerRow) {
        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset("strict_csv", glob, Map.of("schema_resolution", "strict", "header_row", headerRow));
        String query = "FROM " + dataset + " | STATS count = COUNT(*)";

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            try (var response = run(syncEsqlQueryRequest(query))) {
                // should not reach here
            }
        });
        assertThat(ex.getMessage(), allOf(containsString("Schema mismatch"), containsString("union_by_name")));
    }
}
