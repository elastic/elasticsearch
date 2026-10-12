/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * What a query that reads rows sees after a query that only asked for a schema.
 * <p>
 * Schema discovery may stop listing once it has a schema, which leaves it holding a prefix of the dataset rather
 * than the dataset. The danger is not that prefix — it is where it can travel. The listing cache is keyed by the
 * path and its filters and knows nothing about what the query that filled it happened to need, so a prefix
 * written there would be served to the next query over the same glob, which would read a fraction of the data,
 * report success, and be believed.
 * <p>
 * Each test therefore discovers the schema first, on a dataset wide enough that the bound bites, and then
 * reads the same dataset in the same cluster and counts what comes back. Every row is the assertion. Ordering
 * matters and is the point: reversing it would let both queries pass while proving nothing.
 */
public class ExternalSchemaDiscoveryBoundIT extends AbstractExternalDataSourceIT {

    /** Past the 1,000-key default, so an unbounded answer and a bounded one cannot be confused. */
    private static final int FILE_COUNT = 2_500;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testReadingQueryAfterSchemaDiscoverySeesEveryFile() throws Exception {
        String glob = writeDataset();
        String dataset = registerLocalFileDataset("bound_ffw", glob, Map.of("format", "csv", "schema_resolution", "first_file_wins"));

        try (EsqlQueryResponse discovery = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
            assertThat("no rows are read", getValuesList(discovery).size(), equalTo(0));
            // The exact columns, not a lower bound: the risk a bound introduces is a DIFFERENT schema, and a
            // count that is merely large enough cannot see that. Inferred, so `id` is whatever the reader makes
            // of the CSV — not the `long` the declared case below asks for.
            assertThat(columnsOf(discovery), equalTo(List.of("id:integer", "v:keyword")));
        }

        assertEveryRowIsReadable(dataset);
    }

    /** The same, for a declared mapping, whose listing is bounded on a different rail of the resolver. */
    public void testReadingQueryAfterSchemaDiscoveryOnDeclaredDatasetSeesEveryFile() throws Exception {
        String glob = writeDataset();
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("v", new DatasetFieldMapping("keyword", null));
        String dataset = registerStrictDataset("bound_declared", glob, properties, Map.of("format", "csv"));

        try (EsqlQueryResponse discovery = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
            assertThat(getValuesList(discovery).size(), equalTo(0));
            assertThat(columnsOf(discovery), equalTo(List.of("id:long", "v:keyword")));
        }

        assertEveryRowIsReadable(dataset);
    }

    /**
     * Schema discovery twice over. The first leaves nothing cached under the dataset's key, so the second
     * must not find a prefix there and must still answer the same schema.
     */
    public void testRepeatedSchemaDiscoveryAnswersTheSameSchema() throws Exception {
        String glob = writeDataset();
        String dataset = registerLocalFileDataset("bound_repeat", glob, Map.of("format", "csv"));

        List<String> first;
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
            first = response.columns().stream().map(c -> c.name() + ":" + c.type()).toList();
        }
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
            assertThat(response.columns().stream().map(c -> c.name() + ":" + c.type()).toList(), equalTo(first));
        }

        assertEveryRowIsReadable(dataset);
    }

    /**
     * Counts rows rather than files: one row per file, so a listing that stopped at the bound reports 1,000 here
     * and a complete one reports every file. That difference is exactly what a poisoned cache would produce.
     */
    private void assertEveryRowIsReadable(String dataset) {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            assertThat(
                "a query that reads rows must see the whole dataset, whatever earlier schema discovery listed",
                ((Number) values.get(0).get(0)).longValue(),
                equalTo((long) FILE_COUNT)
            );
        }
    }

    private static List<String> columnsOf(EsqlQueryResponse response) {
        return response.columns().stream().map(c -> c.name() + ":" + c.type().esType()).toList();
    }

    private String writeDataset() throws IOException {
        Path dir = createTempDir();
        for (int f = 0; f < FILE_COUNT; f++) {
            Files.writeString(dir.resolve(String.format(Locale.ROOT, "part-%06d.csv", f)), "id,v\n" + f + ",a\n");
        }
        return StoragePath.fileUri(dir) + "/*.csv";
    }
}
