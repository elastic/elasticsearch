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
 * than the dataset. The danger is not that prefix — it is where it can travel. A prefix served to a query that
 * asked for the whole glob would read a fraction of the data, report success, and be believed. What stops that is
 * the listing cache's key: how far the listing was asked to run is part of it, so a prefix is addressed as a prefix
 * and the query that wants the dataset cannot be handed one. These tests are the end-to-end pin on that key, and
 * they are the reason it has to be in the key rather than in a rule about who may write to the cache.
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
        // first_file_wins explicitly: with schema_resolution absent, DatasetRewriter hydrates union_by_name
        // onto the query config, FileOrderConfig#forListing then answers NAME_ASC rather than DEFAULT, and
        // listingExtentsFor refuses to bound a non-default order. This case asserted nothing for as long as
        // it omitted the key - the listing it called bounded was the whole glob.
        String dataset = registerStrictDataset(
            "bound_declared",
            glob,
            properties,
            Map.of("format", "csv", "schema_resolution", "first_file_wins")
        );

        try (EsqlQueryResponse discovery = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
            assertThat(getValuesList(discovery).size(), equalTo(0));
            assertThat(columnsOf(discovery), equalTo(List.of("id:long", "v:keyword")));
        }

        assertEveryRowIsReadable(dataset);
    }

    /**
     * Schema discovery twice over. The second is served the first's prefix, from the cache entry the bounded ask
     * addresses, and must still answer the same schema — and the read that follows must still see every file,
     * because it asks unbounded and so asks a different key.
     */
    public void testRepeatedSchemaDiscoveryAnswersTheSameSchema() throws Exception {
        String glob = writeDataset();
        // Explicit, for the reason the declared case above states: an omitted schema_resolution is hydrated to
        // union_by_name, which costs the listing its bound and left this case asserting nothing either.
        String dataset = registerLocalFileDataset("bound_repeat", glob, Map.of("format", "csv", "schema_resolution", "first_file_wins"));

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
