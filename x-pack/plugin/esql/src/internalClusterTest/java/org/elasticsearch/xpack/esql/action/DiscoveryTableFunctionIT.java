/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code FROM DATA_SOURCES()} and {@code FROM DATASETS()} end to end.
 * <p>
 * Real queries rather than plan assertions, because these functions exist to be queried. An endpoint answers one
 * fixed table; a relation answers whatever the language asks of it, and the {@code WHERE} and {@code STATS} below
 * are the whole difference between the two.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DiscoveryTableFunctionIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    public void testDataSourcesListsARegisteredOne() {
        registerDataSource("discovery_src", Map.of());

        try (EsqlQueryResponse response = run("FROM DATA_SOURCES() | KEEP name, type")) {
            assertThat(getValuesList(response).stream().map(row -> row.get(0)).toList(), hasItem("discovery_src"));
        }
    }

    /** The whole language applies, which is the reason for a relation rather than an endpoint. */
    public void testDataSourcesFiltersAndAggregates() {
        registerDataSource("discovery_a", Map.of());
        registerDataSource("discovery_b", Map.of());

        try (EsqlQueryResponse response = run("FROM DATA_SOURCES() | WHERE STARTS_WITH(name, \"discovery_\") | STATS n = COUNT(*)")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(2L));
        }
    }

    public void testDataSourcesColumns() {
        try (EsqlQueryResponse response = run("FROM DATA_SOURCES()")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("name", "type", "description"));
        }
    }

    public void testDatasetsListsARegisteredOne() throws Exception {
        Path file = writeRows();
        registerDataset("discovery_ds", StoragePath.fileUri(file), Map.of("format", "ndjson"));

        try (EsqlQueryResponse response = run("FROM DATASETS() | WHERE name == \"discovery_ds\" | KEEP name, resource, format")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("discovery_ds"));
            assertThat(rows.get(0).get(1), equalTo(StoragePath.fileUri(file)));
            assertThat(rows.get(0).get(2), equalTo("ndjson"));
        }
    }

    /** Nothing registered: an empty answer with the columns still declared, not a failure. */
    public void testDatasetsWithNoneRegistered() {
        try (EsqlQueryResponse response = run("FROM DATASETS()")) {
            assertThat(
                response.columns().stream().map(c -> c.name()).toList(),
                contains("name", "data_source", "resource", "format", "description")
            );
            assertThat(getValuesList(response), hasSize(0));
        }
    }

    /**
     * A function stands where an index pattern stands, so it mixes with one. The index declares {@code name} as a
     * keyword rather than letting dynamic mapping make it text: a union over branches whose columns disagree on
     * type is refused, and that refusal is about the mapping, not about table functions.
     */
    public void testBesideAnIndexPattern() {
        registerDataSource("discovery_mixed", Map.of());
        assertAcked(prepareCreate("discovery_index").setMapping("name", "type=keyword"));
        prepareIndex("discovery_index").setSource("name", "from_the_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM discovery_index, DATA_SOURCES() | KEEP name | SORT name")) {
            var names = getValuesList(response).stream().map(row -> row.get(0)).toList();
            assertThat(names, hasItem("discovery_mixed"));
            assertThat(names, hasItem("from_the_index"));
        }
    }

    private Path writeRows() throws Exception {
        Path dir = createTempDir().resolve("discovery");
        Files.createDirectories(dir);
        Path file = dir.resolve("rows.ndjson");
        Files.writeString(file, "{\"a\": 1}\n", StandardCharsets.UTF_8);
        return file;
    }
}
