/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * {@code FROM DATASET(...)} read end to end on a cluster with <b>two</b> data nodes, which is what makes it a test
 * of the node stage rather than of the coordinator: the definition only matters on a node that has to rebuild the
 * read configuration from it, and on one data node the coordinator is that node. Every query here is sent to a
 * pinned coordinator, so a driver reported on the other node's name is a node that bound a definition it did not
 * plan.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class InlineDatasetReadIT extends AbstractExternalDataSourceIT {

    private static final String DATA_SOURCE = "inline_ds";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Deterministic planner shapes, so the external source operator is reliably present in the profile. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /** Every query goes through one node, so "a driver ran elsewhere" means what it says. */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(coordinator()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    /**
     * The rows of every file behind the glob, read under a definition no registration ever saw, with at least one
     * split bound on the node that did not plan the query.
     */
    public void testInlineDefinitionReadsOnEveryDataNode() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());

        var request = syncEsqlQueryRequest("FROM DATASET(" + DATA_SOURCE + ", \"" + glob(dir) + "\") | SORT a");
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"), List.of("b1"), List.of("c1"))));
            Set<String> readers = externalReadNodeNames(response);
            assertThat("the read must be distributed to prove the node stage ran", readers, hasSize(greaterThanOrEqualTo(2)));
            assertThat(readers, not(equalTo(Set.of(coordinator()))));
        }
    }

    /** The {@code {...}} form of the same definition is the same read. */
    public void testTheNamedValuesFormReadsTheSameRows() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());

        String definition = "{\"resource\": \"" + glob(dir) + "\"}";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM DATASET(" + DATA_SOURCE + ", " + definition + ") | SORT a"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("a1"), List.of("a2"), List.of("b1"), List.of("c1"))));
        }
    }

    /** A declared mapping is part of a definition, so an inline one declares a column's type the same way. */
    public void testADeclaredMappingInTheDefinitionIsHonoured() throws IOException {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("n.csv"), "a\n1\n2\n", StandardCharsets.UTF_8);
        registerDataSource(DATA_SOURCE, Map.of());

        String definition = "{\"resource\": \"" + glob(dir) + "\", \"mappings\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}}";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM DATASET(" + DATA_SOURCE + ", " + definition + ") | SORT a"))) {
            assertThat(response.columns().get(0).type().esType(), equalTo("keyword"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("1"), List.of("2"))));
        }
    }

    /** A data source nobody registered is not a door into the node's default credentials. */
    public void testAnUnregisteredDataSourceIsRefused() throws IOException {
        Path dir = csvFixture();

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(nobody_registered_this, \"" + glob(dir) + "\")")).close()
        );
        assertThat(e.getMessage(), containsString("data source [nobody_registered_this] not found"));
    }

    /** The first argument is one data source, so a pattern there is a parse error rather than an expansion. */
    public void testAPatternForTheDataSourceIsRefused() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(\"inline_*\", \"" + glob(dir) + "\")")).close()
        );
        assertThat(e.getMessage(), containsString("[DATASET] reads one data source, found [inline_*]"));
    }

    /** Whatever a registration refuses in a definition is refused here, in the registration's own words. */
    public void testADefinitionSettingOutsideTheVocabularyIsRefused() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());

        String definition = "{\"resource\": \"" + glob(dir) + "\", \"description\": 1}";
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(" + DATA_SOURCE + ", " + definition + ")")).close()
        );
        assertThat(e.getMessage(), containsString("description"));
    }

    /** An inline read sits in a source list beside an index pattern, as a registered dataset does. */
    public void testItReadsBesideAnIndex() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());
        createIndex("inline_idx");
        client().prepareIndex("inline_idx").setId("1").setSource("a", "idx1").get();
        client().admin().indices().prepareRefresh("inline_idx").get();

        String query = "FROM inline_idx, DATASET(" + DATA_SOURCE + ", \"" + glob(dir) + "\") | STATS c = COUNT(*)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    /**
     * The same beside an index <i>pattern</i> rather than a concrete name, which is the form the facility is meant
     * to serve: {@code FROM foo, bar*, DATASET(...)}. A pattern reaches the leaf through the dataset rewrite and the
     * branch plan rather than the concrete-name path, so it is a different route to the same heterogeneous source
     * list, and the two indices prove the pattern expanded rather than matching one by luck.
     */
    public void testItReadsBesideAnIndexPattern() throws IOException {
        Path dir = csvFixture();
        registerDataSource(DATA_SOURCE, Map.of());
        for (String index : List.of("inline_pat_1", "inline_pat_2")) {
            createIndex(index);
            client().prepareIndex(index).setId("1").setSource("a", index).get();
        }
        client().admin().indices().prepareRefresh("inline_pat_1", "inline_pat_2").get();

        String query = "FROM inline_pat_*, DATASET(" + DATA_SOURCE + ", \"" + glob(dir) + "\") | STATS c = COUNT(*)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(6L));
        }
    }

    /** Four rows over three files, so the splits can be spread over both data nodes. */
    private Path csvFixture() throws IOException {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("a.csv"), "a\na1\na2\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "a\nb1\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("c.csv"), "a\nc1\n", StandardCharsets.UTF_8);
        return dir;
    }

    private static String glob(Path dir) {
        return StoragePath.fileUri(dir) + "/*.csv";
    }

    private String coordinator() {
        return internalCluster().getMasterName();
    }

    /** The node names of the drivers that actually ran an external read. */
    private static Set<String> externalReadNodeNames(EsqlQueryResponse response) {
        assertThat(response.profile(), notNullValue());
        Set<String> names = new LinkedHashSet<>();
        for (var driver : response.profile().drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status) {
                    names.add(driver.nodeName());
                }
            }
        }
        return names;
    }
}
