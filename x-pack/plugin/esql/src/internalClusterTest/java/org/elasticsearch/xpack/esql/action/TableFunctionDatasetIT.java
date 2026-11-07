/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code FROM DATASET(<data source>, {<definition>})} end to end: a dataset that was never registered, read over a
 * data source that was.
 * <p>
 * The point of reading real rows here rather than asserting a plan is that everything between the two is what the
 * feature actually consists of — the definition meeting the registration API's validator, the data source supplying
 * credentials and endpoint from cluster state, and the resulting leaf being planned and executed as any external
 * source is.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class TableFunctionDatasetIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    private Path writeRows(String dirName, String fileName, String content) throws Exception {
        Path dir = createTempDir().resolve(dirName);
        Files.createDirectories(dir);
        Path file = dir.resolve(fileName);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    public void testReadsAnUnregisteredDefinitionOverARegisteredDataSource() throws Exception {
        Path file = writeRows("inline_basic", "a.ndjson", "{\"id\":1,\"v\":10}\n{\"id\":2,\"v\":20}\n");
        String dataSource = registerLocalFileDataSource();
        String query = "FROM DATASET(" + dataSource + ", {\"resource\": \"" + StoragePath.fileUri(file) + "\"}) | SORT id | KEEP id, v";
        try (var response = run(syncEsqlQueryRequest(query))) {
            // NDJSON infers a whole number as `integer`; COUNT/SUM below are Long because the aggregate is.
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, 10), List.of(2, 20))));
        }
    }

    /** A glob in the definition's resource is read as one in a registered dataset's resource is. */
    public void testReadsAGlobbedResource() throws Exception {
        Path file = writeRows("inline_glob", "a.ndjson", "{\"id\":1,\"v\":10}\n");
        Files.writeString(file.getParent().resolve("b.ndjson"), "{\"id\":2,\"v\":20}\n", StandardCharsets.UTF_8);
        String dataSource = registerLocalFileDataSource();
        String query = "FROM DATASET("
            + dataSource
            + ", {\"resource\": \""
            + StoragePath.fileUri(file.getParent())
            + "/*.ndjson\"}) | STATS count = COUNT(*), total = SUM(v)";
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2L, 30L))));
        }
    }

    /**
     * A pattern cannot name the one data source a read runs under. Unquoted it cannot even be written — {@code *} is
     * not in the grammar's identifier body — so the refusal that the function itself raises is reachable only through
     * a quoted name. Both paths are pinned, because only the second is this function's own check.
     */
    public void testDataSourcePatternIsRefused() throws Exception {
        Path file = writeRows("inline_pattern", "a.ndjson", "{\"id\":1}\n");
        String resource = StoragePath.fileUri(file);
        Exception unquoted = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(file_*, {\"resource\": \"" + resource + "\"})"))
        );
        assertThat(unquoted.getMessage(), containsString("extraneous input '*'"));
        Exception quoted = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(\"file_*\", {\"resource\": \"" + resource + "\"})"))
        );
        assertThat(quoted.getMessage(), containsString("reads one data source"));
    }

    public void testUnknownDataSourceIsRefused() throws Exception {
        Path file = writeRows("inline_unknown", "a.ndjson", "{\"id\":1}\n");
        String query = "FROM DATASET(no_such_source, {\"resource\": \"" + StoragePath.fileUri(file) + "\"})";
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query)));
        assertThat(e.getMessage(), containsString("no_such_source"));
    }

    /**
     * The definition goes through the registration API's own validator — asserted as parity rather than by matching a
     * message: the same bad definition is offered to {@code PUT _query/dataset} and to an inline call, and the two
     * refusals have to agree. A message assertion would pass for any error that merely echoed the key back.
     */
    public void testInlineDefinitionDrawsTheSameRefusalAsPut() throws Exception {
        Path file = writeRows("inline_parity", "a.ndjson", "{\"id\":1}\n");
        String dataSource = registerLocalFileDataSource();
        String resource = StoragePath.fileUri(file);

        Exception viaPut = expectThrows(
            Exception.class,
            () -> client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "inline_parity_dataset",
                    dataSource,
                    resource,
                    null,
                    new HashMap<>(Map.of("no_such_setting", "x"))
                )
            ).actionGet()
        );
        Exception viaQuery = expectThrows(
            Exception.class,
            () -> run(
                syncEsqlQueryRequest(
                    "FROM DATASET(" + dataSource + ", {\"resource\": \"" + resource + "\", \"settings\": {\"no_such_setting\": \"x\"}})"
                )
            )
        );

        String putCause = ExceptionsHelper.unwrapCause(viaPut).getMessage();
        assertThat(putCause, containsString("no_such_setting"));
        assertThat(ExceptionsHelper.unwrapCause(viaQuery).getMessage(), equalTo(putCause));
    }

    /** The resource is the definition's own, so omitting it is refused rather than defaulted. */
    public void testDefinitionWithoutAResourceIsRefused() {
        String dataSource = registerLocalFileDataSource();
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM DATASET(" + dataSource + ", {\"description\": \"no resource\"})"))
        );
        assertThat(e.getMessage(), containsString("resource"));
    }

    /** The data source is the positional argument; naming it again in the definition has no single reading. */
    public void testDataSourceInThePayloadIsRefused() throws Exception {
        Path file = writeRows("inline_dup_source", "a.ndjson", "{\"id\":1}\n");
        String dataSource = registerLocalFileDataSource();
        String query = "FROM DATASET("
            + dataSource
            + ", {\"resource\": \""
            + StoragePath.fileUri(file)
            + "\", \"data_source\": \"other\"})";
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query)));
        assertThat(e.getMessage(), containsString("data_source"));
    }

    /** A source list mixing an index, a pattern and an inline definition reads all of them. */
    public void testHeterogeneousSourceListReadsBothSides() throws Exception {
        Path file = writeRows("inline_mixed", "a.ndjson", "{\"id\":7}\n");
        String dataSource = registerLocalFileDataSource();
        createIndex("local_idx");
        client().prepareIndex("local_idx").setSource("id", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        String query = "FROM local_idx, DATASET("
            + dataSource
            + ", {\"resource\": \""
            + StoragePath.fileUri(file)
            + "\"}) | STATS count = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2L))));
        }
    }

    /** Unchanged behaviour: a definition carries no name, so it registers nothing a later query could read. */
    public void testInlineDefinitionRegistersNothing() throws Exception {
        Path file = writeRows("inline_no_register", "a.ndjson", "{\"id\":1}\n");
        String dataSource = registerLocalFileDataSource();
        try (
            var response = run(
                syncEsqlQueryRequest("FROM DATASET(" + dataSource + ", {\"resource\": \"" + StoragePath.fileUri(file) + "\"})")
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1))));
        }
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM " + dataSource)));
        assertThat(e.getMessage(), containsString(dataSource));
    }

    /**
     * Two definitions over one file differing ONLY in their declared mapping are two different reads.
     * <p>
     * Worth pinning because the obvious reading of the cache addresses says they might not be: a strict declared
     * mapping's schema record is keyed {@code DECLARED_STRICT} and holds the declared schema, while
     * {@code DefinitionVersion.of} folds the resource and the settings but not the mapping — so nothing in the
     * address appears to separate two different declared schemas over one file. It holds anyway. Registering the
     * same two definitions as datasets needs a PUT between them; inline, one query can follow the other, which is
     * why the case is cheap to assert here and was not assertable before.
     */
    public void testTwoDefinitionsDifferingOnlyInTheirMapping() throws Exception {
        Path file = writeRows("inline_mapping_clash", "a.ndjson", "{\"id\":1}\n");
        String dataSource = registerLocalFileDataSource();
        String resource = StoragePath.fileUri(file);
        String asKeyword = "FROM DATASET("
            + dataSource
            + ", {\"resource\": \""
            + resource
            + "\", \"mappings\": {\"dynamic\": false, \"properties\": {\"id\": {\"type\": \"keyword\"}}}}) | KEEP id";
        String asLong = "FROM DATASET("
            + dataSource
            + ", {\"resource\": \""
            + resource
            + "\", \"mappings\": {\"dynamic\": false, \"properties\": {\"id\": {\"type\": \"long\"}}}}) | KEEP id";
        try (var keyword = run(syncEsqlQueryRequest(asKeyword))) {
            assertThat(keyword.columns().get(0).type().typeName(), equalTo("keyword"));
        }
        try (var asLongResponse = run(syncEsqlQueryRequest(asLong))) {
            assertThat(asLongResponse.columns().get(0).type().typeName(), equalTo("long"));
        }
    }

    /** Two definitions over one data source in one query are separate reads, not one. */
    public void testTwoDefinitionsOverOneDataSource() throws Exception {
        Path first = writeRows("inline_two_a", "a.ndjson", "{\"id\":1,\"v\":10}\n");
        Path second = writeRows("inline_two_b", "b.ndjson", "{\"id\":2,\"v\":20}\n");
        String dataSource = registerLocalFileDataSource();
        String query = "FROM DATASET("
            + dataSource
            + ", {\"resource\": \""
            + StoragePath.fileUri(first)
            + "\"}), DATASET("
            + dataSource
            + ", {\"resource\": \""
            + StoragePath.fileUri(second)
            + "\"}) | STATS count = COUNT(*), total = SUM(v)";
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2L, 30L))));
        }
    }
}
