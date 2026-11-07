/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * A dataset whose files disagree about a column's type, read with the union pragma on. The answer must be the one the
 * pragma-less read gives, because a dataset resolves such a disagreement itself and never produces the conflict the
 * union rule looks for.
 * <p>
 * Two different mechanisms answer the same question in this engine and only one of them is the union type's. An index
 * conflict reaches the analyzer as a {@code TypeConflictedField} on a {@code FieldAttribute}, which is what
 * {@code UnionTypesCleanup} rewrites. A dataset's files are reconciled before the analyzer sees them -
 * {@code SchemaReconciliation} widens losslessly where it can and falls back to {@code KEYWORD} - so the column arrives
 * as a {@code ReferenceAttribute} of one ordinary type with no conflict left in it. Nothing in the dataset path
 * constructs a {@code TypeConflictedField} at all.
 * </p>
 * <p>
 * So this is a test that a feature does nothing, and the hazard in that is passing because the feature was never
 * switched on. {@link #testTheSamePragmaTurnsAnIndexConflictIntoAUnion} is the control against exactly that: it is the
 * same class, the same cluster and the same {@link #getPragmas()}, and it fails if the pragma is not reaching the
 * queries - which it was not when this test was first written, because {@code run(EsqlQueryRequest)} does not apply
 * {@code getPragmas()} the way {@code run(String)} does.
 * </p>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvUnionTypedPragmaIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put(randomPragmas().getSettings()).put("union_typed_fields", true).build());
    }

    /**
     * A dataset read has to go through a request object to carry its own settings, and that overload of {@code run} does
     * not apply {@link #getPragmas()}, so every query here applies it by hand.
     */
    private EsqlQueryResponse read(String query) {
        return run(syncEsqlQueryRequest(query).pragmas(getPragmas()));
    }

    /** The column the files disagree about, registered so each test reads the same disagreement. */
    private String datasetWithADisagreeingColumn(String name) throws Exception {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        // col is numeric in a.csv's sample and text in b.csv's, which is the pair that has no lossless supertype.
        Files.writeString(dir.resolve("a.csv"), "id,col,note\n1,100,alpha\n2,200,beta\n3,oops,gamma\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.csv"), "id,col,note\n4,abc,delta\n5,def,epsilon\n", StandardCharsets.UTF_8);
        return registerDataset(
            name,
            StoragePath.fileUri(dir) + "/*.csv",
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );
    }

    /**
     * The reconciled type reaches the response, not a union: {@code col} is one keyword column and the numeric file's
     * values are its string forms. A union would have rendered 100 and 200 as longs and declared two legs, so the
     * values alone tell the two outcomes apart. Pinned against
     * {@link ExternalCsvUnionByNameNumericWideningIT#testNumericInferredColumnKeepsTextTailWhenWidenedToKeyword}, which
     * is the same scenario with no pragma, so the two going out of step is what fails here.
     */
    public void testADisagreeingColumnStillReconcilesWithThePragmaOn() throws Exception {
        String dataset = datasetWithADisagreeingColumn("union_pragma_reconciles");
        try (var response = read("FROM " + dataset + " | SORT id ASC | KEEP col")) {
            assertColumnTypes(response.columns(), List.of("keyword"));
            assertNull("a reconciled column declares no legs", response.columns().getFirst().originalTypes());
            assertThat(
                "the pragma-less answer, value for value",
                getValuesList(response).stream().map(row -> row.get(0)).toList(),
                contains("100", "200", "oops", "abc", "def")
            );
        }
    }

    /**
     * The same question asked per value rather than per column, which is what would survive a renderer that published a
     * union as its widest leg. {@code TYPEOF} reads a value's own tag, so a union of long and keyword answers
     * differently down the column; one answer for every row is the column being a single type rather than a union being
     * flattened on the way out.
     */
    public void testEveryValueOfAReconciledColumnHasTheSameType() throws Exception {
        String dataset = datasetWithADisagreeingColumn("union_pragma_typeof");
        try (var response = read("FROM " + dataset + " | SORT id ASC | EVAL t = TYPEOF(col) | KEEP t")) {
            assertThat(
                "a reconciled column is one type for every value, whatever file it came from",
                getValuesList(response).stream().map(row -> row.get(0)).toList(),
                contains("keyword", "keyword", "keyword", "keyword", "keyword")
            );
        }
    }

    /**
     * The control for the two tests above: an index conflict in this same cluster, under this same pragma, does become a
     * union. Without it, a pragma that silently stopped reaching the query would make both of them pass while proving
     * nothing - which is the state they were in when first written.
     */
    public void testTheSamePragmaTurnsAnIndexConflictIntoAUnion() {
        assertAcked(client().admin().indices().prepareCreate("pragma-long").setMapping("col", "type=long"));
        assertAcked(client().admin().indices().prepareCreate("pragma-keyword").setMapping("col", "type=keyword"));
        client().prepareIndex("pragma-long").setSource("col", 100).get();
        client().prepareIndex("pragma-keyword").setSource("col", "abc").get();
        client().admin().indices().prepareRefresh("pragma-long", "pragma-keyword").get();

        try (var response = read("FROM pragma-long,pragma-keyword | KEEP col")) {
            assertColumnTypes(response.columns(), List.of("union"));
            assertThat(response.columns().getFirst().originalTypes(), equalTo(List.of("keyword", "long")));
            // No SORT: a union has no order, so the rows arrive in whatever order the shards answer in.
            assertThat(Set.copyOf(getValuesList(response)), equalTo(Set.of(List.of(100L), List.of("abc"))));
        }
    }
}
