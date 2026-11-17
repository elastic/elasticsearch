/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.sourcefunction.SourceFunction;
import org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionInvocation;
import org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionSignature;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Where source-function resolution sits relative to the dataset rewrite, asserted through a query rather than
 * read off the code.
 * <p>
 * Resolution runs before the rewrite, so a name a function hands back is a name the rewrite still sees, and can
 * therefore be a dataset. Run the two the other way round -- which is how this branch had them until the move --
 * and the rewrite has finished by the time the function is asked, so the same name comes back as an unknown index
 * and a function could never return a dataset at all. Nothing else in the tree notices the difference: every other test of this facility
 * returns rows or an index, and the whole dataset suite names its datasets in the query.
 * <p>
 * Single-node, as every dataset IT is: the subject is the resolution order, not cluster-state propagation.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SourceFunctionBeforeDatasetRewriteIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;

    /**
     * Hands back whatever name it was given, as an index pattern and nothing more. The facility's other arm --
     * a function that builds its own rows -- cannot show this, because rows are already final and the rewrite
     * has nothing to do to them.
     */
    private static final class Named implements SourceFunction {

        @Override
        public String name() {
            return "named";
        }

        @Override
        public SourceFunctionSignature signature() {
            return SourceFunctionSignature.of(SourceFunctionSignature.string("name"));
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            listener.onResponse(
                new UnresolvedRelation(
                    call.source(),
                    new IndexPattern(call.source(), call.string(0)),
                    false,
                    metadataFields,
                    null,
                    SourceCommand.FROM
                )
            );
        }
    }

    /**
     * The datasource SPI needs {@code loadExtensions} delegated, as the base class's own plugin does, and the test
     * function needs registering -- and both are an {@code EsqlPlugin}, of which a node loads one.
     */
    public static final class EsqlWithDatasourcesAndTestSourceFunction extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }

        @Override
        protected List<SourceFunction> sourceFunctions() {
            return List.of(new Named());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlWithDatasourcesAndTestSourceFunction.class);
        return plugins;
    }

    /** Determinism over planner diversity: these tests turn on which pass saw the name, not on plan shape. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireSourceFunctions() {
        assumeTrue("source functions are snapshot-only", EsqlCapabilities.Cap.FROM_SOURCE_FUNCTION.isEnabled());
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("source-function-dataset-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
    }

    /**
     * The control, and it is not decoration: it is what separates "the fixture does not read" from "the function's
     * name never reached the rewrite". Both arms of this test fail together without it and name nothing.
     */
    public void testTheDatasetReadsWhenTheQueryNamesItDirectly() {
        registerDataSource("sf_ds", Map.of());
        registerDataset("sf_employees", "sf_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM sf_employees | SORT emp_no"), TIMEOUT)) {
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(0).get(1).toString(), equalTo("Alice"));
        }
    }

    /** And the same dataset reads when a function is what names it, which is only true in this order. */
    public void testADatasetNamedByAFunctionIsRewrittenAndRead() {
        registerDataSource("sf_ds", Map.of());
        registerDataset("sf_employees", "sf_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM named(\"sf_employees\") | SORT emp_no"), TIMEOUT)) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), equalTo(List.of("emp_no", "first_name")));
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(0).get(0), equalTo(1));
            assertThat(getValuesList(response).get(0).get(1).toString(), equalTo("Alice"));
            assertThat(getValuesList(response).get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    /** The rest of the query reaches those rows, so what came back is a relation and not an endpoint. */
    public void testADatasetNamedByAFunctionComposesWithTheLanguage() {
        registerDataSource("sf_ds", Map.of());
        registerDataset("sf_employees", "sf_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM named(\"sf_employees\") | WHERE emp_no > 1 | STATS c = COUNT(*)"), TIMEOUT)) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(2L));
        }
    }

    /**
     * A dataset beside an index in one source list, both named by a function and by the query. The rewrite walks
     * the whole list once, so a function's leaf has to be in it by then rather than arriving afterwards.
     */
    public void testADatasetNamedByAFunctionBesideAnIndex() {
        registerDataSource("sf_ds", Map.of());
        registerDataset("sf_employees", "sf_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
        assertAcked(client().admin().indices().prepareCreate("sf_index").setMapping("first_name", "type=keyword"));
        client().prepareIndex("sf_index").setSource("first_name", "Dave").setRefreshPolicy(IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("FROM sf_index, named(\"sf_employees\") | STATS c = COUNT(*)"), TIMEOUT)) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(4L));
        }
    }
}
