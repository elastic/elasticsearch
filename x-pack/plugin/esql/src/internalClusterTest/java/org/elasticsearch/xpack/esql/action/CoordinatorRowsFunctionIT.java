/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationBuilder;
import org.elasticsearch.xpack.esql.tablefunction.CoordinatorRowsFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * A function written on this base class, driven by a real query.
 * <p>
 * The unit tests reach {@code toRelation} directly and stop at the page. This is the only place a table the base
 * class built is read back through the engine — the columns it declared become the response's columns, the values
 * it appended become rows, and a cell it left empty arrives as null rather than as text.
 */
public class CoordinatorRowsFunctionIT extends AbstractEsqlIntegTestCase {

    public static final class EsqlWithCoordinatorRowsFunction extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected List<TableFunction> tableFunctions(Client client) {
            return List.of(new Sizes());
        }
    }

    /**
     * One row per character of its argument, with a column of each type the table can declare, so the engine is
     * made to read back more than text. The last row leaves every nullable column empty.
     */
    private static final class Sizes extends CoordinatorRowsFunction {

        @Override
        public String name() {
            return "sizes";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("of"));
        }

        @Override
        protected void write(
            TableFunctionInvocation call,
            ProjectMetadata projectMetadata,
            LocalRelationBuilder rows,
            ActionListener<Void> listener
        ) {
            computed(listener, out -> {
                LocalRelationBuilder.Keyword letter = out.keyword("letter");
                LocalRelationBuilder.Longs position = out.longs("position");
                LocalRelationBuilder.Doubles half = out.doubles("half");
                LocalRelationBuilder.Booleans vowel = out.booleans("vowel");

                char[] letters = call.name(0).toCharArray();
                for (int i = 0; i < letters.length; i++) {
                    letter.append(String.valueOf(letters[i]));
                    position.append(i);
                    half.append(i / 2.0);
                    vowel.append("aeiou".indexOf(letters[i]) >= 0);
                }
                // One row of empties, so a null cell is read back as null rather than as the word.
                letter.appendNull();
                position.appendNull();
                half.appendNull();
                vowel.appendNull();
            }, rows);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return CollectionUtils.appendToCopy(plugins, EsqlWithCoordinatorRowsFunction.class);
    }

    @Before
    public void checkCapability() {
        assumeTrue("table functions are snapshot-only", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    /** The declared columns are the response's columns, in the order they were declared. */
    public void testTheDeclaredColumnsAreTheResponseColumns() {
        try (EsqlQueryResponse response = run("FROM sizes(\"ab\")")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("letter", "position", "half", "vowel"));
            assertThat(
                response.columns().stream().map(c -> c.type().typeName()).toList(),
                contains("keyword", "long", "double", "boolean")
            );
        }
    }

    /** Each handle's values arrive as that column's type, not as the text of one. */
    public void testEachColumnArrivesAsItsOwnType() {
        try (EsqlQueryResponse response = run("FROM sizes(\"ab\") | WHERE letter == \"b\"")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("b"));
            assertThat(rows.get(0).get(1), equalTo(1L));
            assertThat(rows.get(0).get(2), equalTo(0.5));
            assertThat(rows.get(0).get(3), equalTo(false));
        }
    }

    /** An empty cell is an empty cell all the way out. */
    public void testAnEmptyCellIsNull() {
        try (EsqlQueryResponse response = run("FROM sizes(\"ab\") | WHERE letter IS NULL")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), nullValue());
            assertThat(rows.get(0).get(1), nullValue());
            assertThat(rows.get(0).get(2), nullValue());
            assertThat(rows.get(0).get(3), nullValue());
        }
    }

    /** The numbers are numbers: an aggregate over them works without a cast. */
    public void testTheLanguageComputesOverTheColumns() {
        try (EsqlQueryResponse response = run("FROM sizes(\"abcd\") | STATS total = SUM(position), vowels = COUNT(*) WHERE vowel")) {
            var rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(6L));
            assertThat(rows.get(0).get(1), equalTo(1L));
        }
    }

    /** A function of this shape stands beside an index pattern like any other relation. */
    public void testBesideAnIndexPattern() {
        assertAcked(prepareCreate("letters-idx").setMapping("letter", "type=keyword"));
        prepareIndex("letters-idx").setSource("letter", "z").setRefreshPolicy(IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM letters-idx, sizes(\"a\") | WHERE letter IS NOT NULL | KEEP letter | SORT letter")) {
            assertThat(getValuesList(response).stream().map(row -> row.get(0)).toList(), contains("a", "z"));
        }
    }
}
