/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetDefinitionValidator;
import org.elasticsearch.xpack.esql.tablefunction.CoordinatorRowsFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionContext;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The table function facility driven by a query, with a function that exists only here.
 * <p>
 * Everything else covering this facility stops at a plan: the parser tests stop at the unresolved call, the
 * resolver tests start from a hand-built one. Neither notices if the resolver stops being called at all —
 * deleting its invocation from {@code EsqlSession} leaves every one of them green, which is what this exists to
 * catch. It is also the only place the analyzer sees a function's relation: beside an index pattern under a
 * union, and under a {@code METADATA} clause whose names the leaf does not carry.
 * <p>
 * The function is defined in the test rather than being one that ships, because that is the only way to show the
 * shared path was not shaped by a real function's needs.
 */
public class TableFunctionFacilityIT extends AbstractEsqlIntegTestCase {

    /** Registers the test function in place of this node's set, so the facility stands alone in these tests. */
    public static final class EsqlWithTestTableFunction extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected List<TableFunction> tableFunctions(Client client, DatasetDefinitionValidator datasetDefinitionValidator) {
            return List.of(new Letters());
        }
    }

    /**
     * One required argument, one {@code KEYWORD} column, one row per character of the argument. Deliberately
     * argument-dependent in both its rows and its row count, so a test can tell the argument reached it.
     */
    private static final class Letters extends CoordinatorRowsFunction {

        @Override
        public String name() {
            return "letters";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("of"));
        }

        @Override
        protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
            return List.of(column("letter", DataType.KEYWORD));
        }

        @Override
        protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
            computed(listener, () -> {
                List<List<Object>> rows = new ArrayList<>();
                for (char c : call.name(0).toCharArray()) {
                    rows.add(List.of(String.valueOf(c)));
                }
                return rows;
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return CollectionUtils.appendToCopy(plugins, EsqlWithTestTableFunction.class);
    }

    @Before
    public void checkCapability() {
        assumeTrue("table functions are snapshot-only", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    /** The whole path: a parsed query, a registered function, rows out. */
    public void testAFunctionInAQueryReturnsItsRows() {
        try (EsqlQueryResponse response = run("FROM letters(\"abc\")")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("letter"));
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(0).get(0), equalTo("a"));
        }
    }

    /** The argument reaches the function, rather than the function answering the same way regardless. */
    public void testTheArgumentDecidesTheRows() {
        try (EsqlQueryResponse response = run("FROM letters(\"ab\")")) {
            assertThat(getValuesList(response), hasSize(2));
        }
        try (EsqlQueryResponse response = run("FROM letters(\"abcde\")")) {
            assertThat(getValuesList(response), hasSize(5));
        }
    }

    /** The rest of the language reaches the rows, which is the point of a relation rather than an endpoint. */
    public void testTheRowsComposeWithTheLanguage() {
        try (EsqlQueryResponse response = run("FROM letters(\"abcabc\") | STATS n = COUNT(*) BY letter | SORT letter")) {
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(0).get(0), equalTo(2L));
        }
    }

    /** A source list is heterogeneous, so a function's relation has to survive the analyzer beside an index. */
    public void testAFunctionBesideAnIndexPattern() {
        assertAcked(client().admin().indices().prepareCreate("facility_index").setMapping("letter", "type=keyword"));
        client().prepareIndex("facility_index").setSource("letter", "z").setRefreshPolicy(IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM facility_index, letters(\"ab\") | SORT letter")) {
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(2).get(0), equalTo("z"));
        }
    }

    /**
     * A leaf that binds no metadata still answers a METADATA clause, because the analyzer fills a name absent
     * from a leaf's output with null. {@code CoordinatorRowsFunction} leans on that rather than binding metadata
     * itself, and nothing else in the tree exercises the claim over a table function.
     */
    public void testMetadataOverAFunctionIsNullFilled() {
        try (EsqlQueryResponse response = run("FROM letters(\"ab\") METADATA _index | KEEP letter, _index")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("letter", "_index"));
            assertThat(getValuesList(response), hasSize(2));
            assertThat(getValuesList(response).get(0).get(1), equalTo(null));
        }
    }

    /** An unknown name is answered by the facility, at the call, rather than read as a missing index. */
    public void testAnUnknownFunctionNamesTheOnesThatExist() {
        var e = expectThrows(Exception.class, () -> run("FROM nosuchfunction(\"a\")").close());
        assertThat(e.getMessage(), containsString("unknown table function [nosuchfunction]"));
        assertThat(e.getMessage(), containsString("letters"));
    }
}
