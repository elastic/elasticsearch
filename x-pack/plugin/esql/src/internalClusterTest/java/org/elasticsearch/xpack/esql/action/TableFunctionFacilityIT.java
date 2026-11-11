/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
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

    /** Registers the test function in place of this node's set, which on this branch is empty anyway. */
    public static final class EsqlWithTestTableFunction extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected List<TableFunction> tableFunctions(Client client) {
            return List.of(new Letters(), new Indexed());
        }
    }

    /**
     * One required argument, one {@code KEYWORD} column, one row per character of it — argument-dependent in both
     * its rows and its row count, so a test can tell the argument reached it. It builds its leaf by hand, which is
     * all the facility asks for: {@code toRelation} completes with any {@link LeafPlan}.
     */
    private static final class Letters implements TableFunction {

        @Override
        public String name() {
            return "letters";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("of"));
        }

        @Override
        public void toRelation(
            TableFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            char[] letters = call.name(0).toCharArray();
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = PlannerUtils.NON_BREAKING_BLOCK_FACTORY.newBytesRefBlockBuilder(letters.length)) {
                for (char c : letters) {
                    builder.appendBytesRef(new BytesRef(String.valueOf(c)));
                }
                block = builder.build();
            }
            listener.onResponse(
                new LocalRelation(
                    call.source(),
                    List.of(new ReferenceAttribute(Source.EMPTY, "letter", DataType.KEYWORD)),
                    LocalSupplier.of(new Page(letters.length, block))
                )
            );
        }
    }

    /**
     * Returns an <i>unresolved</i> leaf over the pattern it was handed, which the ordinary analyzer then resolves
     * into an index read. This is the contract's second arm — a function whose columns are only knowable by
     * looking returns an unresolved relation rather than one it built itself — and nothing else in the tree takes
     * it, so without this the arm is documented and unreached.
     * <p>
     * It also pins where the facility sits in the pipeline, which no other test here does. Resolution runs before
     * pre-analysis, and pre-analysis is where a source leaf's fields are collected; move the resolver after it and
     * this leaf arrives at the analyzer still unresolved, while every function that builds its own relation goes
     * on passing.
     */
    private static final class Indexed implements TableFunction {

        @Override
        public String name() {
            return "indexed";
        }

        @Override
        public TableFunctionSignature signature() {
            return TableFunctionSignature.of(TableFunctionSignature.name("pattern"));
        }

        @Override
        public void toRelation(
            TableFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            // The METADATA names are threaded on rather than dropped, which is what lets this leaf bind _index
            // where Letters leaves it to be null-filled.
            listener.onResponse(
                new UnresolvedRelation(
                    call.source(),
                    new IndexPattern(call.source(), call.name(0)),
                    false,
                    metadataFields,
                    null,
                    SourceCommand.FROM
                )
            );
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
     * from a leaf's output with null. A function that binds no metadata leans on that, and nothing else in the
     * tree exercises the claim over a table function.
     */
    public void testMetadataOverAFunctionIsNullFilled() {
        try (EsqlQueryResponse response = run("FROM letters(\"ab\") METADATA _index | KEEP letter, _index")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("letter", "_index"));
            assertThat(getValuesList(response), hasSize(2));
            assertThat(getValuesList(response).get(0).get(1), equalTo(null));
        }
    }

    /**
     * One call read by several consumers. FORK hands every branch the one plan above it, so a call written once
     * ends up under each branch, and the relation the function returned is read once per branch.
     * <p>
     * A relation holding a page cannot survive that on its own: the page is handed downstream and released by the
     * first reader, and the second finds it gone. The engine already meets this wherever a {@code LocalRelation}
     * is read twice -- {@code InlineJoin} and {@code Approximation} both rewrite one to copy per read -- and this
     * is the same situation reached by a different route.
     */
    public void testOneCallReadByEveryForkBranch() {
        try (
            EsqlQueryResponse response = run(
                "FROM letters(\"abc\") | FORK (WHERE letter == \"a\") (WHERE letter == \"b\") | STATS c = COUNT(*)"
            )
        ) {
            assertThat(getValuesList(response), hasSize(1));
            assertThat(getValuesList(response).get(0).get(0), equalTo(2L));
        }
    }

    /** An unknown name is answered by the facility, at the call, rather than read as a missing index. */
    public void testAnUnknownFunctionNamesTheOnesThatExist() {
        var e = expectThrows(ParsingException.class, () -> run("FROM nosuchfunction(\"a\")").close());
        assertThat(e.getMessage(), containsString("unknown table function [nosuchfunction]"));
        assertThat(e.getMessage(), containsString("letters"));
    }

    /**
     * The contract's unresolved arm, end to end: the function returns a leaf it has not resolved and the rows come
     * back from the index, so the analyzer resolved it rather than the function answering with rows of its own.
     * <p>
     * The row count is randomized because a fixed one does not discriminate — a function returning a relation it
     * built by hand satisfies an assertion on rows it could have hardcoded, which is exactly the arm this is here
     * to tell apart. A count chosen per run cannot be answered without looking.
     */
    public void testAFunctionMayReturnAnUnresolvedLeaf() {
        int docs = randomIntBetween(2, 8);
        assertAcked(client().admin().indices().prepareCreate("unresolved_leaf_index").setMapping("letter", "type=keyword"));
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("unresolved_leaf_index").setSource("letter", "y").setRefreshPolicy(IMMEDIATE).get();
        }

        try (EsqlQueryResponse response = run("FROM indexed(\"unresolved_leaf_index\")")) {
            assertThat(response.columns().stream().map(c -> c.name()).toList(), contains("letter"));
            assertThat(getValuesList(response), hasSize(docs));
            assertThat(getValuesList(response).get(0).get(0), equalTo("y"));
        }
    }

    /**
     * The other half of what {@link #testMetadataOverAFunctionIsNullFilled} claims. The METADATA names reach every
     * leaf in the source list; a leaf that carries them answers with the real value, so the null there is the
     * analyzer filling a name that leaf does not bind rather than the names never arriving.
     */
    public void testMetadataOverAnUnresolvedLeafIsBoundNotNullFilled() {
        assertAcked(client().admin().indices().prepareCreate("metadata_leaf_index").setMapping("letter", "type=keyword"));
        client().prepareIndex("metadata_leaf_index").setSource("letter", "y").setRefreshPolicy(IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM indexed(\"metadata_leaf_index\") METADATA _index | KEEP letter, _index")) {
            assertThat(getValuesList(response), hasSize(1));
            assertThat(getValuesList(response).get(0).get(1), equalTo("metadata_leaf_index"));
        }
    }

    /**
     * Both arms in one source list. A function describes one leaf and never assumes it is alone, so a relation a
     * function built and a relation the analyzer resolved have to survive beside each other — and the two calls
     * bind concurrently, each into its own slot.
     */
    public void testBothArmsOfTheContractInOneSourceList() {
        assertAcked(client().admin().indices().prepareCreate("both_arms_index").setMapping("letter", "type=keyword"));
        client().prepareIndex("both_arms_index").setSource("letter", "y").setRefreshPolicy(IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM letters(\"ab\"), indexed(\"both_arms_index\") | SORT letter")) {
            assertThat(getValuesList(response), hasSize(3));
            assertThat(getValuesList(response).get(0).get(0), equalTo("a"));
            assertThat(getValuesList(response).get(2).get(0), equalTo("y"));
        }
    }
}
