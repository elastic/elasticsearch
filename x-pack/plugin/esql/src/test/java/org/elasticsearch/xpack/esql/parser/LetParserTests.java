/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class LetParserTests extends AbstractStatementParserTests {

    private void assumeLet() {
        assumeTrue("requires NAMED_SUBQUERY_LET capability", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
    }

    // -----------------------------------------------------------------------
    // Basic parsing
    // -----------------------------------------------------------------------

    public void testSingleBinding() {
        assumeLet();
        EsqlStatement stmt = statement("LET top3 = (FROM logs | LIMIT 3); ROW a = 1");
        assertThat(stmt.plan(), instanceOf(Row.class));
        assertThat(stmt.letBindings().size(), is(1));
        LetBinding b = stmt.letBindings().get(0);
        assertThat(b.name(), is("top3"));
        // The body is a Limit plan wrapping an UnresolvedRelation
        assertThat(b.plan(), instanceOf(Limit.class));
    }

    public void testMultipleBindings() {
        assumeLet();
        EsqlStatement stmt = statement("""
            LET a = (FROM idx1 | LIMIT 3),
                b = (FROM idx2 | LIMIT 5);
            ROW x = 1
            """);
        assertThat(stmt.letBindings().size(), is(2));
        assertThat(stmt.letBindings().get(0).name(), is("a"));
        assertThat(stmt.letBindings().get(1).name(), is("b"));
    }

    public void testQuotedBindingNameRejected() {
        // letBinding now uses UNQUOTED_IDENTIFIER directly; quoted names are rejected by the grammar.
        assumeLet();
        expectValidationError("LET `my binding` = (FROM idx | LIMIT 1); ROW a = 1", "mismatched input");
    }

    public void testBindingWithStatsAndSort() {
        assumeLet();
        EsqlStatement stmt = statement("""
            LET top3_ext = (
                FROM kibana_sample_data_logs
                | STATS AVG(bytes) BY extension
                | SORT `AVG(bytes)` DESC
                | LIMIT 3
                | KEEP extension
            );
            FROM top3_ext
            """);
        assertThat(stmt.letBindings().size(), is(1));
        assertThat(stmt.letBindings().get(0).name(), is("top3_ext"));
        // The main plan is an UnresolvedRelation (not yet substituted — that happens in LetResolver)
        assertThat(stmt.plan(), instanceOf(UnresolvedRelation.class));
        UnresolvedRelation ur = (UnresolvedRelation) stmt.plan();
        assertThat(ur.indexPattern().indexPattern(), is("top3_ext"));
    }

    public void testBindingWithForkFollowedByCommand() {
        assumeLet();
        assumeTrue("requires FORK_V9 capability", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        EsqlStatement stmt = statement("""
            LET branched = (
                FROM idx
                | FORK (WHERE a > 0) (WHERE a <= 0)
                | LIMIT 10
            );
            FROM branched
            """);
        assertThat(stmt.letBindings().size(), is(1));
        assertThat(stmt.letBindings().get(0).name(), is("branched"));
        assertThat(stmt.letBindings().get(0).plan(), instanceOf(Limit.class));
        assertThat(((Limit) stmt.letBindings().get(0).plan()).child(), instanceOf(Fork.class));
    }

    public void testBindingWithForkAsLastCommand() {
        assumeLet();
        assumeTrue("requires FORK_V9 capability", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        EsqlStatement stmt = statement("""
            LET branched = (
                FROM idx
                | FORK (WHERE a > 0) (WHERE a <= 0)
            );
            FROM branched
            """);
        assertThat(stmt.letBindings().size(), is(1));
        assertThat(stmt.letBindings().get(0).name(), is("branched"));
        assertThat(stmt.letBindings().get(0).plan(), instanceOf(Fork.class));
    }

    public void testChainedLetWithForks() {
        assumeLet();
        assumeTrue("requires FORK_V9 capability", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        EsqlStatement stmt = statement("""
            LET
            top3_extensions = (
               FROM kibana_sample_data_logs
                  | STATS AVG(bytes) BY extension
                  | SORT `AVG(bytes)` DESC
                  | LIMIT 3
                  | KEEP extension
            ),
            first_column = (
               FROM kibana_sample_data_logs
                  | FORK (WHERE extension IN (FROM top3_extensions))
                         (WHERE extension NOT IN (FROM top3_extensions)
                             | EVAL extension = "other")
            ),
            top3_geo_dest_by_ext = (
               FROM first_column
                  | STATS AVG(bytes) BY extension, geo.dest
                  | SORT `AVG(bytes)` DESC
                  | LIMIT 3 BY extension
                  | KEEP extension, geo.dest
            ),
            first_and_second_column = (
              FROM top3_geo_dest_by_ext
                  | FORK (WHERE (extension, geo.dest) IN (FROM top3_geo_dest_by_ext))
                         (WHERE (extension, geo.dest) NOT IN (FROM top3_geo_dest_by_ext)
                             | EVAL geo.dest = "other"::keyword)
            );
            FROM first_and_second_column
            | STATS AVG(bytes) BY extension, geo.dest
            """);
        assertThat(stmt.letBindings().size(), is(4));
        assertThat(stmt.letBindings().get(0).name(), is("top3_extensions"));
        assertThat(stmt.letBindings().get(1).name(), is("first_column"));
        assertThat(stmt.letBindings().get(2).name(), is("top3_geo_dest_by_ext"));
        assertThat(stmt.letBindings().get(3).name(), is("first_and_second_column"));
    }

    // -----------------------------------------------------------------------
    // IN operand forms — a LET name is referenced through an explicit FROM subquery,
    // so it parses to an InSubquery / MultiColumnInSubquery over an UnresolvedRelation
    // carrying the binding name.
    // -----------------------------------------------------------------------

    /** {@code x IN (FROM name)} — single-column form */
    public void testInFromBindingForm() {
        assumeLet();
        EsqlStatement stmt = statement("LET top3 = (FROM idx | LIMIT 3); FROM src | WHERE ext IN (FROM top3)");
        assertThat(stmt.letBindings().size(), is(1));
        Filter filter = as(stmt.plan(), Filter.class);
        InSubquery inSub = as(filter.condition(), InSubquery.class);
        UnresolvedRelation ur = as(inSub.subquery(), UnresolvedRelation.class);
        assertThat(ur.indexPattern().indexPattern(), is("top3"));
    }

    /** {@code x NOT IN (FROM name)} — single-column negated form */
    public void testNotInFromBindingForm() {
        assumeLet();
        EsqlStatement stmt = statement("LET top3 = (FROM idx | LIMIT 3); FROM src | WHERE ext NOT IN (FROM top3)");
        Filter filter = as(stmt.plan(), Filter.class);
        // NOT wraps the InSubquery
        assertThat(filter.condition().children().get(0), instanceOf(InSubquery.class));
    }

    /** {@code (a, b) IN (FROM name)} — multi-column form */
    public void testInMultiColumnFromBindingForm() {
        assumeLet();
        EsqlStatement stmt = statement("LET tbl = (FROM idx | LIMIT 3); FROM src | WHERE (ext, geo) IN (FROM tbl)");
        Filter filter = as(stmt.plan(), Filter.class);
        MultiColumnInSubquery inSub = as(filter.condition(), MultiColumnInSubquery.class);
        UnresolvedRelation ur = as(inSub.subquery(), UnresolvedRelation.class);
        assertThat(ur.indexPattern().indexPattern(), is("tbl"));
    }

    /** The bare {@code x IN name} form is not part of the grammar: a subquery must be written with FROM. */
    public void testBareInBindingRejected() {
        assumeLet();
        expectValidationError("LET top3 = (FROM idx | LIMIT 3); FROM src | WHERE ext IN top3", "no viable alternative");
        expectValidationError("LET tbl = (FROM idx | LIMIT 3); FROM src | WHERE (ext, geo) IN tbl", "mismatched input 'tbl' expecting '('");
    }

    /** {@code x IN (name)} stays a plain single-element value list (folded to equality), even when name is a LET binding. */
    public void testInParenthesisedNameIsValueList() {
        assumeLet();
        EsqlStatement stmt = statement("LET top3 = (FROM idx | LIMIT 3); FROM src | WHERE ext IN (top3)");
        Filter filter = as(stmt.plan(), Filter.class);
        assertThat(filter.condition(), instanceOf(Equals.class));
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    public void testDuplicateBindingName() {
        assumeLet();
        expectValidationError("LET a = (FROM idx1 | LIMIT 1), a = (FROM idx2 | LIMIT 1); ROW x = 1", "duplicate LET binding name [a]");
    }

    public void testBindingNameWithStar() {
        assumeLet();
        expectValidationError("LET `a*b` = (FROM idx | LIMIT 1); ROW x = 1", "mismatched input");
    }

    public void testBindingNameWithComma() {
        assumeLet();
        expectValidationError("LET `a,b` = (FROM idx | LIMIT 1); ROW x = 1", "mismatched input");
    }

    public void testBindingNameWithColon() {
        assumeLet();
        expectValidationError("LET `a:b` = (FROM idx | LIMIT 1); ROW x = 1", "mismatched input");
    }

    public void testBindingNameWithDot() {
        assumeLet();
        expectValidationError("LET `a.b` = (FROM idx | LIMIT 1); ROW x = 1", "mismatched input");
    }

    public void testLetInsideViewBodyParsed() {
        assumeLet();
        // parseView is called by EsqlParser, not the test parser. Verify that LET is accepted
        // in a view body and that the parsed statement carries the bindings.
        var parser = new EsqlParser(new EsqlConfig(org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY));
        var stmt = parser.parseView(
            "LET x = (FROM a | LIMIT 1); FROM x",
            new QueryParams(),
            new org.elasticsearch.xpack.esql.inference.InferenceSettings(org.elasticsearch.common.settings.Settings.EMPTY),
            "my_view"
        );
        assertThat(stmt.letBindings().size(), is(1));
        assertThat(stmt.letBindings().get(0).name(), is("x"));
    }

    // -----------------------------------------------------------------------
    // Query parameters inside LET binding bodies
    // -----------------------------------------------------------------------

    /** Named query parameters inside a LET binding body are substituted at parse time. */
    public void testLetBindingWithQueryParam() {
        assumeLet();
        var params = new QueryParams(List.of(paramAsConstant("threshold", 10020)));
        EsqlStatement stmt = statement("LET top = (FROM employees | WHERE emp_no > ?threshold | LIMIT 3);\nFROM top | SORT emp_no", params);
        assertThat(stmt.letBindings().size(), is(1));
        LetBinding binding = stmt.letBindings().get(0);
        assertThat(binding.name(), is("top"));
        // body: Limit(Filter(UnresolvedRelation("employees")))
        LogicalPlan body = binding.plan();
        assertThat(body, instanceOf(Limit.class));
        Filter filter = as(((Limit) body).child(), Filter.class);
        GreaterThan gt = as(filter.condition(), GreaterThan.class);
        assertThat(gt.right().fold(FoldContext.small()), equalTo(10020));
    }

    // -----------------------------------------------------------------------
    // Snapshot gating: `let` must be invisible in production builds
    // -----------------------------------------------------------------------

    public void testLetInvisibleInProductionBuild() {
        // Only meaningful when we can actually disable dev mode.
        assumeTrue("requires snapshot builds to disable dev mode", Build.current().isSnapshot());

        EsqlConfig prodConfig = new EsqlConfig(false, org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY);
        EsqlParser prodParser = new EsqlParser(prodConfig);

        ParsingException pe = expectThrows(ParsingException.class, () -> prodParser.createStatement("LET x = (FROM a | LIMIT 1); FROM x"));
        assertThat(pe.getMessage(), containsString("mismatched input 'LET'"));
        // The DEV_ prefix must have been stripped from the error message.
        assertThat(pe.getMessage(), not(containsString("DEV_")));
    }
}
