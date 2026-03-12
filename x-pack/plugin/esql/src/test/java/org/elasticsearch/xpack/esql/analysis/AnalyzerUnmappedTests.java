/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class AnalyzerUnmappedTests extends ESTestCase {

    public void testFailKeepAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailKeepAndMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailAfterKeep() {
        var query = """
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """;
        var failure = "Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropWithNonMatchingStar() {
        var query = """
            FROM test
            | DROP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropWithMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | DROP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailEvalAfterDrop() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """;

        var failure = "3:12: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropThenKeep() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailDropThenEval() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailEvalThenDropThenEval() {
        var query = """
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field::LONG + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field::LONG + 2
            """;
        var failure = "line 6:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenKeep() {
        var query = """
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenEval() {
        var query = """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """;
        var failure = "line 3:12: Unknown column [does_not_exist_field]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailStatsThenEvalExistingField() {
        var query = """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = emp_no
            """;
        var failure = "line 3:12: Unknown column [emp_no]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM employees
                 | STATS c = COUNT(*))
            | SORT does_not_exist
            """;
        var failure = "line 4:8: Unknown column [does_not_exist]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    // same tree as above, except for the source nodes

    public void testFailSubquerysWithNoMainAndStatsOnlyNullify() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Fork[[_meta_field{r}#103, emp_no{r}#104, first_name{r}#105, gender{r}#106, hire_date{r}#107, job{r}#108, job.raw{r}#109,
     *      languages{r}#110, last_name{r}#111, long_noidx{r}#112, salary{r}#113, does_not_exist1{r}#114, does_not_exist2{r}#115,
     *      does_not_exist3{r}#116, does_not_exist2 IS NULL{r}#117, _fork{r}#118, does_not_exist4{r}#119, xyz{r}#120, x{r}#121, y{r}#122]]
     *   |_Limit[10000[INTEGER],false,false]
     *   | \_Project[[_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, gender{f}#31, hire_date{f}#36, job{f}#37, job.raw{f}#38,
     *          languages{f}#32, last_name{f}#33, long_noidx{f}#39, salary{f}#34, does_not_exist1{r}#62, does_not_exist2{r}#68,
     *          does_not_exist3{r}#74, does_not_exist2 IS NULL{r}#6, _fork{r}#9, does_not_exist4{r}#80, xyz{r}#81, x{r}#82, y{r}#83]]
     *   |   \_Eval[[null[NULL] AS does_not_exist4#80, null[KEYWORD] AS xyz#81, null[DOUBLE] AS x#82, null[DOUBLE] AS y#83]]
     *   |     \_Eval[[fork1[KEYWORD] AS _fork#9]]
     *   |       \_Limit[7[INTEGER],false,false]
     *   |         \_OrderBy[[Order[does_not_exist3{r}#74,ASC,LAST]]]
     *   |           \_Filter[emp_no{f}#29 > 3[INTEGER]]
     *   |             \_Eval[[ISNULL(does_not_exist2{r}#68) AS does_not_exist2 IS NULL#6]]
     *   |               \_Filter[first_name{f}#30 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#62) > 5[INTEGER]]
     *   |                 \_Eval[[null[NULL] AS does_not_exist1#62, null[NULL] AS does_not_exist2#68, null[NULL] AS does_not_exist3#74]]
     *   |                   \_EsRelation[test][_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, ..]
     *   |_Limit[1000[INTEGER],false,false]
     *   | \_Project[[_meta_field{f}#46, emp_no{f}#40, first_name{f}#41, gender{f}#42, hire_date{f}#47, job{f}#48, job.raw{f}#49,
     *          languages{f}#43, last_name{f}#44, long_noidx{f}#50, salary{f}#45, does_not_exist1{r}#64, does_not_exist2{r}#70,
     *          does_not_exist3{r}#84, does_not_exist2 IS NULL{r}#6, _fork{r}#9, does_not_exist4{r}#76, xyz{r}#21, x{r}#85, y{r}#86]]
     *   |   \_Eval[[null[NULL] AS does_not_exist3#84, null[DOUBLE] AS x#85, null[DOUBLE] AS y#86]]
     *   |     \_Eval[[fork2[KEYWORD] AS _fork#9]]
     *   |       \_Eval[[TOSTRING(does_not_exist4{r}#76) AS xyz#21]]
     *   |         \_Filter[emp_no{f}#40 > 2[INTEGER]]
     *   |           \_Eval[[ISNULL(does_not_exist2{r}#70) AS does_not_exist2 IS NULL#6]]
     *   |             \_Filter[first_name{f}#41 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#64) > 5[INTEGER]]
     *   |               \_Eval[[null[NULL] AS does_not_exist1#64, null[NULL] AS does_not_exist2#70, null[NULL] AS does_not_exist4#76]]
     *   |                 \_EsRelation[test][_meta_field{f}#46, emp_no{f}#40, first_name{f}#41, ..]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Project[[_meta_field{r}#87, emp_no{r}#88, first_name{r}#89, gender{r}#90, hire_date{r}#91, job{r}#92, job.raw{r}#93,
     *          languages{r}#94, last_name{r}#95, long_noidx{r}#96, salary{r}#97, does_not_exist1{r}#98, does_not_exist2{r}#99,
     *          does_not_exist3{r}#100, does_not_exist2 IS NULL{r}#101, _fork{r}#9, does_not_exist4{r}#102, xyz{r}#27, x{r}#13, y{r}#16]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#87, null[INTEGER] AS emp_no#88, null[KEYWORD] AS first_name#89, null[TEXT] AS gender#90,
     *              null[DATETIME] AS hire_date#91, null[TEXT] AS job#92, null[KEYWORD] AS job.raw#93, null[INTEGER] AS languages#94,
     *              null[KEYWORD] AS last_name#95, null[LONG] AS long_noidx#96, null[INTEGER] AS salary#97,
     *              null[NULL] AS does_not_exist1#98, null[NULL] AS does_not_exist2#99, null[NULL] AS does_not_exist3#100,
     *              null[BOOLEAN] AS does_not_exist2 IS NULL#101, null[NULL] AS does_not_exist4#102]]
     *         \_Eval[[fork3[KEYWORD] AS _fork#9]]
     *           \_Eval[[abc[KEYWORD] AS xyz#27]]
     *             \_Aggregate[[],[MIN(TODOUBLE(d{r}#22),true[BOOLEAN],PT0S[TIME_DURATION]) AS x#13,
     *                  FilteredExpression[MAX(TODOUBLE(e{r}#23),true[BOOLEAN],PT0S[TIME_DURATION]),
     *                  TODOUBLE(d{r}#22) > 1000[INTEGER] + TODOUBLE(does_not_exist5{r}#78)] AS y#16]]
     *               \_Dissect[first_name{f}#52,Parser[pattern=%{d} %{e} %{f}, appendSeparator=,
     *                      parser=org.elasticsearch.dissect.DissectParser@4ba4d16b],[d{r}#22, e{r}#23, f{r}#24]]
     *                 \_Eval[[ISNULL(does_not_exist2{r}#72) AS does_not_exist2 IS NULL#6]]
     *                   \_Filter[first_name{f}#52 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#66) > 5[INTEGER]]
     *                     \_Eval[[null[NULL] AS does_not_exist1#66, null[NULL] AS does_not_exist2#72, null[NULL] AS does_not_exist5#78]]
     *                       \_EsRelation[test][_meta_field{f}#57, emp_no{f}#51, first_name{f}#52, ..]
     */
    public void testForkBranchesWithDifferentSchemas() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE first_name == "Chris" AND does_not_exist1::LONG > 5
            | EVAL does_not_exist2 IS NULL
            | FORK (WHERE emp_no > 3 | SORT does_not_exist3 | LIMIT 7 )
                   (WHERE emp_no > 2 | EVAL xyz = does_not_exist4::KEYWORD )
                   (DISSECT first_name "%{d} %{e} %{f}"
                    | STATS x = MIN(d::DOUBLE), y = MAX(e::DOUBLE) WHERE d::DOUBLE > 1000 + does_not_exist5::DOUBLE
                    | EVAL xyz = "abc")
            """));

        // Top implicit limit
        var topLimit = as(plan, Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(10000));

        // Fork node
        var fork = as(topLimit.child(), Fork.class);
        assertThat(fork.children(), hasSize(3));

        // Branch 0
        var b0Limit = as(fork.children().get(0), Limit.class);
        assertThat(b0Limit.limit().fold(FoldContext.small()), is(10000));
        var b0Proj = as(b0Limit.child(), Project.class);

        // Adds dne4/xyz/x/y nulls, verify does_not_exist4 NULL
        var b0Eval4 = as(b0Proj.child(), Eval.class);
        assertThat(b0Eval4.fields(), hasSize(4));
        assertThat(as(as(b0Eval4.fields().get(0), Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)); // does_not_exist4

        // Fork label
        var b0EvalFork = as(b0Eval4.child(), Eval.class);
        var b0ForkAlias = as(b0EvalFork.fields().getFirst(), Alias.class);
        assertThat(b0ForkAlias.name(), is("_fork"));

        // Inner limit -> orderBy -> filter chain
        var b0InnerLimit = as(b0EvalFork.child(), Limit.class);
        assertThat(b0InnerLimit.limit().fold(FoldContext.small()), is(7));
        var b0OrderBy = as(b0InnerLimit.child(), OrderBy.class);
        var b0FilterEmp = b0OrderBy.child();

        // EVAL does_not_exist2 IS NULL (boolean alias present)
        var b0IsNull = as(b0FilterEmp, Filter.class);
        var b0IsNullEval = as(b0IsNull.child(), Eval.class);
        var b0IsNullAlias = as(b0IsNullEval.fields().getFirst(), Alias.class);
        assertThat(b0IsNullAlias.name(), is("does_not_exist2 IS NULL"));

        // WHERE first_name == Chris AND ToLong(does_not_exist1) > 5
        var b0Filter = as(b0IsNullEval.child(), Filter.class);
        var b0And = as(b0Filter.condition(), And.class);
        var b0RightGt = as(b0And.right(), GreaterThan.class);
        var b0RightToLong = as(b0RightGt.left(), ToLong.class);
        assertThat(Expressions.name(b0RightToLong.field()), is("does_not_exist1"));
        assertThat(as(b0RightGt.right(), Literal.class).value(), is(5));

        // Chain of Evals adding dne1/dne2/dne3 NULLs
        var b0EvalDne1 = as(b0Filter.child(), Eval.class);
        var b0EvalDne1Alias = as(b0EvalDne1.fields().get(0), Alias.class);
        assertThat(b0EvalDne1Alias.name(), is("does_not_exist1"));
        assertThat(as(b0EvalDne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b0EvalDne2Alias = as(b0EvalDne1.fields().get(1), Alias.class);
        assertThat(b0EvalDne2Alias.name(), is("does_not_exist2"));
        assertThat(as(b0EvalDne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b0EvalDne3Alias = as(b0EvalDne1.fields().get(2), Alias.class);
        assertThat(b0EvalDne3Alias.name(), is("does_not_exist3"));
        assertThat(as(b0EvalDne3Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b0Rel = as(b0EvalDne1.child(), EsRelation.class);
        assertThat(b0Rel.indexPattern(), is("test"));

        // Branch 1
        var b1Limit = as(fork.children().get(1), Limit.class);
        assertThat(b1Limit.limit().fold(FoldContext.small()), is(1000));
        var b1Proj = as(b1Limit.child(), Project.class);

        // Adds dne3,x,y NULLs at top
        var b1Eval3 = as(b1Proj.child(), Eval.class);
        assertThat(b1Eval3.fields(), hasSize(3));
        assertThat(as(as(b1Eval3.fields().get(0), Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)); // does_not_exist3

        // Fork label
        var b1EvalFork = as(b1Eval3.child(), Eval.class);
        var b1ForkAlias = as(b1EvalFork.fields().getFirst(), Alias.class);
        assertThat(b1ForkAlias.name(), is("_fork"));

        // xyz = ToString(does_not_exist4)
        var b1EvalXyz = as(b1EvalFork.child(), Eval.class);
        var b1XyzAlias = as(b1EvalXyz.fields().getFirst(), Alias.class);
        assertThat(b1XyzAlias.name(), is("xyz"));
        as(b1XyzAlias.child(), ToString.class);

        // WHERE emp_no > 2
        var b1FilterEmp = as(b1EvalXyz.child(), Filter.class);

        // EVAL does_not_exist2 IS NULL (boolean alias present)
        var b1IsNullEval = as(b1FilterEmp.child(), Eval.class);
        var b1IsNullAlias = as(b1IsNullEval.fields().getFirst(), Alias.class);
        assertThat(b1IsNullAlias.name(), is("does_not_exist2 IS NULL"));

        // WHERE first_name == Chris AND ToLong(does_not_exist1) > 5
        var b1Filter = as(b1IsNullEval.child(), Filter.class);
        var b1And = as(b1Filter.condition(), And.class);
        var b1RightGt = as(b1And.right(), GreaterThan.class);
        var b1RightToLong = as(b1RightGt.left(), ToLong.class);
        assertThat(Expressions.name(b1RightToLong.field()), is("does_not_exist1"));
        assertThat(as(b1RightGt.right(), Literal.class).value(), is(5));

        // Chain of Evals adding dne1/dne2/dne4 NULLs
        var b1EvalDne1 = as(b1Filter.child(), Eval.class);
        var b1EvalDne1Alias = as(b1EvalDne1.fields().get(0), Alias.class);
        assertThat(b1EvalDne1Alias.name(), is("does_not_exist1"));
        assertThat(as(b1EvalDne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b1EvalDne2Alias = as(b1EvalDne1.fields().get(1), Alias.class);
        assertThat(b1EvalDne2Alias.name(), is("does_not_exist2"));
        assertThat(as(b1EvalDne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b1EvalDne3Alias = as(b1EvalDne1.fields().get(2), Alias.class);
        assertThat(b1EvalDne3Alias.name(), is("does_not_exist4"));
        assertThat(as(b1EvalDne3Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var b1Rel = as(b1EvalDne1.child(), EsRelation.class);
        assertThat(b1Rel.indexPattern(), is("test"));

        // Branch 2
        var b2Limit = as(fork.children().get(2), Limit.class);
        assertThat(b2Limit.limit().fold(FoldContext.small()), is(1000));
        var b2Proj = as(b2Limit.child(), Project.class);

        // Many nulls including does_not_exist1/2/3/4
        var b2EvalNulls = as(b2Proj.child(), Eval.class);
        assertThat(b2EvalNulls.fields(), hasSize(16));
        // Spot-check presence and NULL types for does_not_exist1..4
        var b2NullNames = Expressions.names(b2EvalNulls.fields());
        assertThat(b2NullNames.contains("does_not_exist1"), is(true));
        assertThat(b2NullNames.contains("does_not_exist2"), is(true));
        assertThat(b2NullNames.contains("does_not_exist3"), is(true));
        assertThat(b2NullNames.contains("does_not_exist4"), is(true));
        // Verify their datatypes are NULL
        for (var alias : b2EvalNulls.fields()) {
            var a = as(alias, Alias.class);
            if (a.name().startsWith("does_not_exist2 IS NULL")) {
                assertThat(as(a.child(), Literal.class).dataType(), is(DataType.BOOLEAN));
            } else if (a.name().startsWith("does_not_exist")) {
                assertThat(as(a.child(), Literal.class).dataType(), is(DataType.NULL));
            }
        }

        // Fork label
        var b2EvalFork = as(b2EvalNulls.child(), Eval.class);
        var b2ForkAlias = as(b2EvalFork.fields().getFirst(), Alias.class);
        assertThat(b2ForkAlias.name(), is("_fork"));

        // xyz constant then Aggregate with FilteredExpression using does_not_exist5
        var b2EvalXyz = as(b2EvalFork.child(), Eval.class);
        var b2Agg = as(b2EvalXyz.child(), Aggregate.class);
        assertThat(b2Agg.groupings(), hasSize(0));
        assertThat(b2Agg.aggregates(), hasSize(2));
        var filteredAlias = as(b2Agg.aggregates().get(1), Alias.class);
        var filtered = as(filteredAlias.child(), FilteredExpression.class);
        as(filtered.delegate(), Max.class);
        var feCondGT = as(filtered.filter(), GreaterThan.class);
        var feCondGTAdd = as(feCondGT.right(), Add.class);
        // Right side of Add must be ToDouble(does_not_exist5)
        var dne5Convert = as(feCondGTAdd.right(), ConvertFunction.class);
        var dne5Ref = as(dne5Convert.field(), ReferenceAttribute.class);
        assertThat(dne5Ref.name(), is("does_not_exist5"));

        var dissect = as(b2Agg.child(), Dissect.class);
        var evalDne2IsNull = as(dissect.child(), Eval.class);
        var dne2IsNullAlias = as(evalDne2IsNull.fields().getFirst(), Alias.class);
        assertThat(dne2IsNullAlias.name(), is("does_not_exist2 IS NULL"));
        var filter = as(evalDne2IsNull.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        var rightGt = as(and.right(), GreaterThan.class);
        var rightToLong = as(rightGt.left(), ToLong.class);
        assertThat(Expressions.name(rightToLong.field()), is("does_not_exist1"));
        assertThat(as(rightGt.right(), Literal.class).value(), is(5));

        var evalDne1 = as(filter.child(), Eval.class);
        var dne1Alias = as(evalDne1.fields().get(0), Alias.class);
        assertThat(dne1Alias.name(), is("does_not_exist1"));
        assertThat(as(dne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var dne2Alias = as(evalDne1.fields().get(1), Alias.class);
        assertThat(dne2Alias.name(), is("does_not_exist2"));
        assertThat(as(dne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var dne3Alias = as(evalDne1.fields().get(2), Alias.class);
        assertThat(dne3Alias.name(), is("does_not_exist5"));
        assertThat(as(dne3Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var rel = as(evalDne1.child(), EsRelation.class);
        assertThat(rel.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[does_not_exist2{r}#48,ASC,LAST]]]
     *   \_Fork[[c{r}#45, _fork{r}#46, d{r}#47, does_not_exist2{r}#48]]
     *     |_Limit[1000[INTEGER],false,false]
     *     | \_Project[[c{r}#5, _fork{r}#6, d{r}#42, does_not_exist2{r}#43]]
     *     |   \_Eval[[null[DOUBLE] AS d#42, null[NULL] AS does_not_exist2#43]]
     *     |     \_Eval[[fork1[KEYWORD] AS _fork#6]]
     *     |       \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#5]]
     *     |         \_Filter[ISNULL(does_not_exist1{r}#35)]
     *     |           \_Eval[[null[NULL] AS does_not_exist1#35]]
     *     |             \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     *     \_Limit[1000[INTEGER],false,false]
     *       \_Project[[c{r}#44, _fork{r}#6, d{r}#10, does_not_exist2{r}#39]]
     *         \_Eval[[null[LONG] AS c#44]]
     *           \_Eval[[fork2[KEYWORD] AS _fork#6]]
     *             \_Aggregate[[does_not_exist2{r}#39],[AVG(salary{f}#29,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS d#10,
     *                  does_not_exist2{r}#39]]
     *               \_Filter[ISNULL(does_not_exist1{r}#37)]
     *                 \_Eval[[null[NULL] AS does_not_exist1#37, null[NULL] AS does_not_exist2#39]]
     *                   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     */
    public void testForkBranchesAfterStats2ndBranch() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary) BY does_not_exist2)
            | SORT does_not_exist2
            """));

        // TODO: golden testing
        assertThat(Expressions.names(plan.output()), is(List.of("c", "_fork", "d", "does_not_exist2")));
    }

    public void testFailAfterForkOfStats() {
        var query = """
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary))
                   (DISSECT hire_date::KEYWORD "%{year}-%{month}-%{day}T"
                    | STATS x = MIN(year::LONG), y = MAX(month::LONG) WHERE year::LONG > 1000 + does_not_exist2::DOUBLE)
            | EVAL e = does_not_exist3 + 1
            """;
        var failure = "line 7:12: Unknown column [does_not_exist3]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailLookupJoinWithoutCast() {
        String query = """
            FROM test
            | EVAL language_code = does_not_exist
            | LOOKUP JOIN languages_lookup ON language_code
            """;
        var failure = "is incompatible with right field [language_code] of type [INTEGER]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Enrich[ANY,languages[KEYWORD],x{r}#5,{"match":{"indices":[],"match_field":"language_code",
     *      "enrich_fields":["language_name"]}},{=languages_idx},[language_name{r}#21]]
     *   \_Eval[[TOSTRING(does_not_exist{r}#22) AS x#5]]
     *     \_Eval[[null[NULL] AS does_not_exist#22]]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testEnrich() {
        String query = """
            FROM test
            | EVAL x = does_not_exist::KEYWORD
            | ENRICH languages ON x
            """;
        var plan = analyzeStatement(setUnmappedNullify(query));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Enrich over alias `x` produced by TOSTRING(does_not_exist)
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.matchField().name(), is("x"));
        assertThat(Expressions.names(enrich.enrichFields()), contains("language_name"));

        // Left child: EVAL x = TOSTRING(does_not_exist), with upstream NULL alias for does_not_exist
        var leftEval = as(enrich.child(), Eval.class);
        assertThat(leftEval.fields(), hasSize(1));
        var xAlias = as(leftEval.fields().getFirst(), Alias.class);
        assertThat(xAlias.name(), is("x"));
        as(xAlias.child(), ToString.class);

        var upstreamEval = as(leftEval.child(), Eval.class);
        assertThat(upstreamEval.fields(), hasSize(1));
        var dneAlias = as(upstreamEval.fields().getFirst(), Alias.class);
        assertThat(dneAlias.name(), is("does_not_exist"));
        assertThat(as(dneAlias.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(upstreamEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[KNN(does_not_exist{r}#16,TODENSEVECTOR([0, 1, 2][INTEGER]))]
     *   \_Eval[[null[NULL] AS does_not_exist#16]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testSemanticText() {
        String query = """
            FROM test
            | WHERE KNN(does_not_exist, [0, 1, 2])
            """;
        var plan = analyzeStatement(setUnmappedNullify(query));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Filter node
        var filter = as(limit.child(), Filter.class);
        assertNotNull(filter.condition()); // KNN(does_not_exist, TODENSEVECTOR([...]))

        // Upstream Eval introduces does_not_exist as NULL
        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var dneAlias = as(eval.fields().getFirst(), Alias.class);
        assertThat(dneAlias.name(), is("does_not_exist"));
        assertThat(as(dneAlias.child(), Literal.class).dataType(), is(DataType.NULL));

        // Underlying relation
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testFailSemanticTextLoad() {
        String query = """
            FROM test
            | WHERE KNN(does_not_exist, [0, 1, 2])
            """;
        var failure = "first argument of [KNN(does_not_exist, [0, 1, 2])] must be [dense_vector, null, text], "
            + "found value [does_not_exist] type [keyword]";
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailRateLoad() {
        String query = """
            TS k8s
            | STATS max(rate(does_not_exist))
            """;
        var failure = "first argument of [rate(does_not_exist)] must be [counter_long, counter_integer or counter_double], "
            + "found value [does_not_exist] type [keyword]";
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInKeep() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | KEEP " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInEval() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | EVAL x = " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInWhere() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | WHERE " + field + " IS NOT NULL";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInSort() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | SORT " + field;
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInStats() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | STATS x = COUNT(" + field + ")";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldInRename() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | RENAME " + field + " AS renamed";
            var failure = "Unknown column [" + field + "]";
            verificationFailure(setUnmappedNullify(query), failure);
            verificationFailure(setUnmappedLoad(query), failure);
        }
    }

    public void testFailMetadataFieldAfterStats() {
        var query = """
            FROM test
            | STATS c = COUNT(*)
            | KEEP _score
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInFork() {
        var query = """
            FROM test
            | FORK (WHERE _score > 1)
                   (WHERE salary > 50000)
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    public void testFailMetadataFieldInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM test
                 | WHERE _score > 1)
            """;
        var failure = "Unknown column [_score]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredNullify() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = analyzeStatement(setUnmappedNullify("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // No Eval(NULL) — the field was resolved via METADATA, not nullified
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredLoad() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = analyzeStatement(setUnmappedLoad("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // The field was resolved via METADATA, not loaded as an unmapped field into EsRelation
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    public void testChangedTimestmapFieldWithRate() {
        verificationFailure(setUnmappedNullify("""
            TS k8s
            | RENAME @timestamp AS newTs
            | STATS max(rate(network.total_cost))  BY tbucket = bucket(newTs, 1hour)
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);

        verificationFailure(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);
    }

    // This test verifies that we do not allow an unmapped @timestamp fields in tbucket.
    public void testFailNoUnmappedTimestamp() throws Exception {
        String query = ("""
            FROM employees
            | STATS c = COUNT(*) BY tbucket(1 hour)
            """);
        var failure = "[tbucket(1 hour)] requires the [@timestamp] field";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }

    private void verificationFailure(String statement, String expectedFailure) {
        var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
        assertThat(e.getMessage(), containsString(expectedFailure));
    }

    private static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V2.isEnabled());
        return "SET unmapped_fields=\"load\"; " + query;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
