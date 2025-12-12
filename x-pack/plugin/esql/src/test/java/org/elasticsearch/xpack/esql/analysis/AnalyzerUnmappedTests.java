/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class AnalyzerUnmappedTests extends ESTestCase {

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[does_not_exist_field{r}#16]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#16]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testKeep() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP does_not_exist_field
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.name(project.projections().getFirst()), is("does_not_exist_field"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /**
     * Limit[1000[INTEGER],false,false]
     * \_Project[[does_not_exist_field{r}#18]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#17]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testKeepRepeated() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP does_not_exist_field, does_not_exist_field
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.name(project.projections().getFirst()), is("does_not_exist_field"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testKeepAndNonMatchingStar() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | KEEP does_not_exist_field*
            """), "No matches found for pattern [does_not_exist_field*]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[emp_no{f}#6, does_not_exist_field{r}#18]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testKeepAndMatchingStar() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP emp_*, does_not_exist_field
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "does_not_exist_field")));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testKeepAndMatchingAndNonMatchingStar() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """), "No matches found for pattern [does_not_exist_field*]");
    }

    public void testAfterKeep() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """), "Unknown column [does_not_exist_field]");
    }

    public void testAfterKeepStar() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL does_not_exist_field
            """), "Unknown column [does_not_exist_field]");
    }

    public void testAfterRename() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | RENAME emp_no AS employee_number
            | EVAL does_not_exist_field
            """), "Unknown column [does_not_exist_field]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *      languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testDrop() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | DROP does_not_exist_field
            """ + randomFrom("", ", does_not_exist_field", ", neither_this"))); // add emp_no to avoid "no fields left" case

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        // All fields from the original relation are projected, as the dropped field did not exist.
        assertThat(project.projections(), hasSize(11));
        assertThat(
            Expressions.names(project.projections()),
            is(
                List.of(
                    "_meta_field",
                    "emp_no",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary"
                )
            )
        );

        var relation = as(project.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testDropWithNonMatchingStar() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | DROP does_not_exist_field*
            """), "No matches found for pattern [does_not_exist_field*]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#12, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, languages{f}#9,
     *      last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testDropWithMatchingStar() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | DROP emp_*, does_not_exist_field
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(10));
        assertThat(
            Expressions.names(project.projections()),
            is(
                List.of(
                    "_meta_field",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary"
                )
            )
        );

        var relation = as(project.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testDropWithMatchingAndNonMatchingStar() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | DROP emp_*, does_not_exist_field*
            """), "No matches found for pattern [does_not_exist_field*]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#16, emp_no{f}#10 AS employee_number#8, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18,
     *      job.raw{f}#19, languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15,
     *      now_it_does#12 AS does_not_exist_field{r}#21]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#21]]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     */
    public void testRename() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | RENAME does_not_exist_field AS now_it_does, emp_no AS employee_number
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(12));
        assertThat(
            Expressions.names(project.projections()),
            is(
                List.of(
                    "_meta_field",
                    "employee_number",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary",
                    "now_it_does"
                )
            )
        );

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /**
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#19, emp_no{f}#13 AS employee_number#11, first_name{f}#14, gender{f}#15, hire_date{f}#20,
     *      job{f}#21, job.raw{f}#22, languages{f}#16, last_name{f}#17, long_noidx{f}#23, salary{f}#18,
     *      neither_does_this{r}#25 AS now_it_does#8]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#24, null[NULL] AS neither_does_this#25]]
     *     \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testRenameShadowed() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | RENAME does_not_exist_field AS now_it_does, neither_does_this AS now_it_does, emp_no AS employee_number
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(12));
        assertThat(
            Expressions.names(project.projections()),
            is(
                List.of(
                    "_meta_field",
                    "employee_number",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary",
                    "now_it_does"
                )
            )
        );

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));
        alias = as(eval.fields().getLast(), Alias.class);
        assertThat(alias.name(), is("neither_does_this"));
        literal = as(alias.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[does_not_exist_field{r}#18 + 1[INTEGER] AS x#5]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testEval() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL x = does_not_exist_field + 1
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var outerEval = as(limit.child(), Eval.class);
        assertThat(outerEval.fields(), hasSize(1));
        var aliasX = as(outerEval.fields().getFirst(), Alias.class);
        assertThat(aliasX.name(), is("x"));
        assertThat(Expressions.name(aliasX.child()), is("does_not_exist_field + 1"));

        var innerEval = as(outerEval.child(), Eval.class);
        assertThat(innerEval.fields(), hasSize(1));
        var aliasField = as(innerEval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[TOLONG(does_not_exist_field{r}#18) AS x#5]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testCasting() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL x = does_not_exist_field::LONG
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var outerEval = as(limit.child(), Eval.class);
        assertThat(outerEval.fields(), hasSize(1));
        var aliasX = as(outerEval.fields().getFirst(), Alias.class);
        assertThat(aliasX.name(), is("x"));
        assertThat(Expressions.name(aliasX.child()), is("does_not_exist_field::LONG"));

        var innerEval = as(outerEval.child(), Eval.class);
        assertThat(innerEval.fields(), hasSize(1));
        var aliasField = as(innerEval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[42[INTEGER] AS does_not_exist_field#7]]
     *   \_Eval[[does_not_exist_field{r}#20 + 1[INTEGER] AS x#5]]
     *     \_Eval[[null[NULL] AS does_not_exist_field#20]]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testShadowingAfterEval() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL x = does_not_exist_field + 1
            | EVAL does_not_exist_field = 42
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var outerMostEval = as(limit.child(), Eval.class);
        assertThat(outerMostEval.fields(), hasSize(1));
        var aliasShadow = as(outerMostEval.fields().getFirst(), Alias.class);
        assertThat(aliasShadow.name(), is("does_not_exist_field"));
        assertThat(Expressions.name(aliasShadow.child()), is("42"));

        var middleEval = as(outerMostEval.child(), Eval.class);
        assertThat(middleEval.fields(), hasSize(1));
        var aliasX = as(middleEval.fields().getFirst(), Alias.class);
        assertThat(aliasX.name(), is("x"));
        assertThat(Expressions.name(aliasX.child()), is("does_not_exist_field + 1"));

        var innerEval = as(middleEval.child(), Eval.class);
        assertThat(innerEval.fields(), hasSize(1));
        var aliasField = as(innerEval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[42[INTEGER] AS does_not_exist_field#5]]
     *   \_Project[[does_not_exist_field{r}#18]]
     *     \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testShadowingAfterKeep() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP does_not_exist_field
            | EVAL does_not_exist_field = 42
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var outerMostEval = as(limit.child(), Eval.class);
        assertThat(outerMostEval.fields(), hasSize(1));
        var aliasShadow = as(outerMostEval.fields().getFirst(), Alias.class);
        assertThat(aliasShadow.name(), is("does_not_exist_field"));
        assertThat(Expressions.name(aliasShadow.child()), is("42"));

        var project = as(outerMostEval.child(), Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.name(project.projections().getFirst()), is("does_not_exist_field"));

        var innerEval = as(project.child(), Eval.class);
        assertThat(innerEval.fields(), hasSize(1));
        var aliasField = as(innerEval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    public void testDropThenKeep() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """), "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testDropThenEval() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """), "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testEvalThenDropThenEval() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """), "line 6:8: Unknown column [does_not_exist_field]");
    }

    public void testAggThenKeep() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """), "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testAggThenEval() {
        verificationFailure(setUnmappedNullify("""
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """), "line 3:12: Unknown column [does_not_exist_field]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(does_not_exist_field{r}#18,true[BOOLEAN],PT0S[TIME_DURATION]) AS cnt#5]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testStatsAgg() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS cnt = COUNT(does_not_exist_field)
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("cnt"));
        assertThat(Expressions.name(alias.child()), is("COUNT(does_not_exist_field)"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var aliasField = as(eval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist_field{r}#16],[does_not_exist_field{r}#16]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#16]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testStatsGroup() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS BY does_not_exist_field
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(1));
        assertThat(Expressions.name(agg.groupings().getFirst()), is("does_not_exist_field"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var aliasField = as(eval.fields().getFirst(), Alias.class);
        assertThat(aliasField.name(), is("does_not_exist_field"));
        var literal = as(aliasField.child(), Literal.class);
        assertThat(literal.dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist2{r}#19],[SUM(does_not_exist1{r}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS s
     * #6, does_not_exist2{r}#19]]
     *   \_Eval[[null[NULL] AS does_not_exist2#19, null[NULL] AS does_not_exist1#20]]
     *     \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testStatsAggAndGroup() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS s = SUM(does_not_exist1) BY does_not_exist2
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(1));
        assertThat(Expressions.name(agg.groupings().getFirst()), is("does_not_exist2"));
        assertThat(agg.aggregates(), hasSize(2)); // includes grouping key
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("s"));
        assertThat(Expressions.name(alias.child()), is("SUM(does_not_exist1)"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias2 = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias2.name(), is("does_not_exist2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias1 = as(eval.fields().getLast(), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist2{r}#24 AS d2#5, emp_no{f}#13],[SUM(does_not_exist1{r}#25,true[BOOLEAN],PT0S[TIME_DURATION],
     *      compensated[KEYWORD]) + d2{r}#5 AS s#10, d2{r}#5, emp_no{f}#13]]
     *   \_Eval[[null[NULL] AS does_not_exist2#24, null[NULL] AS does_not_exist1#25]]
     *     \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testStatsAggAndAliasedGroup() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS s = SUM(does_not_exist1) + d2 BY d2 = does_not_exist2, emp_no
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        var groupAlias = as(agg.groupings().getFirst(), Alias.class);
        assertThat(groupAlias.name(), is("d2"));
        assertThat(Expressions.name(groupAlias.child()), is("does_not_exist2"));
        assertThat(Expressions.name(agg.groupings().get(1)), is("emp_no"));

        assertThat(agg.aggregates(), hasSize(3)); // includes grouping keys
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("s"));
        assertThat(Expressions.name(alias.child()), is("SUM(does_not_exist1) + d2"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias2 = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias2.name(), is("does_not_exist2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias1 = as(eval.fields().getLast(), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist2{r}#29 + does_not_exist3{r}#30 AS s0#6, emp_no{f}#18 AS s1#9],[SUM(does_not_exist1{r}#31,
     *      true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) + s0{r}#6 + s1{r}#9 AS sum#14, s0{r}#6, s1{r}#9]]
     *   \_Eval[[null[NULL] AS does_not_exist2#29, null[NULL] AS does_not_exist3#30, null[NULL] AS does_not_exist1#31]]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testStatsAggAndAliasedGroupWithExpression() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS sum = SUM(does_not_exist1) + s0 + s1 BY s0 = does_not_exist2 + does_not_exist3, s1 = emp_no
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        assertThat(Expressions.names(agg.groupings()), is(List.of("s0", "s1")));

        assertThat(agg.aggregates(), hasSize(3)); // includes grouping keys
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("sum"));
        assertThat(Expressions.name(alias.child()), is("SUM(does_not_exist1) + s0 + s1"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist2", "does_not_exist3", "does_not_exist1")));
        eval.fields().forEach(a -> assertThat(as(as(a, Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
    * Limit[1000[INTEGER],false,false]
    * \_Aggregate[[does_not_exist2{r}#22, emp_no{f}#11],[SUM(does_not_exist1{r}#23,true[BOOLEAN],PT0S[TIME_DURATION],
    *      compensated[KEYWORD]) AS s#7, COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#9, does_not_exist2{r}#22, emp_no{f}#11]]
    *   \_Eval[[null[NULL] AS does_not_exist2#22, null[NULL] AS does_not_exist1#23]]
    *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
    */
    public void testStatsMixed() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS s = SUM(does_not_exist1), c = COUNT(*) BY does_not_exist2, emp_no
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        assertThat(Expressions.names(agg.groupings()), is(List.of("does_not_exist2", "emp_no")));

        assertThat(agg.aggregates(), hasSize(4)); // includes grouping keys
        assertThat(Expressions.names(agg.aggregates()), is(List.of("s", "c", "does_not_exist2", "emp_no")));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist2", "does_not_exist1")));
        eval.fields().forEach(a -> assertThat(as(as(a, Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_InlineStats[]
     *   \_Aggregate[[does_not_exist2{r}#22, emp_no{f}#11],[SUM(does_not_exist1{r}#23,true[BOOLEAN],PT0S[TIME_DURATION],compensated[
     * KEYWORD]) AS s#5, COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#7, does_not_exist2{r}#22, emp_no{f}#11]]
     *     \_Eval[[null[NULL] AS does_not_exist2#22, null[NULL] AS does_not_exist1#23]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testInlineStatsMixed() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | INLINE STATS s = SUM(does_not_exist1), c = COUNT(*) BY does_not_exist2, emp_no
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var inlineStats = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.InlineStats.class);
        var agg = as(inlineStats.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        assertThat(Expressions.names(agg.groupings()), is(List.of("does_not_exist2", "emp_no")));

        assertThat(agg.aggregates(), hasSize(4)); // includes grouping keys
        assertThat(Expressions.names(agg.aggregates()), is(List.of("s", "c", "does_not_exist2", "emp_no")));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist2", "does_not_exist1")));
        eval.fields().forEach(a -> assertThat(as(as(a, Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist3{r}#24, emp_no{f}#13, does_not_exist2{r}#25],[SUM(does_not_exist1{r}#26,true[BOOLEAN],
     *      PT0S[TIME_DURATION],compensated[KEYWORD]) + does_not_exist2{r}#25 AS s#9, COUNT(*[KEYWORD],true[BOOLEAN],
     *      PT0S[TIME_DURATION]) AS c#11, does_not_exist3{r}#24, emp_no{f}#13, does_not_exist2{r}#25]]
     *   \_Eval[[null[NULL] AS does_not_exist3#24, null[NULL] AS does_not_exist2#25]]
     *     \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testStatsMixedAndExpressions() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS s = SUM(does_not_exist1) + does_not_exist2, c = COUNT(*) BY does_not_exist3, emp_no, does_not_exist2
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(3));
        assertThat(Expressions.names(agg.groupings()), is(List.of("does_not_exist3", "emp_no", "does_not_exist2")));

        assertThat(agg.aggregates(), hasSize(5)); // includes grouping keys
        assertThat(Expressions.names(agg.aggregates()), is(List.of("s", "c", "does_not_exist3", "emp_no", "does_not_exist2")));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist3", "does_not_exist2", "does_not_exist1")));
        eval.fields().forEach(a -> assertThat(as(as(a, Alias.class).child(), Literal.class).dataType(), is(DataType.NULL)));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    // enrich
    // lookup
    // sort
    // where
    // semantic text
    // fork
    // union

    private void verificationFailure(String statement, String expectedFailure) {
        var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
        assertThat(e.getMessage(), containsString(expectedFailure));
    }

    private static String setUnmappedNullify(String query) {
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
