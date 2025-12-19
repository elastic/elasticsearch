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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.contains;
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

    /*
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_EsqlProject[[does_not_exist_field1{r}#20, does_not_exist_field2{r}#23]]
     *   \_Eval[[TOINTEGER(does_not_exist_field1{r}#20) + 42[INTEGER] AS x#5]]
     *     \_Eval[[null[NULL] AS does_not_exist_field1#20]]
     *       \_Eval[[null[NULL] AS does_not_exist_field2#23]]
     *         \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testEvalAndKeep() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL x = does_not_exist_field1::INTEGER + 42
            | KEEP does_not_exist_field1, does_not_exist_field2
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("does_not_exist_field1", "does_not_exist_field2")));

        var eval1 = as(project.child(), Eval.class);
        assertThat(eval1.fields(), hasSize(1));
        var aliasX = as(eval1.fields().getFirst(), Alias.class);
        assertThat(aliasX.name(), is("x"));
        assertThat(Expressions.name(aliasX.child()), is("does_not_exist_field1::INTEGER + 42"));

        var eval2 = as(eval1.child(), Eval.class);
        assertThat(eval2.fields(), hasSize(1));
        var alias1 = as(eval2.fields().getFirst(), Alias.class);
        assertThat(alias1.name(), is("does_not_exist_field1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));

        var eval3 = as(eval2.child(), Eval.class);
        assertThat(eval3.fields(), hasSize(1));
        var alias2 = as(eval3.fields().getFirst(), Alias.class);
        assertThat(alias2.name(), is("does_not_exist_field2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval3.child(), EsRelation.class);
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

    /*
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist{r}#33) > 0[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist#33]]
     *     \_EsRelation[test][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     */
    public void testWhere() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE does_not_exist::LONG > 0
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var filter = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Filter.class);
        assertThat(Expressions.name(filter.condition()), is("does_not_exist::LONG > 0"));

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist{r}#195) > 0[INTEGER] OR emp_no{f}#184 > 0[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist#195]]
     *     \_EsRelation[test][_meta_field{f}#190, emp_no{f}#184, first_name{f}#18..]
     */
    public void testWhereConjunction() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE does_not_exist::LONG > 0 OR emp_no > 0
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var filter = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Filter.class);
        assertThat(Expressions.name(filter.condition()), is("does_not_exist::LONG > 0 OR emp_no > 0"));

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist1{r}#491) > 0[INTEGER] OR emp_no{f}#480 > 0[INTEGER]
     *      AND TOLONG(does_not_exist2{r}#492) < 100[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist1#491, null[NULL] AS does_not_exist2#492]]
     *     \_EsRelation[test][_meta_field{f}#486, emp_no{f}#480, first_name{f}#48..]
     */
    public void testWhereConjunctionMultipleFields() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var filter = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Filter.class);
        assertThat(Expressions.name(filter.condition()), is("does_not_exist1::LONG > 0 OR emp_no > 0 AND does_not_exist2::LONG < 100"));

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias1 = as(eval.fields().get(0), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias2 = as(eval.fields().get(1), Alias.class);
        assertThat(alias2.name(), is("does_not_exist2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[FilteredExpression[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]),
     *      TOLONG(does_not_exist1{r}#94) > 0[INTEGER]] AS c#81]]
     *   \_Eval[[null[NULL] AS does_not_exist1#94]]
     *     \_EsRelation[test][_meta_field{f}#89, emp_no{f}#83, first_name{f}#84, ..]
     */
    public void testAggsFiltering() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS c = COUNT(*) WHERE does_not_exist1::LONG > 0
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("c"));
        var filteredExpr = as(alias.child(), FilteredExpression.class);
        var delegate = as(filteredExpr.delegate(), Count.class);
        var greaterThan = as(filteredExpr.filter(), GreaterThan.class);
        var tolong = as(greaterThan.left(), ToLong.class);
        assertThat(Expressions.name(tolong.field()), is("does_not_exist1"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias1 = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[FilteredExpression[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]),
     *      TOLONG(does_not_exist1{r}#620) > 0[INTEGER] OR emp_no{f}#609 > 0[INTEGER]
     *      OR TOLONG(does_not_exist2{r}#621) < 100[INTEGER]] AS c1#602,
     *      FilteredExpression[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]),ISNULL(does_not_exist3{r}#622)] AS c2#607]]
     *   \_Eval[[null[NULL] AS does_not_exist1#620, null[NULL] AS does_not_exist2#621, null[NULL] AS does_not_exist3#622]]
     *     \_EsRelation[test][_meta_field{f}#615, emp_no{f}#609, first_name{f}#61..]
     */
    public void testAggsFilteringMultipleFields() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100,
                    c2 = COUNT(*) WHERE does_not_exist3 IS NULL
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(2));

        var alias1 = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias1.name(), is("c1"));
        assertThat(
            Expressions.name(alias1.child()),
            is("c1 = COUNT(*) WHERE does_not_exist1::LONG > 0 OR emp_no > 0 OR does_not_exist2::LONG < 100")
        );

        var alias2 = as(agg.aggregates().get(1), Alias.class);
        assertThat(alias2.name(), is("c2"));
        assertThat(Expressions.name(alias2.child()), is("c2 = COUNT(*) WHERE does_not_exist3 IS NULL"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        var aliasDne1 = as(eval.fields().get(0), Alias.class);
        assertThat(aliasDne1.name(), is("does_not_exist1"));
        assertThat(as(aliasDne1.child(), Literal.class).dataType(), is(DataType.NULL));
        var aliasDne2 = as(eval.fields().get(1), Alias.class);
        assertThat(aliasDne2.name(), is("does_not_exist2"));
        assertThat(as(aliasDne2.child(), Literal.class).dataType(), is(DataType.NULL));
        var aliasDne3 = as(eval.fields().get(2), Alias.class);
        assertThat(aliasDne3.name(), is("does_not_exist3"));
        assertThat(as(aliasDne3.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[does_not_exist{r}#16,ASC,LAST]]]
     *   \_Eval[[null[NULL] AS does_not_exist#16]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testSort() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | SORT does_not_exist ASC
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // OrderBy over the Eval-produced alias
        var orderBy = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.OrderBy.class);

        // Eval introduces does_not_exist as NULL
        var eval = as(orderBy.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        // Underlying relation
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[TOLONG(does_not_exist{r}#485) + 1[INTEGER],ASC,LAST]]]
     *   \_Eval[[null[NULL] AS does_not_exist#485]]
     *     \_EsRelation[test][_meta_field{f}#480, emp_no{f}#474, first_name{f}#47..]
     */
    public void testSortExpression() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | SORT does_not_exist::LONG + 1
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var orderBy = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.OrderBy.class);
        assertThat(orderBy.order(), hasSize(1));
        assertThat(Expressions.name(orderBy.order().getFirst().child()), is("does_not_exist::LONG + 1"));

        var eval = as(orderBy.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[TOLONG(does_not_exist{r}#370) + 1[INTEGER],ASC,LAST], Order[does_not_exist2{r}#371,DESC,FIRST],
     *      Order[emp_no{f}#359,ASC,LAST]]]
     *   \_Eval[[null[NULL] AS does_not_exist1#370, null[NULL] AS does_not_exist2#371]]
     *     \_EsRelation[test][_meta_field{f}#365, emp_no{f}#359, first_name{f}#36..]
     */
    public void testSortExpressionMultipleFields() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | SORT does_not_exist1::LONG + 1, does_not_exist2 DESC, emp_no ASC
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var orderBy = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.OrderBy.class);
        assertThat(orderBy.order(), hasSize(3));
        assertThat(Expressions.name(orderBy.order().get(0).child()), is("does_not_exist1::LONG + 1"));
        assertThat(Expressions.name(orderBy.order().get(1).child()), is("does_not_exist2"));
        assertThat(Expressions.name(orderBy.order().get(2).child()), is("emp_no"));

        var eval = as(orderBy.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        var alias1 = as(eval.fields().get(0), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias2 = as(eval.fields().get(1), Alias.class);
        assertThat(alias2.name(), is("does_not_exist2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /**
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[does_not_exist{r}#17,does_not_exist{r}#20]
     *   \_Eval[[null[NULL] AS does_not_exist#17]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testMvExpand() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | MV_EXPAND does_not_exist
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var mvExpand = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.MvExpand.class);
        assertThat(Expressions.name(mvExpand.expanded()), is("does_not_exist"));

        var eval = as(mvExpand.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist{r}#566) > 1[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist#566]]
     *     \_EsRelation[languages][language_code{f}#564, language_name{f}#565]
     */
    public void testSubqueryOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist::LONG > 1)
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var filter = as(limit.child(), Filter.class);
        var gt = as(filter.condition(), GreaterThan.class);
        var toLong = as(gt.left(), ToLong.class);
        assertThat(Expressions.name(toLong.field()), is("does_not_exist"));

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("languages"));

    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[language_code{r}#22, language_name{r}#23, does_not_exist1{r}#24, @timestamp{r}#25, client_ip{r}#26, event_dur
     * ation{r}#27, message{r}#28]]
     *   |_Limit[1000[INTEGER],false,false]
     *   | \_EsqlProject[[language_code{f}#6, language_name{f}#7, does_not_exist1{r}#12, @timestamp{r}#16, client_ip{r}#17, event_durat
     * ion{r}#18, message{r}#19]]
     *   |   \_Eval[[null[DATETIME] AS @timestamp#16, null[IP] AS client_ip#17, null[LONG] AS event_duration#18, null[KEYWORD] AS
     * message#19]]
     *   |     \_Subquery[]
     *   |       \_Filter[TOLONG(does_not_exist1{r}#12) > 1[INTEGER]]
     *   |         \_Eval[[null[NULL] AS does_not_exist1#12]]
     *   |           \_EsRelation[languages][language_code{f}#6, language_name{f}#7]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsqlProject[[language_code{r}#20, language_name{r}#21, does_not_exist1{r}#14, @timestamp{f}#8, client_ip{f}#9, event_durat
     * ion{f}#10, message{f}#11]]
     *       \_Eval[[null[INTEGER] AS language_code#20, null[KEYWORD] AS language_name#21]]
     *         \_Subquery[]
     *           \_Filter[TODOUBLE(does_not_exist1{r}#14) > 10.0[DOUBLE]]
     *             \_Eval[[null[NULL] AS does_not_exist1#14]]
     *               \_EsRelation[sample_data][@timestamp{f}#8, client_ip{f}#9, event_duration{f}#..]
     */
    public void testDoubleSubqueryOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            """));

        var topLimit = as(plan, Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(1000));

        var union = as(topLimit.child(), UnionAll.class);
        assertThat(union.children(), hasSize(2));

        // Left branch: languages
        var leftLimit = as(union.children().get(0), Limit.class);
        assertThat(leftLimit.limit().fold(FoldContext.small()), is(1000));

        var leftProject = as(leftLimit.child(), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        // Verify unmapped null aliases for @timestamp, client_ip, event_duration, message
        assertThat(leftEval.fields(), hasSize(4));
        var leftTs = as(leftEval.fields().get(0), Alias.class);
        assertThat(leftTs.name(), is("@timestamp"));
        assertThat(as(leftTs.child(), Literal.class).dataType(), is(DataType.DATETIME));
        var leftIp = as(leftEval.fields().get(1), Alias.class);
        assertThat(leftIp.name(), is("client_ip"));
        assertThat(as(leftIp.child(), Literal.class).dataType(), is(DataType.IP));
        var leftDur = as(leftEval.fields().get(2), Alias.class);
        assertThat(leftDur.name(), is("event_duration"));
        assertThat(as(leftDur.child(), Literal.class).dataType(), is(DataType.LONG));
        var leftMsg = as(leftEval.fields().get(3), Alias.class);
        assertThat(leftMsg.name(), is("message"));
        assertThat(as(leftMsg.child(), Literal.class).dataType(), is(DataType.KEYWORD));

        var leftSubquery = as(leftEval.child(), Subquery.class);
        var leftSubFilter = as(leftSubquery.child(), Filter.class);
        var leftGt = as(leftSubFilter.condition(), GreaterThan.class);
        var leftToLong = as(leftGt.left(), ToLong.class);
        assertThat(Expressions.name(leftToLong.field()), is("does_not_exist1"));

        var leftSubEval = as(leftSubFilter.child(), Eval.class);
        assertThat(leftSubEval.fields(), hasSize(1));
        var leftDoesNotExist = as(leftSubEval.fields().getFirst(), Alias.class);
        assertThat(leftDoesNotExist.name(), is("does_not_exist1"));
        assertThat(as(leftDoesNotExist.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(leftSubEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("languages"));

        // Right branch: sample_data
        var rightLimit = as(union.children().get(1), Limit.class);
        assertThat(rightLimit.limit().fold(FoldContext.small()), is(1000));

        var rightProject = as(rightLimit.child(), Project.class);
        var rightEval = as(rightProject.child(), Eval.class);
        // Verify unmapped null aliases for language_code, language_name
        assertThat(rightEval.fields(), hasSize(2));
        var rightCode = as(rightEval.fields().get(0), Alias.class);
        assertThat(rightCode.name(), is("language_code"));
        assertThat(as(rightCode.child(), Literal.class).dataType(), is(DataType.INTEGER));
        var rightName = as(rightEval.fields().get(1), Alias.class);
        assertThat(rightName.name(), is("language_name"));
        assertThat(as(rightName.child(), Literal.class).dataType(), is(DataType.KEYWORD));

        var rightSubquery = as(rightEval.child(), Subquery.class);
        var rightSubFilter = as(rightSubquery.child(), Filter.class);
        var rightGt = as(rightSubFilter.condition(), GreaterThan.class);
        var rightToDouble = as(rightGt.left(), ToDouble.class);
        assertThat(Expressions.name(rightToDouble.field()), is("does_not_exist1"));

        var rightSubEval = as(rightSubFilter.child(), Eval.class);
        assertThat(rightSubEval.fields(), hasSize(1));
        var rightDoesNotExist = as(rightSubEval.fields().getFirst(), Alias.class);
        assertThat(rightDoesNotExist.name(), is("does_not_exist1"));
        assertThat(as(rightDoesNotExist.child(), Literal.class).dataType(), is(DataType.NULL));

        var rightRel = as(rightSubEval.child(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("sample_data"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist2{r}#30) < 100[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist2#30]]
     *     \_UnionAll[[language_code{r}#23, language_name{r}#24, does_not_exist1{r}#25, @timestamp{r}#26, client_ip{r}#27,
     *              event_duration{r}#28, message{r}#29]]
     *       |_Limit[1000[INTEGER],false,false]
     *       | \_EsqlProject[[language_code{f}#7, language_name{f}#8, does_not_exist1{r}#13, @timestamp{r}#17, client_ip{r}#18,
     *              event_duration{r}#19, message{r}#20]]
     *       |   \_Eval[[null[DATETIME] AS @timestamp#17, null[IP] AS client_ip#18, null[LONG] AS event_duration#19,
     *                  null[KEYWORD] AS message#20]]
     *       |     \_Subquery[]
     *       |       \_Filter[TOLONG(does_not_exist1{r}#13) > 1[INTEGER]]
     *       |         \_Eval[[null[NULL] AS does_not_exist1#13]]
     *       |           \_EsRelation[languages][language_code{f}#7, language_name{f}#8]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_EsqlProject[[language_code{r}#21, language_name{r}#22, does_not_exist1{r}#15, @timestamp{f}#9, client_ip{f}#10,
     *                  event_duration{f}#11, message{f}#12]]
     *           \_Eval[[null[INTEGER] AS language_code#21, null[KEYWORD] AS language_name#22]]
     *             \_Subquery[]
     *               \_Filter[TODOUBLE(does_not_exist1{r}#15) > 10.0[DOUBLE]]
     *                 \_Eval[[null[NULL] AS does_not_exist1#15]]
     *                   \_EsRelation[sample_data][@timestamp{f}#9, client_ip{f}#10, event_duration{f}..]
     */
    public void testDoubleSubqueryOnlyWithTopFilter() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """));

        // Top implicit limit
        var topLimit = as(plan, Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(1000));

        // Top filter: TOLONG(does_not_exist2) < 100
        var topFilter = as(topLimit.child(), Filter.class);
        var topLt = as(topFilter.condition(), org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan.class);
        var topToLong = as(topLt.left(), ToLong.class);
        assertThat(Expressions.name(topToLong.field()), is("does_not_exist2"));

        // Top eval null-alias for does_not_exist2
        var topEval = as(topFilter.child(), Eval.class);
        assertThat(topEval.fields(), hasSize(1));
        var topDoesNotExist2 = as(topEval.fields().getFirst(), Alias.class);
        assertThat(topDoesNotExist2.name(), is("does_not_exist2"));
        assertThat(as(topDoesNotExist2.child(), Literal.class).dataType(), is(DataType.NULL));

        // UnionAll with two branches
        var union = as(topEval.child(), UnionAll.class);
        assertThat(union.children(), hasSize(2));

        // Left branch: languages
        var leftLimit = as(union.children().get(0), Limit.class);
        assertThat(leftLimit.limit().fold(FoldContext.small()), is(1000));

        var leftProject = as(leftLimit.child(), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        // Unmapped null aliases for @timestamp, client_ip, event_duration, message
        assertThat(leftEval.fields(), hasSize(4));
        var leftTs = as(leftEval.fields().get(0), Alias.class);
        assertThat(leftTs.name(), is("@timestamp"));
        assertThat(as(leftTs.child(), Literal.class).dataType(), is(DataType.DATETIME));
        var leftIp = as(leftEval.fields().get(1), Alias.class);
        assertThat(leftIp.name(), is("client_ip"));
        assertThat(as(leftIp.child(), Literal.class).dataType(), is(DataType.IP));
        var leftDur = as(leftEval.fields().get(2), Alias.class);
        assertThat(leftDur.name(), is("event_duration"));
        assertThat(as(leftDur.child(), Literal.class).dataType(), is(DataType.LONG));
        var leftMsg = as(leftEval.fields().get(3), Alias.class);
        assertThat(leftMsg.name(), is("message"));
        assertThat(as(leftMsg.child(), Literal.class).dataType(), is(DataType.KEYWORD));

        var leftSubquery = as(leftEval.child(), org.elasticsearch.xpack.esql.plan.logical.Subquery.class);
        var leftSubFilter = as(leftSubquery.child(), Filter.class);
        var leftGt = as(leftSubFilter.condition(), GreaterThan.class);
        var leftToLong = as(leftGt.left(), ToLong.class);
        assertThat(Expressions.name(leftToLong.field()), is("does_not_exist1"));

        var leftSubEval = as(leftSubFilter.child(), Eval.class);
        assertThat(leftSubEval.fields(), hasSize(1));
        var leftDoesNotExist1 = as(leftSubEval.fields().getFirst(), Alias.class);
        assertThat(leftDoesNotExist1.name(), is("does_not_exist1"));
        assertThat(as(leftDoesNotExist1.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(leftSubEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("languages"));

        // Right branch: sample_data
        var rightLimit = as(union.children().get(1), Limit.class);
        assertThat(rightLimit.limit().fold(FoldContext.small()), is(1000));

        var rightProject = as(rightLimit.child(), Project.class);
        var rightEval = as(rightProject.child(), Eval.class);
        // Unmapped null aliases for language_code, language_name
        assertThat(rightEval.fields(), hasSize(2));
        var rightCode = as(rightEval.fields().get(0), Alias.class);
        assertThat(rightCode.name(), is("language_code"));
        assertThat(as(rightCode.child(), Literal.class).dataType(), is(DataType.INTEGER));
        var rightName = as(rightEval.fields().get(1), Alias.class);
        assertThat(rightName.name(), is("language_name"));
        assertThat(as(rightName.child(), Literal.class).dataType(), is(DataType.KEYWORD));

        var rightSubquery = as(rightEval.child(), org.elasticsearch.xpack.esql.plan.logical.Subquery.class);
        var rightSubFilter = as(rightSubquery.child(), Filter.class);
        var rightGt = as(rightSubFilter.condition(), GreaterThan.class);
        var rightToDouble = as(rightGt.left(), ToDouble.class);
        assertThat(Expressions.name(rightToDouble.field()), is("does_not_exist1"));

        var rightSubEval = as(rightSubFilter.child(), Eval.class);
        assertThat(rightSubEval.fields(), hasSize(1));
        var rightDoesNotExist1 = as(rightSubEval.fields().getFirst(), Alias.class);
        assertThat(rightDoesNotExist1.name(), is("does_not_exist1"));
        assertThat(as(rightDoesNotExist1.child(), Literal.class).dataType(), is(DataType.NULL));

        var rightRel = as(rightSubEval.child(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("sample_data"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist2{r}#50) < 10[INTEGER] AND emp_no{r}#37 > 0[INTEGER]]
     *   \_Eval[[null[NULL] AS does_not_exist2#50]]
     *     \_UnionAll[[_meta_field{r}#36, emp_no{r}#37, first_name{r}#38, gender{r}#39, hire_date{r}#40, job{r}#41, job.raw{r}#42,
     *          languages{r}#43, last_name{r}#44, long_noidx{r}#45, salary{r}#46, language_code{r}#47,
     *          language_name{r}#48, does_not_exist1{r}#49]]
     *       |_Limit[1000[INTEGER],false,false]
     *       | \_EsqlProject[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *              languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#22, language_name{r}#23,
     *              does_not_exist1{r}#24]]
     *       |   \_Eval[[null[INTEGER] AS language_code#22, null[KEYWORD] AS language_name#23, null[NULL] AS does_not_exist1#24]]
     *       |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_EsqlProject[[_meta_field{r}#25, emp_no{r}#26, first_name{r}#27, gender{r}#28, hire_date{r}#29, job{r}#30, job.raw{r}#31,
     *              languages{r}#32, last_name{r}#33, long_noidx{r}#34, salary{r}#35, language_code{f}#18, language_name{f}#19,
     *              does_not_exist1{r}#20]]
     *           \_Eval[[null[KEYWORD] AS _meta_field#25, null[INTEGER] AS emp_no#26, null[KEYWORD] AS first_name#27,
     *                  null[TEXT] AS gender#28, null[DATETIME] AS hire_date#29, null[TEXT] AS job#30, null[KEYWORD] AS job.raw#31,
     *                  null[INTEGER] AS languages#32, null[KEYWORD] AS last_name#33, null[LONG] AS long_noidx#34,
     *                  null[INTEGER] AS salary#35]]
     *             \_Subquery[]
     *               \_Filter[TOLONG(does_not_exist1{r}#20) > 1[INTEGER]]
     *                 \_Eval[[null[NULL] AS does_not_exist1#20]]
     *                   \_EsRelation[languages][language_code{f}#18, language_name{f}#19]
     */
    public void testSubqueryAndMainQuery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """));

        // Top implicit limit
        var topLimit = as(plan, Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(1000));

        // Top filter: TOLONG(does_not_exist2) < 10 AND emp_no > 0
        var topFilter = as(topLimit.child(), Filter.class);
        var topAnd = as(topFilter.condition(), And.class);

        var leftCond = as(topAnd.left(), LessThan.class);
        var leftToLong = as(leftCond.left(), ToLong.class);
        assertThat(Expressions.name(leftToLong.field()), is("does_not_exist2"));
        assertThat(as(leftCond.right(), Literal.class).value(), is(10));

        var rightCond = as(topAnd.right(), GreaterThan.class);
        var rightAttr = as(rightCond.left(), ReferenceAttribute.class);
        assertThat(rightAttr.name(), is("emp_no"));
        assertThat(as(rightCond.right(), Literal.class).value(), is(0));

        // Top eval null-alias for does_not_exist2
        var topEval = as(topFilter.child(), Eval.class);
        assertThat(topEval.fields(), hasSize(1));
        var topDoesNotExist2 = as(topEval.fields().getFirst(), Alias.class);
        assertThat(topDoesNotExist2.name(), is("does_not_exist2"));
        assertThat(as(topDoesNotExist2.child(), Literal.class).dataType(), is(DataType.NULL));

        // UnionAll with two branches
        var union = as(topEval.child(), UnionAll.class);
        assertThat(union.children(), hasSize(2));

        // Left branch: EsRelation[test] with EsqlProject + Eval nulls
        var leftLimit = as(union.children().get(0), Limit.class);
        assertThat(leftLimit.limit().fold(FoldContext.small()), is(1000));

        var leftProject = as(leftLimit.child(), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(leftEval.fields(), hasSize(3));
        var leftLangCode = as(leftEval.fields().get(0), Alias.class);
        assertThat(leftLangCode.name(), is("language_code"));
        assertThat(as(leftLangCode.child(), Literal.class).dataType(), is(DataType.INTEGER));
        var leftLangName = as(leftEval.fields().get(1), Alias.class);
        assertThat(leftLangName.name(), is("language_name"));
        assertThat(as(leftLangName.child(), Literal.class).dataType(), is(DataType.KEYWORD));
        var leftDne1 = as(leftEval.fields().get(2), Alias.class);
        assertThat(leftDne1.name(), is("does_not_exist1"));
        assertThat(as(leftDne1.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(leftEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("test"));

        // Right branch: EsqlProject + Eval many nulls, Subquery -> Filter -> Eval -> EsRelation[languages]
        var rightLimit = as(union.children().get(1), Limit.class);
        assertThat(rightLimit.limit().fold(FoldContext.small()), is(1000));

        var rightProject = as(rightLimit.child(), Project.class);
        var rightEval = as(rightProject.child(), Eval.class);
        assertThat(rightEval.fields(), hasSize(11));
        assertThat(
            Expressions.names(rightEval.fields()),
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

        var rightSub = as(rightEval.child(), Subquery.class);
        var rightSubFilter = as(rightSub.child(), Filter.class);
        var rightGt = as(rightSubFilter.condition(), GreaterThan.class);
        var rightToLongOnDne1 = as(rightGt.left(), ToLong.class);
        assertThat(Expressions.name(rightToLongOnDne1.field()), is("does_not_exist1"));

        var rightSubEval = as(rightSubFilter.child(), Eval.class);
        assertThat(rightSubEval.fields(), hasSize(1));
        var rightDne1 = as(rightSubEval.fields().getFirst(), Alias.class);
        assertThat(rightDne1.name(), is("does_not_exist1"));
        assertThat(as(rightDne1.child(), Literal.class).dataType(), is(DataType.NULL));

        var rightRel = as(rightSubEval.child(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("languages"));
    }

    /*
     * Project[[_meta_field{r}#53, emp_no{r}#54, first_name{r}#55, gender{r}#56, hire_date{r}#57, job{r}#58, job.raw{r}#59,
     *      languages{r}#60, last_name{r}#61, long_noidx{r}#62, salary{r}#63, language_code{r}#64, language_name{r}#65,
     *      does_not_exist1{r}#66, does_not_exist2{r}#71]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[TOLONG(does_not_exist2{r}#71) < 10[INTEGER] AND emp_no{r}#54 > 0[INTEGER]
     *          OR $$does_not_exist1$converted_to$long{r$}#70 < 11[INTEGER]]
     *     \_Eval[[null[NULL] AS does_not_exist2#71]]
     *       \_UnionAll[[_meta_field{r}#53, emp_no{r}#54, first_name{r}#55, gender{r}#56, hire_date{r}#57, job{r}#58, job.raw{r}#59,
     *              languages{r}#60, last_name{r}#61, long_noidx{r}#62, salary{r}#63, language_code{r}#64, language_name{r}#65,
     *              does_not_exist1{r}#66, $$does_not_exist1$converted_to$long{r$}#70]]
     *         |_Limit[1000[INTEGER],false,false]
     *         | \_EsqlProject[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *                  languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, language_code{r}#28, language_name{r}#29,
     *                  does_not_exist1{r}#30, $$does_not_exist1$converted_to$long{r}#67]]
     *         |   \_Eval[[TOLONG(does_not_exist1{r}#30) AS $$does_not_exist1$converted_to$long#67]]
     *         |     \_Eval[[null[INTEGER] AS language_code#28, null[KEYWORD] AS language_name#29, null[NULL] AS does_not_exist1#30]]
     *         |       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *         |_Limit[1000[INTEGER],false,false]
     *         | \_EsqlProject[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37,
     *                  languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, language_code{f}#20, language_name{f}#21,
     *                  does_not_exist1{r}#24, $$does_not_exist1$converted_to$long{r}#68]]
     *         |   \_Eval[[TOLONG(does_not_exist1{r}#24) AS $$does_not_exist1$converted_to$long#68]]
     *         |     \_Eval[[null[KEYWORD] AS _meta_field#31, null[INTEGER] AS emp_no#32, null[KEYWORD] AS first_name#33,
     *                      null[TEXT] AS gender#34, null[DATETIME] AS hire_date#35, null[TEXT] AS job#36, null[KEYWORD] AS job.raw#37,
     *                      null[INTEGER] AS languages#38, null[KEYWORD] AS last_name#39, null[LONG] AS long_noidx#40,
     *                      null[INTEGER] AS salary#41]]
     *         |       \_Subquery[]
     *         |         \_Filter[TOLONG(does_not_exist1{r}#24) > 1[INTEGER]]
     *         |           \_Eval[[null[NULL] AS does_not_exist1#24]]
     *         |             \_EsRelation[languages][language_code{f}#20, language_name{f}#21]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_EsqlProject[[_meta_field{r}#42, emp_no{r}#43, first_name{r}#44, gender{r}#45, hire_date{r}#46, job{r}#47, job.raw{r}#48,
     *                  languages{r}#49, last_name{r}#50, long_noidx{r}#51, salary{r}#52, language_code{f}#22, language_name{f}#23,
     *                  does_not_exist1{r}#26, $$does_not_exist1$converted_to$long{r}#69]]
     *             \_Eval[[TOLONG(does_not_exist1{r}#26) AS $$does_not_exist1$converted_to$long#69]]
     *               \_Eval[[null[KEYWORD] AS _meta_field#42, null[INTEGER] AS emp_no#43, null[KEYWORD] AS first_name#44,
     *                      null[TEXT] AS gender#45, null[DATETIME] AS hire_date#46, null[TEXT] AS job#47, null[KEYWORD] AS job.raw#48,
     *                      null[INTEGER] AS languages#49, null[KEYWORD] AS last_name#50, null[LONG] AS long_noidx#51,
     *                      null[INTEGER] AS salary#52]]
     *                 \_Subquery[]
     *                   \_Filter[TOLONG(does_not_exist1{r}#26) > 2[INTEGER]]
     *                     \_Eval[[null[NULL] AS does_not_exist1#26]]
     *                       \_EsRelation[languages][language_code{f}#22, language_name{f}#23]
     */
    public void testSubquerysWithSameOptional() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM languages
                 | WHERE does_not_exist1::LONG > 2)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0 OR does_not_exist1::LONG < 11
            """));

        // Top Project
        var topProject = as(plan, Project.class);

        // Top implicit limit
        var topLimit = as(topProject.child(), Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(1000));

        // Top filter: TOLONG(does_not_exist2) < 10 AND emp_no > 0 OR $$does_not_exist1$converted_to$long < 11
        var topFilter = as(topLimit.child(), Filter.class);
        var topOr = as(topFilter.condition(), Or.class);

        var leftAnd = as(topOr.left(), And.class);
        var andLeftLt = as(leftAnd.left(), LessThan.class);
        var andLeftToLong = as(andLeftLt.left(), ToLong.class);
        assertThat(Expressions.name(andLeftToLong.field()), is("does_not_exist2"));
        assertThat(as(andLeftLt.right(), Literal.class).value(), is(10));

        var andRightGt = as(leftAnd.right(), GreaterThan.class);
        var andRightAttr = as(andRightGt.left(), ReferenceAttribute.class);
        assertThat(andRightAttr.name(), is("emp_no"));
        assertThat(as(andRightGt.right(), Literal.class).value(), is(0));

        var rightLt = as(topOr.right(), LessThan.class);
        var rightAttr = as(rightLt.left(), ReferenceAttribute.class);
        assertThat(rightAttr.name(), is("$$does_not_exist1$converted_to$long"));
        assertThat(as(rightLt.right(), Literal.class).value(), is(11));

        // Top eval null-alias for does_not_exist2
        var topEval = as(topFilter.child(), Eval.class);
        assertThat(topEval.fields(), hasSize(1));
        var topDoesNotExist2 = as(topEval.fields().getFirst(), Alias.class);
        assertThat(topDoesNotExist2.name(), is("does_not_exist2"));
        assertThat(as(topDoesNotExist2.child(), Literal.class).dataType(), is(DataType.NULL));

        // UnionAll with three branches
        var union = as(topEval.child(), UnionAll.class);
        assertThat(union.children(), hasSize(3));

        // Branch 1: EsRelation[test] with EsqlProject + Eval(null language_code/name/dne1) + Eval(TOLONG does_not_exist1)
        var b1Limit = as(union.children().get(0), Limit.class);
        assertThat(b1Limit.limit().fold(FoldContext.small()), is(1000));

        var b1Project = as(b1Limit.child(), Project.class);
        var b1EvalToLong = as(b1Project.child(), Eval.class);
        assertThat(b1EvalToLong.fields(), hasSize(1));
        var b1Converted = as(b1EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b1Converted.name(), is("$$does_not_exist1$converted_to$long"));
        var b1ToLong = as(b1Converted.child(), ToLong.class);
        assertThat(Expressions.name(b1ToLong.field()), is("does_not_exist1"));

        var b1EvalNulls = as(b1EvalToLong.child(), Eval.class);
        assertThat(b1EvalNulls.fields(), hasSize(3));
        assertThat(Expressions.names(b1EvalNulls.fields()), is(List.of("language_code", "language_name", "does_not_exist1")));
        var b1Dne1 = as(b1EvalNulls.fields().get(2), Alias.class);
        assertThat(b1Dne1.name(), is("does_not_exist1"));
        assertThat(as(b1Dne1.child(), Literal.class).dataType(), is(DataType.NULL));

        var b1Rel = as(b1EvalNulls.child(), EsRelation.class);
        assertThat(b1Rel.indexPattern(), is("test"));

        // Branch 2: Subquery[languages] with Filter TOLONG(does_not_exist1) > 1, wrapped by EsqlProject nulls + Eval(TOLONG dne1)
        var b2Limit = as(union.children().get(1), Limit.class);
        assertThat(b2Limit.limit().fold(FoldContext.small()), is(1000));

        var b2Project = as(b2Limit.child(), Project.class);
        var b2EvalToLong = as(b2Project.child(), Eval.class);
        assertThat(b2EvalToLong.fields(), hasSize(1));
        var b2Converted = as(b2EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b2Converted.name(), is("$$does_not_exist1$converted_to$long"));
        var b2ToLong = as(b2Converted.child(), ToLong.class);
        assertThat(Expressions.name(b2ToLong.field()), is("does_not_exist1"));

        var b2EvalNulls = as(b2EvalToLong.child(), Eval.class);
        assertThat(b2EvalNulls.fields(), hasSize(11)); // null meta+many fields

        var b2Sub = as(b2EvalNulls.child(), Subquery.class);
        var b2Filter = as(b2Sub.child(), Filter.class);
        var b2Gt = as(b2Filter.condition(), GreaterThan.class);
        var b2GtToLong = as(b2Gt.left(), ToLong.class);
        assertThat(Expressions.name(b2GtToLong.field()), is("does_not_exist1"));
        var b2SubEval = as(b2Filter.child(), Eval.class);
        assertThat(b2SubEval.fields(), hasSize(1));
        assertThat(as(as(b2SubEval.fields().getFirst(), Alias.class).child(), Literal.class).dataType(), is(DataType.NULL));
        var b2Rel = as(b2SubEval.child(), EsRelation.class);
        assertThat(b2Rel.indexPattern(), is("languages"));

        // Branch 3: Subquery[languages] with Filter TOLONG(does_not_exist1) > 2, wrapped by EsqlProject nulls + Eval(TOLONG dne1)
        var b3Limit = as(union.children().get(2), Limit.class);
        assertThat(b3Limit.limit().fold(FoldContext.small()), is(1000));

        var b3Project = as(b3Limit.child(), Project.class);
        var b3EvalToLong = as(b3Project.child(), Eval.class);
        assertThat(b3EvalToLong.fields(), hasSize(1));
        var b3Converted = as(b3EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b3Converted.name(), is("$$does_not_exist1$converted_to$long"));
        var b3ToLong = as(b3Converted.child(), ToLong.class);
        assertThat(Expressions.name(b3ToLong.field()), is("does_not_exist1"));

        var b3EvalNulls = as(b3EvalToLong.child(), Eval.class);
        assertThat(b3EvalNulls.fields(), hasSize(11));
        var b3Sub = as(b3EvalNulls.child(), Subquery.class);
        var b3Filter = as(b3Sub.child(), Filter.class);
        var b3Gt = as(b3Filter.condition(), GreaterThan.class);
        var b3GtToLong = as(b3Gt.left(), ToLong.class);
        assertThat(Expressions.name(b3GtToLong.field()), is("does_not_exist1"));
        var b3SubEval = as(b3Filter.child(), Eval.class);
        assertThat(b3SubEval.fields(), hasSize(1));
        assertThat(as(as(b3SubEval.fields().getFirst(), Alias.class).child(), Literal.class).dataType(), is(DataType.NULL));
        var b3Rel = as(b3SubEval.child(), EsRelation.class);
        assertThat(b3Rel.indexPattern(), is("languages"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[languageCode{r}#24,languageCode{r}#113]
     *   \_EsqlProject[[count(*){r}#18, emp_no{r}#92 AS empNo#21, language_code{r}#102 AS languageCode#24, does_not_exist2{r}#108]]
     *     \_Aggregate[[emp_no{r}#92, language_code{r}#102, does_not_exist2{r}#108],[COUNT(*[KEYWORD],true[BOOLEAN],
     *          PT0S[TIME_DURATION]) AS count(*)#18, emp_no{r}#92, language_code{r}#102, does_not_exist2{r}#108]]
     *       \_Filter[emp_no{r}#92 > 10000[INTEGER] OR TOLONG(does_not_exist1{r}#106) < 10[INTEGER]]
     *         \_Eval[[null[NULL] AS does_not_exist1#106]]
     *           \_Eval[[null[NULL] AS does_not_exist2#108]]
     *             \_UnionAll[[_meta_field{r}#91, emp_no{r}#92, first_name{r}#93, gender{r}#94, hire_date{r}#95, job{r}#96, job.raw{r}#97,
     *                      languages{r}#98, last_name{r}#99, long_noidx{r}#100, salary{r}#101, language_code{r}#102, languageName{r}#103,
     *                      max(@timestamp){r}#104, language_name{r}#105]]
     *               |_Limit[1000[INTEGER],false,false]
     *               | \_EsqlProject[[_meta_field{f}#34, emp_no{f}#28, first_name{f}#29, gender{f}#30, hire_date{f}#35, job{f}#36,
     *                      job.raw{f}#37, languages{f}#31, last_name{f}#32, long_noidx{f}#38, salary{f}#33, language_code{r}#58,
     *                      languageName{r}#59, max(@timestamp){r}#60, language_name{r}#61]]
     *               |   \_Eval[[null[INTEGER] AS language_code#58, null[KEYWORD] AS languageName#59, null[DATETIME] AS max(@timestamp)#60,
     *                          null[KEYWORD] AS language_name#61]]
     *               |     \_EsRelation[test][_meta_field{f}#34, emp_no{f}#28, first_name{f}#29, ..]
     *               |_Limit[1000[INTEGER],false,false]
     *               | \_EsqlProject[[_meta_field{r}#62, emp_no{r}#63, first_name{r}#64, gender{r}#65, hire_date{r}#66, job{r}#67,
     *                      job.raw{r}#68, languages{r}#69, last_name{r}#70, long_noidx{r}#71, salary{r}#72, language_code{f}#39,
     *                      languageName{r}#6, max(@timestamp){r}#73, language_name{r}#74]]
     *               |   \_Eval[[null[KEYWORD] AS _meta_field#62, null[INTEGER] AS emp_no#63, null[KEYWORD] AS first_name#64,
     *                          null[TEXT] AS gender#65, null[DATETIME] AS hire_date#66, null[TEXT] AS job#67, null[KEYWORD] AS job.raw#68,
     *                          null[INTEGER] AS languages#69, null[KEYWORD] AS last_name#70, null[LONG] AS long_noidx#71,
     *                          null[INTEGER] AS salary#72, null[DATETIME] AS max(@timestamp)#73, null[KEYWORD] AS language_name#74]]
     *               |     \_Subquery[]
     *               |       \_EsqlProject[[language_code{f}#39, language_name{f}#40 AS languageName#6]]
     *               |         \_Filter[language_code{f}#39 > 10[INTEGER]]
     *               |           \_EsRelation[languages][language_code{f}#39, language_name{f}#40]
     *               |_Limit[1000[INTEGER],false,false]
     *               | \_EsqlProject[[_meta_field{r}#75, emp_no{r}#76, first_name{r}#77, gender{r}#78, hire_date{r}#79, job{r}#80,
     *                          job.raw{r}#81, languages{r}#82, last_name{r}#83, long_noidx{r}#84, salary{r}#85, language_code{r}#86,
     *                          languageName{r}#87, max(@timestamp){r}#8, language_name{r}#88]]
     *               |   \_Eval[[null[KEYWORD] AS _meta_field#75, null[INTEGER] AS emp_no#76, null[KEYWORD] AS first_name#77,
     *                          null[TEXT] AS gender#78, null[DATETIME] AS hire_date#79, null[TEXT] AS job#80, null[KEYWORD] AS job.raw#81,
     *                          null[INTEGER] AS languages#82, null[KEYWORD] AS last_name#83, null[LONG] AS long_noidx#84,
     *                          null[INTEGER] AS salary#85, null[INTEGER] AS language_code#86, null[KEYWORD] AS languageName#87,
     *                          null[KEYWORD] AS language_name#88]]
     *               |     \_Subquery[]
     *               |       \_Aggregate[[],[MAX(@timestamp{f}#41,true[BOOLEAN],PT0S[TIME_DURATION]) AS max(@timestamp)#8]]
     *               |         \_EsRelation[sample_data][@timestamp{f}#41, client_ip{f}#42, event_duration{f..]
     *               \_Limit[1000[INTEGER],false,false]
     *                 \_EsqlProject[[_meta_field{f}#51, emp_no{f}#45, first_name{f}#46, gender{f}#47, hire_date{f}#52, job{f}#53,
     *                          job.raw{f}#54, languages{f}#48, last_name{f}#49, long_noidx{f}#55, salary{f}#50, language_code{r}#12,
     *                          languageName{r}#89, max(@timestamp){r}#90, language_name{f}#57]]
     *                   \_Eval[[null[KEYWORD] AS languageName#89, null[DATETIME] AS max(@timestamp)#90]]
     *                     \_Subquery[]
     *                       \_LookupJoin[LEFT,[language_code{r}#12],[language_code{f}#56],false,null]
     *                         |_Eval[[languages{f}#48 AS language_code#12]]
     *                         | \_EsRelation[test][_meta_field{f}#51, emp_no{f}#45, first_name{f}#46, ..]
     *                         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#56, language_name{f}#57]
     */
    public void testSubquerysMixAndLookupJoin() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test,
                (FROM languages
                 | WHERE language_code > 10
                 | RENAME language_name as languageName),
                (FROM sample_data
                | STATS max(@timestamp)),
                (FROM test
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000 OR does_not_exist1::LONG < 10
            | STATS count(*) BY emp_no, language_code, does_not_exist2
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """));

        // Top implicit limit
        var topLimit = as(plan, Limit.class);
        assertThat(topLimit.limit().fold(FoldContext.small()), is(1000));

        // MvExpand on languageCode
        var mvExpand = as(topLimit.child(), MvExpand.class);
        var mvAttr = mvExpand.target();
        assertThat(Expressions.name(mvAttr), is("languageCode"));

        // EsqlProject above Aggregate
        var topProject = as(mvExpand.child(), EsqlProject.class);

        var agg = as(topProject.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(3));
        assertThat(Expressions.names(agg.groupings()), is(List.of("emp_no", "language_code", "does_not_exist2")));
        assertThat(agg.aggregates(), hasSize(4));
        assertThat(Expressions.names(agg.aggregates()), is(List.of("count(*)", "emp_no", "language_code", "does_not_exist2")));

        // Filter: emp_no > 10000 OR TOLONG(does_not_exist1) < 10
        var topFilter = as(agg.child(), Filter.class);
        var or = as(topFilter.condition(), Or.class);

        var leftGt = as(or.left(), GreaterThan.class);
        var leftAttr = as(leftGt.left(), ReferenceAttribute.class);
        assertThat(leftAttr.name(), is("emp_no"));
        assertThat(as(leftGt.right(), Literal.class).value(), is(10000));

        var rightLt = as(or.right(), LessThan.class);
        var rightToLong = as(rightLt.left(), ToLong.class);
        assertThat(Expressions.name(rightToLong.field()), is("does_not_exist1"));
        assertThat(as(rightLt.right(), Literal.class).value(), is(10));

        // Two top Evals: does_not_exist1, does_not_exist2 as NULLs
        var topEval1 = as(topFilter.child(), Eval.class);
        assertThat(topEval1.fields(), hasSize(1));
        var dne1 = as(topEval1.fields().getFirst(), Alias.class);
        assertThat(dne1.name(), is("does_not_exist1"));
        assertThat(as(dne1.child(), Literal.class).dataType(), is(DataType.NULL));

        var topEval2 = as(topEval1.child(), Eval.class);
        assertThat(topEval2.fields(), hasSize(1));
        var dne2 = as(topEval2.fields().getFirst(), Alias.class);
        assertThat(dne2.name(), is("does_not_exist2"));
        assertThat(as(dne2.child(), Literal.class).dataType(), is(DataType.NULL));

        // UnionAll with four children
        var union = as(topEval2.child(), UnionAll.class);
        assertThat(union.children(), hasSize(4));

        // Child 0: Limit -> EsqlProject -> Eval nulls -> EsRelation[test]
        var c0Limit = as(union.children().get(0), Limit.class);
        assertThat(c0Limit.limit().fold(FoldContext.small()), is(1000));
        var c0Proj = as(c0Limit.child(), Project.class);
        var c0Eval = as(c0Proj.child(), Eval.class);
        assertThat(c0Eval.fields(), hasSize(4));
        assertThat(Expressions.names(c0Eval.fields()), is(List.of("language_code", "languageName", "max(@timestamp)", "language_name")));
        var c0Rel = as(c0Eval.child(), EsRelation.class);
        assertThat(c0Rel.indexPattern(), is("test"));

        // Child 1: Limit -> EsqlProject -> Eval many nulls -> Subquery -> EsqlProject -> Filter -> EsRelation[languages]
        var c1Limit = as(union.children().get(1), Limit.class);
        assertThat(c1Limit.limit().fold(FoldContext.small()), is(1000));
        var c1Proj = as(c1Limit.child(), Project.class);
        var c1Eval = as(c1Proj.child(), Eval.class);
        assertThat(c1Eval.fields(), hasSize(13)); // many nulls incl max(@timestamp)
        var c1Sub = as(c1Eval.child(), Subquery.class);
        var c1SubProj = as(c1Sub.child(), Project.class);
        var c1SubFilter = as(c1SubProj.child(), Filter.class);
        var c1Lt = as(c1SubFilter.condition(), GreaterThan.class);
        var c1LeftAttr = as(c1Lt.left(), FieldAttribute.class);
        assertThat(c1LeftAttr.name(), is("language_code"));
        assertThat(as(c1Lt.right(), Literal.class).value(), is(10));
        var c1Rel = as(c1SubFilter.child(), EsRelation.class);
        assertThat(c1Rel.indexPattern(), is("languages"));

        // Child 2: Limit -> EsqlProject -> Eval many nulls -> Subquery -> Aggregate -> EsRelation[sample_data]
        var c2Limit = as(union.children().get(2), Limit.class);
        assertThat(c2Limit.limit().fold(FoldContext.small()), is(1000));
        var c2Proj = as(c2Limit.child(), Project.class);
        var c2Eval = as(c2Proj.child(), Eval.class);
        assertThat(c2Eval.fields(), hasSize(14));
        var c2Sub = as(c2Eval.child(), Subquery.class);
        var c2Agg = as(c2Sub.child(), org.elasticsearch.xpack.esql.plan.logical.Aggregate.class);
        assertThat(c2Agg.groupings(), hasSize(0));
        assertThat(Expressions.names(c2Agg.aggregates()), is(List.of("max(@timestamp)")));
        var c2Rel = as(c2Agg.child(), EsRelation.class);
        assertThat(c2Rel.indexPattern(), is("sample_data"));

        // Child 3: Limit -> EsqlProject -> Eval nulls -> Subquery -> LookupJoin(LEFT) languages_lookup
        var c3Limit = as(union.children().get(3), Limit.class);
        assertThat(c3Limit.limit().fold(FoldContext.small()), is(1000));
        var c3Proj = as(c3Limit.child(), Project.class);
        var c3Eval = as(c3Proj.child(), Eval.class);
        assertThat(c3Eval.fields(), hasSize(2));
        assertThat(Expressions.names(c3Eval.fields()), is(List.of("languageName", "max(@timestamp)")));
        var c3Sub = as(c3Eval.child(), Subquery.class);
        var c3Lookup = as(c3Sub.child(), LookupJoin.class);
        assertThat(c3Lookup.config().type(), is(JoinTypes.LEFT));
        var c3LeftEval = as(c3Lookup.left(), Eval.class);
        var c3LeftRel = as(c3LeftEval.child(), EsRelation.class);
        assertThat(c3LeftRel.indexPattern(), is("test"));
        var c3RightRel = as(c3Lookup.right(), EsRelation.class);
        assertThat(c3RightRel.indexPattern(), is("languages_lookup"));
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Fork[[_meta_field{r}#106, emp_no{r}#107, first_name{r}#108, gender{r}#109, hire_date{r}#110, job{r}#111, job.raw{r}#112,
     *          languages{r}#113, last_name{r}#114, long_noidx{r}#115, salary{r}#116, does_not_exist3{r}#117, does_not_exist2{r}#118,
     *          does_not_exist1{r}#119, does_not_exist2 IS NULL{r}#120, _fork{r}#121, does_not_exist4{r}#122, xyz{r}#123, x{r}#124,
     *          y{r}#125]]
     *   |_Limit[10000[INTEGER],false,false]
     *   | \_EsqlProject[[_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, gender{f}#31, hire_date{f}#36, job{f}#37, job.raw{f}#38,
     *          languages{f}#32, last_name{f}#33, long_noidx{f}#39, salary{f}#34, does_not_exist3{r}#67, does_not_exist2{r}#64,
     *          does_not_exist1{r}#62, does_not_exist2 IS NULL{r}#6, _fork{r}#9, does_not_exist4{r}#83, xyz{r}#84, x{r}#85, y{r}#86]]
     *   |   \_Eval[[null[NULL] AS does_not_exist4#83, null[KEYWORD] AS xyz#84, null[DOUBLE] AS x#85, null[DOUBLE] AS y#86]]
     *   |     \_Eval[[fork1[KEYWORD] AS _fork#9]]
     *   |       \_Limit[7[INTEGER],false,false]
     *   |         \_OrderBy[[Order[does_not_exist3{r}#67,ASC,LAST]]]
     *   |           \_Filter[emp_no{f}#29 > 3[INTEGER]]
     *   |             \_Eval[[ISNULL(does_not_exist2{r}#64) AS does_not_exist2 IS NULL#6]]
     *   |               \_Filter[first_name{f}#30 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#62) > 5[INTEGER]]
     *   |                 \_Eval[[null[NULL] AS does_not_exist1#62]]
     *   |                   \_Eval[[null[NULL] AS does_not_exist2#64]]
     *   |                     \_Eval[[null[NULL] AS does_not_exist3#67]]
     *   |                       \_EsRelation[test][_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, ..]
     *   |_Limit[1000[INTEGER],false,false]
     *   | \_EsqlProject[[_meta_field{f}#46, emp_no{f}#40, first_name{f}#41, gender{f}#42, hire_date{f}#47, job{f}#48, job.raw{f}#49,
     *              languages{f}#43, last_name{f}#44, long_noidx{f}#50, salary{f}#45, does_not_exist3{r}#87, does_not_exist2{r}#71,
     *              does_not_exist1{r}#69, does_not_exist2 IS NULL{r}#6, _fork{r}#9, does_not_exist4{r}#74, xyz{r}#21, x{r}#88, y{r}#89]]
     *   |   \_Eval[[null[NULL] AS does_not_exist3#87, null[DOUBLE] AS x#88, null[DOUBLE] AS y#89]]
     *   |     \_Eval[[fork2[KEYWORD] AS _fork#9]]
     *   |       \_Eval[[TOSTRING(does_not_exist4{r}#74) AS xyz#21]]
     *   |         \_Filter[emp_no{f}#40 > 2[INTEGER]]
     *   |           \_Eval[[ISNULL(does_not_exist2{r}#71) AS does_not_exist2 IS NULL#6]]
     *   |             \_Filter[first_name{f}#41 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#69) > 5[INTEGER]]
     *   |               \_Eval[[null[NULL] AS does_not_exist1#69]]
     *   |                 \_Eval[[null[NULL] AS does_not_exist2#71]]
     *   |                   \_Eval[[null[NULL] AS does_not_exist4#74]]
     *   |                     \_EsRelation[test][_meta_field{f}#46, emp_no{f}#40, first_name{f}#41, ..]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsqlProject[[_meta_field{r}#90, emp_no{r}#91, first_name{r}#92, gender{r}#93, hire_date{r}#94, job{r}#95, job.raw{r}#96,
     *              languages{r}#97, last_name{r}#98, long_noidx{r}#99, salary{r}#100, does_not_exist3{r}#101, does_not_exist2{r}#102,
     *              does_not_exist1{r}#103, does_not_exist2 IS NULL{r}#104, _fork{r}#9, does_not_exist4{r}#105, xyz{r}#27, x{r}#13,
     *              y{r}#16]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#90, null[INTEGER] AS emp_no#91, null[KEYWORD] AS first_name#92, null[TEXT] AS gender#93,
     *              null[DATETIME] AS hire_date#94, null[TEXT] AS job#95, null[KEYWORD] AS job.raw#96, null[INTEGER] AS languages#97,
     *              null[KEYWORD] AS last_name#98, null[LONG] AS long_noidx#99, null[INTEGER] AS salary#100,
     *              null[NULL] AS does_not_exist3#101, null[NULL] AS does_not_exist2#102, null[NULL] AS does_not_exist1#103,
     *              null[BOOLEAN] AS does_not_exist2 IS NULL#104, null[NULL] AS does_not_exist4#105]]
     *         \_Eval[[fork3[KEYWORD] AS _fork#9]]
     *           \_Eval[[abc[KEYWORD] AS xyz#27]]
     *             \_Aggregate[[],[MIN(TODOUBLE(d{r}#22),true[BOOLEAN],PT0S[TIME_DURATION]) AS x#13,
     *                  FilteredExpression[MAX(TODOUBLE(e{r}#23),true[BOOLEAN],PT0S[TIME_DURATION]),
     *                  TODOUBLE(d{r}#22) > 1000[INTEGER] + TODOUBLE(does_not_exist5{r}#81)] AS y#16]]
     *               \_Dissect[first_name{f}#52,Parser[pattern=%{d} %{e} %{f}, appendSeparator=,
     *                      parser=org.elasticsearch.dissect.DissectParser@6d208bc5],[d{r}#22, e{r}#23, f{r}#24]]
     *                 \_Eval[[ISNULL(does_not_exist2{r}#78) AS does_not_exist2 IS NULL#6]]
     *                   \_Filter[first_name{f}#52 == Chris[KEYWORD] AND TOLONG(does_not_exist1{r}#76) > 5[INTEGER]]
     *                     \_Eval[[null[NULL] AS does_not_exist1#76]]
     *                       \_Eval[[null[NULL] AS does_not_exist2#78]]
     *                         \_Eval[[null[NULL] AS does_not_exist5#81]]
     *                           \_EsRelation[test][_meta_field{f}#57, emp_no{f}#51, first_name{f}#52, ..]
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
        var fork = as(topLimit.child(), org.elasticsearch.xpack.esql.plan.logical.Fork.class);
        assertThat(fork.children(), hasSize(3));

        // Branch 0
        var b0Limit = as(fork.children().get(0), Limit.class);
        assertThat(b0Limit.limit().fold(FoldContext.small()), is(10000));
        var b0Proj = as(b0Limit.child(), EsqlProject.class);

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
        var b0OrderBy = as(b0InnerLimit.child(), org.elasticsearch.xpack.esql.plan.logical.OrderBy.class);
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
        var b0EvalDne1Alias = as(b0EvalDne1.fields().getFirst(), Alias.class);
        assertThat(b0EvalDne1Alias.name(), is("does_not_exist1"));
        assertThat(as(b0EvalDne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b0EvalDne2 = as(b0EvalDne1.child(), Eval.class);
        var b0EvalDne2Alias = as(b0EvalDne2.fields().getFirst(), Alias.class);
        assertThat(b0EvalDne2Alias.name(), is("does_not_exist2"));
        assertThat(as(b0EvalDne2Alias.child(), Literal.class).dataType(), is(DataType.NULL)); // does_not_exist2
        var b0EvalDne3 = as(b0EvalDne2.child(), Eval.class);
        var b0EvalDne3Alias = as(b0EvalDne3.fields().getFirst(), Alias.class);
        assertThat(b0EvalDne3Alias.name(), is("does_not_exist3"));
        assertThat(as(b0EvalDne3Alias.child(), Literal.class).dataType(), is(DataType.NULL)); // does_not_exist3

        var b0Rel = as(b0EvalDne3.child(), EsRelation.class);
        assertThat(b0Rel.indexPattern(), is("test"));

        // Branch 1
        var b1Limit = as(fork.children().get(1), Limit.class);
        assertThat(b1Limit.limit().fold(FoldContext.small()), is(1000));
        var b1Proj = as(b1Limit.child(), EsqlProject.class);

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
        var b1EvalDne1Alias = as(b1EvalDne1.fields().getFirst(), Alias.class);
        assertThat(b1EvalDne1Alias.name(), is("does_not_exist1"));
        assertThat(as(b1EvalDne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b1EvalDne2 = as(b1EvalDne1.child(), Eval.class);
        var b1EvalDne2Alias = as(b1EvalDne2.fields().getFirst(), Alias.class);
        assertThat(b1EvalDne2Alias.name(), is("does_not_exist2"));
        assertThat(as(b1EvalDne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var b1EvalDne4 = as(b1EvalDne2.child(), Eval.class);
        var b1EvalDne4Alias = as(b1EvalDne4.fields().getFirst(), Alias.class);
        assertThat(b1EvalDne4Alias.name(), is("does_not_exist4"));
        assertThat(as(b1EvalDne4Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var b1Rel = as(b1EvalDne4.child(), EsRelation.class);
        assertThat(b1Rel.indexPattern(), is("test"));

        // Branch 2
        var b2Limit = as(fork.children().get(2), Limit.class);
        assertThat(b2Limit.limit().fold(FoldContext.small()), is(1000));
        var b2Proj = as(b2Limit.child(), EsqlProject.class);

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
        var dne1Alias = as(evalDne1.fields().getFirst(), Alias.class);
        assertThat(dne1Alias.name(), is("does_not_exist1"));
        assertThat(as(dne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var evalDne2 = as(evalDne1.child(), Eval.class);
        var dne2Alias = as(evalDne2.fields().getFirst(), Alias.class);
        assertThat(dne2Alias.name(), is("does_not_exist2"));
        assertThat(as(dne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));
        var evalDne5 = as(evalDne2.child(), Eval.class);
        var dne5Alias = as(evalDne5.fields().getFirst(), Alias.class);
        assertThat(dne5Alias.name(), is("does_not_exist5"));
        assertThat(as(dne5Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var rel = as(evalDne5.child(), EsRelation.class);
        assertThat(rel.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_InlineStats[]
     *   \_Aggregate[[does_not_exist2{r}#19],[SUM(does_not_exist1{r}#20,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS c#5,
     *          does_not_exist2{r}#19]]
     *     \_Eval[[null[NULL] AS does_not_exist2#19, null[NULL] AS does_not_exist1#20]]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testInlineStats() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | INLINE STATS c = SUM(does_not_exist1) BY does_not_exist2
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // InlineStats wrapping Aggregate
        var inlineStats = as(limit.child(), org.elasticsearch.xpack.esql.plan.logical.InlineStats.class);
        var agg = as(inlineStats.child(), Aggregate.class);

        // Grouping by does_not_exist2 and SUM over does_not_exist1
        assertThat(agg.groupings(), hasSize(1));
        var groupRef = as(agg.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(groupRef.name(), is("does_not_exist2"));

        assertThat(agg.aggregates(), hasSize(2));
        var cAlias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(cAlias.name(), is("c"));
        as(cAlias.child(), org.elasticsearch.xpack.esql.expression.function.aggregate.Sum.class);

        // Upstream Eval introduces does_not_exist2 and does_not_exist1 as NULL
        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));

        var dne2Alias = as(eval.fields().get(0), Alias.class);
        assertThat(dne2Alias.name(), is("does_not_exist2"));
        assertThat(as(dne2Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        var dne1Alias = as(eval.fields().get(1), Alias.class);
        assertThat(dne1Alias.name(), is("does_not_exist1"));
        assertThat(as(dne1Alias.child(), Literal.class).dataType(), is(DataType.NULL));

        // Underlying relation
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_LookupJoin[LEFT,[language_code{r}#5],[language_code{f}#19],false,null]
     *   |_Eval[[TOINTEGER(does_not_exist{r}#21) AS language_code#5]]
     *   | \_Eval[[null[NULL] AS does_not_exist#21]]
     *   |   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     */
    public void testLookupJoin() {
        String query = """
            FROM test
            | EVAL language_code = does_not_exist::INTEGER
            | LOOKUP JOIN languages_lookup ON language_code
            """;
        var plan = analyzeStatement(setUnmappedNullify(query));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // LookupJoin over alias `language_code`
        var lj = as(limit.child(), LookupJoin.class);
        assertThat(lj.config().type(), is(JoinTypes.LEFT));

        // Left child: EVAL language_code = TOINTEGER(does_not_exist), with upstream NULL alias for does_not_exist
        var leftEval = as(lj.left(), Eval.class);
        assertThat(leftEval.fields(), hasSize(1));
        var langCodeAlias = as(leftEval.fields().getFirst(), Alias.class);
        assertThat(langCodeAlias.name(), is("language_code"));
        as(langCodeAlias.child(), ToInteger.class);

        var upstreamEval = as(leftEval.child(), Eval.class);
        assertThat(upstreamEval.fields(), hasSize(1));
        var dneAlias = as(upstreamEval.fields().getFirst(), Alias.class);
        assertThat(dneAlias.name(), is("does_not_exist"));
        assertThat(as(dneAlias.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(upstreamEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("test"));

        // Right lookup table
        var rightRel = as(lj.right(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("languages_lookup"));
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
