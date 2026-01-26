/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
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
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
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

    public void testFailKeepAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
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
     * \_Project[[does_not_exist_field1{r}#20, does_not_exist_field2{r}#22]]
     *   \_Eval[[TOINTEGER(does_not_exist_field1{r}#20) + 42[INTEGER] AS x#5]]
     *     \_Eval[[null[NULL] AS does_not_exist_field1#20, null[NULL] AS does_not_exist_field2#22]]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
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
        assertThat(Expressions.names(eval2.fields()), is(List.of("does_not_exist_field1", "does_not_exist_field2")));

        var relation = as(eval2.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[does_not_exist_field{r}#22 + 2[INTEGER] AS y#9]]
     *   \_Eval[[emp_no{f}#11 + 1[INTEGER] AS x#6]]
     *     \_Project[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20,
     *          languages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16, does_not_exist_field{r}#22]]
     *       \_Eval[[null[NULL] AS does_not_exist_field#22]]
     *         \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testEvalAfterKeepStar() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP *
            | EVAL x = emp_no + 1
            | EVAL y = does_not_exist_field + 2
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Eval for y = does_not_exist_field + 2
        var evalY = as(limit.child(), Eval.class);
        assertThat(evalY.fields(), hasSize(1));
        assertThat(evalY.fields().get(0).name(), is("y"));

        // The child is Eval for x = emp_no + 1
        var evalX = as(evalY.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        assertThat(evalX.fields().get(0).name(), is("x"));

        // The child is Project with all fields plus does_not_exist_field
        var esqlProject = as(evalX.child(), Project.class);
        assertThat(
            Expressions.names(esqlProject.output()),
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
                    "salary",
                    "does_not_exist_field"
                )
            )
        );

        // The child is Eval introducing does_not_exist_field as null
        var evalNull = as(esqlProject.child(), Eval.class);
        assertThat(evalNull.fields(), hasSize(1));
        var alias = as(evalNull.fields().get(0), Alias.class);
        assertThat(alias.name(), is("does_not_exist_field"));
        var lit = as(alias.child(), Literal.class);
        assertThat(lit.dataType(), is(DataType.NULL));

        // The child is EsRelation
        var relation = as(evalNull.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[emp_does_not_exist_field{r}#23 + 2[INTEGER] AS y#10]]
     *   \_Eval[[emp_no{f}#12 + 1[INTEGER] AS x#7]]
     *     \_Project[[emp_no{f}#12, _meta_field{f}#18, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     *          languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17, emp_does_not_exist_field{r}#23]]
     *       \_Eval[[null[NULL] AS emp_does_not_exist_field#23]]
     *         \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testEvalAfterMatchingKeepWithWildcard() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP emp_no, *
            | EVAL x = emp_no + 1
            | EVAL y = emp_does_not_exist_field + 2
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Eval for y = emp_does_not_exist_field + 2
        var evalY = as(limit.child(), Eval.class);
        assertThat(evalY.fields(), hasSize(1));
        assertThat(evalY.fields().get(0).name(), is("y"));

        // The child is Eval for x = emp_no + 1
        var evalX = as(evalY.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        assertThat(evalX.fields().get(0).name(), is("x"));

        // The child is Project with all fields plus emp_does_not_exist_field
        var esqlProject = as(evalX.child(), Project.class);
        assertThat(
            Expressions.names(esqlProject.output()),
            is(
                List.of(
                    "emp_no",
                    "_meta_field",
                    "first_name",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "last_name",
                    "long_noidx",
                    "salary",
                    "emp_does_not_exist_field"
                )
            )
        );

        // The child is Eval introducing emp_does_not_exist_field as null
        var evalNull = as(esqlProject.child(), Eval.class);
        assertThat(evalNull.fields(), hasSize(1));
        var alias = as(evalNull.fields().get(0), Alias.class);
        assertThat(alias.name(), is("emp_does_not_exist_field"));
        var lit = as(alias.child(), Literal.class);
        assertThat(lit.dataType(), is(DataType.NULL));

        // The child is EsRelation
        var relation = as(evalNull.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, languages{f}#9,
     *      last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#17, null[NULL] AS neither_this#18]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testDrop() {
        var extraField = randomFrom("", "does_not_exist_field", "neither_this");
        var hasExtraField = extraField.isEmpty() == false;
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | DROP does_not_exist_field
            """ + (hasExtraField ? ", " : "") + extraField)); // add emp_no to avoid "no fields left" case

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

        var eval = as(project.child(), Eval.class);
        var expectedNames = hasExtraField && extraField.equals("does_not_exist_field") == false
            ? List.of("does_not_exist_field", extraField)
            : List.of("does_not_exist_field");
        assertThat(Expressions.names(eval.fields()), is(expectedNames));
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_meta_field{f}#12, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, languages{f}#9,
     *      last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#18]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
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

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist_field")));
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
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
     * \_Eval[[does_not_exist{r}#21 + 1[INTEGER] AS x#8]]
     *   \_Project[[_meta_field{f}#16, emp_no{f}#10 AS employee_number#5, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18,
     *          job.raw{f}#19, languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15, does_not_exist{r}#21]]
     *     \_Eval[[null[NULL] AS does_not_exist#21]]
     *       \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     */
    public void testEvalAfterRename() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | RENAME emp_no AS employee_number
            | EVAL x = does_not_exist + 1
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        assertThat(eval.fields().get(0).name(), is("x"));
        assertThat(
            Expressions.names(eval.output()),
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
                    "does_not_exist",
                    "x"
                )
            )
        );

        var esqlProject = as(eval.child(), Project.class);
        assertThat(
            Expressions.names(esqlProject.output()),
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
                    "does_not_exist"
                )
            )
        );

        var evalNull = as(esqlProject.child(), Eval.class);
        assertThat(evalNull.fields(), hasSize(1));
        var alias = as(evalNull.fields().get(0), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        var lit = as(alias.child(), Literal.class);
        assertThat(lit.dataType(), is(DataType.NULL));

        var relation = as(evalNull.child(), EsRelation.class);
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
     * \_Eval[[b{r}#25 + c{r}#27 AS y#12]]
     *   \_Eval[[a{r}#4 + b{r}#25 AS x#8]]
     *     \_Eval[[1[INTEGER] AS a#4, null[NULL] AS b#25, null[NULL] AS c#27]]
     *       \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     */
    public void testMultipleEvaled() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL a = 1
            | EVAL x = a + b
            | EVAL y = b + c
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var evalY = as(limit.child(), Eval.class);
        assertThat(evalY.fields(), hasSize(1));
        assertThat(evalY.fields().get(0).name(), is("y"));

        var evalX = as(evalY.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        assertThat(evalX.fields().get(0).name(), is("x"));

        var evalABC = as(evalX.child(), Eval.class);
        assertThat(Expressions.names(evalABC.fields()), is(List.of("a", "b", "c")));
        for (var field : evalABC.fields()) {
            var alias = as(field, Alias.class);
            var literal = as(alias.child(), Literal.class);
            if (alias.name().equals("a") == false) {
                assertThat(literal.dataType(), is(DataType.NULL));
                assertThat(literal.value(), is(nullValue()));
            }
        }

        var relation = as(evalABC.child(), EsRelation.class);
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
     * \_Eval[[TOLONG(does_not_exist_field{r}#17) AS does_not_exist_field::LONG#4]]
     *   \_Eval[[null[NULL] AS does_not_exist_field#17]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testCastingNoAliasing() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | EVAL does_not_exist_field::LONG
            """));

        assertThat(Expressions.names(plan.output()), hasItems("does_not_exist_field", "does_not_exist_field::LONG"));
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

        var agg = as(limit.child(), Aggregate.class);
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

        var agg = as(limit.child(), Aggregate.class);
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

        var agg = as(limit.child(), Aggregate.class);
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
     *   \_Eval[[null[NULL] AS does_not_exist2#24, null[NULL] AS does_not_exist1#25, null[NULL] AS d2#26]]
     *     \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testStatsAggAndAliasedGroup() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS s = SUM(does_not_exist1) + d2 BY d2 = does_not_exist2, emp_no
            """));

        assertThat(Expressions.names(plan.output()), is(List.of("s", "d2", "emp_no")));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), Aggregate.class);
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
        assertThat(eval.fields(), hasSize(3));
        var alias2 = as(eval.fields().get(0), Alias.class);
        assertThat(alias2.name(), is("does_not_exist2"));
        assertThat(as(alias2.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias1 = as(eval.fields().get(1), Alias.class);
        assertThat(alias1.name(), is("does_not_exist1"));
        assertThat(as(alias1.child(), Literal.class).dataType(), is(DataType.NULL));
        var alias0 = as(eval.fields().get(2), Alias.class);
        assertThat(alias0.name(), is("d2"));
        assertThat(as(alias0.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[does_not_exist2{r}#29 + does_not_exist3{r}#30 AS s0#6, emp_no{f}#18 AS s1#9],[SUM(does_not_exist1{r}#31,true[B
     * OOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) + s0{r}#6 + s1{r}#9 AS sum#14, s0{r}#6, s1{r}#9]]
     *   \_Eval[[null[NULL] AS does_not_exist2#29, null[NULL] AS does_not_exist3#30, null[NULL] AS does_not_exist1#31,
     *          null[NULL] AS s0#32]]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testStatsAggAndAliasedGroupWithExpression() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | STATS sum = SUM(does_not_exist1) + s0 + s1 BY s0 = does_not_exist2 + does_not_exist3, s1 = emp_no
            """));

        assertThat(Expressions.names(plan.output()), is(List.of("sum", "s0", "s1")));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        assertThat(Expressions.names(agg.groupings()), is(List.of("s0", "s1")));

        assertThat(agg.aggregates(), hasSize(3)); // includes grouping keys
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("sum"));
        assertThat(Expressions.name(alias.child()), is("SUM(does_not_exist1) + s0 + s1"));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(4));
        assertThat(Expressions.names(eval.fields()), is(List.of("does_not_exist2", "does_not_exist3", "does_not_exist1", "s0")));
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

        var agg = as(limit.child(), Aggregate.class);
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

        var inlineStats = as(limit.child(), InlineStats.class);
        var agg = as(inlineStats.child(), Aggregate.class);
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

        var agg = as(limit.child(), Aggregate.class);
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

        var filter = as(limit.child(), Filter.class);
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

        var filter = as(limit.child(), Filter.class);
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

        var filter = as(limit.child(), Filter.class);
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

        var agg = as(limit.child(), Aggregate.class);
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

        var agg = as(limit.child(), Aggregate.class);
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
        var orderBy = as(limit.child(), OrderBy.class);

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

        var orderBy = as(limit.child(), OrderBy.class);
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

        var orderBy = as(limit.child(), OrderBy.class);
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

    /*
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

        var mvExpand = as(limit.child(), MvExpand.class);
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
     * \_UnionAll[[language_code{r}#22, language_name{r}#23, does_not_exist1{r}#24, @timestamp{r}#25, client_ip{r}#26,
     *      event_duration{r}#27, message{r}#28]]
     *   |_Project[[language_code{f}#6, language_name{f}#7, does_not_exist1{r}#12, @timestamp{r}#16, client_ip{r}#17, event_duration{r}#18,
     *          message{r}#19]]
     *   | \_Eval[[null[DATETIME] AS @timestamp#16, null[IP] AS client_ip#17, null[LONG] AS event_duration#18, null[KEYWORD] AS message#19]]
     *   |   \_Subquery[]
     *   |     \_Filter[TOLONG(does_not_exist1{r}#12) > 1[INTEGER]]
     *   |       \_Eval[[null[NULL] AS does_not_exist1#12]]
     *   |         \_EsRelation[languages][language_code{f}#6, language_name{f}#7]
     *   \_Project[[language_code{r}#20, language_name{r}#21, does_not_exist1{r}#14, @timestamp{f}#8, client_ip{f}#9, event_duration{f}#10,
     *          message{f}#11]]
     *     \_Eval[[null[INTEGER] AS language_code#20, null[KEYWORD] AS language_name#21]]
     *       \_Subquery[]
     *         \_Filter[TODOUBLE(does_not_exist1{r}#14) > 10.0[DOUBLE]]
     *           \_Eval[[null[NULL] AS does_not_exist1#14]]
     *             \_EsRelation[sample_data][@timestamp{f}#8, client_ip{f}#9, event_duration{f}#..]
     */
    public void testDoubleSubqueryOnly() {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

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
        var leftProject = as(union.children().get(0), Project.class);
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
        var rightProject = as(union.children().get(1), Project.class);
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
     * Project[[language_code{r}#34, language_name{r}#35, does_not_exist1{r}#36, @timestamp{r}#37, client_ip{r}#38, event_duration{r}#39,
     *      message{r}#40, does_not_exist2{r}#41]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[$$does_not_exist2$converted_to$long{r$}#44 < 100[INTEGER]]
     *     \_UnionAll[[language_code{r}#34, language_name{r}#35, does_not_exist1{r}#36, @timestamp{r}#37, client_ip{r}#38,
     *              event_duration{r}#39, message{r}#40, does_not_exist2{r}#41, $$does_not_exist2$converted_to$long{r$}#44]]
     *       |_Project[[language_code{f}#7, language_name{f}#8, does_not_exist1{r}#13, @timestamp{r}#17, client_ip{r}#18,
     *              event_duration{r}#19, message{r}#20, does_not_exist2{r}#30, $$does_not_exist2$converted_to$long{r$}#42]]
     *       | \_Eval[[TOLONG(does_not_exist2{r}#30) AS $$does_not_exist2$converted_to$long#42]]
     *       |   \_Eval[[null[DATETIME] AS @timestamp#17, null[IP] AS client_ip#18, null[LONG] AS event_duration#19,
     *                  null[KEYWORD] AS message#20]]
     *       |     \_Subquery[]
     *       |       \_Filter[TOLONG(does_not_exist1{r}#13) > 1[INTEGER]]
     *       |         \_Eval[[null[NULL] AS does_not_exist1#13, null[NULL] AS does_not_exist2#30]]
     *       |           \_EsRelation[languages][language_code{f}#7, language_name{f}#8]
     *       \_Project[[language_code{r}#21, language_name{r}#22, does_not_exist1{r}#15, @timestamp{f}#9, client_ip{f}#10,
     *              event_duration{f}#11, message{f}#12, does_not_exist2{r}#31, $$does_not_exist2$converted_to$long{r$}#43]]
     *         \_Eval[[TOLONG(does_not_exist2{r}#31) AS $$does_not_exist2$converted_to$long#43]]
     *           \_Eval[[null[INTEGER] AS language_code#21, null[KEYWORD] AS language_name#22]]
     *             \_Subquery[]
     *               \_Filter[TODOUBLE(does_not_exist1{r}#15) > 10.0[DOUBLE]]
     *                 \_Eval[[null[NULL] AS does_not_exist1#15, null[NULL] AS does_not_exist2#31]]
     *                   \_EsRelation[sample_data][@timestamp{f}#9, client_ip{f}#10, event_duration{f}..]
     */
    public void testDoubleSubqueryOnlyWithTopFilterAndNoMain() {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1),
                (FROM sample_data
                 | WHERE does_not_exist1::DOUBLE > 10.)
            | WHERE does_not_exist2::LONG < 100
            """));

        // Top-level Project wrapping the plan
        var topProject = as(plan, Project.class);
        assertThat(
            Expressions.names(topProject.output()),
            is(
                List.of(
                    "language_code",
                    "language_name",
                    "does_not_exist1",
                    "@timestamp",
                    "client_ip",
                    "event_duration",
                    "message",
                    "does_not_exist2"
                )
            )
        );
        var topProjectAttribute_does_not_exist1 = topProject.output().get(2);
        var topProjectAttribute_does_not_exist2 = topProject.output().get(7);

        // Below Project is Limit
        var limit = as(topProject.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Below Limit is Filter with does_not_exist2 conversion
        var filter = as(limit.child(), Filter.class);
        var filterCondition = as(filter.condition(), LessThan.class);
        assertThat(Expressions.name(filterCondition.right()), is("100"));

        // Below Filter is UnionAll
        var union = as(filter.child(), UnionAll.class);
        assertThat(
            Expressions.names(union.output()),
            is(
                List.of(
                    "language_code",
                    "language_name",
                    "does_not_exist1",
                    "@timestamp",
                    "client_ip",
                    "event_duration",
                    "message",
                    "does_not_exist2",
                    "$$does_not_exist2$converted_to$long"
                )
            )
        );
        var unionAllAttribute_does_not_exist1 = union.output().get(2);
        assertThat(topProjectAttribute_does_not_exist1, is(unionAllAttribute_does_not_exist1)); // reference is kept
        var unionAllAttribute_does_not_exist2 = union.output().get(7);
        assertThat(topProjectAttribute_does_not_exist2, is(unionAllAttribute_does_not_exist2)); // reference is kept
        assertThat(union.children(), hasSize(2));

        // Left branch: languages
        var leftProject = as(union.children().get(0), Project.class);
        assertThat(
            Expressions.names(leftProject.output()),
            is(
                List.of(
                    "language_code",
                    "language_name",
                    "does_not_exist1",
                    "@timestamp",
                    "client_ip",
                    "event_duration",
                    "message",
                    "does_not_exist2",
                    "$$does_not_exist2$converted_to$long"
                )
            )
        );
        var unionAllLeftProjectAttribute_does_not_exist1 = leftProject.output().get(2);
        assertThat(unionAllLeftProjectAttribute_does_not_exist1, not(unionAllAttribute_does_not_exist1)); // name remains, ID changes
        var unionAllLeftProjectAttribute_does_not_exist2 = leftProject.output().get(7);
        assertThat(unionAllLeftProjectAttribute_does_not_exist2, not(unionAllAttribute_does_not_exist2)); // name remains, ID changes
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(leftEval.fields(), hasSize(1));
        assertThat(Expressions.name(leftEval.fields().getFirst()), is("$$does_not_exist2$converted_to$long"));
        var leftEvalEval = as(leftEval.child(), Eval.class);
        // Verify unmapped null aliases for @timestamp, client_ip, event_duration, message
        assertThat(Expressions.names(leftEvalEval.fields()), is(List.of("@timestamp", "client_ip", "event_duration", "message")));

        var leftSubquery = as(leftEvalEval.child(), Subquery.class);
        var leftSubFilter = as(leftSubquery.child(), Filter.class);
        var leftGt = as(leftSubFilter.condition(), GreaterThan.class);
        var leftToLong = as(leftGt.left(), ToLong.class);
        assertThat(Expressions.name(leftToLong.field()), is("does_not_exist1"));

        var leftSubEval = as(leftSubFilter.child(), Eval.class);
        assertThat(leftSubEval.fields(), hasSize(2));
        var leftDoesNotExist1 = as(leftSubEval.fields().get(0), Alias.class);
        assertThat(leftDoesNotExist1.name(), is("does_not_exist1"));
        assertThat(as(leftDoesNotExist1.child(), Literal.class).dataType(), is(DataType.NULL));
        assertThat(leftDoesNotExist1.id(), is(unionAllLeftProjectAttribute_does_not_exist1.id())); // same IDs withing the branch
        var leftDoesNotExist2 = as(leftSubEval.fields().get(1), Alias.class);
        assertThat(leftDoesNotExist2.name(), is("does_not_exist2"));
        assertThat(as(leftDoesNotExist2.child(), Literal.class).dataType(), is(DataType.NULL));
        assertThat(leftDoesNotExist2.id(), is(unionAllLeftProjectAttribute_does_not_exist2.id())); // same IDs withing the branch

        var leftRel = as(leftSubEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("languages"));

        // Right branch: sample_data
        var rightProject = as(union.children().get(1), Project.class);
        assertThat(
            Expressions.names(rightProject.output()),
            is(
                List.of(
                    "language_code",
                    "language_name",
                    "does_not_exist1",
                    "@timestamp",
                    "client_ip",
                    "event_duration",
                    "message",
                    "does_not_exist2",
                    "$$does_not_exist2$converted_to$long"
                )
            )
        );
        var unionAllRightProjectAttribute_does_not_exist1 = rightProject.output().get(2);
        assertThat(unionAllRightProjectAttribute_does_not_exist1, not(unionAllAttribute_does_not_exist1)); // name remains, ID changes
        var unionAllRightProjectAttribute_does_not_exist2 = rightProject.output().get(7);
        assertThat(unionAllRightProjectAttribute_does_not_exist2, not(unionAllLeftProjectAttribute_does_not_exist2)); // not same ID
        var rightEval = as(rightProject.child(), Eval.class);
        assertThat(Expressions.name(rightEval.fields().getFirst()), is("$$does_not_exist2$converted_to$long"));
        var rightEvalEval = as(rightEval.child(), Eval.class);
        assertThat(Expressions.names(rightEvalEval.fields()), is(List.of("language_code", "language_name")));

        var rightSubquery = as(rightEvalEval.child(), Subquery.class);
        var rightSubFilter = as(rightSubquery.child(), Filter.class);
        var rightGt = as(rightSubFilter.condition(), GreaterThan.class);
        var rightToDouble = as(rightGt.left(), ToDouble.class);
        assertThat(Expressions.name(rightToDouble.field()), is("does_not_exist1"));

        var rightSubEval = as(rightSubFilter.child(), Eval.class);
        assertThat(rightSubEval.fields(), hasSize(2));
        var rightDoesNotExist1 = as(rightSubEval.fields().get(0), Alias.class);
        assertThat(rightDoesNotExist1.name(), is("does_not_exist1"));
        assertThat(as(rightDoesNotExist1.child(), Literal.class).dataType(), is(DataType.NULL));
        assertThat(rightDoesNotExist1.id(), is(unionAllRightProjectAttribute_does_not_exist1.id())); // same IDs withing the branch
        var rightDoesNotExist2 = as(rightSubEval.fields().get(1), Alias.class);
        assertThat(rightDoesNotExist2.name(), is("does_not_exist2"));
        assertThat(as(rightDoesNotExist2.child(), Literal.class).dataType(), is(DataType.NULL));
        assertThat(rightDoesNotExist2.id(), is(unionAllRightProjectAttribute_does_not_exist2.id())); // same IDs withing the branch

        var rightRel = as(rightSubEval.child(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("sample_data"));
    }

    /*
     * Project[[_meta_field{r}#54, emp_no{r}#55, first_name{r}#56, gender{r}#57, hire_date{r}#58, job{r}#59, job.raw{r}#60,
     *      languages{r}#61, last_name{r}#62, long_noidx{r}#63, salary{r}#64, language_code{r}#65, language_name{r}#66,
     *      does_not_exist1{r}#67, does_not_exist2{r}#68]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[$$does_not_exist2$converted_to$long{r$}#71 < 10[INTEGER] AND emp_no{r}#37 > 0[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#54, emp_no{r}#55, first_name{r}#56, gender{r}#57, hire_date{r}#58, job{r}#59, job.raw{r}#60,
     *          languages{r}#61, last_name{r}#62, long_noidx{r}#63, salary{r}#64, language_code{r}#65, language_name{r}#66,
     *          does_not_exist1{r}#67, does_not_exist2{r}#68, $$does_not_exist2$converted_to$long{r$}#71]]
     *       |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *              languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#22, language_name{r}#23,
     *              does_not_exist1{r}#24, does_not_exist2{r}#50, $$does_not_exist2$converted_to$long{r$}#69]]
     *       | \_Eval[[TOLONG(does_not_exist2{r}#50) AS $$does_not_exist2$converted_to$long#69]]
     *       |   \_Eval[[null[INTEGER] AS language_code#22, null[KEYWORD] AS language_name#23, null[NULL] AS does_not_exist1#24,
     *                  null[NULL] AS does_not_exist2#50]]
     *       |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_Project[[_meta_field{r}#25, emp_no{r}#26, first_name{r}#27, gender{r}#28, hire_date{r}#29, job{r}#30, job.raw{r}#31,
     *              languages{r}#32, last_name{r}#33, long_noidx{r}#34, salary{r}#35, language_code{f}#18, language_name{f}#19,
     *              does_not_exist1{r}#20, does_not_exist2{r}#51, $$does_not_exist2$converted_to$long{r$}#70]]
     *         \_Eval[[TOLONG(does_not_exist2{r}#51) AS $$does_not_exist2$converted_to$long#70]]
     *           \_Eval[[null[KEYWORD] AS _meta_field#25, null[INTEGER] AS emp_no#26, null[KEYWORD] AS first_name#27,
     *                  null[TEXT] AS gender#28, null[DATETIME] AS hire_date#29, null[TEXT] AS job#30, null[KEYWORD] AS job.raw#31,
     *                  null[INTEGER] AS languages#32, null[KEYWORD] AS last_name#33, null[LONG] AS long_noidx#34,
     *                  null[INTEGER] AS salary#35]]
     *             \_Subquery[]
     *               \_Filter[TOLONG(does_not_exist1{r}#20) > 1[INTEGER]]
     *                 \_Eval[[null[NULL] AS does_not_exist1#20, null[NULL] AS does_not_exist2#51]]
     *                   \_EsRelation[languages][language_code{f}#18, language_name{f}#19]
     */
    public void testSubqueryAndMainQuery() {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test,
                (FROM languages
                 | WHERE does_not_exist1::LONG > 1)
            | WHERE does_not_exist2::LONG < 10 AND emp_no > 0
            """));

        // Top implicit limit
        var project = as(plan, Project.class);
        assertThat(
            Expressions.names(project.output()),
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
                    "salary",
                    "language_code",
                    "language_name",
                    "does_not_exist1",
                    "does_not_exist2"
                )
            )
        );

        var limit = as(project.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // Top filter: TOLONG(does_not_exist2) < 10 AND emp_no > 0
        var topFilter = as(limit.child(), Filter.class);
        var topAnd = as(topFilter.condition(), And.class);

        var leftCond = as(topAnd.left(), LessThan.class);
        var leftToLong = as(leftCond.left(), ReferenceAttribute.class);
        assertThat(Expressions.name(leftToLong), is("$$does_not_exist2$converted_to$long"));
        assertThat(as(leftCond.right(), Literal.class).value(), is(10));

        var rightCond = as(topAnd.right(), GreaterThan.class);
        var rightAttr = as(rightCond.left(), ReferenceAttribute.class);
        assertThat(rightAttr.name(), is("emp_no"));
        assertThat(as(rightCond.right(), Literal.class).value(), is(0));

        // UnionAll with two branches
        var union = as(topFilter.child(), UnionAll.class);
        assertThat(union.children(), hasSize(2));

        // Left branch: EsRelation[test] with Project + Eval nulls
        var leftProject = as(union.children().get(0), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), is(List.of("$$does_not_exist2$converted_to$long")));
        var leftEvalEval = as(leftEval.child(), Eval.class);
        var leftLangCode = as(leftEvalEval.fields().get(0), Alias.class);
        assertThat(leftLangCode.name(), is("language_code"));
        assertThat(as(leftLangCode.child(), Literal.class).dataType(), is(DataType.INTEGER));
        var leftLangName = as(leftEvalEval.fields().get(1), Alias.class);
        assertThat(leftLangName.name(), is("language_name"));
        assertThat(as(leftLangName.child(), Literal.class).dataType(), is(DataType.KEYWORD));
        var leftDne1 = as(leftEvalEval.fields().get(2), Alias.class);
        assertThat(leftDne1.name(), is("does_not_exist1"));
        assertThat(as(leftDne1.child(), Literal.class).dataType(), is(DataType.NULL));

        var leftRel = as(leftEvalEval.child(), EsRelation.class);
        assertThat(leftRel.indexPattern(), is("test"));

        // Right branch: Project + Eval many nulls, Subquery -> Filter -> Eval -> EsRelation[languages]
        var rightProject = as(union.children().get(1), Project.class);
        var rightEval = as(rightProject.child(), Eval.class);
        assertThat(Expressions.names(rightEval.fields()), is(List.of("$$does_not_exist2$converted_to$long")));
        var rightEvalEval = as(rightEval.child(), Eval.class);
        assertThat(
            Expressions.names(rightEvalEval.fields()),
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

        var rightSub = as(rightEvalEval.child(), Subquery.class);
        var rightSubFilter = as(rightSub.child(), Filter.class);
        var rightGt = as(rightSubFilter.condition(), GreaterThan.class);
        var rightToLongOnDne1 = as(rightGt.left(), ToLong.class);
        assertThat(Expressions.name(rightToLongOnDne1.field()), is("does_not_exist1"));

        var rightSubEval = as(rightSubFilter.child(), Eval.class);
        assertThat(Expressions.names(rightSubEval.fields()), is(List.of("does_not_exist1", "does_not_exist2")));

        var rightRel = as(rightSubEval.child(), EsRelation.class);
        assertThat(rightRel.indexPattern(), is("languages"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{f}#11,ASC,LAST], Order[emp_no_plus{r}#6,ASC,LAST]]]
     *   \_Project[[emp_no{f}#11, emp_no_foo{r}#22, emp_no_plus{r}#6]]
     *     \_Filter[emp_no{f}#11 < 10003[INTEGER]]
     *       \_Eval[[TOLONG(emp_no_foo{r}#22) + 1[INTEGER] AS emp_no_plus#6]]
     *         \_Eval[[null[NULL] AS emp_no_foo#22]]
     *           \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testSubqueryMix() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | KEEP emp_no*
            | SORT emp_no, emp_no_plus
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var orderBy = as(limit.child(), OrderBy.class);
        assertThat(orderBy.order(), hasSize(2));
        assertThat(Expressions.name(orderBy.order().get(0).child()), is("emp_no"));
        assertThat(Expressions.name(orderBy.order().get(1).child()), is("emp_no_plus"));

        var project = as(orderBy.child(), Project.class);
        assertThat(project.projections(), hasSize(3));
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "emp_no_foo", "emp_no_plus")));

        var filter = as(project.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("emp_no < 10003"));

        var evalPlus = as(filter.child(), Eval.class);
        assertThat(evalPlus.fields(), hasSize(1));
        var aliasPlus = as(evalPlus.fields().getFirst(), Alias.class);
        assertThat(aliasPlus.name(), is("emp_no_plus"));
        assertThat(Expressions.name(aliasPlus.child()), is("emp_no_foo::LONG + 1"));

        var evalFoo = as(evalPlus.child(), Eval.class);
        assertThat(evalFoo.fields(), hasSize(1));
        var aliasFoo = as(evalFoo.fields().getFirst(), Alias.class);
        assertThat(aliasFoo.name(), is("emp_no_foo"));
        assertThat(as(aliasFoo.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(evalFoo.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("employees"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{f}#11,ASC,LAST], Order[emp_no_plus{r}#6,ASC,LAST]]]
     *   \_Project[[_meta_field{f}#17, emp_no{f}#11, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20, languages{f}#14,
     *          long_noidx{f}#21, salary{f}#16, emp_no_foo{r}#22, emp_no_plus{r}#6]]
     *     \_Filter[emp_no{f}#11 < 10003[INTEGER]]
     *       \_Eval[[TOLONG(emp_no_foo{r}#22) + 1[INTEGER] AS emp_no_plus#6]]
     *         \_Eval[[null[NULL] AS emp_no_foo#22]]
     *           \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testSubqueryMixWithDropPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM employees
                 | EVAL emp_no_plus = emp_no_foo::LONG + 1
                 | WHERE emp_no < 10003)
            | DROP *_name
            | SORT emp_no, emp_no_plus
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var orderBy = as(limit.child(), OrderBy.class);
        assertThat(orderBy.order(), hasSize(2));
        assertThat(Expressions.name(orderBy.order().get(0).child()), is("emp_no"));
        assertThat(Expressions.name(orderBy.order().get(1).child()), is("emp_no_plus"));

        var project = as(orderBy.child(), Project.class);
        assertThat(project.projections(), hasSize(11));
        assertThat(
            Expressions.names(project.projections()),
            is(
                List.of(
                    "_meta_field",
                    "emp_no",
                    "gender",
                    "hire_date",
                    "job",
                    "job.raw",
                    "languages",
                    "long_noidx",
                    "salary",
                    "emp_no_foo",
                    "emp_no_plus"
                )
            )
        );

        var filter = as(project.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("emp_no < 10003"));

        var evalPlus = as(filter.child(), Eval.class);
        assertThat(evalPlus.fields(), hasSize(1));
        var aliasPlus = as(evalPlus.fields().getFirst(), Alias.class);
        assertThat(aliasPlus.name(), is("emp_no_plus"));
        assertThat(Expressions.name(aliasPlus.child()), is("emp_no_foo::LONG + 1"));

        var evalFoo = as(evalPlus.child(), Eval.class);
        assertThat(evalFoo.fields(), hasSize(1));
        var aliasFoo = as(evalFoo.fields().getFirst(), Alias.class);
        assertThat(aliasFoo.name(), is("emp_no_foo"));
        assertThat(as(aliasFoo.child(), Literal.class).dataType(), is(DataType.NULL));

        var relation = as(evalFoo.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("employees"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[does_not_exist{r}#19,ASC,LAST]]]
     *   \_Aggregate[[does_not_exist{r}#19],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#5, does_not_exist{r}#19]]
     *     \_Eval[[null[NULL] AS does_not_exist#19]]
     *       \_EsRelation[employees][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testSubqueryAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM
                (FROM employees
                 | STATS c = COUNT(*) BY does_not_exist)
            | SORT does_not_exist
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // OrderBy over the Aggregate-produced grouping
        var orderBy = as(limit.child(), OrderBy.class);
        assertThat(orderBy.order(), hasSize(1));
        assertThat(Expressions.name(orderBy.order().get(0).child()), is("does_not_exist"));

        // Aggregate with grouping by does_not_exist
        var agg = as(orderBy.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(1));
        assertThat(Expressions.name(agg.groupings().get(0)), is("does_not_exist"));
        assertThat(agg.aggregates(), hasSize(2)); // c and does_not_exist

        // Eval introduces does_not_exist as NULL
        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), is("does_not_exist"));
        assertThat(as(alias.child(), Literal.class).dataType(), is(DataType.NULL));

        // Underlying relation
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("employees"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[does_not_exist{r}#83,ASC,LAST]]]
     *   \_UnionAll[[_meta_field{r}#71, emp_no{r}#72, first_name{r}#73, gender{r}#74, hire_date{r}#75, job{r}#76, job.raw{r}#77,
     *          languages{r}#78, last_name{r}#79, long_noidx{r}#80, salary{r}#81, c{r}#82, does_not_exist{r}#83]]
     *     |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *          languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, c{r}#29, does_not_exist{r}#53]]
     *     | \_Eval[[null[LONG] AS c#29, null[NULL] AS does_not_exist#53]]
     *     |   \_EsRelation[employees][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *     \_Project[[_meta_field{r}#30, emp_no{r}#31, first_name{r}#32, gender{r}#33, hire_date{r}#34, job{r}#35, job.raw{r}#36,
     *          languages{r}#37, last_name{r}#38, long_noidx{r}#39, salary{r}#40, c{r}#4, does_not_exist{r}#70]]
     *       \_Eval[[null[NULL] AS does_not_exist#70]]
     *         \_Project[[_meta_field{r}#30, emp_no{r}#31, first_name{r}#32, gender{r}#33, hire_date{r}#34, job{r}#35, job.raw{r}#36,
     *              languages{r}#37, last_name{r}#38, long_noidx{r}#39, salary{r}#40, c{r}#4]]
     *           \_Eval[[null[KEYWORD] AS _meta_field#30, null[INTEGER] AS emp_no#31, null[KEYWORD] AS first_name#32,
     *                  null[TEXT] AS gender#33, null[DATETIME] AS hire_date#34, null[TEXT] AS job#35, null[KEYWORD] AS job.raw#36,
     *                  null[INTEGER] AS languages#37, null[KEYWORD] AS last_name#38, null[LONG] AS long_noidx#39,
     *                  null[INTEGER] AS salary#40]]
     *             \_Subquery[]
     *               \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#4]]
     *                 \_Eval[[null[NULL] AS does_not_exist#54]]
     *                   \_EsRelation[employees][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testSubqueryAfterUnionAllOfStatsAndMain() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM employees,
                (FROM employees | STATS c = count(*))
            | SORT does_not_exist
            """));

        // Top implicit limit 1000
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // OrderBy over the does_not_exist field
        var orderBy = as(limit.child(), OrderBy.class);
        assertThat(orderBy.order(), hasSize(1));
        assertThat(Expressions.name(orderBy.order().get(0).child()), is("does_not_exist"));

        // UnionAll node
        var union = as(orderBy.child(), UnionAll.class);
        assertThat(union.children(), hasSize(2));
        assertThat(
            Expressions.names(union.output()),
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
                    "salary",
                    "c",
                    "does_not_exist"
                )
            )
        );
        var unionAllAttribute_does_not_exist = union.output().get(12);

        // --- Left branch: main FROM employees ---
        var leftProject = as(union.children().get(0), Project.class);
        assertThat(
            Expressions.names(leftProject.output()),
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
                    "salary",
                    "c",
                    "does_not_exist"
                )
            )
        );
        var leftProjectAttribute_does_not_exist = leftProject.output().get(12);
        assertThat(leftProjectAttribute_does_not_exist, not(unionAllAttribute_does_not_exist)); // ID is refreshed
        var leftEval = as(leftProject.child(), Eval.class);
        // c and does_not_exist are introduced as nulls
        assertThat(leftEval.fields(), hasSize(2));
        for (var alias : leftEval.fields()) {
            var a = as(alias, Alias.class);
            var lit = as(a.child(), Literal.class);
        }
        assertThat(leftEval.fields().get(0).name(), is("c"));
        assertThat(leftEval.fields().get(0).dataType(), is(DataType.LONG));
        assertThat(leftEval.fields().get(1).name(), is("does_not_exist"));
        assertThat(leftEval.fields().get(1).dataType(), is(DataType.NULL));
        assertThat(leftEval.fields().get(1).id(), is(leftProjectAttribute_does_not_exist.id())); // same ID within the branch
        var leftRelation = as(leftEval.child(), EsRelation.class);
        assertThat(leftRelation.indexPattern(), is("employees"));

        // --- Right branch: subquery (stats) ---
        var rightProject = as(union.children().get(1), Project.class);
        assertThat(
            Expressions.names(rightProject.output()),
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
                    "salary",
                    "c",
                    "does_not_exist"
                )
            )
        );
        var rightProjectAttribute_does_not_exist = rightProject.output().get(12);
        assertThat(rightProjectAttribute_does_not_exist, not(unionAllAttribute_does_not_exist)); // ID is refreshed
        var rightEval = as(rightProject.child(), Eval.class);
        // does_not_exist is introduced as null
        assertThat(rightEval.fields(), hasSize(1));
        {
            var a = as(rightEval.fields().get(0), Alias.class);
            var lit = as(a.child(), Literal.class);
            assertThat(lit.dataType(), is(DataType.NULL));
            assertThat(a.name(), is("does_not_exist"));
        }
        // same ID within the upper part of the branch
        assertThat(rightEval.fields().get(0).id(), is(rightProjectAttribute_does_not_exist.id()));

        var rightProject2 = as(rightEval.child(), Project.class);
        assertThat(
            Expressions.names(rightProject2.output()),
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
                    "salary",
                    "c"
                )
            )
        );
        var rightEval2 = as(rightProject2.child(), Eval.class);
        // All fields are introduced as nulls for the subquery
        assertThat(rightEval2.fields(), hasSize(11));
        for (var alias : rightEval2.fields()) {
            var a = as(alias, Alias.class);
            var lit = as(a.child(), Literal.class);
        }
        assertThat(
            rightEval2.fields().stream().map(Alias::name).toList(),
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
        var rightSubquery = as(rightEval2.child(), Subquery.class);
        var rightAgg = as(rightSubquery.child(), Aggregate.class);
        assertThat(rightAgg.aggregates(), hasSize(1));
        assertThat(Expressions.name(rightAgg.aggregates().get(0)), is("c"));
        var rightEval3 = as(rightAgg.child(), Eval.class);
        // does_not_exist is introduced as null for the stats subquery
        assertThat(rightEval3.fields(), hasSize(1));
        {
            var a = as(rightEval3.fields().get(0), Alias.class);
            var lit = as(a.child(), Literal.class);
            assertThat(lit.dataType(), is(DataType.NULL));
            assertThat(a.name(), is("does_not_exist"));
        }
        // the upper Eval is generated by ResolveUnmapped#patchFork(), this one by ResolvedUnmapped#evalUnresolvedAtopUnary
        assertThat(rightEval3.fields().get(0).id(), not(rightProjectAttribute_does_not_exist.id()));
        var rightRelation2 = as(rightEval3.child(), EsRelation.class);
        assertThat(rightRelation2.indexPattern(), is("employees"));
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

    /*
     * Project[[_meta_field{r}#76, emp_no{r}#77, first_name{r}#78, gender{r}#79, hire_date{r}#80, job{r}#81, job.raw{r}#82,
     *      languages{r}#83, last_name{r}#84, long_noidx{r}#85, salary{r}#86, language_code{r}#87, language_name{r}#88,
     *      does_not_exist1{r}#89, does_not_exist2{r}#91]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[$$does_not_exist2$converted_to$long{r$}#95 < 10[INTEGER] AND emp_no{r}#54 > 0[INTEGER] OR
     *          $$does_not_exist1$converted_to$long{r$}#70 < 11[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#76, emp_no{r}#77, first_name{r}#78, gender{r}#79, hire_date{r}#80, job{r}#81, job.raw{r}#82,
     *          languages{r}#83, last_name{r}#84, long_noidx{r}#85, salary{r}#86, language_code{r}#87, language_name{r}#88,
     *          does_not_exist1{r}#89, $$does_not_exist1$converted_to$long{r$}#90, does_not_exist2{r}#91,
     *          $$does_not_exist2$converted_to$long{r$}#95]]
     *       |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, language_code{r}#28, language_name{r}#29,
     *              does_not_exist1{r}#30, $$does_not_exist1$converted_to$long{r$}#67, does_not_exist2{r}#71,
     *              $$does_not_exist2$converted_to$long{r$}#92]]
     *       | \_Eval[[TOLONG(does_not_exist2{r}#71) AS $$does_not_exist2$converted_to$long#92]]
     *       |   \_Eval[[TOLONG(does_not_exist1{r}#30) AS $$does_not_exist1$converted_to$long#67]]
     *       |     \_Eval[[null[INTEGER] AS language_code#28, null[KEYWORD] AS language_name#29, null[NULL] AS does_not_exist1#30,
     *                  null[NULL] AS does_not_exist2#71]]
     *       |       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *       |_Project[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37,
     *              languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, language_code{f}#20, language_name{f}#21,
     *              does_not_exist1{r}#24, $$does_not_exist1$converted_to$long{r$}#68, does_not_exist2{r}#72,
     *              $$does_not_exist2$converted_to$long{r$}#93]]
     *       | \_Eval[[TOLONG(does_not_exist2{r}#72) AS $$does_not_exist2$converted_to$long#93]]
     *       |   \_Eval[[TOLONG(does_not_exist1{r}#24) AS $$does_not_exist1$converted_to$long#68]]
     *       |     \_Eval[[null[KEYWORD] AS _meta_field#31, null[INTEGER] AS emp_no#32, null[KEYWORD] AS first_name#33,
     *                  null[TEXT] AS gender#34, null[DATETIME] AS hire_date#35, null[TEXT] AS job#36, null[KEYWORD] AS job.raw#37,
     *                  null[INTEGER] AS languages#38, null[KEYWORD] AS last_name#39, null[LONG] AS long_noidx#40,
     *                  null[INTEGER] AS salary#41]]
     *       |       \_Subquery[]
     *       |         \_Filter[TOLONG(does_not_exist1{r}#24) > 1[INTEGER]]
     *       |           \_Eval[[null[NULL] AS does_not_exist1#24, null[NULL] AS does_not_exist2#72]]
     *       |             \_EsRelation[languages][language_code{f}#20, language_name{f}#21]
     *       \_Project[[_meta_field{r}#42, emp_no{r}#43, first_name{r}#44, gender{r}#45, hire_date{r}#46, job{r}#47, job.raw{r}#48,
     *              languages{r}#49, last_name{r}#50, long_noidx{r}#51, salary{r}#52, language_code{f}#22, language_name{f}#23,
     *              does_not_exist1{r}#26, $$does_not_exist1$converted_to$long{r$}#69, does_not_exist2{r}#73,
     *              $$does_not_exist2$converted_to$long{r$}#94]]
     *         \_Eval[[TOLONG(does_not_exist2{r}#73) AS $$does_not_exist2$converted_to$long#94]]
     *           \_Eval[[TOLONG(does_not_exist1{r}#26) AS $$does_not_exist1$converted_to$long#69]]
     *             \_Eval[[null[KEYWORD] AS _meta_field#42, null[INTEGER] AS emp_no#43, null[KEYWORD] AS first_name#44,
     *                  null[TEXT] AS gender#45, null[DATETIME] AS hire_date#46, null[TEXT] AS job#47, null[KEYWORD] AS job.raw#48,
     *                  null[INTEGER] AS languages#49, null[KEYWORD] AS last_name#50, null[LONG] AS long_noidx#51,
     *                  null[INTEGER] AS salary#52]]
     *               \_Subquery[]
     *                 \_Filter[TOLONG(does_not_exist1{r}#26) > 2[INTEGER]]
     *                   \_Eval[[null[NULL] AS does_not_exist1#26, null[NULL] AS does_not_exist2#73]]
     *                     \_EsRelation[languages][language_code{f}#22, language_name{f}#23]
     */
    public void testSubquerysWithMainAndSameOptional() {
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

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
        var andLeftToLong = as(andLeftLt.left(), ReferenceAttribute.class);
        assertThat(andLeftToLong.name(), is("$$does_not_exist2$converted_to$long"));
        assertThat(as(andLeftLt.right(), Literal.class).value(), is(10));

        var andRightGt = as(leftAnd.right(), GreaterThan.class);
        var andRightAttr = as(andRightGt.left(), ReferenceAttribute.class);
        assertThat(andRightAttr.name(), is("emp_no"));
        assertThat(as(andRightGt.right(), Literal.class).value(), is(0));

        var rightLt = as(topOr.right(), LessThan.class);
        var rightAttr = as(rightLt.left(), ReferenceAttribute.class);
        assertThat(rightAttr.name(), is("$$does_not_exist1$converted_to$long"));
        assertThat(as(rightLt.right(), Literal.class).value(), is(11));

        // UnionAll with three branches
        var union = as(topFilter.child(), UnionAll.class);
        assertThat(union.children(), hasSize(3));

        // Branch 1: EsRelation[test] with Project + Eval(null language_code/name/dne1) + Eval(TOLONG does_not_exist1)
        var b1Project = as(union.children().get(0), Project.class);
        var b1EvalToLong = as(b1Project.child(), Eval.class);
        assertThat(b1EvalToLong.fields(), hasSize(1));
        var b1Converted = as(b1EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b1Converted.name(), is("$$does_not_exist2$converted_to$long"));
        var b1ToLong = as(b1Converted.child(), ToLong.class);
        assertThat(Expressions.name(b1ToLong.field()), is("does_not_exist2"));

        var b1EvalConvert = as(b1EvalToLong.child(), Eval.class);
        assertThat(Expressions.names(b1EvalConvert.fields()), is(List.of("$$does_not_exist1$converted_to$long")));
        var b1EvalNulls = as(b1EvalConvert.child(), Eval.class);
        assertThat(
            Expressions.names(b1EvalNulls.fields()),
            is(List.of("language_code", "language_name", "does_not_exist1", "does_not_exist2"))
        );

        var b1Rel = as(b1EvalNulls.child(), EsRelation.class);
        assertThat(b1Rel.indexPattern(), is("test"));

        // Branch 2: Subquery[languages] with Filter TOLONG(does_not_exist1) > 1, wrapped by Project nulls + Eval(TOLONG dne1)
        var b2Project = as(union.children().get(1), Project.class);
        var b2EvalToLong = as(b2Project.child(), Eval.class);
        assertThat(b2EvalToLong.fields(), hasSize(1));
        var b2Converted = as(b2EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b2Converted.name(), is("$$does_not_exist2$converted_to$long"));
        var b2ToLong = as(b2Converted.child(), ToLong.class);
        assertThat(Expressions.name(b2ToLong.field()), is("does_not_exist2"));

        var b2EvalConvert = as(b2EvalToLong.child(), Eval.class);
        assertThat(Expressions.names(b2EvalConvert.fields()), is(List.of("$$does_not_exist1$converted_to$long")));
        var b2EvalNulls = as(b2EvalConvert.child(), Eval.class);
        assertThat(b2EvalNulls.fields(), hasSize(11)); // null meta+many fields

        var b2Sub = as(b2EvalNulls.child(), Subquery.class);
        var b2Filter = as(b2Sub.child(), Filter.class);
        var b2Gt = as(b2Filter.condition(), GreaterThan.class);
        var b2GtToLong = as(b2Gt.left(), ToLong.class);
        assertThat(Expressions.name(b2GtToLong.field()), is("does_not_exist1"));
        var b2SubEval = as(b2Filter.child(), Eval.class);
        assertThat(Expressions.names(b2SubEval.fields()), is(List.of("does_not_exist1", "does_not_exist2")));
        var b2Rel = as(b2SubEval.child(), EsRelation.class);
        assertThat(b2Rel.indexPattern(), is("languages"));

        // Branch 3: Subquery[languages] with Filter TOLONG(does_not_exist1) > 2, wrapped by Project nulls + Eval(TOLONG dne1)
        var b3Project = as(union.children().get(2), Project.class);
        var b3EvalToLong = as(b3Project.child(), Eval.class);
        assertThat(b3EvalToLong.fields(), hasSize(1));
        var b3Converted = as(b3EvalToLong.fields().getFirst(), Alias.class);
        assertThat(b3Converted.name(), is("$$does_not_exist2$converted_to$long"));
        var b3ToLong = as(b3Converted.child(), ToLong.class);
        assertThat(Expressions.name(b3ToLong.field()), is("does_not_exist2"));

        var b3EvalConversion = as(b3EvalToLong.child(), Eval.class);
        assertThat(Expressions.names(b3EvalConversion.fields()), is(List.of("$$does_not_exist1$converted_to$long")));
        var b3EvalNulls = as(b3EvalConversion.child(), Eval.class);
        assertThat(b3EvalNulls.fields(), hasSize(11));
        var b3Sub = as(b3EvalNulls.child(), Subquery.class);
        var b3Filter = as(b3Sub.child(), Filter.class);
        var b3Gt = as(b3Filter.condition(), GreaterThan.class);
        var b3GtToLong = as(b3Gt.left(), ToLong.class);
        assertThat(Expressions.name(b3GtToLong.field()), is("does_not_exist1"));
        var b3SubEval = as(b3Filter.child(), Eval.class);
        assertThat(Expressions.names(b3SubEval.fields()), is(List.of("does_not_exist1", "does_not_exist2")));
        var b3Rel = as(b3SubEval.child(), EsRelation.class);
        assertThat(b3Rel.indexPattern(), is("languages"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[languageCode{r}#24,languageCode{r}#197]
     *   \_Project[[count(*){r}#18, emp_no{r}#131 AS empNo#21, language_code{r}#141 AS languageCode#24, does_not_exist2{r}#196]]
     *     \_Aggregate[[emp_no{r}#131, language_code{r}#141, does_not_exist2{r}#196],
     *          [COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#18, emp_no{r}#131, language_code{r}#141,
     *          does_not_exist2{r}#196]]
     *       \_Filter[emp_no{r}#92 > 10000[INTEGER] OR $$does_not_exist1$converted_to$long{r$}#150 < 10[INTEGER]]
     *         \_UnionAll[[_meta_field{r}#179, emp_no{r}#180, first_name{r}#181, gender{r}#182, hire_date{r}#183, job{r}#184,
     *              job.raw{r}#185, languages{r}#186, last_name{r}#187, long_noidx{r}#188, salary{r}#189, language_code{r}#190,
     *              languageName{r}#191, max(@timestamp){r}#192, language_name{r}#193, does_not_exist1{r}#194,
     *              $$does_not_exist1$converted_to$long{r$}#195, does_not_exist2{r}#196]]
     *           |_Project[[_meta_field{f}#34, emp_no{f}#28, first_name{f}#29, gender{f}#30, hire_date{f}#35, job{f}#36, job.raw{f}#37,
     *                  languages{f}#31, last_name{f}#32, long_noidx{f}#38, salary{f}#33, language_code{r}#58, languageName{r}#59,
     *                  max(@timestamp){r}#60, language_name{r}#61, does_not_exist1{r}#106, $$does_not_exist1$converted_to$long{r$}#146,
     *                  does_not_exist2{r}#151]]
     *           | \_Eval[[TOLONG(does_not_exist1{r}#106) AS $$does_not_exist1$converted_to$long#146]]
     *           |   \_Eval[[null[INTEGER] AS language_code#58, null[KEYWORD] AS languageName#59, null[DATETIME] AS max(@timestamp)#60,
     *                      null[KEYWORD] AS language_name#61, null[NULL] AS does_not_exist1#106, null[NULL] AS does_not_exist2#151]]
     *           |     \_EsRelation[test][_meta_field{f}#34, emp_no{f}#28, first_name{f}#29, ..]
     *           |_Project[[_meta_field{r}#62, emp_no{r}#63, first_name{r}#64, gender{r}#65, hire_date{r}#66, job{r}#67, job.raw{r}#68,
     *                  languages{r}#69, last_name{r}#70, long_noidx{r}#71, salary{r}#72, language_code{f}#39, languageName{r}#6,
     *                  max(@timestamp){r}#73, language_name{r}#74, does_not_exist1{r}#107, $$does_not_exist1$converted_to$long{r$}#147,
     *                  does_not_exist2{r}#152]]
     *           | \_Eval[[TOLONG(does_not_exist1{r}#107) AS $$does_not_exist1$converted_to$long#147]]
     *           |   \_Eval[[null[KEYWORD] AS _meta_field#62, null[INTEGER] AS emp_no#63, null[KEYWORD] AS first_name#64,
     *                      null[TEXT] AS gender#65, null[DATETIME] AS hire_date#66, null[TEXT] AS job#67, null[KEYWORD] AS job.raw#68,
     *                      null[INTEGER] AS languages#69, null[KEYWORD] AS last_name#70, null[LONG] AS long_noidx#71,
     *                      null[INTEGER] AS salary#72, null[DATETIME] AS max(@timestamp)#73, null[KEYWORD] AS language_name#74]]
     *           |     \_Subquery[]
     *           |       \_Project[[language_code{f}#39, language_name{f}#40 AS languageName#6, does_not_exist1{r}#107,
     *                          does_not_exist2{r}#152]]
     *           |         \_Filter[language_code{f}#39 > 10[INTEGER]]
     *           |           \_Eval[[null[NULL] AS does_not_exist1#107, null[NULL] AS does_not_exist2#152]]
     *           |             \_EsRelation[languages][language_code{f}#39, language_name{f}#40]
     *           |_Project[[_meta_field{r}#75, emp_no{r}#76, first_name{r}#77, gender{r}#78, hire_date{r}#79, job{r}#80, job.raw{r}#81,
     *                  languages{r}#82, last_name{r}#83, long_noidx{r}#84, salary{r}#85, language_code{r}#86, languageName{r}#87,
     *                  max(@timestamp){r}#8, language_name{r}#88, does_not_exist1{r}#129, $$does_not_exist1$converted_to$long{r$}#148,
     *                  does_not_exist2{r}#178]]
     *           | \_Eval[[null[NULL] AS does_not_exist2#178]]
     *           |   \_Project[[_meta_field{r}#75, emp_no{r}#76, first_name{r}#77, gender{r}#78, hire_date{r}#79, job{r}#80, job.raw{r}#81,
     *                      languages{r}#82, last_name{r}#83, long_noidx{r}#84, salary{r}#85, language_code{r}#86, languageName{r}#87,
     *                      max(@timestamp){r}#8, language_name{r}#88, does_not_exist1{r}#129, $$does_not_exist1$converted_to$long{r$}#148]]
     *           |     \_Eval[[TOLONG(does_not_exist1{r}#129) AS $$does_not_exist1$converted_to$long#148]]
     *           |       \_Eval[[null[NULL] AS does_not_exist1#129]]
     *           |         \_Project[[_meta_field{r}#75, emp_no{r}#76, first_name{r}#77, gender{r}#78, hire_date{r}#79, job{r}#80,
     *                          job.raw{r}#81, languages{r}#82, last_name{r}#83, long_noidx{r}#84, salary{r}#85, language_code{r}#86,
     *                          languageName{r}#87, max(@timestamp){r}#8, language_name{r}#88]]
     *           |           \_Eval[[null[KEYWORD] AS _meta_field#75, null[INTEGER] AS emp_no#76, null[KEYWORD] AS first_name#77,
     *                              null[TEXT] AS gender#78, null[DATETIME] AS hire_date#79, null[TEXT] AS job#80,
     *                              null[KEYWORD] AS job.raw#81, null[INTEGER] AS languages#82, null[KEYWORD] AS last_name#83,
     *                              null[LONG] AS long_noidx#84, null[INTEGER] AS salary#85, null[INTEGER] AS language_code#86,
     *                              null[KEYWORD] AS languageName#87, null[KEYWORD] AS language_name#88]]
     *           |             \_Subquery[]
     *           |               \_Aggregate[[],[MAX(@timestamp{f}#41,true[BOOLEAN],PT0S[TIME_DURATION]) AS max(@timestamp)#8]]
     *           |                 \_Eval[[null[NULL] AS does_not_exist1#108, null[NULL] AS does_not_exist2#153]]
     *           |                   \_EsRelation[sample_data][@timestamp{f}#41, client_ip{f}#42, event_duration{f..]
     *           \_Project[[_meta_field{f}#51, emp_no{f}#45, first_name{f}#46, gender{f}#47, hire_date{f}#52, job{f}#53, job.raw{f}#54,
     *                  languages{f}#48, last_name{f}#49, long_noidx{f}#55, salary{f}#50, language_code{r}#12, languageName{r}#89,
     *                  max(@timestamp){r}#90, language_name{f}#57, does_not_exist1{r}#110, $$does_not_exist1$converted_to$long{r$}#149,
     *                  does_not_exist2{r}#155]]
     *             \_Eval[[TOLONG(does_not_exist1{r}#110) AS $$does_not_exist1$converted_to$long#149]]
     *               \_Eval[[null[KEYWORD] AS languageName#89, null[DATETIME] AS max(@timestamp)#90]]
     *                 \_Subquery[]
     *                   \_LookupJoin[LEFT,[language_code{r}#12],[language_code{f}#56],false,null]
     *                     |_Eval[[languages{f}#48 AS language_code#12, null[NULL] AS does_not_exist1#109,
     *                          null[NULL] AS does_not_exist2#154]]
     *                     | \_EsRelation[test][_meta_field{f}#51, emp_no{f}#45, first_name{f}#46, ..]
     *                     \_Eval[[null[NULL] AS does_not_exist1#110, null[NULL] AS does_not_exist2#155]]
     *                       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#56, language_name{f}#57]
     */
    public void testSubquerysMixAndLookupJoinNullify() {
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

        // TODO: golden testing
        assertThat(Expressions.names(plan.output()), is(List.of("count(*)", "empNo", "languageCode", "does_not_exist2")));
    }

    // same tree as above, except for the source nodes
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedLoad("""
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

        // TODO: golden testing
        assertThat(Expressions.names(plan.output()), is(List.of("count(*)", "empNo", "languageCode", "does_not_exist2")));

        List<EsRelation> esRelations = plan.collect(EsRelation.class);
        assertThat(
            esRelations.stream().map(EsRelation::indexPattern).toList(),
            is(
                List.of(
                    "test", // FROM
                    "languages",
                    "sample_data",
                    "test", // LOOKUP JOIN
                    "languages_lookup"
                )
            )
        );
        for (var esr : esRelations) {
            if (esr.indexMode() != IndexMode.LOOKUP) {
                var dne = esr.output().stream().filter(a -> a.name().startsWith("does_not_exist")).toList();
                assertThat(dne.size(), is(2));
                var dne1 = as(dne.getFirst(), FieldAttribute.class);
                var dne2 = as(dne.getLast(), FieldAttribute.class);
                var pukesf1 = as(dne1.field(), PotentiallyUnmappedKeywordEsField.class);
                var pukesf2 = as(dne2.field(), PotentiallyUnmappedKeywordEsField.class);
                assertThat(pukesf1.getName(), is("does_not_exist1"));
                assertThat(pukesf2.getName(), is("does_not_exist2"));
            }
        }
    }

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
     * Project[[_meta_field{r}#115, first_name{r}#116, gender{r}#117, hire_date{r}#118, job{r}#119, job.raw{r}#120, languages{r}#121,
     *      last_name{r}#122, long_noidx{r}#123, salary{r}#124, c{r}#125, does_not_exist1{r}#126, a{r}#127, does_not_exist2{r}#128,
     *      does_not_exist3{r}#129, emp_no{r}#130, x{r}#13]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Eval[[does_not_exist3{r}#129 AS x#13]]
     *     \_Filter[$$does_not_exist2$converted_to$long{r$}#107 < 10[INTEGER]]
     *       \_UnionAll[[_meta_field{r}#115, first_name{r}#116, gender{r}#117, hire_date{r}#118, job{r}#119, job.raw{r}#120,
     *              languages{r}#121, last_name{r}#122, long_noidx{r}#123, salary{r}#124, c{r}#125, does_not_exist1{r}#126, a{r}#127,
     *              does_not_exist2{r}#128, does_not_exist3{r}#129, emp_no{r}#130, $$does_not_exist2$converted_to$long{r$}#131]]
     *         |_Project[[_meta_field{f}#21, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, languages{f}#18,
     *              last_name{f}#19, long_noidx{f}#25, salary{f}#20, c{r}#38, does_not_exist1{r}#39, a{r}#40, does_not_exist2{r}#82,
     *              does_not_exist3{r}#108, emp_no{r}#79, $$does_not_exist2$converted_to$long{r$}#104]]
     *         | \_Eval[[TOLONG(does_not_exist2{r}#82) AS $$does_not_exist2$converted_to$long#104]]
     *         |   \_Eval[[null[KEYWORD] AS emp_no#79]]
     *         |     \_Eval[[null[LONG] AS c#38, null[NULL] AS does_not_exist1#39, null[DOUBLE] AS a#40, null[NULL] AS does_not_exist2#82,
     *                      null[NULL] AS does_not_exist3#108]]
     *         |       \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *         |_Project[[_meta_field{r}#41, first_name{r}#42, gender{r}#43, hire_date{r}#44, job{r}#45, job.raw{r}#46, languages{r}#47,
     *                  last_name{r}#48, long_noidx{r}#49, salary{r}#50, c{r}#6, does_not_exist1{r}#31, a{r}#51, does_not_exist2{r}#87,
     *                  does_not_exist3{r}#113, emp_no{r}#80, $$does_not_exist2$converted_to$long{r$}#105]]
     *         | \_Eval[[null[NULL] AS does_not_exist3#113]]
     *         |   \_Eval[[TOLONG(does_not_exist2{r}#87) AS $$does_not_exist2$converted_to$long#105]]
     *         |     \_Eval[[null[NULL] AS does_not_exist2#87]]
     *         |       \_Eval[[null[KEYWORD] AS emp_no#80]]
     *         |         \_Eval[[null[KEYWORD] AS _meta_field#41, null[KEYWORD] AS first_name#42, null[TEXT] AS gender#43,
     *                          null[DATETIME] AS hire_date#44, null[TEXT] AS job#45, null[KEYWORD] AS job.raw#46,
     *                          null[INTEGER] AS languages#47, null[KEYWORD] AS last_name#48, null[LONG] AS long_noidx#49,
     *                          null[INTEGER] AS salary#50, null[DOUBLE] AS a#51]]
     *         |           \_Subquery[]
     *         |             \_Aggregate[[emp_no{r}#30, does_not_exist1{r}#31],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#6,
     *                              emp_no{r}#30, does_not_exist1{r}#31]]
     *         |               \_Eval[[null[NULL] AS emp_no#30, null[NULL] AS does_not_exist1#31, null[NULL] AS does_not_exist2#83,
     *                                  null[NULL] AS does_not_exist3#109]]
     *         |                 \_EsRelation[languages][language_code{f}#26, language_name{f}#27]
     *         \_Project[[_meta_field{r}#52, first_name{r}#54, gender{r}#55, hire_date{r}#56, job{r}#57, job.raw{r}#58, languages{r}#59,
     *                  last_name{r}#60, long_noidx{r}#61, salary{r}#62, c{r}#63, does_not_exist1{r}#64, a{r}#9, does_not_exist2{r}#88,
     *                  does_not_exist3{r}#114, emp_no{r}#81, $$does_not_exist2$converted_to$long{r$}#106]]
     *           \_Eval[[null[NULL] AS does_not_exist3#114]]
     *             \_Eval[[TOLONG(does_not_exist2{r}#88) AS $$does_not_exist2$converted_to$long#106]]
     *               \_Eval[[null[NULL] AS does_not_exist2#88]]
     *                 \_Eval[[null[KEYWORD] AS emp_no#81]]
     *                   \_Eval[[null[KEYWORD] AS _meta_field#52, null[INTEGER] AS emp_no#53, null[KEYWORD] AS first_name#54,
     *                          null[TEXT] AS gender#55, null[DATETIME] AS hire_date#56, null[TEXT] AS job#57, null[KEYWORD] AS job.raw#58,
     *                          null[INTEGER] AS languages#59, null[KEYWORD] AS last_name#60, null[LONG] AS long_noidx#61,
     *                          null[INTEGER] AS salary#62, null[LONG] AS c#63, null[NULL] AS does_not_exist1#64]]
     *                     \_Subquery[]
     *                       \_Aggregate[[],[AVG(salary{r}#36,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS a#9]]
     *                         \_Eval[[null[NULL] AS salary#36, null[NULL] AS does_not_exist2#84, null[NULL] AS does_not_exist3#110]]
     *                           \_EsRelation[languages][language_code{f}#28, language_name{f}#29]
     */
    public void testSubquerysWithMainAndStatsOnly() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test, // adding a "main" index/pattern makes does_not_exist2 & 3 resolved (compared to the same query above, w/o it)
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary))
            | WHERE does_not_exist2::LONG < 10
            | EVAL x = does_not_exist3
            """));

        // TODO: golden testing
        assertThat(
            Expressions.names(plan.output()),
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
                    "salary",
                    "c",
                    "does_not_exist1",
                    "a",
                    "does_not_exist2",
                    "does_not_exist3",
                    "x"
                )
            )
        );
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
     * \_OrderBy[[Order[does_not_exist2{r}#46,ASC,LAST]]]
     *   \_Fork[[c{r}#45, does_not_exist2{r}#46, _fork{r}#47, d{r}#48]]
     *     |_Limit[1000[INTEGER],false,false]
     *     | \_Project[[c{r}#6, does_not_exist2{r}#39, _fork{r}#7, d{r}#42]]
     *     |   \_Eval[[null[DOUBLE] AS d#42]]
     *     |     \_Eval[[fork1[KEYWORD] AS _fork#7]]
     *     |       \_Aggregate[[does_not_exist2{r}#39],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c#6, does_not_exist2{r}#39]]
     *     |         \_Filter[ISNULL(does_not_exist1{r}#35)]
     *     |           \_Eval[[null[NULL] AS does_not_exist1#35, null[NULL] AS does_not_exist2#39]]
     *     |             \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     *     \_Limit[1000[INTEGER],false,false]
     *       \_Project[[c{r}#43, does_not_exist2{r}#44, _fork{r}#7, d{r}#10]]
     *         \_Eval[[null[LONG] AS c#43, null[NULL] AS does_not_exist2#44]]
     *           \_Eval[[fork2[KEYWORD] AS _fork#7]]
     *             \_Aggregate[[],[AVG(salary{f}#29,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS d#10]]
     *               \_Filter[ISNULL(does_not_exist1{r}#37)]
     *                 \_Eval[[null[NULL] AS does_not_exist1#37]]
     *                   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     */
    public void testForkBranchesAfterStats1stBranch() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*) BY does_not_exist2)
                   (STATS d = AVG(salary))
            | SORT does_not_exist2
            """));

        // TODO: golden testing
        assertThat(Expressions.names(plan.output()), is(List.of("c", "does_not_exist2", "_fork", "d")));
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
        var inlineStats = as(limit.child(), InlineStats.class);
        var agg = as(inlineStats.child(), Aggregate.class);

        // Grouping by does_not_exist2 and SUM over does_not_exist1
        assertThat(agg.groupings(), hasSize(1));
        var groupRef = as(agg.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(groupRef.name(), is("does_not_exist2"));

        assertThat(agg.aggregates(), hasSize(2));
        var cAlias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(cAlias.name(), is("c"));
        as(cAlias.child(), Sum.class);

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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[x{r}#4, does_not_exist_field1{r}#12, y{r}#8, does_not_exist_field2{r}#14]]
     *   \_Eval[[TOINTEGER(does_not_exist_field1{r}#12) + x{r}#4 AS y#8]]
     *     \_Eval[[null[NULL] AS does_not_exist_field1#12, null[NULL] AS does_not_exist_field2#14]]
     *       \_Row[[1[INTEGER] AS x#4]]
     */
    public void testRow() {
        var plan = analyzeStatement(setUnmappedNullify("""
            ROW x = 1
            | EVAL y = does_not_exist_field1::INTEGER + x
            | KEEP *, does_not_exist_field2
            """));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(4));
        assertThat(Expressions.names(project.projections()), is(List.of("x", "does_not_exist_field1", "y", "does_not_exist_field2")));

        var evalY = as(project.child(), Eval.class);
        assertThat(evalY.fields(), hasSize(1));
        var aliasY = as(evalY.fields().getFirst(), Alias.class);
        assertThat(aliasY.name(), is("y"));
        assertThat(Expressions.name(aliasY.child()), is("does_not_exist_field1::INTEGER + x"));

        var evalDne1 = as(evalY.child(), Eval.class);
        assertThat(Expressions.names(evalDne1.fields()), is(List.of("does_not_exist_field1", "does_not_exist_field2")));

        var row = as(evalDne1.child(), Row.class);
        assertThat(row.fields(), hasSize(1));
        assertThat(Expressions.name(row.fields().getFirst()), is("x"));
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

    private void verificationFailure(String statement, String expectedFailure) {
        var e = expectThrows(VerificationException.class, () -> analyzeStatement(statement));
        assertThat(e.getMessage(), containsString(expectedFailure));
    }

    private static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS", EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled());
        return "SET unmapped_fields=\"load\"; " + query;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
