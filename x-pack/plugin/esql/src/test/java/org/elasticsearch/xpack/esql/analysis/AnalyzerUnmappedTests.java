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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
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
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzeStatement;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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
    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[emp_does_not_exist_field{f}#23 + 2[INTEGER] AS y#9]]
     *   \_Eval[[emp_no{f}#11 + 1[INTEGER] AS x#6]]
     *     \_Project[[emp_no{f}#11, emp_does_not_exist_field{f}#23]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    public void testEvalAfterMatchingKeepWithFieldWildcard() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | KEEP emp_*
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

        // The child is Project with emp_no and emp_does_not_exist_field
        var project = as(evalX.child(), Project.class);
        assertThat(Expressions.names(project.output()), is(List.of("emp_no", "emp_does_not_exist_field")));

        // The child is EsRelation
        var relation = as(project.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
        var dneAttr = relation.output().stream().filter(a -> a.name().equals("emp_does_not_exist_field")).findFirst().orElseThrow();
        assertThat(dneAttr.dataType(), is(DataType.NULL));
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

    public void testFailFilterAfterDrop() {
        var query = """
            FROM test
            | WHERE emp_no > 1000
            | DROP emp_no
            | WHERE emp_no < 2000
            """;

        var failure = "line 4:9: Unknown column [emp_no]";
        verificationFailure(setUnmappedNullify(query), failure);
        verificationFailure(setUnmappedLoad(query), failure);
    }
    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[does_not_exist{r}#21 + 1[INTEGER] AS x#8]]
     *   \_Project[[_meta_field{f}#16, emp_no{f}#10 AS employee_number#5, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18,
     *          job.raw{f}#19, languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15, does_not_exist{r}#21]]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, .., does_not_exist{f}#21]
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

        var relation = as(esqlProject.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
        var dneAttr = relation.output().stream().filter(a -> a.name().equals("does_not_exist")).findFirst().orElseThrow();
        assertThat(dneAttr.dataType(), is(DataType.NULL));
    }
    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[b{r}#25 + c{r}#27 AS y#12]]
     *   \_Eval[[a{r}#4 + b{r}#25 AS x#8]]
     *     \_Eval[[1[INTEGER] AS a#4]]
     *       \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, .., b{f}#25, c{f}#27]
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
        assertThat(Expressions.names(evalABC.fields()), is(List.of("a")));

        var relation = as(evalABC.child(), EsRelation.class);
        var dneAttrs = relation.output().stream().filter(a -> a.name().equals("b") || a.name().equals("c")).toList();
        assertThat(dneAttrs, hasSize(2));
        dneAttrs.forEach(a -> assertThat(a.dataType(), is(DataType.NULL)));
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

    public void testFailStatsThenKeepShadowing() {
        var query = """
            FROM employees
            | STATS count(*)
            | EVAL foo = emp_no
            """;

        var failure = "line 3:14: Unknown column [emp_no]";
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
     * \_Aggregate[[does_not_exist{f}#14 AS language_code#6, language_name{f}#13],[COUNT(*[KEYWORD],true[BOOLEAN],
     *              PT0S[TIME_DURATION]) AS c#9, language_code{r}#6, language_name{f}#13]]
     *   \_Filter[language_code{f}#12 == 1[INTEGER]]
     *     \_EsRelation[languages][language_code{f}#12, language_name{f}#13, does_not_..]
     */
    public void testStatsAggAndAliasedShadowingGroup() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) BY language_code = does_not_exist, language_name
            """));

        assertThat(Expressions.names(plan.output()), is(List.of("c", "language_code", "language_name")));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        var group_language_code = as(agg.groupings().getFirst(), Alias.class);
        assertThat(group_language_code.name(), is("language_code"));
        assertThat(Expressions.name(group_language_code.child()), is("does_not_exist"));
        assertThat(Expressions.name(agg.groupings().get(1)), is("language_name"));

        assertThat(agg.aggregates(), hasSize(3)); // includes grouping keys
        var alias = as(agg.aggregates().get(0), Alias.class);
        assertThat(alias.name(), is("c"));
        assertThat(Expressions.name(alias.child()), is("COUNT(*)"));
        var agg_language_code = as(agg.aggregates().get(1), ReferenceAttribute.class);
        assertThat(agg_language_code.id(), is(group_language_code.id()));

        var filter = as(agg.child(), Filter.class);

        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("languages"));
        var dneAttr = relation.output().stream().filter(a -> a.name().equals("does_not_exist")).findFirst().orElseThrow();
        assertThat(dneAttr.dataType(), is(DataType.NULL));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[TOINTEGER(does_not_exist1{f}#18) + TOINTEGER(does_not_exist2{f}#19) + language_code{f}#15 AS language_code#8,
     *              language_name{f}#16],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) + language_code{r}#8 AS c#12,
     *              language_code{r}#8, language_name{f}#16]]
     *   \_Filter[language_code{f}#15 == 1[INTEGER]]
     *     \_EsRelation[languages][language_code{f}#15, language_name{f}#16, does_not_..]
     */
    public void testStatsAggAndAliasedShadowingGroupOverExpression() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM languages
            | WHERE language_code == 1
            | STATS c = COUNT(*) + language_code
                    BY language_code = does_not_exist1::INTEGER + does_not_exist2::INTEGER + language_code, language_name
            """));

        assertThat(Expressions.names(plan.output()), is(List.of("c", "language_code", "language_name")));

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(2));
        var groupAlias = as(agg.groupings().getFirst(), Alias.class);
        assertThat(groupAlias.name(), is("language_code"));
        assertThat(Expressions.name(groupAlias.child()), is("does_not_exist1::INTEGER + does_not_exist2::INTEGER + language_code"));
        assertThat(Expressions.name(agg.groupings().get(1)), is("language_name"));

        assertThat(agg.aggregates(), hasSize(3)); // includes grouping keys
        var alias = as(agg.aggregates().getFirst(), Alias.class);
        assertThat(alias.name(), is("c"));
        assertThat(Expressions.name(alias.child()), is("COUNT(*) + language_code"));

        var filter = as(agg.child(), Filter.class);

        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("languages"));
        var dneAttrs = relation.output().stream().filter(a -> a.name().startsWith("does_not_exist")).toList();
        assertThat(dneAttrs, hasSize(2));
        dneAttrs.forEach(a -> assertThat(a.dataType(), is(DataType.NULL)));
    }
    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist{r}#195) > 0[INTEGER] OR emp_no{f}#184 > 0[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#190, emp_no{f}#184, first_name{f}#18.., does_not_exist{f}#195]
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

        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
        var dneAttr = relation.output().stream().filter(a -> a.name().equals("does_not_exist")).findFirst().orElseThrow();
        assertThat(dneAttr.dataType(), is(DataType.NULL));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[TOLONG(does_not_exist1{r}#491) > 0[INTEGER] OR emp_no{f}#480 > 0[INTEGER]
     *      AND TOLONG(does_not_exist2{r}#492) < 100[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#486, emp_no{f}#480, first_name{f}#48.., does_not_exist1{f}#491, does_not_exist2{f}#492]
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

        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[FilteredExpression[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]),
     *      TOLONG(does_not_exist1{r}#94) > 0[INTEGER]] AS c#81]]
     *   \_EsRelation[test][_meta_field{f}#89, emp_no{f}#83, first_name{f}#84, .., does_not_exist1{f}#94]
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

        var relation = as(agg.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
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
            | STATS max(rate(network.total_cost)) BY tbucket = BUCKET(newTs, 1hour)
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);

        verificationFailure(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """), "3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142968
     * KQL (and QSTR) functions should be allowed in WHERE immediately after FROM,
     * even when an unmapped field is referenced later in the query.
     */
    public void testKqlWithUnmappedFieldInEval() {
        // This should NOT throw a verification exception.
        // The KQL function is correctly placed in a WHERE directly after FROM.
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | WHERE kql("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """));
        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        // Unmapped field is in EsRelation output; Filter sits directly above EsRelation
        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.indexPattern(), is("test"));
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/142959
     * QSTR functions should be allowed after SORT, even when an unmapped field is used later.
     */
    public void testQstrAfterSortWithUnmappedField() {
        var plan = analyzeStatement(setUnmappedNullify("""
            FROM test
            | SORT first_name
            | WHERE qstr("first_name: test")
            | EVAL x = does_not_exist_field + 1
            """));
        var limit = as(plan, Limit.class);
        var eval = as(limit.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        var orderBy = as(filter.child(), OrderBy.class);
        assertThat(orderBy, not(nullValue()));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Project[[bar{r}#14]]
     *   \_Filter[a{r}#9 == 1[INTEGER]]
     *     \_Fork[[a{r}#9, _fork{r}#10, bar{r}#14]]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Project[[a{r}#4, _fork{r}#5, bar{r}#11]]
     *           \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *             \_Filter[true[BOOLEAN]]
     *               \_Eval[[null[NULL] AS bar#11]]
     *                 \_Row[[1[INTEGER] AS a#4]]
     * }</pre>
     */
    public void testForkWithRow() {
        var plan = analyzeStatement(setUnmappedNullify("""
            ROW a = 1
            | FORK  (where true)
            | WHERE a == 1
            | KEEP bar
            """));

        // Top implicit limit
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // KEEP bar
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("bar")));

        // WHERE a == 1
        var filter = as(project.child(), Filter.class);

        // Fork node with one branch
        var fork = as(filter.child(), Fork.class);
        assertThat(fork.children(), hasSize(1));

        // Branch 0: Limit -> Project -> Eval(_fork) -> Filter(true) -> Eval(bar=null) -> Row
        var b0Limit = as(fork.children().getFirst(), Limit.class);
        var b0Project = as(b0Limit.child(), Project.class);
        assertThat(b0Project.projections(), hasSize(3));
        assertThat(Expressions.names(b0Project.projections()), hasItems("a", "_fork", "bar"));

        var b0EvalFork = as(b0Project.child(), Eval.class);
        var b0ForkAlias = as(b0EvalFork.fields().getFirst(), Alias.class);
        assertThat(b0ForkAlias.name(), is("_fork"));

        var b0FilterTrue = as(b0EvalFork.child(), Filter.class);

        var b0EvalBar = as(b0FilterTrue.child(), Eval.class);
        assertThat(b0EvalBar.fields(), hasSize(1));
        var barAlias = as(b0EvalBar.fields().getFirst(), Alias.class);
        assertThat(barAlias.name(), is("bar"));
        assertThat(as(barAlias.child(), Literal.class).dataType(), is(DataType.NULL));

        as(b0EvalBar.child(), Row.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[COALESCE(bar{r}#59,baz{r}#60) AS y#11]]
     *   \_Project[[_meta_field{r}#38, emp_no{r}#39, first_name{r}#40, gender{r}#41, hire_date{r}#42, job{r}#43, job.raw{r}#44, l
     * anguages{r}#45, last_name{r}#46, long_noidx{r}#47, salary{r}#48, foo{r}#49, bar{r}#59, baz{r}#60]]
     *     \_Filter[_fork{r}#50 == fork1[KEYWORD]]
     *       \_Fork[[_meta_field{r}#38, emp_no{r}#39, first_name{r}#40, gender{r}#41, hire_date{r}#42, job{r}#43, job.raw{r}#44, l
     * anguages{r}#45, last_name{r}#46, long_noidx{r}#47, salary{r}#48, foo{r}#49, _fork{r}#50, bar{r}#59, baz{r}#60]]
     *         |_Limit[1000[INTEGER],false,false]
     *         | \_Project[[_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, gender{f}#15, hire_date{f}#20, job{f}#21, job.raw{f}#22, l
     * anguages{f}#16, last_name{f}#17, long_noidx{f}#23, salary{f}#18, foo{f}#35, _fork{r}#4, bar{f}#51, baz{f}#52]]
     *         |   \_Eval[[fork1[KEYWORD] AS _fork#4]]
     *         |     \_Filter[NOT(foo{f}#35 == 84[INTEGER])]
     *         |       \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, .., foo{f}#35, bar{f}#51, baz{f}#52]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Project[[_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, gender{f}#26, hire_date{f}#31, job{f}#32, job.raw{f}#33, l
     * anguages{f}#27, last_name{f}#28, long_noidx{f}#34, salary{f}#29, foo{r}#37, _fork{r}#5, bar{f}#53, baz{f}#54]]
     *             \_Eval[[null[NULL] AS foo#37]]
     *               \_Eval[[fork2[KEYWORD] AS _fork#5]]
     *                 \_Filter[true[BOOLEAN]]
     *                   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, .., bar{f}#53, baz{f}#54]
     * }</pre>
     */
    public void testForkWithFrom() {
        var plan = analyzeStatement(setUnmappedNullify("""
            from test
            | FORK  (where foo != 84) (where true)
            | WHERE _fork == "fork1"
            | DROP _fork
            | eval y = coalesce(bar, baz)
            """));

        // Top implicit limit
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // EVAL y = coalesce(bar, baz)
        var evalY = as(limit.child(), Eval.class);
        assertThat(evalY.fields(), hasSize(1));
        assertThat(evalY.fields().getFirst().name(), is("y"));
        assertThat(as(evalY.fields().getFirst(), Alias.class).dataType(), is(DataType.NULL));

        // DROP _fork -> Project without _fork
        var dropProject = as(evalY.child(), Project.class);
        assertThat(Expressions.names(dropProject.projections()), not(hasItems("_fork")));
        assertThat(Expressions.names(dropProject.projections()), hasItems("foo", "bar", "baz"));

        // WHERE _fork == "fork1"
        var filterFork = as(dropProject.child(), Filter.class);

        // Fork node with two branches
        var fork = as(filterFork.child(), Fork.class);
        assertThat(fork.children(), hasSize(2));

        // Branch 0: (where foo != 84) -> unmapped foo/bar/baz added as MissingEsField in EsRelation
        var b0Limit = as(fork.children().getFirst(), Limit.class);
        var b0Project = as(b0Limit.child(), Project.class);
        assertThat(Expressions.names(b0Project.projections()), hasItems("foo", "_fork", "bar", "baz"));

        var b0EvalFork = as(b0Project.child(), Eval.class);
        var b0ForkAlias = as(b0EvalFork.fields().getFirst(), Alias.class);
        assertThat(b0ForkAlias.name(), is("_fork"));

        // Filter foo != 84
        var b0Filter = as(b0EvalFork.child(), Filter.class);

        // foo, bar, baz are FieldAttributes with MissingEsField/DataType.NULL directly in EsRelation
        var b0EsRelation = as(b0Filter.child(), EsRelation.class);
        assertThat(Expressions.names(b0EsRelation.output()), hasItems("foo", "bar", "baz"));

        // Branch 1: (where true) -> bar/baz added as MissingEsField in EsRelation, foo null-aliased via Eval
        var b1Limit = as(fork.children().get(1), Limit.class);
        var b1Project = as(b1Limit.child(), Project.class);
        assertThat(Expressions.names(b1Project.projections()), hasItems("foo", "_fork", "bar", "baz"));

        // foo is not referenced in this branch's filter, so it's introduced as null-Eval by the Fork patching mechanism
        var b1EvalFoo = as(b1Project.child(), Eval.class);
        assertThat(Expressions.names(b1EvalFoo.fields()), hasItems("foo"));

        var b1EvalFork = as(b1EvalFoo.child(), Eval.class);
        var b1ForkAlias = as(b1EvalFork.fields().getFirst(), Alias.class);
        assertThat(b1ForkAlias.name(), is("_fork"));

        var b1FilterTrue = as(b1EvalFork.child(), Filter.class);

        // bar, baz are FieldAttributes with MissingEsField/DataType.NULL directly in EsRelation
        var b1EsRelation = as(b1FilterTrue.child(), EsRelation.class);
        assertThat(Expressions.names(b1EsRelation.output()), hasItems("bar", "baz"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Project[[emp_no{r}#8]]
     *   \_Eval[[LEAST(emp_no{r}#8,TOLONG(52[INTEGER]),TOLONG(60[INTEGER])) AS x#11]]
     *     \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS emp_no#8]]
     *       \_Fork[[emp_no{r}#26, _fork{r}#27]]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Project[[emp_no{r}#25, _fork{r}#5]]
     *             \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *               \_MvExpand[emp_no{f}#14,emp_no{r}#25]
     *                 \_Filter[true[BOOLEAN]]
     *                   \_Project[[emp_no{f}#14]]
     *                     \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }</pre>
     */
    public void testForkWithUnmappedStatsEvalKeep() {
        var plan = analyzeStatement(setUnmappedNullify("""
            from test
            | keep emp_no
            | FORK  (where true | mv_expand emp_no)
            | stats emp_no = count(*)
            | eval  x = least(emp_no, 52, 60)
            | keep emp_no
            """));

        // Limit[1000]
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // KEEP emp_no → Project[[emp_no]]
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));

        // EVAL x = LEAST(emp_no, 52, 60)
        var evalX = as(project.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        assertThat(evalX.fields().getFirst().name(), is("x"));

        // STATS emp_no = COUNT(*)
        var agg = as(evalX.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        assertThat(agg.aggregates().getFirst().name(), is("emp_no"));
        as(as(agg.aggregates().getFirst(), Alias.class).child(), Count.class);

        // Fork with one branch
        var fork = as(agg.child(), Fork.class);
        assertThat(fork.children(), hasSize(1));
        assertThat(Expressions.names(fork.output()), hasItems("emp_no", "_fork"));

        // Branch 0: Limit → Project → Eval[_fork] → MvExpand[emp_no] → Filter[true] → Project[[emp_no]] → EsRelation
        var b0Limit = as(fork.children().getFirst(), Limit.class);
        var b0Project = as(b0Limit.child(), Project.class);
        assertThat(Expressions.names(b0Project.projections()), hasItems("emp_no", "_fork"));

        var b0EvalFork = as(b0Project.child(), Eval.class);
        assertThat(b0EvalFork.fields().getFirst().name(), is("_fork"));

        var b0MvExpand = as(b0EvalFork.child(), MvExpand.class);
        assertThat(b0MvExpand.target().name(), is("emp_no"));

        var b0Filter = as(b0MvExpand.child(), Filter.class);
        assertThat(as(b0Filter.condition(), Literal.class).value(), is(true));

        // Inner Project narrowing to emp_no (emp_no is mapped, so no null-Eval)
        var b0InnerProject = as(b0Filter.child(), Project.class);
        assertThat(Expressions.names(b0InnerProject.projections()), is(List.of("emp_no")));

        as(b0InnerProject.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Project[[emp_no{r}#8]]
     *   \_Eval[[LEAST(emp_no{r}#8,TOLONG(52[INTEGER]),TOLONG(60[INTEGER])) AS x#11]]
     *     \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS emp_no#8]]
     *       \_Fork[[emp_no{r}#37, _fork{r}#38]]
     *         |_Limit[1000[INTEGER],false,false]
     *         | \_Project[[emp_no{r}#36, _fork{r}#5]]
     *         |   \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *         |     \_MvExpand[emp_no{f}#14,emp_no{r}#36]
     *         |       \_Filter[true[BOOLEAN]]
     *         |         \_Project[[emp_no{f}#14]]
     *         |           \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Project[[emp_no{f}#25, _fork{r}#5]]
     *             \_Eval[[fork2[KEYWORD] AS _fork#5]]
     *               \_Sample[0.5[DOUBLE]]
     *                 \_Filter[true[BOOLEAN]]
     *                   \_Project[[emp_no{f}#25]]
     *                     \_EsRelation[test][_meta_field{f}#31, emp_no{f}#25, first_name{f}#26, ..]
     * }</pre>
     */
    public void testForkWithUnmappedStatsEvalKeepTwoBranches() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var plan = analyzeStatement(setUnmappedNullify("""
            from test
            | keep emp_no
            | FORK  (where true | mv_expand emp_no) (where true | SAMPLE 0.5)
            | stats emp_no = count(*)
            | eval  x = least(emp_no, 52, 60)
            | keep emp_no
            """));

        // Limit[1000]
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // KEEP emp_no → Project[[emp_no]]
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));

        // EVAL x = LEAST(emp_no, 52, 60)
        var evalX = as(project.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        assertThat(evalX.fields().getFirst().name(), is("x"));

        // STATS emp_no = COUNT(*) — no groupings, single aggregate
        var agg = as(evalX.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        assertThat(agg.aggregates().getFirst().name(), is("emp_no"));
        as(as(agg.aggregates().getFirst(), Alias.class).child(), Count.class);

        // Fork with two branches; output carries emp_no and _fork
        var fork = as(agg.child(), Fork.class);
        assertThat(fork.children(), hasSize(2));
        assertThat(Expressions.names(fork.output()), hasItems("emp_no", "_fork"));

        // Branch 0: Limit → Project → Eval[_fork] → MvExpand[emp_no] → Filter[true] → Project[[emp_no]] → EsRelation
        // emp_no is a mapped field, so there is no null-Eval before EsRelation
        var b0Limit = as(fork.children().get(0), Limit.class);
        var b0Project = as(b0Limit.child(), Project.class);
        assertThat(Expressions.names(b0Project.projections()), hasItems("emp_no", "_fork"));

        var b0EvalFork = as(b0Project.child(), Eval.class);
        assertThat(b0EvalFork.fields().getFirst().name(), is("_fork"));

        var b0MvExpand = as(b0EvalFork.child(), MvExpand.class);
        assertThat(b0MvExpand.target().name(), is("emp_no"));

        var b0Filter = as(b0MvExpand.child(), Filter.class);
        assertThat(as(b0Filter.condition(), Literal.class).value(), is(true));

        var b0InnerProject = as(b0Filter.child(), Project.class);
        assertThat(Expressions.names(b0InnerProject.projections()), is(List.of("emp_no")));
        as(b0InnerProject.child(), EsRelation.class);

        // Branch 1: Limit → Project → Eval[_fork] → Sample[0.5] → Filter[true] → Project[[emp_no]] → EsRelation
        // emp_no is a mapped field, so there is no null-Eval before EsRelation
        var b1Limit = as(fork.children().get(1), Limit.class);
        var b1Project = as(b1Limit.child(), Project.class);
        assertThat(Expressions.names(b1Project.projections()), hasItems("emp_no", "_fork"));

        var b1EvalFork = as(b1Project.child(), Eval.class);
        assertThat(b1EvalFork.fields().getFirst().name(), is("_fork"));

        var b1Sample = as(b1EvalFork.child(), Sample.class);
        assertThat(as(b1Sample.probability(), Literal.class).value(), is(0.5));

        var b1Filter = as(b1Sample.child(), Filter.class);
        assertThat(as(b1Filter.condition(), Literal.class).value(), is(true));

        var b1InnerProject = as(b1Filter.child(), Project.class);
        assertThat(Expressions.names(b1InnerProject.projections()), is(List.of("emp_no")));
        as(b1InnerProject.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_fork{r}#12, x{r}#8]]
     *   \_Eval[[COALESCE(a{r}#11,TOLONG(5[INTEGER])) AS x#8]]
     *     \_Fork[[a{r}#11, _fork{r}#12]]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Project[[a{r}#4, _fork{r}#5]]
     *           \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *             \_Filter[true[BOOLEAN]]
     *               \_Row[[TOLONG(12[INTEGER]) AS a#4]]
     * }</pre>
     */
    public void testForkWithRowCoalesceAndDrop() {
        var plan = analyzeStatement(setUnmappedNullify("""
            ROW a = 12::long
            | fork (where true)
            | eval x = Coalesce(a, 5)
            | drop a
            """));

        // Limit[1000]
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), is(1000));

        // DROP a → Project[[_fork, x]] (a is excluded)
        var project = as(limit.child(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("_fork", "x")));
        assertThat(Expressions.names(project.projections()), not(hasItems("a")));

        // EVAL x = COALESCE(a, TOLONG(5))
        var evalX = as(project.child(), Eval.class);
        assertThat(evalX.fields(), hasSize(1));
        var xAlias = as(evalX.fields().getFirst(), Alias.class);
        assertThat(xAlias.name(), is("x"));
        as(xAlias.child(), Coalesce.class);

        // Fork with one branch
        var fork = as(evalX.child(), Fork.class);
        assertThat(fork.children(), hasSize(1));
        assertThat(Expressions.names(fork.output()), hasItems("a", "_fork"));

        // Branch 0: Limit → Project[a, _fork] → Eval[_fork] → Filter[true] → Row[a = TOLONG(12)]
        var b0Limit = as(fork.children().getFirst(), Limit.class);

        var b0Project = as(b0Limit.child(), Project.class);
        assertThat(Expressions.names(b0Project.projections()), hasItems("a", "_fork"));

        var b0EvalFork = as(b0Project.child(), Eval.class);
        assertThat(b0EvalFork.fields().getFirst().name(), is("_fork"));

        var b0Filter = as(b0EvalFork.child(), Filter.class);
        assertThat(as(b0Filter.condition(), Literal.class).value(), is(true));

        var row = as(b0Filter.child(), Row.class);
        assertThat(row.fields(), hasSize(1));
        assertThat(row.fields().getFirst().name(), is("a"));
    }

    /*
     * Reproducer for https://github.com/elastic/elasticsearch/issues/141870
     * ResolveRefs processes the EVAL only after ImplicitCasting processes the implicit cast in the WHERE.
     * This means that ResolveUnmapped will see the EVAL with a yet-to-be-resolved reference to nanos.
     * It should not treat it as unmapped, because there is clearly a nanos attribute in the EVAL's input.
     *
     * <pre>
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[MVMIN(nanos{r}#6) AS nanos#11]]
     *   \_Filter[millis{r}#4 &lt; 946684800000[DATETIME]]
     *     \_OrderBy[[Order[millis{r}#4,ASC,LAST]]]
     *       \_Row[[TODATETIME(1970-01-01T00:00:00Z[KEYWORD]) AS millis#4, TODATENANOS(1970-01-01T00:00:00Z[KEYWORD]) AS nanos#6]]
     * </pre>
     */
    public void testDoNotResolveUnmappedFieldPresentInChildren() {
        String query = """
                ROW millis = "1970-01-01T00:00:00Z"::date, nanos = "1970-01-01T00:00:00Z"::date_nanos
                | SORT millis ASC
                | WHERE millis < "2000-01-01"
                | EVAL nanos = MV_MIN(nanos)
            """;
        boolean useNullify = randomBoolean();
        var plan = analyzeStatement(useNullify ? setUnmappedNullify(query) : setUnmappedLoad(query));

        var limit = asLimit(plan, 1000);
        var eval = as(limit.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        var orderBy = as(filter.child(), OrderBy.class);
        // There should be no EVAL injected with NULL for nanos
        var row = as(orderBy.child(), Row.class);

        var output = plan.output();
        assertThat(output, hasSize(2));
        var millisAttr = output.get(0);
        var nanosAttr = output.get(1);
        assertThat(millisAttr.name(), is("millis"));
        assertThat(millisAttr.dataType(), is(DataType.DATETIME));
        assertThat(nanosAttr.name(), is("nanos"));
        assertThat(nanosAttr.dataType(), is(DataType.DATE_NANOS));
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Unmapped fields with dotted names (e.g. host.entity.id) should be nullified in STATS WHERE, even when an EVAL before the STATS
     * creates a field whose name is a suffix of the unmapped field name (e.g. entity.id).
     *
     * <pre>
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[entity.id{r}#6],[FilteredExpression[VALUES(host.entity.id{r}#13,true[BOOLEAN],PT0S[TIME_DURATION]),
     *                               ISNOTNULL(host.entity.id{r}#13)] AS host.entity.id#10, entity.id{r}#6]]
     *   \_Eval[[foo[KEYWORD] AS entity.id#6, null[NULL] AS host.entity.id#13]]
     *     \_Row[[1[INTEGER] AS x#4]]
     * </pre>
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedField() {
        var plan = analyzeStatement(setUnmappedNullify("""
            ROW x = 1
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """));

        // TODO: golden testing
        var limit = as(plan, Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.output()), is(List.of("host.entity.id", "entity.id")));
        assertThat(agg.groupings(), hasSize(1));
        assertThat(Expressions.name(agg.groupings().getFirst()), is("entity.id"));

        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), hasItems("entity.id", "host.entity.id"));
        var hostEntityIdAlias = eval.fields().stream().filter(a -> a.name().equals("host.entity.id")).findFirst().orElseThrow();
        assertThat(hostEntityIdAlias.child(), instanceOf(Literal.class));
        assertThat(hostEntityIdAlias.child().dataType(), is(DataType.NULL));

        as(eval.child(), Row.class);
    }

    /**
     * Reproducer for https://github.com/elastic/elasticsearch/issues/143991
     * Same as {@link #testStatsFilteredAggAfterEvalWithDottedUnmappedField()} but with FROM instead of ROW.
     * Tests both nullify and load modes: the plan shape is the same, only the field type in the EsRelation differs.
     *
     * <pre>
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[entity.id{r}#4],[FilteredExpression[VALUES(host.entity.id{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]),
     *                               ISNOTNULL(host.entity.id{f}#22)] AS host.entity.id#8, entity.id{r}#4]]
     *   \_Eval[[foo[KEYWORD] AS entity.id#4]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * </pre>
     */
    public void testStatsFilteredAggAfterEvalWithDottedUnmappedFieldFromIndex() {
        boolean load = randomBoolean();
        String query = """
            FROM test
            | EVAL entity.id = "foo"
            | STATS host.entity.id = VALUES(host.entity.id) WHERE host.entity.id IS NOT NULL BY entity.id
            """;
        var plan = analyzeStatement(load ? setUnmappedLoad(query) : setUnmappedNullify(query));

        // TODO: golden testing
        var limit = as(plan, Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.output()), is(List.of("host.entity.id", "entity.id")));
        assertThat(agg.groupings(), hasSize(1));
        assertThat(Expressions.name(agg.groupings().getFirst()), is("entity.id"));

        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("entity.id")));

        var relation = as(eval.child(), EsRelation.class);
        var dneAttr = as(
            relation.output().stream().filter(a -> a.name().equals("host.entity.id")).findFirst().orElseThrow(),
            FieldAttribute.class
        );
        if (load) {
            as(dneAttr.field(), PotentiallyUnmappedKeywordEsField.class);
        } else {
            assertThat(dneAttr.dataType(), is(DataType.NULL));
        }
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
