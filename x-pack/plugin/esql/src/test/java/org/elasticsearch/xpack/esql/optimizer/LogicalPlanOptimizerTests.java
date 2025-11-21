/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.LiteralsOnTheRight;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEnrich;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownInferencePlan;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownRegexExtract;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SplitInWithFoldableValue;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.containsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ignoreIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.localSource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultAnalyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.EQ;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.GT;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.GTE;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.LT;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.LTE;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.DeduplicateAggsTests.aggFieldName;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.DeduplicateAggsTests.aliased;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.DOWN;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {
    private static final LiteralsOnTheRight LITERALS_ON_THE_RIGHT = new LiteralsOnTheRight();

    public void testEvalWithScoreImplicitLimit() {
        var plan = plan("""
            FROM test
            | EVAL s = SCORE(MATCH(last_name, "high"))
            """);
        var limit = as(plan, Limit.class);
        assertThat(limit.child(), instanceOf(Eval.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(1000));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).child(), instanceOf(Score.class));
    }

    public void testEvalWithScoreExplicitLimit() {
        var plan = plan("""
            FROM test
            | EVAL s = SCORE(MATCH(last_name, "high"))
            | LIMIT 42
            """);
        var limit = as(plan, Limit.class);
        assertThat(limit.child(), instanceOf(Eval.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(42));
        var eval = as(limit.child(), Eval.class);
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).child(), instanceOf(Score.class));
    }

    public void testEmptyProjections() {
        var plan = plan("""
            from test
            | keep salary
            | drop salary
            """);

        var project = as(plan, EsqlProject.class);
        assertThat(project.expressions(), is(empty()));
        var limit = as(project.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testEmptyProjectionInStat() {
        var plan = plan("""
            from test
            | stats c = count(salary)
            | drop c
            """);
        var limit = as(plan, Limit.class);
        var relation = as(limit.child(), LocalRelation.class);
        assertThat(relation.output(), is(empty()));
        Page page = relation.supplier().get();
        assertThat(page.getBlockCount(), is(0));
        assertThat(page.getPositionCount(), is(1));
    }

    /**
     * Expects
     *
     * <pre>{@code
     * EsqlProject[[x{r}#6]]
     * \_Eval[[1[INTEGER] AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_LocalRelation[[{e}#18],[ConstantNullBlock[positions=1]]]
     * }</pre>
     */
    public void testEmptyProjectInStatWithEval() {
        var plan = plan("""
            from test
            | where languages > 1
            | stats c = count(salary)
            | eval x = 1, c2 = c*2
            | drop c, c2
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        var singleRowRelation = as(limit.child(), LocalRelation.class);
        var singleRow = singleRowRelation.supplier().get();
        assertThat(singleRow.getBlockCount(), equalTo(1));
        assertThat(singleRow.getBlock(0).getPositionCount(), equalTo(1));

        var exprs = eval.fields();
        assertThat(exprs.size(), equalTo(1));
        var alias = as(exprs.get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));
        assertThat(alias.child().fold(FoldContext.small()), equalTo(1));
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[x{r}#8]]
     * \_Eval[[1[INTEGER] AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[emp_no{f}#15],[emp_no{f}#15]]
     *       \_Filter[languages{f}#18 > 1[INTEGER]]
     *         \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testEmptyProjectInStatWithGroupAndEval() {
        var plan = plan("""
            from test
            | where languages > 1
            | stats c = count(salary) by emp_no
            | eval x = 1, c2 = c*2
            | drop c, emp_no, c2
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        var relation = as(filter.child(), EsRelation.class);

        assertThat(Expressions.names(agg.groupings()), contains("emp_no"));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        assertThat(agg.aggregates().get(0).id(), equalTo(Expressions.attribute(agg.groupings().get(0)).id()));

        var exprs = eval.fields();
        assertThat(exprs.size(), equalTo(1));
        var alias = as(exprs.get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));
        assertThat(alias.child().fold(FoldContext.small()), equalTo(1));

        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("languages"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1));
    }

    public void testCombineProjections() {
        var plan = plan("""
            from test
            | keep emp_no, *name, salary
            | keep last_name
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("last_name"));
        var limit = as(keep.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[languages{f}#12 AS f2]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testCombineProjectionsWithEvalAndDrop() {
        var plan = plan("""
            from test
            | eval f1 = languages, f2 = f1
            | keep f2
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("f2"));
        assertThat(Expressions.name(Alias.unwrap(keep.projections().get(0))), is("languages"));
        var limit = as(keep.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);

    }

    /**
     * Expects
     * <pre>{@code
     * Project[[last_name{f}#26, languages{f}#25 AS f2, f4{r}#13]]
     * \_Eval[[languages{f}#25 + 3[INTEGER] AS f4]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     * }</pre>
     */
    public void testCombineProjectionsWithEval() {
        var plan = plan("""
            from test
            | eval f1 = languages, f2 = f1, f3 = 1 + 2, f4 = f3 + languages
            | keep emp_no, *name, salary, f*
            | drop f3
            | keep last_name, f2, f4
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("last_name", "f2", "f4"));
        var eval = as(keep.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("f4"));
        var add = as(Alias.unwrap(eval.fields().get(0)), Add.class);
        var limit = as(eval.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
    }

    public void testCombineProjectionWithFilterInBetween() {
        var plan = plan("""
            from test
            | keep *name, salary
            | where salary > 10
            | keep last_name
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("last_name"));
    }

    public void testCombineProjectionWhilePreservingAlias() {
        var plan = plan("""
            from test
            | rename first_name as x
            | keep x, salary
            | where salary > 10
            | rename x as y
            | keep y
            """);

        var keep = as(plan, Project.class);
        assertThat(Expressions.names(keep.projections()), contains("y"));
        var p = keep.projections().get(0);
        var alias = as(p, Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("first_name"));
    }

    public void testCombineProjectionWithAggregation() {
        var plan = plan("""
            from test
            | stats s = sum(salary) by last_name, first_name
            | keep s, last_name, first_name
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name"));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[last_name{f}#23, first_name{f}#20],[SUM(salary{f}#24) AS s, last_name{f}#23, first_name{f}#20, first_name{f}#2
     * 0 AS k]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }</pre>
     */
    public void testCombineProjectionWithAggregationAndEval() {
        var plan = plan("""
            from test
            | eval k = first_name, k1 = k
            | stats s = sum(salary) by last_name, first_name, k, k1
            | keep s, last_name, first_name, k
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name", "k"));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * TopN[[Order[x{r}#10,ASC,LAST]],1000[INTEGER]]
     * \_Aggregate[[languages{f}#16],[MAX(emp_no{f}#13) AS x, languages{f}#16]]
     *   \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testRemoveOverridesInAggregate() throws Exception {
        var plan = plan("""
            from test
            | stats x = count(emp_no), x = min(emp_no), x = max(emp_no) by languages
            | sort x
            """);

        var topN = as(plan, TopN.class);
        var agg = as(topN.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(2));
        assertThat(Expressions.names(aggregates), contains("x", "languages"));
        var alias = as(aggregates.get(0), Alias.class);
        var max = as(alias.child(), Max.class);
        assertThat(Expressions.name(max.arguments().get(0)), equalTo("emp_no"));
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:28: Field 'x' shadowed by field at line 2:45",
            "Line 2:9: Field 'x' shadowed by field at line 2:45"
        );
    }

    // expected stats b by b (grouping overrides the rest of the aggs)

    /**
     * Expects
     * <pre>{@code
     * TopN[[Order[b{r}#10,ASC,LAST]],1000[INTEGER]]
     * \_Aggregate[[b{r}#10],[languages{f}#16 AS b]]
     *   \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testAggsWithOverridingInputAndGrouping() throws Exception {
        var plan = plan("""
            from test
            | stats b = count(emp_no), b = max(emp_no) by b = languages
            | sort b
            """);

        var topN = as(plan, TopN.class);
        var agg = as(topN.child(), Aggregate.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates, hasSize(1));
        assertThat(Expressions.names(aggregates), contains("b"));
        assertWarnings(
            "No limit defined, adding default limit of [1000]",
            "Line 2:28: Field 'b' shadowed by field at line 2:47",
            "Line 2:9: Field 'b' shadowed by field at line 2:47"
        );
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[STANDARD,[],[SUM(salary{f}#12,true[BOOLEAN]) AS sum(salary), SUM(salary{f}#12,last_name{f}#11 == [44 6f 65][KEYW
     * ORD]) AS sum(salary) WheRe last_name ==   "Doe"]]
     *   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testStatsWithFilteringDefaultAliasing() {
        var plan = plan("""
            from test
            | stats sum(salary), sum(salary) WheRe last_name ==   "Doe"
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates(), hasSize(2));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(salary)", "sum(salary) WheRe last_name ==   \"Doe\""));
    }

    public void testExtractStatsCommonFilter() {
        var plan = plan("""
            from test
            | stats m = min(salary) where emp_no > 1,
                    max(salary) where emp_no > 1
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        var filter = as(agg.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("emp_no > 1"));

        var source = as(filter.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[],[MIN(salary{f}#15,true[BOOLEAN]) AS m1#4, MAX(salary{f}#15,true[BOOLEAN]) AS m2#8]]
     * \_Filter[emp_no{f}#10 > 1[INTEGER]]
     * \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testExtractStatsCommonAlwaysTruePlusOtherFilter() {
        var plan = plan("""
            from test
            | stats m1 = min(salary) where (true and emp_no > 1),
                    m2 = max(salary) where (1==1 and emp_no > 1)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        var filter = as(agg.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("emp_no > 1"));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterUsingAliases() {
        var plan = plan("""
            from test
            | eval eno = emp_no
            | drop emp_no
            | stats min(salary) where eno > 1,
                    max(salary) where eno > 1
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        var filter = as(agg.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("eno > 1"));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterUsingJustOneAlias() {
        var plan = plan("""
            from test
            | eval eno = emp_no
            | stats min(salary) where emp_no > 1,
                    max(salary) where eno > 1
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        var filter = as(agg.child(), Filter.class);
        var gt = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(gt.left()), is("emp_no"));
        assertTrue(gt.right().foldable());
        assertThat(gt.right().fold(FoldContext.small()), is(1));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterSkippedNotSameFilter() {
        var plan = plan("""
            from test
            | stats min(salary) where emp_no > 1,
                    max(salary) where emp_no > 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(BinaryComparison.class));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(BinaryComparison.class));

        var source = as(agg.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterSkippedOnLackingFilter() {
        var plan = plan("""
            from test
            | stats min(salary),
                    max(salary) where emp_no > 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(BinaryComparison.class));

        var source = as(agg.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterSkippedWithGroups() {
        var plan = plan("""
            from test
            | stats min(salary) where emp_no > 2,
                    max(salary) where emp_no > 2 by first_name
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(3));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(BinaryComparison.class));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(BinaryComparison.class));

        var source = as(agg.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterNormalizeAndCombineWithExistingFilter() {
        var plan = plan("""
            from test
            | where emp_no > 3
            | stats min(salary) where emp_no > 2,
                    max(salary) where 2 < emp_no
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), is(Literal.TRUE));

        var filter = as(agg.child(), Filter.class);
        assertThat(Expressions.name(filter.condition()), is("emp_no > 3"));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterInConjunction() {
        var plan = plan("""
            from test
            | stats min(salary) where emp_no > 2 and first_name == "John",
                    max(salary) where emp_no > 1 + 1 and length(last_name) < 19
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.filter()), is("first_name == \"John\""));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.filter()), is("length(last_name) < 19"));

        var filter = as(agg.child(), Filter.class);
        var gt = as(filter.condition(), GreaterThan.class); // name is "emp_no > 1 + 1"
        assertThat(Expressions.name(gt.left()), is("emp_no"));
        assertTrue(gt.right().foldable());
        assertThat(gt.right().fold(FoldContext.small()), is(2));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterInConjunctionWithMultipleCommonConjunctions() {
        var plan = plan("""
            from test
            | stats min(salary) where emp_no < 10 and first_name == "John" and last_name == "Doe",
                    max(salary) where emp_no - 1 < 2 + 7 and length(last_name) < 19 and last_name == "Doe"
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.filter()), is("first_name == \"John\""));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.filter()), is("length(last_name) < 19"));

        var filter = as(agg.child(), Filter.class);
        var and = as(filter.condition(), And.class);

        var lt = as(and.left(), LessThan.class);
        assertThat(Expressions.name(lt.left()), is("emp_no"));
        assertTrue(lt.right().foldable());
        assertThat(lt.right().fold(FoldContext.small()), is(10));

        var equals = as(and.right(), Equals.class);
        assertThat(Expressions.name(equals.left()), is("last_name"));
        assertTrue(equals.right().foldable());
        assertThat(equals.right().fold(FoldContext.small()), is(BytesRefs.toBytesRef("Doe")));

        var source = as(filter.child(), EsRelation.class);
    }

    public void testExtractStatsCommonFilterSkippedDueToDisjunction() {
        // same query as in testExtractStatsCommonFilterInConjunction, except for the OR in the filter
        var plan = plan("""
            from test
            | stats min(salary) where emp_no > 2 OR first_name == "John",
                    max(salary) where emp_no > 1 + 1 and length(last_name) < 19
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.aggregates().size(), is(2));

        var alias = as(agg.aggregates().get(0), Alias.class);
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(Or.class));

        alias = as(agg.aggregates().get(1), Alias.class);
        aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc.filter(), instanceOf(And.class));

        var source = as(agg.child(), EsRelation.class);
    }

    public void testQlComparisonOptimizationsApply() {
        var plan = plan("""
            from test
            | where (1 + 4) < salary
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        // The core QL optimizations rotate constants to the right.
        var condition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(condition.left()), equalTo("salary"));
        assertThat(Expressions.name(condition.right()), equalTo("1 + 4"));
        var con = as(condition.right(), Literal.class);
        assertThat(con.value(), equalTo(5));
    }

    public void testCombineDisjunctionToInEquals() {
        LogicalPlan plan = plan("""
            from test
            | where emp_no == 1 or emp_no == 2
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var condition = as(filter.condition(), In.class);
        assertThat(condition.list(), equalTo(List.of(new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, 2, INTEGER))));
    }

    public void testCombineDisjunctionToInMixed() {
        LogicalPlan plan = plan("""
            from test
            | where emp_no == 1 or emp_no in (2)
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var condition = as(filter.condition(), In.class);
        assertThat(condition.list(), equalTo(List.of(new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, 2, INTEGER))));
    }

    public void testCombineDisjunctionToInFromIn() {
        LogicalPlan plan = plan("""
            from test
            | where emp_no in (1) or emp_no in (2)
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var condition = as(filter.condition(), In.class);
        assertThat(condition.list(), equalTo(List.of(new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, 2, INTEGER))));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#12],[COUNT(salary{f}#16) AS count(salary), first_name{f}#12 AS x]]
     *   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    public void testCombineProjectionWithPruning() {
        var plan = plan("""
            from test
            | rename first_name as x
            | keep x, salary, last_name
            | stats count(salary) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("count(salary)", "x"));
        assertThat(Expressions.names(agg.groupings()), contains("first_name"));
        var alias = as(agg.aggregates().get(1), Alias.class);
        var field = as(alias.child(), FieldAttribute.class);
        assertThat(field.name(), is("first_name"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#16],[SUM(emp_no{f}#15) AS s, COUNT(first_name{f}#16) AS c, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testCombineProjectionWithAggregationFirstAndAliasedGroupingUsedInAgg() {
        var plan = plan("""
            from test
            | rename emp_no as e, first_name as f
            | stats s = sum(e), c = count(f) by f
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("s", "c", "f"));
        Alias as = as(aggs.get(0), Alias.class);
        var sum = as(as.child(), Sum.class);
        assertThat(Expressions.name(sum.field()), is("emp_no"));
        as = as(aggs.get(1), Alias.class);
        var count = as(as.child(), Count.class);
        assertThat(Expressions.name(count.field()), is("first_name"));

        as = as(aggs.get(2), Alias.class);
        assertThat(Expressions.name(as.child()), is("first_name"));

        assertThat(Expressions.names(agg.groupings()), contains("first_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[STANDARD,[CATEGORIZE(first_name{f}#18) AS cat],[SUM(salary{f}#22,true[BOOLEAN]) AS s, cat{r}#10]]
     *   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testCombineProjectionWithCategorizeGrouping() {
        var plan = plan("""
            from test
            | eval k = first_name, k1 = k
            | stats s = sum(salary) by cat = CATEGORIZE(k1)
            | keep s, cat
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.child(), instanceOf(EsRelation.class));

        assertThat(Expressions.names(agg.aggregates()), contains("s", "cat"));
        assertThat(Expressions.names(agg.groupings()), contains("cat"));

        var categorizeAlias = as(agg.groupings().get(0), Alias.class);
        var categorize = as(categorizeAlias.child(), Categorize.class);
        var categorizeField = as(categorize.field(), FieldAttribute.class);
        assertThat(categorizeField.name(), is("first_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#16],[SUM(emp_no{f}#15) AS s, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testCombineProjectionWithAggregationFirstAndAliasedGroupingUnused() {
        var plan = plan("""
            from test
            | rename emp_no as e, first_name as f, last_name as l
            | stats s = sum(e) by f
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("s", "f"));
        Alias as = as(aggs.get(0), Alias.class);
        var aggFunc = as(as.child(), AggregateFunction.class);
        assertThat(Expressions.name(aggFunc.field()), is("emp_no"));
        as = as(aggs.get(1), Alias.class);
        assertThat(Expressions.name(as.child()), is("first_name"));

        assertThat(Expressions.names(agg.groupings()), contains("first_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[x{r}#3, y{r}#6]]
     * \_Eval[[emp_no{f}#9 + 2[INTEGER] AS x, salary{f}#14 + 3[INTEGER] AS y]]
     *   \_Limit[10000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testCombineEvals() {
        var plan = plan("""
            from test
            | eval x = emp_no + 2
            | eval y = salary + 3
            | keep x, y
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "y"));
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testCombineLimits() {
        var limitValues = new int[] { randomIntBetween(10, 99), randomIntBetween(100, 1000) };
        var firstLimit = randomBoolean() ? 0 : 1;
        var secondLimit = firstLimit == 0 ? 1 : 0;
        var oneLimit = new Limit(EMPTY, L(limitValues[firstLimit]), emptySource());
        var anotherLimit = new Limit(EMPTY, L(limitValues[secondLimit]), oneLimit);
        assertEquals(
            new Limit(EMPTY, L(Math.min(limitValues[0], limitValues[1])), emptySource()),
            new PushDownAndCombineLimits().rule(anotherLimit, logicalOptimizerCtx)
        );
    }

    public void testPushdownLimitsPastLeftJoin() {
        var rule = new PushDownAndCombineLimits();

        var leftChild = emptySource();
        var rightChild = new LocalRelation(Source.EMPTY, List.of(fieldAttribute()), EmptyLocalSupplier.EMPTY);
        assertNotEquals(leftChild, rightChild);

        var joinConfig = new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), null);
        var join = switch (randomIntBetween(0, 2)) {
            case 0 -> new Join(EMPTY, leftChild, rightChild, joinConfig);
            case 1 -> new LookupJoin(EMPTY, leftChild, rightChild, joinConfig, false);
            case 2 -> new InlineJoin(EMPTY, leftChild, rightChild, joinConfig);
            default -> throw new IllegalArgumentException();
        };

        var limit = new Limit(EMPTY, L(10), join);

        var optimizedPlan = rule.apply(limit, logicalOptimizerCtx);

        var expectedPlan = join instanceof InlineJoin
            ? new Limit(limit.source(), limit.limit(), join, false, false)
            : new Limit(limit.source(), limit.limit(), join.replaceChildren(limit.replaceChild(join.left()), join.right()), true, false);

        assertEquals(expectedPlan, optimizedPlan);

        var optimizedTwice = rule.apply(optimizedPlan, logicalOptimizerCtx);
        // We mustn't create the limit after the JOIN multiple times when the rule is applied multiple times, that'd lead to infinite loops.
        assertEquals(optimizedPlan, optimizedTwice);
    }

    public void testMultipleCombineLimits() {
        var numberOfLimits = randomIntBetween(3, 10);
        var minimum = randomIntBetween(10, 99);
        var limitWithMinimum = randomIntBetween(0, numberOfLimits - 1);

        var fa = getFieldAttribute("a", INTEGER);
        var relation = localSource(TestBlockFactory.getNonBreakingInstance(), singletonList(fa), singletonList(1));
        LogicalPlan plan = relation;

        for (int i = 0; i < numberOfLimits; i++) {
            var value = i == limitWithMinimum ? minimum : randomIntBetween(100, 1000);
            plan = new Limit(EMPTY, L(value), plan);
        }
        assertEquals(new Limit(EMPTY, L(minimum), relation), logicalOptimizer.optimize(plan));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/115311")
    public void testSelectivelyPushDownFilterPastRefAgg() {
        // expected plan: "from test | where emp_no > 1 and emp_no < 3 | stats x = count(1) by emp_no | where x > 7"
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no > 1
            | stats x = count(1) by emp_no
            | where x + 2 > 9
            | where emp_no < 3""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof GreaterThan);
        var gt = (GreaterThan) filter.condition();
        assertTrue(gt.left() instanceof ReferenceAttribute);
        var refAttr = (ReferenceAttribute) gt.left();
        assertEquals("x", refAttr.name());
        assertEquals(L(7), gt.right());

        var agg = as(filter.child(), Aggregate.class);

        filter = as(agg.child(), Filter.class);
        assertTrue(filter.condition() instanceof And);
        var and = (And) filter.condition();
        assertTrue(and.left() instanceof GreaterThan);
        gt = (GreaterThan) and.left();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertTrue(and.right() instanceof LessThan);
        var lt = (LessThan) and.right();
        assertTrue(lt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) lt.left()).name());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testNoPushDownOrFilterPastAgg() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats x = count(1) by emp_no
            | where emp_no < 3 or x > 9""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var stats = as(filter.child(), Aggregate.class);
        assertTrue(stats.child() instanceof EsRelation);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/115311")
    public void testSelectivePushDownComplexFilterPastAgg() {
        // expected plan: from test | emp_no > 0 | stats x = count(1) by emp_no | where emp_no < 3 or x > 9
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats x = count(1) by emp_no
            | where (emp_no < 3 or x > 9) and emp_no > 0""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var stats = as(filter.child(), Aggregate.class);
        filter = as(stats.child(), Filter.class);
        assertTrue(filter.condition() instanceof GreaterThan);
        var gt = (GreaterThan) filter.condition();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertEquals(L(0), gt.right());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSelectivelyPushDownFilterPastEval() {
        // expected plan: "from test | where emp_no > 1 and emp_no < 3 | eval x = emp_no + 1 | where x < 7"
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no > 1
            | eval x = emp_no + 1
            | where x + 2 < 9
            | where emp_no < 3""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof LessThan);
        var lt = (LessThan) filter.condition();
        assertTrue(lt.left() instanceof ReferenceAttribute);
        var refAttr = (ReferenceAttribute) lt.left();
        assertEquals("x", refAttr.name());
        assertEquals(L(7), lt.right());

        var eval = as(filter.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertTrue(eval.fields().get(0) instanceof Alias);
        assertEquals("x", (eval.fields().get(0)).name());

        filter = as(eval.child(), Filter.class);
        assertTrue(filter.condition() instanceof And);
        var and = (And) filter.condition();
        assertTrue(and.left() instanceof GreaterThan);
        var gt = (GreaterThan) and.left();
        assertTrue(gt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) gt.left()).name());
        assertTrue(and.right() instanceof LessThan);
        lt = (LessThan) and.right();
        assertTrue(lt.left() instanceof FieldAttribute);
        assertEquals("emp_no", ((FieldAttribute) lt.left()).name());

        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testNoPushDownOrFilterPastLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 3
            | where emp_no < 3 or salary > 9""");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Or);
        var or = (Or) filter.condition();
        assertTrue(or.left() instanceof LessThan);
        assertTrue(or.right() instanceof GreaterThan);

        var limit2 = as(filter.child(), Limit.class);
        assertTrue(limit2.child() instanceof EsRelation);
    }

    public void testPushDownFilterPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as x
            | keep x
            | where x > 10""");

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, FieldAttribute.class).name(), is("emp_no"));
    }

    public void testPushDownEvalPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as x
            | keep x
            | eval y = x * 2""");

        var keep = as(plan, Project.class);
        var eval = as(keep.child(), Eval.class);
        assertThat(
            eval.fields(),
            containsIgnoringIds(
                new Alias(
                    EMPTY,
                    "y",
                    new Mul(EMPTY, new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")), new Literal(EMPTY, 2, INTEGER))
                )
            )
        );
    }

    public void testPushDownDissectPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | dissect x "%{y}"
            """);

        var keep = as(plan, Project.class);
        var dissect = as(keep.child(), Dissect.class);
        assertThat(dissect.extractedFields(), containsIgnoringIds(referenceAttribute("y", DataType.KEYWORD)));
    }

    public void testPushDownGrokPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | grok x "%{WORD:y}"
            """);

        var keep = as(plan, Project.class);
        var grok = as(keep.child(), Grok.class);
        assertThat(grok.extractedFields(), containsIgnoringIds(referenceAttribute("y", DataType.KEYWORD)));
    }

    public void testPushDownFilterPastProjectUsingEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval y = emp_no + 1
            | rename y as x
            | where x > 10""");

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testPushDownFilterPastProjectUsingDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | dissect first_name "%{y}"
            | rename y as x
            | keep x
            | where x == "foo"
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var dissect = as(filter.child(), Dissect.class);
        as(dissect.child(), EsRelation.class);
    }

    public void testPushDownFilterPastProjectUsingGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | grok first_name "%{WORD:y}"
            | rename y as x
            | keep x
            | where x == "foo"
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var attr = filter.condition().collect(Attribute.class::isInstance).stream().findFirst().get();
        assertThat(as(attr, ReferenceAttribute.class).name(), is("y"));
        var grok = as(filter.child(), Grok.class);
        as(grok.child(), EsRelation.class);
    }

    public void testPushDownLimitPastEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = emp_no + 100
            | limit 10""");

        var eval = as(plan, Eval.class);
        as(eval.child(), Limit.class);
    }

    public void testPushDownLimitPastDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | dissect first_name "%{y}"
            | limit 10""");

        var dissect = as(plan, Dissect.class);
        as(dissect.child(), Limit.class);
    }

    public void testPushDownLimitPastGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | grok first_name "%{WORD:y}"
            | limit 10""");

        var grok = as(plan, Grok.class);
        as(grok.child(), Limit.class);
    }

    public void testPushDownLimitPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no as a
            | keep a
            | limit 10""");

        var keep = as(plan, Project.class);
        as(keep.child(), Limit.class);
    }

    public void testDontPushDownLimitPastFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 100
            | where emp_no > 10
            | limit 10""");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testEliminateHigherLimitDueToDescendantLimit() throws Exception {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 10
            | sort emp_no
            | where emp_no > 10
            | eval c = emp_no + 2
            | limit 100""");

        var topN = as(plan, TopN.class);
        var eval = as(topN.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testDoNotEliminateHigherLimitDueToDescendantLimit() throws Exception {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 10
            | where emp_no > 10
            | stats c = count(emp_no) by emp_no
            | limit 100""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        as(filter.child(), Limit.class);
    }

    public void testPruneSortBeforeStats() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | where emp_no > 10
            | stats x = sum(salary) by first_name""");

        var limit = as(plan, Limit.class);
        var stats = as(limit.child(), Aggregate.class);
        var filter = as(stats.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    public void testDontPruneSortWithLimitBeforeStats() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | limit 100
            | stats x = sum(salary) by first_name""");

        var limit = as(plan, Limit.class);
        var stats = as(limit.child(), Aggregate.class);
        var topN = as(stats.child(), TopN.class);
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderBy() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | sort salary""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | eval x = salary + 1
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var eval = as(topN.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughEvalWithTwoDefs() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | eval x = salary + 1, y = salary + 2
            | eval z = x * y
            | sort z""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("z"));
        var eval = as(topN.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "y", "z"));
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughDissect() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | dissect first_name "%{x}"
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var dissect = as(topN.child(), Dissect.class);
        as(dissect.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughGrok() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | grok first_name "%{WORD:x}"
            | sort x""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("x"));
        var grok = as(topN.child(), Grok.class);
        as(grok.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | keep salary, emp_no
            | sort salary""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProjectAndEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename emp_no as en
            | keep salary, en
            | eval e = en * 2
            | sort salary""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        var eval = as(topN.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("e"));
        as(eval.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughProjectWithAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename salary as l
            | keep l, emp_no
            | sort l""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        as(topN.child(), EsRelation.class);
    }

    public void testCombineOrderByThroughFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | where emp_no > 10
            | sort salary""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("salary"));
        var filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * TopN[[Order[first_name{r}#5575,ASC,LAST]],1000[INTEGER]]
     * \_MvExpand[first_name{f}#5565,first_name{r}#5575,null]
     *   \_EsRelation[test][_meta_field{f}#5570, emp_no{f}#5564, first_name{f}#..]
     * }</pre>
     */
    public void testDontCombineOrderByThroughMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | sort first_name""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("first_name"));
        var mvExpand = as(topN.child(), MvExpand.class);
        as(mvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_MvExpand[x{r}#4,x{r}#19]
     *   \_EsqlProject[[first_name{f}#9 AS x]]
     *     \_Limit[1000[INTEGER],false]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testCopyDefaultLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | mv_expand x
            """);

        var limit = asLimit(plan, 1000, true);
        var mvExpand = as(limit.child(), MvExpand.class);
        var keep = as(mvExpand.child(), EsqlProject.class);
        var limitPastMvExpand = asLimit(keep.child(), 1000, false);
        as(limitPastMvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Project[[languages{f}#10 AS language_code#4, language_name{f}#19]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18]]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     * }</pre>
     */
    public void testCopyDefaultLimitPastLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename languages AS language_code
            | keep language_code
            | lookup join languages_lookup ON language_code
            """);

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 1000, true);
        var join = as(limit.child(), Join.class);
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[10[INTEGER],true]
     * \_MvExpand[first_name{f}#7,first_name{r}#17]
     *   \_EsqlProject[[first_name{f}#7, last_name{f}#10]]
     *     \_Limit[1[INTEGER],false]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testDontPushDownLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 1
            | keep first_name, last_name
            | mv_expand first_name
            | limit 10
            """);

        var limit = asLimit(plan, 10, true);
        var mvExpand = as(limit.child(), MvExpand.class);
        var project = as(mvExpand.child(), EsqlProject.class);
        var limit2 = asLimit(project.child(), 1, false);
        as(limit2.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Project[[languages{f}#11 AS language_code#4, last_name{f}#12, language_name{f}#20]]
     * \_Limit[10[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#11],[languages{f}#11],[language_code{f}#19]]
     *     |_Limit[1[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     * }</pre>
     */
    public void testDontPushDownLimitPastLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 1
            | rename languages AS language_code
            | keep language_code, last_name
            | lookup join languages_lookup on language_code
            | limit 10
            """);

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 10, true);
        var join = as(limit.child(), Join.class);
        var limit2 = asLimit(join.left(), 1, false);
        as(limit2.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#19, first_name{r}#30, languages{f}#22, lll{r}#9, salary{r}#31]]
     * \_TopN[[Order[salary{r}#31,DESC,FIRST]],5[INTEGER]]
     *   \_Limit[5[INTEGER],true]
     *     \_MvExpand[salary{f}#24,salary{r}#31]
     *       \_Eval[[languages{f}#22 + 5[INTEGER] AS lll]]
     *         \_Limit[5[INTEGER],false]
     *           \_Filter[languages{f}#22 > 1[INTEGER]]
     *             \_Limit[10[INTEGER],true]
     *               \_MvExpand[first_name{f}#20,first_name{r}#30]
     *                 \_TopN[[Order[emp_no{f}#19,DESC,FIRST]],10[INTEGER]]
     *                   \_Filter[emp_no{f}#19 &leq; 10006[INTEGER]]
     *                     \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }</pre>
     */
    public void testMultipleMvExpandWithSortAndLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no <= 10006
            | sort emp_no desc
            | mv_expand first_name
            | limit 10
            | where languages > 1
            | eval lll = languages + 5
            | mv_expand salary
            | limit 5
            | sort first_name
            | keep emp_no, first_name, languages, lll, salary
            | sort salary desc
            """);

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var limit5Before = asLimit(topN.child(), 5, true);
        var mvExp = as(limit5Before.child(), MvExpand.class);
        var eval = as(mvExp.child(), Eval.class);
        var limit5 = asLimit(eval.child(), 5, false);
        var filter = as(limit5.child(), Filter.class);
        var limit10Before = asLimit(filter.child(), 10, true);
        mvExp = as(limit10Before.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     *
     * <pre>{@code
     * Project[[emp_no{f}#24, first_name{f}#25, languages{f}#27, lll{r}#11, salary{f}#29, language_name{f}#38]]
     * \_TopN[[Order[salary{f}#29,DESC,FIRST]],5[INTEGER]]
     *   \_Limit[5[INTEGER],true]
     *     \_Join[LEFT,[salary{f}#29],[salary{f}#29],[language_code{f}#37]]
     *       |_Eval[[languages{f}#27 + 5[INTEGER] AS lll#11]]
     *       | \_Limit[5[INTEGER],false]
     *       |   \_Filter[languages{f}#27 &gt; 1[INTEGER]]
     *       |     \_Limit[10[INTEGER],true]
     *       |       \_Join[LEFT,[languages{f}#27],[languages{f}#27],[language_code{f}#35]]
     *       |         |_TopN[[Order[emp_no{f}#24,DESC,FIRST]],10[INTEGER]]
     *       |         | \_Filter[emp_no{f}#24 &leq; 10006[INTEGER]]
     *       |         |   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     *       |         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#35]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#37, language_name{f}#38]
     * }</pre>
     */
    public void testMultipleLookupJoinWithSortAndLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no <= 10006
            | sort emp_no desc
            | eval language_code = languages
            | lookup join languages_lookup on language_code
            | limit 10
            | where languages > 1
            | eval lll = languages + 5
            | eval language_code = salary::integer
            | lookup join languages_lookup on language_code
            | limit 5
            | sort first_name
            | keep emp_no, first_name, languages, lll, salary, language_name
            | sort salary desc
            """);

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var limit5Before = asLimit(topN.child(), 5, true);
        var join = as(limit5Before.child(), Join.class);
        var eval = as(join.left(), Eval.class);
        var limit5 = asLimit(eval.child(), 5, false);
        var filter = as(limit5.child(), Filter.class);
        var limit10Before = asLimit(filter.child(), 10, true);
        join = as(limit10Before.child(), Join.class);
        topN = as(join.left(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        assertThat(orderNames(topN), contains("emp_no"));
        filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#10, first_name{r}#21, salary{f}#15]]
     * \_TopN[[Order[salary{f}#15,ASC,LAST], Order[first_name{r}#21,ASC,LAST]],5[INTEGER]]
     *   \_MvExpand[first_name{f}#11,first_name{r}#21,null]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testPushDownLimitThroughMultipleSort_AfterMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 5""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var mvExp = as(topN.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#2560, first_name{r}#2571, salary{f}#2565]]
     * \_TopN[[Order[first_name{r}#2571,ASC,LAST]],5[INTEGER]]
     *   \_TopN[[Order[salary{f}#2565,ASC,LAST]],5[INTEGER]]
     *     \_MvExpand[first_name{f}#2561,first_name{r}#2571,null]
     *       \_EsRelation[test][_meta_field{f}#2566, emp_no{f}#2560, first_name{f}#..]
     * }</pre>
     */
    public void testPushDownLimitThroughMultipleSort_AfterMvExpand2() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | sort salary
            | limit 5
            | sort first_name""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("first_name"));
        topN = as(topN.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var mvExp = as(topN.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * TODO: Push down the filter correctly https://github.com/elastic/elasticsearch/issues/115311
     *
     * Expected
     * <pre>{@code
     * Limit[5[INTEGER]]
     * \_Filter[ISNOTNULL(first_name{r}#23)]
     *   \_Aggregate[STANDARD,[first_name{r}#23],[MAX(salary{f}#18,true[BOOLEAN]) AS max_s, first_name{r}#23]]
     *     \_MvExpand[first_name{f}#14,first_name{r}#23]
     *       \_TopN[[Order[emp_no{f}#13,ASC,LAST]],50[INTEGER]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testDontPushDownLimitPastAggregate_AndMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | limit 50
            | mv_expand first_name
            | keep emp_no, first_name, salary
            | stats max_s = max(salary) by first_name
            | where first_name is not null
            | limit 5""");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(5));
        var agg = as(filter.child(), Aggregate.class);
        var mvExp = as(agg.child(), MvExpand.class);
        var topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(50));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * TODO: Push down the filter correctly https://github.com/elastic/elasticsearch/issues/115311
     *
     * Expected
     *
     * Limit[5[INTEGER],false]
     * \_Filter[ISNOTNULL(first_name{r}#23)]
     *   \_Aggregate[STANDARD,[first_name{r}#23],[MAX(salary{f}#17,true[BOOLEAN]) AS max_s, first_name{r}#23]]
     *     \_Limit[50[INTEGER],true]
     *       \_MvExpand[first_name{f}#13,first_name{r}#23]
     *         \_Limit[50[INTEGER],false]
     *           \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testPushDown_TheRightLimit_PastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | mv_expand first_name
            | limit 50
            | keep emp_no, first_name, salary
            | stats max_s = max(salary) by first_name
            | where first_name is not null
            | limit 5""");

        var limit = asLimit(plan, 5, false);
        var filter = as(limit.child(), Filter.class);
        var agg = as(filter.child(), Aggregate.class);
        var limit50Before = asLimit(agg.child(), 50, true);
        var mvExp = as(limit50Before.child(), MvExpand.class);
        limit = asLimit(mvExp.child(), 50, false);
        as(limit.child(), EsRelation.class);
    }

    /**
     * TODO: Push down the filter correctly past STATS https://github.com/elastic/elasticsearch/issues/115311
     *
     * Expected
     *
     * <pre>{@code
     * Limit[5[INTEGER],false]
     * \_Filter[ISNOTNULL(first_name{f}#15)]
     *   \_Aggregate[[first_name{f}#15],[MAX(salary{f}#19,true[BOOLEAN]) AS max_s#12, first_name{f}#15]]
     *     \_Limit[50[INTEGER],true]
     *       \_Join[LEFT,[languages{f}#17],[languages{f}#17],[language_code{f}#25]]
     *         |_Limit[50[INTEGER],false]
     *         | \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#25]
     * }</pre>
     */
    public void testPushDown_TheRightLimit_PastLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename languages as language_code
            | lookup join languages_lookup on language_code
            | limit 50
            | keep emp_no, first_name, salary
            | stats max_s = max(salary) by first_name
            | where first_name is not null
            | limit 5""");

        var limit = asLimit(plan, 5, false);
        var filter = as(limit.child(), Filter.class);
        var agg = as(filter.child(), Aggregate.class);
        var limit50Before = asLimit(agg.child(), 50, true);
        var join = as(limit50Before.child(), Join.class);
        limit = asLimit(join.left(), 50, false);
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[first_name{f}#11, emp_no{f}#10, salary{f}#12, b{r}#4]]
     *  \_TopN[[Order[salary{f}#12,ASC,LAST]],5[INTEGER]]
     *    \_Eval[[100[INTEGER] AS b]]
     *      \_MvExpand[first_name{f}#11]
     *        \_EsRelation[employees][emp_no{f}#10, first_name{f}#11, salary{f}#12]
     * }</pre>
     */
    public void testPushDownLimit_PastEvalAndMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort first_name
            | mv_expand first_name
            | eval b = 100
            | sort salary
            | limit 5
            | keep first_name, emp_no, salary, b""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var eval = as(topN.child(), Eval.class);
        var mvExp = as(eval.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#5885, first_name{r}#5896, salary{f}#5890]]
     * \_TopN[[Order[salary{f}#5890,ASC,LAST], Order[first_name{r}#5896,ASC,LAST]],1000[INTEGER]]
     *   \_Filter[gender{f}#5887 == [46][KEYWORD] AND WILDCARDLIKE(first_name{r}#5896)]
     *     \_MvExpand[first_name{f}#5886,first_name{r}#5896,null]
     *       \_EsRelation[test][_meta_field{f}#5891, emp_no{f}#5885, first_name{f}#..]
     * }</pre>
     */
    public void testRedundantSort_BeforeMvExpand_WithFilterOnExpandedField_ResultTruncationDefaultSize() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | where gender == "F"
            | where first_name LIKE "R*"
            | keep emp_no, first_name, salary
            | sort salary, first_name""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected
     *
     * <pre>{@code
     * Limit[10[INTEGER],true]
     * \_MvExpand[first_name{f}#7,first_name{r}#17]
     *   \_TopN[[Order[emp_no{f}#6,DESC,FIRST]],10[INTEGER]]
     *     \_Filter[emp_no{f}#6 &leq; 10006[INTEGER]]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testFilterWithSortBeforeMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no <= 10006
            | sort emp_no desc
            | mv_expand first_name
            | limit 10""");

        var limit = asLimit(plan, 10, true);
        var mvExp = as(limit.child(), MvExpand.class);
        var topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        assertThat(orderNames(topN), contains("emp_no"));
        var filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     *
     * <pre>{@code
     * Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *          languages{f}#11 AS language_code#6, last_name{f}#12, long_noidx{f}#18, salary{f}#13, language_name{f}#20]]
     * \_Limit[10[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#11],[languages{f}#11],[language_code{f}#19]]
     *     |_TopN[[Order[emp_no{f}#8,DESC,FIRST]],10[INTEGER]]
     *     | \_Filter[emp_no{f}#8 &leq; 10006[INTEGER]]
     *     |   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     * }</pre>
     */
    public void testFilterWithSortBeforeLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where emp_no <= 10006
            | sort emp_no desc
            | rename languages as language_code
            | lookup join languages_lookup on language_code
            | limit 10""");

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 10, true);
        var join = as(limit.child(), Join.class);
        var topN = as(join.left(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        assertThat(orderNames(topN), contains("emp_no"));
        var filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     *
     * <pre>{@code
     * TopN[[Order[first_name{f}#10,ASC,LAST]],500[INTEGER]]
     * \_MvExpand[last_name{f}#13,last_name{r}#20,null]
     *   \_Filter[emp_no{r}#19 > 10050[INTEGER]]
     *     \_MvExpand[emp_no{f}#9,emp_no{r}#19,null]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testMultiMvExpand_SortDownBelow() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort last_name ASC
            | mv_expand emp_no
            | where  emp_no > 10050
            | mv_expand last_name
            | sort first_name""");

        var topN = as(plan, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(orderNames(topN), contains("first_name"));
        var mvExpand = as(topN.child(), MvExpand.class);
        var filter = as(mvExpand.child(), Filter.class);
        mvExpand = as(filter.child(), MvExpand.class);
        as(mvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     *
     * <pre>{@code
     * Limit[10000[INTEGER],true]
     * \_MvExpand[c{r}#7,c{r}#16]
     *   \_EsqlProject[[c{r}#7, a{r}#3]]
     *     \_TopN[[Order[a{r}#3,ASC,FIRST]],7300[INTEGER]]
     *       \_Limit[7300[INTEGER],true]
     *         \_MvExpand[b{r}#5,b{r}#15]
     *           \_Limit[7300[INTEGER],false]
     *             \_LocalRelation[[a{r}#3, b{r}#5, c{r}#7],[ConstantNullBlock[positions=1],
     *               IntVectorBlock[vector=ConstantIntVector[positions=1, value=123]],
     *               IntVectorBlock[vector=ConstantIntVector[positions=1, value=234]]]]
     * }</pre>
     */
    public void testLimitThenSortBeforeMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            row  a = null, b = 123, c = 234
            | mv_expand b
            | limit 7300
            | keep c, a
            | sort a NULLS FIRST
            | mv_expand c""");

        var limit10kBefore = asLimit(plan, 10000, true);
        var mvExpand = as(limit10kBefore.child(), MvExpand.class);
        var project = as(mvExpand.child(), EsqlProject.class);
        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(7300));
        assertThat(orderNames(topN), contains("a"));
        var limit7300Before = asLimit(topN.child(), 7300, true);
        mvExpand = as(limit7300Before.child(), MvExpand.class);
        var limit = asLimit(mvExpand.child(), 7300, false);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * <pre>{@code
     * Project[[c{r}#7 AS language_code#14, a{r}#3, language_name{f}#19]]
     * \_Limit[10000[INTEGER],true]
     *   \_Join[LEFT,[c{r}#7],[c{r}#7],[language_code{f}#18]]
     *     |_TopN[[Order[a{r}#3,ASC,FIRST]],7300[INTEGER]]
     *     | \_Limit[7300[INTEGER],true]
     *     |   \_Join[LEFT,[language_code{r}#5],[language_code{r}#5],[language_code{f}#16]]
     *     |     |_Limit[7300[INTEGER],false]
     *     |     | \_LocalRelation[[a{r}#3, language_code{r}#5, c{r}#7],[ConstantNullBlock[positions=1],
     *                                                                   IntVectorBlock[vector=ConstantIntVector[positions=1, value=123]],
     *                                                                   IntVectorBlock[vector=ConstantIntVector[positions=1, value=234]]]]
     *     |     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#16]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     * }</pre>
     */
    public void testLimitThenSortBeforeLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            row  a = null, language_code = 123, c = 234
            | lookup join languages_lookup on language_code
            | limit 7300
            | keep c, a
            | sort a NULLS FIRST
            | rename c as language_code
            | lookup join languages_lookup on language_code
            """);

        var project = as(plan, Project.class);
        var limit10kBefore = asLimit(project.child(), 10000, true);
        var join = as(limit10kBefore.child(), Join.class);
        var topN = as(join.left(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(7300));
        assertThat(orderNames(topN), contains("a"));
        var limit7300Before = asLimit(topN.child(), 7300, true);
        join = as(limit7300Before.child(), Join.class);
        var limit = asLimit(join.left(), 7300, false);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * TopN[[Order[first_name{r}#16,ASC,LAST]],10000[INTEGER]]
     * \_MvExpand[first_name{f}#7,first_name{r}#16]
     *   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testRemoveUnusedSortBeforeMvExpand_DefaultLimit10000() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | sort first_name
            | limit 15000""");

        var topN = as(plan, TopN.class);
        assertThat(orderNames(topN), contains("first_name"));
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10000));
        var mvExpand = as(topN.child(), MvExpand.class);
        as(mvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#3517, first_name{r}#3528, salary{f}#3522]]
     * \_TopN[[Order[salary{f}#3522,ASC,LAST], Order[first_name{r}#3528,ASC,LAST]],15[INTEGER]]
     *   \_Filter[gender{f}#3519 == [46][KEYWORD] AND WILDCARDLIKE(first_name{r}#3528)]
     *     \_MvExpand[first_name{f}#3518,first_name{r}#3528,null]
     *       \_EsRelation[test][_meta_field{f}#3523, emp_no{f}#3517, first_name{f}#..]
     * }</pre>
     */
    public void testRedundantSort_BeforeMvExpand_WithFilterOnExpandedField() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | where gender == "F"
            | where first_name LIKE "R*"
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 15""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#3421, first_name{r}#3432, salary{f}#3426]]
     * \_TopN[[Order[salary{f}#3426,ASC,LAST], Order[first_name{r}#3432,ASC,LAST]],15[INTEGER]]
     *   \_Filter[gender{f}#3423 == [46][KEYWORD] AND salary{f}#3426 > 60000[INTEGER]]
     *     \_MvExpand[first_name{f}#3422,first_name{r}#3432,null]
     *       \_EsRelation[test][_meta_field{f}#3427, emp_no{f}#3421, first_name{f}#..]
     * }</pre>
     */
    public void testRedundantSort_BeforeMvExpand_WithFilter_NOT_OnExpandedField() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand first_name
            | where gender == "F"
            | where salary > 60000
            | keep emp_no, first_name, salary
            | sort salary, first_name
            | limit 15""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * EsqlProject[[emp_no{f}#2085, first_name{r}#2096 AS x, salary{f}#2090]]
     * \_TopN[[Order[salary{f}#2090,ASC,LAST], Order[first_name{r}#2096,ASC,LAST]],15[INTEGER]]
     *   \_Filter[gender{f}#2087 == [46][KEYWORD] AND WILDCARDLIKE(first_name{r}#2096)]
     *     \_MvExpand[first_name{f}#2086,first_name{r}#2096,null]
     *       \_EsRelation[test][_meta_field{f}#2091, emp_no{f}#2085, first_name{f}#..]
     * }</pre>
     */
    public void testRedundantSort_BeforeMvExpand_WithFilterOnExpandedFieldAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort gender
            | mv_expand first_name
            | rename first_name AS x
            | where gender == "F"
            | where x LIKE "A*"
            | keep emp_no, x, salary
            | sort salary, x
            | limit 15""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        as(mvExp.child(), EsRelation.class);
    }

    /**
     * Expected:
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_MvExpand[a{r}#3,a{r}#7]
     *   \_TopN[[Order[a{r}#3,ASC,LAST]],1000[INTEGER]]
     *     \_LocalRelation[[a{r}#3],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     * }</pre>
     */
    public void testSortMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            row a = 1
            | sort a
            | mv_expand a
            """);

        var limit = asLimit(plan, 1000, true);
        var expand = as(limit.child(), MvExpand.class);
        var topN = as(expand.child(), TopN.class);
        var row = as(topN.child(), LocalRelation.class);
    }

    /**
     * Expected:
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[language_code{r}#3],[language_code{r}#3],[language_code{f}#6]]
     *   |_TopN[[Order[language_code{r}#3,ASC,LAST]],1000[INTEGER]]
     *   | \_LocalRelation[[language_code{r}#3],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#6, language_name{f}#7]
     * }</pre>
     */
    public void testSortLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            row language_code = 1
            | sort language_code
            | lookup join languages_lookup on language_code
            """);

        var limit = asLimit(plan, 1000, true);
        var join = as(limit.child(), Join.class);
        var topN = as(join.left(), TopN.class);
        var row = as(topN.child(), LocalRelation.class);
    }

    /**
     * Expected:
     * <pre>{@code
     * Limit[20[INTEGER],true]
     * \_MvExpand[emp_no{f}#5,emp_no{r}#16]
     *   \_TopN[[Order[emp_no{f}#5,ASC,LAST]],20[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     */
    public void testSortMvExpandLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | mv_expand emp_no
            | limit 20""");

        var limit = asLimit(plan, 20, true);
        var expand = as(limit.child(), MvExpand.class);
        var topN = as(expand.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), is(20));
        var row = as(topN.child(), EsRelation.class);
    }

    /**
     * Expected:
     *
     * <pre>{@code
     * Project[[_meta_field{f}#13, emp_no{f}#7 AS language_code#5, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15,
     *          job.raw{f}#16, languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[20[INTEGER],true]
     *   \_Join[LEFT,[emp_no{f}#7],[emp_no{f}#7],[language_code{f}#18]]
     *     |_TopN[[Order[emp_no{f}#7,ASC,LAST]],20[INTEGER]]
     *     | \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     * }</pre>
     */
    public void testSortLookupJoinLimit() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename emp_no as language_code
            | lookup join languages_lookup on language_code
            | limit 20""");

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 20, true);
        var join = as(limit.child(), Join.class);
        var topN = as(join.left(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), is(20));
        var row = as(topN.child(), EsRelation.class);
    }

    /**
     * Expected:
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_MvExpand[b{r}#5,b{r}#9]
     *   \_Limit[1000[INTEGER],false]
     *     \_LocalRelation[[a{r}#3, b{r}#5],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]],
     *       IntVectorBlock[vector=ConstantIntVector[positions=1, value=-15]]]]
     * }</pre>
     *
     *  see <a href="https://github.com/elastic/elasticsearch/issues/102084">https://github.com/elastic/elasticsearch/issues/102084</a>
     */
    public void testWhereMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            row  a = 1, b = -15
            | where b < 3
            | mv_expand b
            """);

        var limit = asLimit(plan, 1000, true);
        var expand = as(limit.child(), MvExpand.class);
        var limit2 = asLimit(expand.child(), 1000, false);
        var row = as(limit2.child(), LocalRelation.class);
    }

    /**
     * Expected:
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[language_code{r}#5],[language_code{r}#5],[language_code{f}#8]]
     *   |_Limit[1000[INTEGER],false]
     *   | \_LocalRelation[[a{r}#3, language_code{r}#5],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]], IntVectorBlock[ve
     * ctor=ConstantIntVector[positions=1, value=-15]]]]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#8, language_name{f}#9]
     * }</pre>
     */
    public void testWhereLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            row  a = 1, language_code = -15
            | where language_code < 3
            | lookup join languages_lookup on language_code
            """);

        var limit = asLimit(plan, 1000, true);
        var join = as(limit.child(), Join.class);
        var limit2 = asLimit(join.left(), 1000, false);
        var row = as(limit2.child(), LocalRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * TopN[[Order[language_code{r}#7,ASC,LAST]],1[INTEGER]]
     * \_Limit[1[INTEGER],true]
     *   \_MvExpand[language_code{r}#3,language_code{r}#7]
     *     \_Limit[1[INTEGER],false]
     *       \_LocalRelation[[language_code{r}#3],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     * }</pre>
     *
     * Notice that the `TopN` at the very top has limit 1, not 3!
     */
    public void testDescendantLimitMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            ROW language_code = 1
            | MV_EXPAND language_code
            | LIMIT 1
            | SORT language_code
            | LIMIT 3
            """);

        var topn = as(plan, TopN.class);
        var limitAfter = asLimit(topn.child(), 1, true);
        var mvExpand = as(limitAfter.child(), MvExpand.class);
        var limitBefore = asLimit(mvExpand.child(), 1, false);
        var localRelation = as(limitBefore.child(), LocalRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * TopN[[Order[language_code{r}#3,ASC,LAST]],1[INTEGER]]
     * \_Limit[1[INTEGER],true]
     *   \_Join[LEFT,[language_code{r}#3],[language_code{r}#3],[language_code{f}#6]]
     *     |_Limit[1[INTEGER],false]
     *     | \_LocalRelation[[language_code{r}#3],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#6, language_name{f}#7]
     * }</pre>
     *
     * Notice that the `TopN` at the very top has limit 1, not 3!
     */
    public void testDescendantLimitLookupJoin() {
        LogicalPlan plan = optimizedPlan("""
            ROW language_code = 1
            | LOOKUP JOIN languages_lookup ON language_code
            | LIMIT 1
            | SORT language_code
            | LIMIT 3
            """);

        var topn = as(plan, TopN.class);
        var limitAfter = asLimit(topn.child(), 1, true);
        var join = as(limitAfter.child(), Join.class);
        var limitBefore = asLimit(join.left(), 1, false);
        var localRelation = as(limitBefore.child(), LocalRelation.class);
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#9, first_name{f}#10, languages{f}#12, language_code{r}#3, language_name{r}#22]]
     * \_Eval[[null[INTEGER] AS language_code#3, null[KEYWORD] AS language_name#22]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testPruneJoinOnNullMatchingField() {
        var plan = optimizedPlan("""
            from test
            | eval language_code = null::integer
            | keep emp_no, first_name, languages, language_code
            | lookup join languages_lookup on language_code
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.output()), contains("emp_no", "first_name", "languages", "language_code", "language_name"));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Project[[emp_no{f}#10, first_name{f}#11, languages{f}#13, language_code_left{r}#3, language_code{r}#21, language_name{
     * r}#22]]
     * \_Eval[[null[INTEGER] AS language_code_left#3, null[INTEGER] AS language_code#21, null[KEYWORD] AS language_name#22]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testPruneJoinOnNullMatchingFieldExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        var plan = optimizedPlan("""
            from test
            | eval language_code_left = null::integer
            | keep emp_no, first_name, languages, language_code_left
            | lookup join languages_lookup on language_code_left == language_code
            """);

        var project = as(plan, Project.class);
        assertThat(
            Expressions.names(project.output()),
            contains("emp_no", "first_name", "languages", "language_code_left", "language_code", "language_name")
        );
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Project[[emp_no{f}#15, first_name{f}#16, my_null{r}#3 AS language_code#9, language_name{r}#27]]
     * \_Eval[[null[INTEGER] AS my_null#3, null[KEYWORD] AS language_name#27]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testPruneJoinOnNullAssignedMatchingField() {
        var plan = optimizedPlan("""
            from test
            | eval my_null = null::integer
            | rename languages as language_code
            | eval language_code = my_null
            | lookup join languages_lookup on language_code
            | keep emp_no, first_name, language_code, language_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.output()), contains("emp_no", "first_name", "language_code", "language_name"));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Project[[emp_no{f}#16, first_name{f}#17, language_code{r}#27, language_name{r}#28]]
     * \_Eval[[null[INTEGER] AS language_code#27, null[KEYWORD] AS language_name#28]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    public void testPruneJoinOnNullAssignedMatchingFieldExpr() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        var plan = optimizedPlan("""
            from test
            | eval my_null = null::integer
            | rename languages as language_code_right
            | eval language_code_right = my_null
            | lookup join languages_lookup on language_code_right > language_code
            | keep emp_no, first_name, language_code, language_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.output()), contains("emp_no", "first_name", "language_code", "language_name"));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var source = as(limit.child(), EsRelation.class);
    }

    private static List<String> orderNames(TopN topN) {
        return topN.order().stream().map(o -> as(o.child(), NamedExpression.class).name()).toList();
    }

    /**
     * Expects
     * <pre>{@code
     * Eval[[2[INTEGER] AS x]]
     * \_Limit[1000[INTEGER],false]
     *   \_LocalRelation[[{e}#9],[ConstantNullBlock[positions=1]]]
     * }</pre>
     */
    public void testEvalAfterStats() {
        var plan = optimizedPlan("""
            ROW foo = 1
            | STATS x = max(foo)
            | EVAL x = 2
            """);
        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var localRelation = as(limit.child(), LocalRelation.class);
        assertThat(Expressions.names(eval.output()), contains("x"));
    }

    /**
     * Expects
     * <pre>{@code
     * Eval[[2[INTEGER] AS x]]
     * \_Limit[1000[INTEGER],false]
     *   \_Aggregate[[foo{r}#3],[foo{r}#3 AS x]]
     *     \_LocalRelation[[foo{r}#3],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     * }</pre>
     */
    public void testEvalAfterGroupBy() {
        var plan = optimizedPlan("""
            ROW foo = 1
            | STATS x = max(foo) by foo
            | KEEP x
            | EVAL x = 2
            """);
        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var localRelation = as(aggregate.child(), LocalRelation.class);
        assertThat(Expressions.names(eval.output()), contains("x"));
    }

    public void testCombineLimitWithOrderByThroughFilterAndEval() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary
            | eval x = emp_no / 2
            | where x > 20
            | sort x
            | limit 10""");

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    public void testCombineMultipleOrderByAndLimits() {
        // expected plan:
        // from test
        // | sort salary, emp_no
        // | limit 100
        // | where salary > 1
        // | sort emp_no, first_name
        // | keep l = salary, emp_no, first_name
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no
            | rename salary as l
            | keep l, emp_no, first_name
            | sort l
            | limit 100
            | sort first_name
            | where l > 1
            | sort emp_no""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(orderNames(topN), contains("emp_no"));
        var filter = as(topN.child(), Filter.class);
        var topN2 = as(filter.child(), TopN.class);
        assertThat(orderNames(topN2), contains("salary"));
        as(topN2.child(), EsRelation.class);
    }

    public void testDontPruneSameFieldDifferentDirectionSortClauses() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary nulls last, emp_no desc nulls first
            | where salary > 2
            | eval e = emp_no * 2
            | keep salary, emp_no, e
            | sort e, emp_no, salary desc, emp_no desc""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(
            topN.order(),
            containsIgnoringIds(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, null, "e", INTEGER, Nullability.TRUE, null, false),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "salary", mapping.get("salary")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                )
            )
        );
        assertThat(topN.child().collect(OrderBy.class::isInstance), is(emptyList()));
    }

    public void testPruneRedundantSortClauses() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort salary desc nulls last, emp_no desc nulls first
            | where salary > 2
            | eval e = emp_no * 2
            | keep salary, emp_no, e
            | sort e, emp_no desc, salary desc, emp_no desc nulls last""");

        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        assertThat(
            topN.order(),
            containsIgnoringIds(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, null, "e", INTEGER, Nullability.TRUE, null, false),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "salary", mapping.get("salary")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                ),
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.LAST
                )
            )
        );
        assertThat(topN.child().collect(OrderBy.class::isInstance), is(emptyList()));
    }

    public void testDontPruneSameFieldDifferentDirectionSortClauses_UsingAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no desc
            | rename emp_no as e
            | keep e
            | sort e""");

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(
            topN.order(),
            containsIgnoringIds(
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.ASC,
                    Order.NullsPosition.LAST
                )
            )
        );
    }

    public void testPruneRedundantSortClausesUsingAlias() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | sort emp_no desc
            | rename emp_no as e
            | keep e
            | sort e desc""");

        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        assertThat(
            topN.order(),
            containsIgnoringIds(
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                )
            )
        );
    }

    public void testInsist_fieldDoesNotExist_createsUnmappedFieldInRelation() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        LogicalPlan plan = optimizedPlan("FROM test | INSIST_\uD83D\uDC14 foo");

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        assertPartialTypeKeyword(relation, "foo");
    }

    public void testInsist_multiIndexFieldPartiallyExistsAndIsKeyword_castsAreNotSupported() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        var plan = planMultiIndex("FROM multi_index | INSIST_\uD83D\uDC14 partial_type_keyword");
        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);

        assertPartialTypeKeyword(relation, "partial_type_keyword");
    }

    public void testInsist_multipleInsistClauses_insistsAreFolded() {
        assumeTrue("Requires UNMAPPED FIELDS", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());

        var plan = planMultiIndex("FROM multi_index | INSIST_\uD83D\uDC14 partial_type_keyword | INSIST_\uD83D\uDC14 foo");
        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);

        assertPartialTypeKeyword(relation, "partial_type_keyword");
        assertPartialTypeKeyword(relation, "foo");
    }

    private static void assertPartialTypeKeyword(EsRelation relation, String name) {
        var attribute = (FieldAttribute) singleValue(relation.output().stream().filter(attr -> attr.name().equals(name)).toList());
        assertThat(attribute.field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    public void testSimplifyLikeNoWildcard() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name like "foo"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Equals);
        Equals equals = as(filter.condition(), Equals.class);
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold(FoldContext.small()));
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyLikeMatchAll() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name like "*"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        as(filter.condition(), IsNotNull.class);
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyRLikeNoWildcard() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name rlike "foo"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        assertTrue(filter.condition() instanceof Equals);
        Equals equals = as(filter.condition(), Equals.class);
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold(FoldContext.small()));
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testSimplifyRLikeMatchAll() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name rlike ".*"
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);

        var isNotNull = as(filter.condition(), IsNotNull.class);
        assertTrue(filter.child() instanceof EsRelation);
    }

    public void testRLikeWrongPattern() {
        String query = "from test | where first_name rlike \"(?i)(^|[^a-zA-Z0-9_-])nmap($|\\\\.)\"";
        String error = "line 1:19: Invalid regex pattern for RLIKE [(?i)(^|[^a-zA-Z0-9_-])nmap($|\\.)]: "
            + "[invalid range: from (95) cannot be > to (93)]";
        ParsingException e = expectThrows(ParsingException.class, () -> plan(query));
        assertThat(e.getMessage(), is(error));
    }

    public void testLikeWrongPattern() {
        String query = "from test | where first_name like \"(?i)(^|[^a-zA-Z0-9_-])nmap($|\\\\.)\"";
        String error = "line 1:19: Invalid pattern for LIKE [(?i)(^|[^a-zA-Z0-9_-])nmap($|\\.)]: "
            + "[Invalid sequence - escape character is not followed by special wildcard char]";
        ParsingException e = expectThrows(ParsingException.class, () -> plan(query));
        assertThat(e.getMessage(), is(error));
    }

    public void testFoldNullInToLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where null in (first_name, ".*")
            """);
        assertThat(plan, instanceOf(LocalRelation.class));
    }

    public void testFoldNullListInToLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where first_name in (null, null)
            """);
        assertThat(plan, instanceOf(LocalRelation.class));
    }

    public void testFoldInKeyword() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where "foo" in ("bar", "baz")
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where "foo" in ("bar", "foo", "baz")
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInIP() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where to_ip("1.1.1.1") in (to_ip("1.1.1.2"), to_ip("1.1.1.2"))
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where to_ip("1.1.1.1") in (to_ip("1.1.1.1"), to_ip("1.1.1.2"))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInVersion() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where to_version("1.2.3") in (to_version("1"), to_version("1.2.4"))
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where to_version("1.2.3") in (to_version("1"), to_version("1.2.3"))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInNumerics() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | where 3 in (4.0, 5, 2147483648)
            """);
        assertThat(plan, instanceOf(LocalRelation.class));

        plan = optimizedPlan("""
            from test
            | where 3 in (4.0, 3.0, to_long(3))
            """);
        var limit = as(plan, Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testFoldInEval() {
        var plan = optimizedPlan("""
            from test
            | eval a = 1, b = a + 1, c = b + a
            | where c > 10
            """);

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), is(EmptyLocalSupplier.EMPTY));
    }

    public void testFoldFromRow() {
        var plan = optimizedPlan("""
              row a = 1, b = 2, c = 3
            | where c > 10
            """);

        as(plan, LocalRelation.class);
    }

    public void testFoldFromRowInEval() {
        var plan = optimizedPlan("""
              row a = 1, b = 2, c = 3
            | eval x = c
            | where x > 10
            """);

        as(plan, LocalRelation.class);
    }

    public void testInvalidFoldDueToReplacement() {
        var plan = optimizedPlan("""
              from test
            | eval x = 1
            | eval x = emp_no
            | where x > 10
            | keep x
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var child = aliased(project.projections().get(0), FieldAttribute.class);
        assertThat(Expressions.name(child), is("emp_no"));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var source = as(filter.child(), EsRelation.class);
    }

    public void testEnrich() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = to_string(languages)
            | enrich languages_idx on x
            """);
        var enrich = as(plan, Enrich.class);
        assertTrue(enrich.policyName().resolved());
        assertThat(enrich.policyName().fold(FoldContext.small()), is(BytesRefs.toBytesRef("languages_idx")));
        var eval = as(enrich.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    public void testPushDownEnrichPastProject() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval a = to_string(languages)
            | rename a as x
            | keep x
            | enrich languages_idx on x
            """);

        var keep = as(plan, Project.class);
        as(keep.child(), Enrich.class);
    }

    public void testTopNEnrich() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename languages as x
            | eval x = to_string(x)
            | keep x
            | enrich languages_idx on x
            | sort language_name
            """);

        var keep = as(plan, Project.class);
        var topN = as(keep.child(), TopN.class);
        as(topN.child(), Enrich.class);
    }

    public void testEnrichNotNullFilter() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | eval x = to_string(languages)
            | enrich languages_idx on x
            | where language_name is not null
            | limit 10
            """);
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var enrich = as(filter.child(), Enrich.class);
        assertTrue(enrich.policyName().resolved());
        assertThat(enrich.policyName().fold(FoldContext.small()), is(BytesRefs.toBytesRef("languages_idx")));
        var eval = as(enrich.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[a{r}#3, last_name{f}#9]]
     * \_Eval[[__a_SUM_123{r}#12 / __a_COUNT_150{r}#13 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#9],[SUM(salary{f}#10) AS __a_SUM_123, COUNT(salary{f}#10) AS __a_COUNT_150, last_nam
     * e{f}#9]]
     *       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     * }</pre>
     */
    public void testSimpleAvgReplacement() {
        var plan = plan("""
              from test
            | stats a = avg(salary) by last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "last_name"));
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        assertThat(a.name(), startsWith("$$SUM$a$"));
        var sum = as(a.child(), Sum.class);

        a = as(aggs.get(1), Alias.class);
        assertThat(a.name(), startsWith("$$COUNT$a$"));
        var count = as(a.child(), Count.class);

        assertThat(Expressions.names(agg.groupings()), contains("last_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / c{r}#6 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS c, SUM(salary{f}#16) AS s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    public void testClashingAggAvgReplacement() {
        var plan = plan("""
            from test
            | stats a = avg(salary), c = count(salary), s = sum(salary) by last_name
            """);

        assertThat(Expressions.names(plan.output()), contains("a", "c", "s", "last_name"));
        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c", "s", "last_name"));
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / __a_COUNT@xxx{r}#18 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS __a_COUNT@xxx, COUNT(languages{f}#14) AS c, SUM(salary{f}#16) AS
     *  s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    public void testSemiClashingAvgReplacement() {
        var plan = plan("""
            from test
            | stats a = avg(salary), c = count(languages), s = sum(salary) by last_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "c", "s", "last_name"));
        var eval = as(project.child(), Eval.class);
        var f = eval.fields();
        assertThat(f, hasSize(1));
        assertThat(f.get(0).name(), is("a"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        assertThat(a.name(), startsWith("$$COUNT$a$0"));
        var sum = as(a.child(), Count.class);

        a = as(aggs.get(1), Alias.class);
        assertThat(a.name(), is("c"));
        var count = as(a.child(), Count.class);

        a = as(aggs.get(2), Alias.class);
        assertThat(a.name(), is("s"));
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[10000[INTEGER]]
     * \_Aggregate[[last_name{f}#9],[PERCENTILE(salary{f}#10,50[INTEGER]) AS m, last_name{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     * }</pre>
     */
    public void testMedianReplacement() {
        var plan = plan("""
              from test
            | stats m = median(salary) by last_name
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("m", "last_name"));
        var aggs = agg.aggregates();
        var a = as(aggs.get(0), Alias.class);
        var per = as(a.child(), Percentile.class);
        var literal = as(per.percentile(), Literal.class);
        assertThat((int) QuantileStates.MEDIAN, is(literal.value()));

        assertThat(Expressions.names(agg.groupings()), contains("last_name"));
    }

    public void testSplittingInWithFoldableValue() {
        FieldAttribute fa = getFieldAttribute("foo");
        In in = new In(EMPTY, ONE, List.of(TWO, THREE, fa, L(null)));
        Or expected = new Or(EMPTY, new In(EMPTY, ONE, List.of(TWO, THREE)), new In(EMPTY, ONE, List.of(fa, L(null))));
        assertThat(new SplitInWithFoldableValue().rule(in, logicalOptimizerCtx), equalTo(expected));
    }

    public void testReplaceFilterWithExact() {
        var plan = plan("""
              from test
            | where job == "foo"
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        FieldAttribute left = as(equals.left(), FieldAttribute.class);
        assertThat(left.name(), equalTo("job"));
    }

    public void testReplaceExpressionWithExact() {
        var plan = plan("""
              from test
            | eval x = job
            """);

        var eval = as(plan, Eval.class);
        var alias = as(eval.fields().get(0), Alias.class);
        var field = as(alias.child(), FieldAttribute.class);
        assertThat(field.name(), equalTo("job"));
    }

    public void testReplaceSortWithExact() {
        var plan = plan("""
              from test
            | sort job
            """);

        var topN = as(plan, TopN.class);
        assertThat(topN.order().size(), equalTo(1));
        var sortField = as(topN.order().get(0).child(), FieldAttribute.class);
        assertThat(sortField.name(), equalTo("job"));
    }

    public void testPruneUnusedEval() {
        var plan = plan("""
              from test
            | eval garbage = salary + 3
            | keep salary
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testPruneChainedEval() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3
            | eval garbage_b = emp_no / garbage_a, garbage_c = garbage_a
            | eval garbage_x = 1 - garbage_b/garbage_c
            | keep salary
            """);
        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testPruneChainedEvalNoProjection() {
        var plan = plan("""
              from test
            | eval garbage = salary + 3
            | eval garbage = emp_no / garbage, garbage = garbage
            | eval garbage = 1
            """);
        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);

        assertEquals(1, eval.fields().size());
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertEquals(alias.name(), "garbage");
        var literal = as(alias.child(), Literal.class);
        assertEquals(1, literal.value());
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#1345) AS c]]
     *   \_EsRelation[test][_meta_field{f}#1346, emp_no{f}#1340, first_name{f}#..]
     * }</pre>
     */
    public void testPruneEvalDueToStats() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3, x = salary
            | eval garbage_b = x + 3
            | stats c = count(x)
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var aggs = aggregate.aggregates();
        assertThat(Expressions.names(aggs), contains("c"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        var source = as(aggregate.child(), EsRelation.class);
    }

    public void testPruneUnusedAggSimple() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | keep c
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        var aggOne = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggOne.name(), is("c"));
        var count = as(aggOne.child(), Count.class);
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#19) AS x]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }</pre>
     */
    public void testPruneUnusedAggMixedWithEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = c
            | keep x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("x"));
        aggFieldName(agg.aggregates().get(0), Count.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    public void testPruneUnusedAggsChainedAgg() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("c"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[c{r}#342]]
     * \_Limit[1000[INTEGER]]
     *   \_Filter[min{r}#348 > 10[INTEGER]]
     *     \_Aggregate[[],[COUNT(salary{f}#367) AS c, MIN(salary{f}#367) AS min]]
     *       \_EsRelation[test][_meta_field{f}#368, emp_no{f}#362, first_name{f}#36..]
     * }</pre>
     */
    public void testPruneMixedAggInsideUnusedEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | where y > 10
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var agg = as(filter.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c", "min"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Eval[[max{r}#6 + min{r}#9 + c{r}#3 AS x, min{r}#9 AS y, c{r}#3 AS z]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[COUNT(salary{f}#26) AS c, MAX(salary{f}#26) AS max, MIN(salary{f}#26) AS min]]
     *     \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     * }</pre>
     */
    public void testNoPruningWhenDealingJustWithEvals() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            """);

        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[y{r}#6 AS z]]
     * \_Eval[[emp_no{f}#11 + 1[INTEGER] AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    public void testNoPruningWhenChainedEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, y = x + 1, z = y
            | keep z
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("z"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("y"));
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[salary{f}#20 AS x, emp_no{f}#15 AS y]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testPruningDuplicateEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | keep x, y
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("x", "y"));
        var child = aliased(projections.get(0), FieldAttribute.class);
        assertThat(child.name(), is("salary"));
        child = aliased(projections.get(1), FieldAttribute.class);
        assertThat(child.name(), is("emp_no"));

        var limit = as(project.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#24) AS cx, COUNT(emp_no{f}#19) AS cy]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }</pre>
     */
    public void testPruneEvalAliasOnAggUngrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cx = count(x), cy = count(y)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cx", "cy"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Count.class, "emp_no");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[x{r}#6],[COUNT(emp_no{f}#17) AS cy, salary{f}#22 AS x]]
     *   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testPruneEvalAliasOnAggGroupedByAlias() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cy = count(y) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "x"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        var x = aliased(aggs.get(1), FieldAttribute.class);
        assertThat(x.name(), is("salary"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(emp_no{f}#20) AS cy, MIN(salary{f}#25) AS cx, gender{f}#22]]
     *   \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     * }</pre>
     */
    public void testPruneEvalAliasOnAggGrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#21],[COUNT(emp_no{f}#19) AS cy, MIN(salary{f}#24) AS cx, gender{f}#21]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }</pre>
     */
    public void testPruneEvalAliasMixedWithRenameOnAggGrouped() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | rename salary as x
            | eval y = emp_no
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "emp_no");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testEvalAliasingAcrossCommands() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1
            | eval y = x
            | eval z = y + 1
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "x");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testEvalAliasingInsideSameCommand() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1, y = x, z = y + 1
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "x");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(z{r}#9) AS cy, MIN(x{r}#3) AS cx, gender{f}#22]]
     *   \_Eval[[emp_no{f}#20 + 1[INTEGER] AS x, x{r}#3 + 1[INTEGER] AS z]]
     *     \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     * }</pre>
     */
    public void testEvalAliasingInsideSameCommandWithShadowing() {
        var plan = plan("""
              from test
            | eval x = emp_no + 1, y = x, z = y + 1, y = z
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "z");
        aggFieldName(aggs.get(1), Min.class, "x");
        var by = as(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(by), is("gender"));
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x", "z"));
        var source = as(eval.child(), EsRelation.class);
    }

    public void testPruneRenameOnAgg() {
        var plan = plan("""
              from test
            | rename emp_no as x
            | rename salary as y
            | stats cy = count(y), cx = min(x) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "gender"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "emp_no");

        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#14],[COUNT(salary{f}#17) AS cy, MIN(emp_no{f}#12) AS cx, gender{f}#14]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     * }</pre>
     */
    public void testPruneRenameOnAggBy() {
        var plan = plan("""
              from test
            | rename emp_no as x
            | rename salary as y, gender as g
            | stats cy = count(y), cx = min(x) by g
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("cy", "cx", "g"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "emp_no");
        var groupby = aliased(aggs.get(2), FieldAttribute.class);
        assertThat(Expressions.name(groupby), is("gender"));

        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[2[INTEGER]]
     * \_Filter[a{r}#6 > 2[INTEGER]]
     *   \_MvExpand[a{r}#2,a{r}#6]
     *     \_Row[[[1, 2, 3][INTEGER] AS a]]
     * }</pre>
     */
    public void testMvExpandFoldable() {
        LogicalPlan plan = optimizedPlan("""
            row a = [1, 2, 3]
            | mv_expand a
            | where a > 2
            | limit 2""");

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var expand = as(filter.child(), MvExpand.class);
        assertThat(filter.condition(), instanceOf(GreaterThan.class));
        var filterProp = ((GreaterThan) filter.condition()).left();
        assertTrue(expand.expanded().semanticEquals(filterProp));
        assertFalse(expand.target().semanticEquals(filterProp));
        var row = as(expand.child(), LocalRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#2],[COUNT([2a][KEYWORD]) AS bar]]
     *   \_Row[[1[INTEGER] AS a]]
     * }</pre>
     */
    public void testRenameStatsDropGroup() {
        LogicalPlan plan = optimizedPlan("""
            row a = 1
            | rename a AS foo
            | stats bar = count(*) by foo
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("a"));
        var row = as(agg.child(), LocalRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#3, b{r}#5],[COUNT([2a][KEYWORD]) AS baz, b{r}#5 AS bar]]
     *   \_Row[[1[INTEGER] AS a, 2[INTEGER] AS b]]
     * }</pre>
     */
    public void testMultipleRenameStatsDropGroup() {
        LogicalPlan plan = optimizedPlan("""
            row a = 1, b = 2
            | rename a AS foo, b as bar
            | stats baz = count(*) by foo, bar
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("a", "b"));
        var row = as(agg.child(), LocalRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#14, gender{f}#16],[MAX(salary{f}#19) AS baz, gender{f}#16 AS bar]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }</pre>
     */
    public void testMultipleRenameStatsDropGroupMultirow() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename emp_no AS foo, gender as bar
            | stats baz = max(salary) by foo, bar
            | drop foo""");

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("emp_no", "gender"));
        var row = as(agg.child(), EsRelation.class);
    }

    public void testLimitZeroUsesLocalRelation() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | stats count=count(*)
            | sort count desc
            | limit 0""");

        assertThat(plan, instanceOf(LocalRelation.class));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#4) AS sum(emp_no)]]
     *   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithoutGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(emp_no)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(empty()));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(emp_no)"));
        var from = as(agg.child(), EsRelation.class);
    }

    public void testIsNotNullConstraintForStatsWithGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(emp_no) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(emp_no)", "salary"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#1185],[SUM(salary{f}#1185) AS sum(salary), salary{f}#1185]]
     *   \_EsRelation[test][_meta_field{f}#1186, emp_no{f}#1180, first_name{f}#..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithAndOnGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats sum(salary) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(salary)", "salary"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#13],[SUM(salary{f}#13) AS sum(salary), salary{f}#13 AS x]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithAndOnGroupingAlias() {
        var plan = optimizedPlan("""
            from test
            | eval x = salary
            | stats sum(salary) by x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(salary)", "x"));
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#13],[SUM(emp_no{f}#8) AS sum(x), salary{f}#13]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testIsNotNullConstraintSkippedForStatsWithAlias() {
        var plan = optimizedPlan("""
            from test
            | eval x = emp_no
            | stats sum(x) by salary
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("sum(x)", "salary"));

        // non null filter for stats
        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#8) AS a, MIN(salary{f}#13) AS b]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithoutGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#11],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, gender{f}#11]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary) by gender
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b", "gender"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#9],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, emp_no{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testIsNotNullConstraintForStatsWithMultiAggWithAndOnGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(emp_no), b = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("a", "b", "emp_no"));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[w{r}#14, g{r}#16],[COUNT(b{r}#24) AS c, w{r}#14, gender{f}#32 AS g]]
     *   \_Eval[[emp_no{f}#30 / 10[INTEGER] AS x, x{r}#4 + salary{f}#35 AS y, y{r}#8 / 4[INTEGER] AS z, z{r}#11 * 2[INTEGER] +
     *  3[INTEGER] AS w, salary{f}#35 + 4[INTEGER] / 2[INTEGER] AS a, a{r}#21 + 3[INTEGER] AS b]]
     *     \_EsRelation[test][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
     * }</pre>
     */
    public void testIsNotNullConstraintForAliasedExpressions() {
        var plan = optimizedPlan("""
            from test
            | eval x = emp_no / 10
            | eval y = x + salary
            | eval z = y / 4
            | eval w = z * 2 + 3
            | rename gender as g, salary as s
            | eval a = (s + 4) / 2
            | eval b = a + 3
            | stats c = count(b) by w, g
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("c", "w", "g"));
        var eval = as(agg.child(), Eval.class);
        var from = as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     *   \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]
     * }</pre>
     */
    public void testSpatialTypesAndStatsUseDocValues() {
        var plan = planAirports("""
            from airports
            | stats centroid = st_centroid_agg(location)
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("centroid"));
        assertTrue("Expected GEO_POINT aggregation for STATS", agg.aggregates().stream().allMatch(aggExp -> {
            var alias = as(aggExp, Alias.class);
            var aggFunc = as(alias.child(), AggregateFunction.class);
            var aggField = as(aggFunc.field(), FieldAttribute.class);
            return aggField.dataType() == GEO_POINT;
        }));

        var from = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     *   \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]
     * }</pre>
     */
    public void testSpatialTypesAndStatsUseDocValuesWithEval() {
        var plan = planAirports("""
            from airports
            | stats centroid = st_centroid_agg(to_geopoint(location))
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("centroid"));
        assertTrue("Expected GEO_POINT aggregation for STATS", agg.aggregates().stream().allMatch(aggExp -> {
            var alias = as(aggExp, Alias.class);
            var aggFunc = as(alias.child(), AggregateFunction.class);
            var aggField = as(aggFunc.field(), FieldAttribute.class);
            return aggField.dataType() == GEO_POINT;
        }));

        as(agg.child(), EsRelation.class);
    }

    /**
     * Expects:
     * <pre>{@code
     * Eval[[types.type{f}#5 AS new_types.type]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     * NOTE: The convert function to_type is removed, since the types match
     * This does not work for to_string(text) since that converts text to keyword
     */
    public void testTrivialTypeConversionWrittenAway() {
        for (String type : new String[] { "keyword", "float", "double", "long", "integer", "boolean", "geo_point" }) {
            var func = switch (type) {
                case "keyword", "text" -> "to_string";
                case "double", "float" -> "to_double";
                case "geo_point" -> "to_geopoint";
                default -> "to_" + type;
            };
            var field = "types." + type;
            var plan = planExtra("from extra | eval new_" + field + " = " + func + "(" + field + ")");
            var eval = as(plan, Eval.class);
            var alias = as(eval.fields().get(0), Alias.class);
            assertThat(func + "(" + field + ")", alias.name(), equalTo("new_" + field));
            var fa = as(alias.child(), FieldAttribute.class);
            assertThat(func + "(" + field + ")", fa.name(), equalTo(field));
            var limit = as(eval.child(), Limit.class);
            as(limit.child(), EsRelation.class);
        }
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#6],[COUNT(salary{f}#12) AS c, emp_no%2{r}#6]]
     *   \_Eval[[emp_no{f}#7 % 2[INTEGER] AS emp_no%2]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testNestedExpressionsInGroups() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary) by emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        var aggs = agg.aggregates();
        var ref = as(groupings.get(0), ReferenceAttribute.class);
        assertThat(aggs.get(1), is(ref));
        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        assertThat(eval.fields().get(0).toAttribute(), is(ref));
        assertThat(eval.fields().get(0).name(), is("emp_no % 2"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[CATEGORIZE($$CONCAT(first_na>$CATEGORIZE(CONC>$0{r$}#1590) AS CATEGORIZE(CONCAT(first_name, "abc"))],[COUNT(sa
     * lary{f}#1584,true[BOOLEAN]) AS c, CATEGORIZE(CONCAT(first_name, "abc")){r}#1574]]
     *   \_Eval[[CONCAT(first_name{f}#1580,[61 62 63][KEYWORD]) AS $$CONCAT(first_na>$CATEGORIZE(CONC>$0]]
     *     \_EsRelation[test][_meta_field{f}#1585, emp_no{f}#1579, first_name{f}#..]
     * }</pre>
     */
    public void testNestedExpressionsInGroupsWithCategorize() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary) by CATEGORIZE(CONCAT(first_name, "abc"))
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        var categorizeAlias = as(groupings.get(0), Alias.class);
        var categorize = as(categorizeAlias.child(), Categorize.class);
        var aggs = agg.aggregates();
        assertThat(aggs.get(1), is(categorizeAlias.toAttribute()));

        var eval = as(agg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var evalFieldAlias = as(eval.fields().get(0), Alias.class);
        var evalField = as(evalFieldAlias.child(), Concat.class);

        assertThat(evalFieldAlias.name(), is("$$CONCAT(first_na>$CATEGORIZE(CONC>$0"));
        assertThat(categorize.field(), is(evalFieldAlias.toAttribute()));
        assertThat(evalField.source().text(), is("CONCAT(first_name, \"abc\")"));
        assertThat(categorizeAlias.source(), is(categorize.source()));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#6],[COUNT(__c_COUNT@1bd45f36{r}#16) AS c, emp_no{f}#6]]
     *   \_Eval[[salary{f}#11 + 1[INTEGER] AS __c_COUNT@1bd45f36]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testNestedExpressionsInAggs() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary + 1) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var count = aliased(aggs.get(0), Count.class);
        var ref = as(count.field(), ReferenceAttribute.class);
        var eval = as(agg.child(), Eval.class);
        var fields = eval.fields();
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).toAttribute(), is(ref));
        var add = aliased(fields.get(0), Add.class);
        assertThat(Expressions.name(add.left()), is("salary"));
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#7],[COUNT(__c_COUNT@fb7855b0{r}#18) AS c, emp_no%2{r}#7]]
     *   \_Eval[[emp_no{f}#8 % 2[INTEGER] AS emp_no%2, 100[INTEGER] / languages{f}#11 + salary{f}#13 + 1[INTEGER] AS __c_COUNT
     * @fb7855b0]]
     *     \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testNestedExpressionsInBothAggsAndGroups() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(salary + 1 + 100 / languages) by emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        var aggs = agg.aggregates();
        var gRef = as(groupings.get(0), ReferenceAttribute.class);
        assertThat(aggs.get(1), is(gRef));

        var count = aliased(aggs.get(0), Count.class);
        var aggRef = as(count.field(), ReferenceAttribute.class);
        var eval = as(agg.child(), Eval.class);
        var fields = eval.fields();
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).toAttribute(), is(gRef));
        assertThat(fields.get(1).toAttribute(), is(aggRef));

        var mod = aliased(fields.get(0), Mod.class);
        assertThat(Expressions.name(mod.left()), is("emp_no"));
        var refs = Expressions.references(singletonList(fields.get(1)));
        assertThat(Expressions.names(refs), containsInAnyOrder("languages", "salary"));
    }

    public void testNestedMultiExpressionsInGroupingAndAggs() {
        var plan = optimizedPlan("""
            from test
            | stats count(salary + 1), max(salary   +  23) by languages   + 1, emp_no %  3
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.output()), contains("count(salary + 1)", "max(salary   +  23)", "languages   + 1", "emp_no %  3"));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[g{r}#8],[COUNT($$emp_no_%_2_+_la>$COUNT$0{r}#20) AS c, g{r}#8]]
     *   \_Eval[[emp_no{f}#10 % 2[INTEGER] AS g, languages{f}#13 + emp_no{f}#10 % 2[INTEGER] AS $$emp_no_%_2_+_la>$COUNT$0]]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testNestedExpressionsWithGroupingKeyInAggs() {
        var plan = optimizedPlan("""
            from test
            | stats c = count(languages + emp_no % 2) by g = emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "g"));
        assertThat(Expressions.names(aggregate.groupings()), contains("g"));
        var eval = as(aggregate.child(), Eval.class);
        var fields = eval.fields();
        // emp_no % 2
        var value = Alias.unwrap(fields.get(0));
        var math = as(value, Mod.class);
        assertThat(Expressions.name(math.left()), is("emp_no"));
        assertThat(math.right().fold(FoldContext.small()), is(2));
        // languages + emp_no % 2
        var add = as(Alias.unwrap(fields.get(1).canonical()), Add.class);
        if (add.left() instanceof Mod mod) {
            add = add.swapLeftAndRight();
        }
        assertThat(Expressions.name(add.left()), is("languages"));
        var mod = as(add.right().canonical(), Mod.class);
        assertThat(Expressions.name(mod.left()), is("emp_no"));
        assertThat(mod.right().fold(FoldContext.small()), is(2));
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no % 2{r}#12, languages + salary{r}#15],[MAX(languages + salary{r}#15) AS m, COUNT($$languages_+_sal>$COUN
     * T$0{r}#28) AS c, emp_no % 2{r}#12, languages + salary{r}#15]]
     *   \_Eval[[emp_no{f}#18 % 2[INTEGER] AS emp_no % 2, languages{f}#21 + salary{f}#23 AS languages + salary, languages{f}#2
     * 1 + salary{f}#23 + emp_no{f}#18 % 2[INTEGER] AS $$languages_+_sal>$COUNT$0]]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "disabled since canonical representation relies on hashing which is runtime defined")
    public void testNestedExpressionsWithMultiGrouping() {
        var plan = optimizedPlan("""
            from test
            | stats m = max(languages + salary), c = count(languages + salary + emp_no % 2) by emp_no % 2, languages + salary
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("m", "c", "emp_no % 2", "languages + salary"));
        assertThat(Expressions.names(aggregate.groupings()), contains("emp_no % 2", "languages + salary"));
        var eval = as(aggregate.child(), Eval.class);
        var fields = eval.fields();
        // emp_no % 2
        var value = Alias.unwrap(fields.get(0).canonical());
        var math = as(value, Mod.class);
        assertThat(Expressions.name(math.left()), is("emp_no"));
        assertThat(math.right().fold(FoldContext.small()), is(2));
        // languages + salary
        var add = as(Alias.unwrap(fields.get(1).canonical()), Add.class);
        assertThat(Expressions.name(add.left()), anyOf(is("languages"), is("salary")));
        assertThat(Expressions.name(add.right()), anyOf(is("salary"), is("languages")));
        // languages + salary + emp_no % 2
        var add2 = as(Alias.unwrap(fields.get(2).canonical()), Add.class);
        if (add2.left() instanceof Mod mod) {
            add2 = add2.swapLeftAndRight();
        }
        var add3 = as(add2.left(), Add.class);
        var mod = as(add2.right(), Mod.class);
        // languages + salary
        assertThat(Expressions.name(add3.left()), anyOf(is("languages"), is("salary")));
        assertThat(Expressions.name(add3.right()), anyOf(is("salary"), is("languages")));
        // emp_no % 2
        assertThat(Expressions.name(mod.left()), is("emp_no"));
        assertThat(mod.right().fold(FoldContext.small()), is(2));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[e{r}#5, languages + emp_no{r}#8]]
     * \_Eval[[$$MAX$max(languages_+>$0{r}#20 + 1[INTEGER] AS e]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[languages + emp_no{r}#8],[MAX(emp_no{f}#10 + languages{f}#13) AS $$MAX$max(languages_+>$0, languages + emp_no{
     * r}#8]]
     *       \_Eval[[languages{f}#13 + emp_no{f}#10 AS languages + emp_no]]
     *         \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     */
    public void testNestedExpressionsInStatsWithExpression() {
        var plan = optimizedPlan("""
            from test
            | stats e = max(languages + emp_no) + 1 by languages + emp_no
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("e"));
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var groupings = agg.groupings();
        assertThat(Expressions.names(groupings), contains("languages + emp_no"));
        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.names(fields), contains("languages + emp_no"));
    }

    public void testBucketAcceptsEvalLiteralReferences() {
        var plan = plan("""
            from test
            | eval bucket_start = 1, bucket_end = 100000
            | stats by bucket(salary, 10, bucket_start, bucket_end)
            """);
        var ab = as(plan, Limit.class);
        assertTrue(ab.optimized());
    }

    public void testBucketFailsOnFieldArgument() {
        VerificationException e = expectThrows(VerificationException.class, () -> plan("""
            from test
            | eval bucket_end = 100000
            | stats by bucket(salary, 10, emp_no, bucket_end)
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        assertEquals(
            "3:31: third argument of [bucket(salary, 10, emp_no, bucket_end)] must be a constant, received [emp_no]",
            e.getMessage().substring(header.length())
        );
    }

    /**
     * <pre>{@code
     * Project[[bucket(salary, 1000.) + 1{r}#3, bucket(salary, 1000.){r}#5]]
     *  \_Eval[[bucket(salary, 1000.){r}#5 + 1[INTEGER] AS bucket(salary, 1000.) + 1]]
     *    \_Limit[1000[INTEGER]]
     *      \_Aggregate[[bucket(salary, 1000.){r}#5],[bucket(salary, 1000.){r}#5]]
     *        \_Eval[[BUCKET(salary{f}#12,1000.0[DOUBLE]) AS bucket(salary, 1000.)]]
     *          \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testBucketWithAggExpression() {
        var plan = plan("""
            from test
            | stats bucket(salary, 1000.) + 1 by bucket(salary, 1000.)
            """);
        var project = as(plan, Project.class);
        var evalTop = as(project.child(), Eval.class);
        var limit = as(evalTop.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var evalBottom = as(agg.child(), Eval.class);
        var relation = as(evalBottom.child(), EsRelation.class);

        assertThat(evalTop.fields().size(), is(1));
        assertThat(evalTop.fields().get(0), instanceOf(Alias.class));
        assertThat(evalTop.fields().get(0).child(), instanceOf(Add.class));
        var add = (Add) evalTop.fields().get(0).child();
        assertThat(add.left(), instanceOf(ReferenceAttribute.class));
        var ref = (ReferenceAttribute) add.left();

        assertThat(evalBottom.fields().size(), is(1));
        assertThat(evalBottom.fields().get(0), instanceOf(Alias.class));
        var alias = evalBottom.fields().get(0);
        assertEquals(ref, alias.toAttribute());

        assertThat(agg.aggregates().size(), is(1));
        assertThat(agg.aggregates().get(0), is(ref));
        assertThat(agg.groupings().size(), is(1));
        assertThat(agg.groupings().get(0), is(ref));
    }

    public void testBucketWithNonFoldingArgs() {
        assertThat(
            typesError("from types | stats max(integer) by bucket(date, integer, \"2000-01-01\", \"2000-01-02\")"),
            containsString(
                "second argument of [bucket(date, integer, \"2000-01-01\", \"2000-01-02\")] must be a constant, " + "received [integer]"
            )
        );

        assertThat(
            typesError("from types | stats max(integer) by bucket(date, 2, date, \"2000-01-02\")"),
            containsString("third argument of [bucket(date, 2, date, \"2000-01-02\")] must be a constant, " + "received [date]")
        );

        assertThat(
            typesError("from types | stats max(integer) by bucket(date, 2, \"2000-01-02\", date)"),
            containsString("fourth argument of [bucket(date, 2, \"2000-01-02\", date)] must be a constant, " + "received [date]")
        );

        assertThat(
            typesError("from types | stats max(integer) by bucket(integer, long, 4, 5)"),
            containsString("second argument of [bucket(integer, long, 4, 5)] must be a constant, " + "received [long]")
        );

        assertThat(
            typesError("from types | stats max(integer) by bucket(integer, 3, long, 5)"),
            containsString("third argument of [bucket(integer, 3, long, 5)] must be a constant, " + "received [long]")
        );

        assertThat(
            typesError("from types | stats max(integer) by bucket(integer, 3, 4, long)"),
            containsString("fourth argument of [bucket(integer, 3, 4, long)] must be a constant, " + "received [long]")
        );
    }

    private String typesError(String query) {
        VerificationException e = expectThrows(VerificationException.class, () -> planTypes(query));
        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[x{r}#5]]
     * \_Eval[[____x_AVG@9efc3cf3_SUM@daf9f221{r}#18 / ____x_AVG@9efc3cf3_COUNT@53cd08ed{r}#19 AS __x_AVG@9efc3cf3, __x_AVG@
     * 9efc3cf3{r}#16 / 2[INTEGER] + __x_MAX@475d0e4d{r}#17 AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[SUM(salary{f}#11) AS ____x_AVG@9efc3cf3_SUM@daf9f221, COUNT(salary{f}#11) AS ____x_AVG@9efc3cf3_COUNT@53cd0
     * 8ed, MAX(salary{f}#11) AS __x_MAX@475d0e4d]]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testStatsExpOverAggs() {
        var plan = optimizedPlan("""
            from test
            | stats x = avg(salary) /2 + max(salary)
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.name(fields.get(1)), is("x"));
        // sum/count to compute avg
        var div = as(fields.get(0).child(), Div.class);
        // avg + max
        var add = as(fields.get(1).child(), Add.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(3));
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        assertThat(Expressions.name(sum.field()), is("salary"));
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        assertThat(Expressions.name(count.field()), is("salary"));
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        assertThat(Expressions.name(max.field()), is("salary"));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[x{r}#5, y{r}#9, z{r}#12]]
     * \_Eval[[$$SUM$$$AVG$avg(salary_%_3)>$0$0{r}#29 / $$COUNT$$$AVG$avg(salary_%_3)>$0$1{r}#30 AS $$AVG$avg(salary_%_3)>$0,
     *   $$AVG$avg(salary_%_3)>$0{r}#23 + $$MAX$avg(salary_%_3)>$1{r}#24 AS x,
     *   $$MIN$min(emp_no_/_3)>$2{r}#25 + 10[INTEGER] - $$MEDIAN$min(emp_no_/_3)>$3{r}#26 AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[z{r}#12],[SUM($$salary_%_3$AVG$0{r}#27) AS $$SUM$$$AVG$avg(salary_%_3)>$0$0,
     *     COUNT($$salary_%_3$AVG$0{r}#27) AS $$COUNT$$$AVG$avg(salary_%_3)>$0$1,
     *     MAX(emp_no{f}#13) AS $$MAX$avg(salary_%_3)>$1,
     *     MIN($$emp_no_/_3$MIN$1{r}#28) AS $$MIN$min(emp_no_/_3)>$2,
     *     PERCENTILE(salary{f}#18,50[INTEGER]) AS $$MEDIAN$min(emp_no_/_3)>$3, z{r}#12]]
     *       \_Eval[[languages{f}#16 % 2[INTEGER] AS z,
     *       salary{f}#18 % 3[INTEGER] AS $$salary_%_3$AVG$0,
     *       emp_no{f}#13 / 3[INTEGER] AS $$emp_no_/_3$MIN$1]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testStatsExpOverAggsMulti() {
        var plan = optimizedPlan("""
            from test
            | stats x = avg(salary % 3) + max(emp_no), y = min(emp_no / 3) + 10 - median(salary) by z = languages % 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x", "y", "z"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // avg + max
        assertThat(Expressions.name(fields.get(1)), containsString("x"));
        assertThat(Alias.unwrap(fields.get(1)), instanceOf(Add.class));
        // min + 10 - median
        assertThat(Expressions.name(fields.get(2)), containsString("y"));
        assertThat(Alias.unwrap(fields.get(2)), instanceOf(Sub.class));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);
        var percentile = as(Alias.unwrap(aggs.get(4)), Percentile.class);

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("z"));
        assertThat(Expressions.name(fields.get(1)), containsString("AVG"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(1))), containsString("salary"));
        assertThat(Expressions.name(fields.get(2)), containsString("MIN"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(2))), containsString("emp_no"));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[x{r}#5, y{r}#9, z{r}#12]]
     * \_Eval[[$$SUM$$$AVG$CONCAT(TO_STRIN>$0$0{r}#29 / $$COUNT$$$AVG$CONCAT(TO_STRIN>$0$1{r}#30 AS $$AVG$CONCAT(TO_STRIN>$0,
     *        CONCAT(TOSTRING($$AVG$CONCAT(TO_STRIN>$0{r}#23),TOSTRING($$MAX$CONCAT(TO_STRIN>$1{r}#24)) AS x,
     *        $$MIN$(MIN(emp_no_/_3>$2{r}#25 + 3.141592653589793[DOUBLE] - $$MEDIAN$(MIN(emp_no_/_3>$3{r}#26 / 2.718281828459045[DOUBLE]
     *         AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[z{r}#12],[SUM($$salary_%_3$AVG$0{r}#27) AS $$SUM$$$AVG$CONCAT(TO_STRIN>$0$0,
     *      COUNT($$salary_%_3$AVG$0{r}#27) AS $$COUNT$$$AVG$CONCAT(TO_STRIN>$0$1,
     *      MAX(emp_no{f}#13) AS $$MAX$CONCAT(TO_STRIN>$1,
     *      MIN($$emp_no_/_3$MIN$1{r}#28) AS $$MIN$(MIN(emp_no_/_3>$2,
     *      PERCENTILE(salary{f}#18,50[INTEGER]) AS $$MEDIAN$(MIN(emp_no_/_3>$3, z{r}#12]]
     *       \_Eval[[languages{f}#16 % 2[INTEGER] AS z,
     *       salary{f}#18 % 3[INTEGER] AS $$salary_%_3$AVG$0,
     *       emp_no{f}#13 / 3[INTEGER] AS $$emp_no_/_3$MIN$1]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testStatsExpOverAggsWithScalars() {
        var plan = optimizedPlan("""
            from test
            | stats x = CONCAT(TO_STRING(AVG(salary % 3)), TO_STRING(MAX(emp_no))),
                    y = (MIN(emp_no / 3) + PI() - MEDIAN(salary))/E()
                    by z = languages % 2
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("x", "y", "z"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // concat(to_string(avg)
        assertThat(Expressions.name(fields.get(1)), containsString("x"));
        var concat = as(Alias.unwrap(fields.get(1)), Concat.class);
        var toString = as(concat.children().get(0), ToString.class);
        toString = as(concat.children().get(1), ToString.class);
        // min + 10 - median/e
        assertThat(Expressions.name(fields.get(2)), containsString("y"));
        assertThat(Alias.unwrap(fields.get(2)), instanceOf(Div.class));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);
        var count = as(Alias.unwrap(aggs.get(1)), Count.class);
        var max = as(Alias.unwrap(aggs.get(2)), Max.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);
        var percentile = as(Alias.unwrap(aggs.get(4)), Percentile.class);
        assertThat(Expressions.name(aggs.get(5)), is("z"));

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("z"));
        assertThat(Expressions.name(fields.get(1)), containsString("AVG"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(1))), containsString("salary"));
        assertThat(Expressions.name(fields.get(2)), containsString("MIN"));
        assertThat(Expressions.name(Alias.unwrap(fields.get(2))), containsString("emp_no"));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[a{r}#5, a{r}#5 AS b, w{r}#12]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[w{r}#12],[SUM($$salary_/_2_+_la>$SUM$0{r}#26) AS a, w{r}#12]]
     *     \_Eval[[emp_no{f}#16 % 2[INTEGER] AS w, salary{f}#21 / 2[INTEGER] + languages{f}#19 AS $$salary_/_2_+_la>$SUM$0]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    public void testStatsWithCanonicalAggregate() throws Exception {
        var plan = optimizedPlan("""
            from test
            | stats a = sum(salary / 2 + languages),
                    b = sum(languages + salary / 2)
                    by w = emp_no % 2
            | keep a, b, w
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("a", "b", "w"));
        assertThat(Expressions.name(Alias.unwrap(project.projections().get(1))), is("a"));
        var limit = as(project.child(), Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var aggregates = aggregate.aggregates();
        assertThat(Expressions.names(aggregates), contains("a", "w"));
        var unwrapped = Alias.unwrap(aggregates.get(0));
        var sum = as(unwrapped, Sum.class);
        var sum_argument = sum.field();
        var grouping = aggregates.get(1);

        var eval = as(aggregate.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.attribute(fields.get(0)), is(Expressions.attribute(grouping)));
        assertThat(Expressions.attribute(fields.get(1)), is(Expressions.attribute(sum_argument)));
    }

    /**
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *     \_Eval[[COALESCE(MVCOUNT([1, 2][INTEGER]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s, COALESCE(MVCOUNT(314.0[DOUBLE] / 100[
     * INTEGER]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s_expr, COALESCE(MVCOUNT(null[NULL]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s_null]]
     *       \_Aggregate[[w{r}#10],[COUNT(*[KEYWORD]) AS $$COUNT$s$0, w{r}#10]]
     *         \_Eval[[emp_no{f}#16 % 2[INTEGER] AS w]]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    public void testCountOfLiteral() {
        var plan = plan("""
            from test
            | stats s = count([1,2]),
                    s_expr = count(314.0/100),
                    s_null = count(null)
                    by w = emp_no % 2
            | keep s, s_expr, s_null, w
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var esqlProject = as(limit.child(), EsqlProject.class);
        var project = as(esqlProject.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(Expressions.names(agg.aggregates()), contains("$$COUNT$s$0", "w"));
        var countAggLiteral = as(as(Alias.unwrap(agg.aggregates().get(0)), Count.class).field(), Literal.class);
        assertTrue(countAggLiteral.semanticEquals(new Literal(EMPTY, BytesRefs.toBytesRef(StringUtils.WILDCARD), DataType.KEYWORD)));

        var exprs = eval.fields();
        // s == mv_count([1,2]) * count(*)
        var s = as(exprs.get(0), Alias.class);
        assertThat(s.name(), equalTo("s"));
        var mul = as(s.child(), Mul.class);
        var mvCoalesce = as(mul.left(), Coalesce.class);
        assertThat(mvCoalesce.children().size(), equalTo(2));
        var mvCount = as(mvCoalesce.children().get(0), MvCount.class);
        assertThat(mvCount.fold(FoldContext.small()), equalTo(2));
        assertThat(mvCoalesce.children().get(1).fold(FoldContext.small()), equalTo(0));
        var count = as(mul.right(), ReferenceAttribute.class);
        assertThat(count.name(), equalTo("$$COUNT$s$0"));

        // s_expr == mv_count(314.0/100) * count(*)
        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.name(), equalTo("s_expr"));
        var mul_expr = as(s_expr.child(), Mul.class);
        var mvCoalesce_expr = as(mul_expr.left(), Coalesce.class);
        assertThat(mvCoalesce_expr.children().size(), equalTo(2));
        var mvCount_expr = as(mvCoalesce_expr.children().get(0), MvCount.class);
        assertThat(mvCount_expr.fold(FoldContext.small()), equalTo(1));
        assertThat(mvCoalesce_expr.children().get(1).fold(FoldContext.small()), equalTo(0));
        var count_expr = as(mul_expr.right(), ReferenceAttribute.class);
        assertThat(count_expr.name(), equalTo("$$COUNT$s$0"));

        // s_null == mv_count(null) * count(*)
        var s_null = as(exprs.get(2), Alias.class);
        assertThat(s_null.name(), equalTo("s_null"));
        var mul_null = as(s_null.child(), Mul.class);
        var mvCoalesce_null = as(mul_null.left(), Coalesce.class);
        assertThat(mvCoalesce_null.children().size(), equalTo(2));
        var mvCount_null = as(mvCoalesce_null.children().get(0), MvCount.class);
        assertThat(mvCount_null.field(), equalTo(NULL));
        assertThat(mvCoalesce_null.children().get(1).fold(FoldContext.small()), equalTo(0));
        var count_null = as(mul_null.right(), ReferenceAttribute.class);
        assertThat(count_null.name(), equalTo("$$COUNT$s$0"));
    }

    /**
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *     \_Eval[[MVSUM([1, 2][INTEGER]) * $$COUNT$s$0{r}#25 AS s, MVSUM(314.0[DOUBLE] / 100[INTEGER]) * $$COUNT$s$0{r}#25 AS s
     * _expr, MVSUM(null[NULL]) * $$COUNT$s$0{r}#25 AS s_null]]
     *       \_Aggregate[[w{r}#10],[COUNT(*[KEYWORD]) AS $$COUNT$s$0, w{r}#10]]
     *         \_Eval[[emp_no{f}#15 % 2[INTEGER] AS w]]
     *           \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testSumOfLiteral() {
        var plan = plan("""
            from test
            | stats s = sum([1,2]),
                    s_expr = sum(314.0/100),
                    s_null = sum(null)
                    by w = emp_no % 2
            | keep s, s_expr, s_null, w
            """, new TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var esqlProject = as(limit.child(), EsqlProject.class);
        var project = as(esqlProject.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        var exprs = eval.fields();
        // s == mv_sum([1,2]) * count(*)
        var s = as(exprs.get(0), Alias.class);
        assertThat(s.name(), equalTo("s"));
        var mul = as(s.child(), Mul.class);
        var mvSum = as(mul.left(), MvSum.class);
        assertThat(mvSum.fold(FoldContext.small()), equalTo(3));
        var count = as(mul.right(), ReferenceAttribute.class);
        assertThat(count.name(), equalTo("$$COUNT$s$0"));

        // s_expr == mv_sum(314.0/100) * count(*)
        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.name(), equalTo("s_expr"));
        var mul_expr = as(s_expr.child(), Mul.class);
        var mvSum_expr = as(mul_expr.left(), MvSum.class);
        assertThat(mvSum_expr.fold(FoldContext.small()), equalTo(3.14));
        var count_expr = as(mul_expr.right(), ReferenceAttribute.class);
        assertThat(count_expr.name(), equalTo("$$COUNT$s$0"));

        // s_null == mv_sum(null) * count(*)
        var s_null = as(exprs.get(2), Alias.class);
        assertThat(s_null.name(), equalTo("s_null"));
        var mul_null = as(s_null.child(), Mul.class);
        var mvSum_null = as(mul_null.left(), MvSum.class);
        assertThat(mvSum_null.field(), equalTo(NULL));
        var count_null = as(mul_null.right(), ReferenceAttribute.class);
        assertThat(count_null.name(), equalTo("$$COUNT$s$0"));

        var countAgg = as(Alias.unwrap(agg.aggregates().get(0)), Count.class);
        assertThat(countAgg.children().get(0), instanceOf(Literal.class));
        var w = as(Alias.unwrap(agg.groupings().get(0)), ReferenceAttribute.class);
        assertThat(w.name(), equalTo("w"));
    }

    private record AggOfLiteralTestCase(
        String aggFunctionTemplate,
        Function<Expression, Expression> replacementForConstant,
        Function<int[], Object> aggMultiValue,
        Function<Double, Object> aggSingleValue
    ) {};

    private static List<AggOfLiteralTestCase> AGG_OF_CONST_CASES = List.of(
        new AggOfLiteralTestCase(
            "avg({})",
            constant -> new MvAvg(EMPTY, constant),
            ints -> ((double) Arrays.stream(ints).sum()) / ints.length,
            d -> d
        ),
        new AggOfLiteralTestCase("min({})", c -> new MvMin(EMPTY, c), ints -> Arrays.stream(ints).min().getAsInt(), d -> d),
        new AggOfLiteralTestCase("max({})", c -> new MvMax(EMPTY, c), ints -> Arrays.stream(ints).max().getAsInt(), d -> d),
        new AggOfLiteralTestCase("median({})", c -> new MvMedian(EMPTY, new ToDouble(EMPTY, c)), ints -> {
            var sortedInts = Arrays.stream(ints).sorted().toArray();
            int middle = ints.length / 2;
            double result = ints.length % 2 == 1 ? sortedInts[middle] : (sortedInts[middle] + sortedInts[middle - 1]) / 2.0;
            return result;
        }, d -> d),
        new AggOfLiteralTestCase(
            "count_distinct({}, 1234)",
            c -> new ToLong(
                EMPTY,
                new Coalesce(EMPTY, new MvCount(EMPTY, new MvDedupe(EMPTY, c)), List.of(new Literal(EMPTY, 0, DataType.INTEGER)))
            ),
            ints -> Arrays.stream(ints).distinct().count(),
            d -> 1L
        )
    );

    /**
     * Aggs of literals in case that the agg can be simply replaced by a corresponding mv-function;
     * e.g. avg([1,2,3]) which is equivalent to mv_avg([1,2,3]).
     * <p>
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7]]
     *     \_Eval[[MVAVG([1, 2][INTEGER]) AS s, MVAVG(314.0[DOUBLE] / 100[INTEGER]) AS s_expr, MVAVG(null[NULL]) AS s_null]]
     *       \_LocalRelation[[{e}#21],[ConstantNullBlock[positions=1]]]
     * }</pre>
     */
    public void testAggOfLiteral() {
        for (AggOfLiteralTestCase testCase : AGG_OF_CONST_CASES) {
            String queryTemplate = """
                from test
                | stats s = {},
                        s_expr = {},
                        s_null = {}
                | keep s, s_expr, s_null
                """;
            String queryWithoutValues = LoggerMessageFormat.format(
                null,
                queryTemplate,
                testCase.aggFunctionTemplate,
                testCase.aggFunctionTemplate,
                testCase.aggFunctionTemplate
            );
            String query = LoggerMessageFormat.format(null, queryWithoutValues, "[1,2]", "314.0/100", "null");

            var plan = plan(query, new TestSubstitutionOnlyOptimizer());

            var limit = as(plan, Limit.class);
            var esqlProject = as(limit.child(), EsqlProject.class);
            var project = as(esqlProject.child(), Project.class);
            var eval = as(project.child(), Eval.class);
            var singleRowRelation = as(eval.child(), LocalRelation.class);
            var singleRow = singleRowRelation.supplier().get();
            assertThat(singleRow.getBlockCount(), equalTo(1));
            assertThat(singleRow.getBlock(0).getPositionCount(), equalTo(1));

            assertAggOfConstExprs(testCase, eval.fields());
        }
    }

    /**
     * Like {@link LogicalPlanOptimizerTests#testAggOfLiteral()} but with a grouping key.
     * <p>
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, emp_no{f}#13]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, emp_no{f}#13]]
     *     \_Eval[[MVAVG([1, 2][INTEGER]) AS s, MVAVG(314.0[DOUBLE] / 100[INTEGER]) AS s_expr, MVAVG(null[NULL]) AS s_null]]
     *       \_Aggregate[[emp_no{f}#13],[emp_no{f}#13]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }</pre>
     */
    public void testAggOfLiteralGrouped() {
        for (AggOfLiteralTestCase testCase : AGG_OF_CONST_CASES) {
            String queryTemplate = """
                    from test
                    | stats s = {},
                            s_expr = {},
                            s_null = {}
                            by emp_no
                    | keep s, s_expr, s_null, emp_no
                """;
            String queryWithoutValues = LoggerMessageFormat.format(
                null,
                queryTemplate,
                testCase.aggFunctionTemplate,
                testCase.aggFunctionTemplate,
                testCase.aggFunctionTemplate
            );
            String query = LoggerMessageFormat.format(null, queryWithoutValues, "[1,2]", "314.0/100", "null");

            var plan = plan(query, new TestSubstitutionOnlyOptimizer());

            var limit = as(plan, Limit.class);
            var esqlProject = as(limit.child(), EsqlProject.class);
            var project = as(esqlProject.child(), Project.class);
            var eval = as(project.child(), Eval.class);
            var agg = as(eval.child(), Aggregate.class);
            assertThat(agg.child(), instanceOf(EsRelation.class));

            // Assert that the aggregate only does the grouping by emp_no
            assertThat(Expressions.names(agg.groupings()), contains("emp_no"));
            assertThat(agg.aggregates().size(), equalTo(1));

            assertAggOfConstExprs(testCase, eval.fields());
        }
    }

    private static void assertAggOfConstExprs(AggOfLiteralTestCase testCase, List<Alias> exprs) {
        var s = as(exprs.get(0), Alias.class);
        assertThat(s.source().toString(), containsString(LoggerMessageFormat.format(null, testCase.aggFunctionTemplate, "[1,2]")));
        assertEquals(s.child(), testCase.replacementForConstant.apply(new Literal(EMPTY, List.of(1, 2), INTEGER)));
        assertEquals(s.child().fold(FoldContext.small()), testCase.aggMultiValue.apply(new int[] { 1, 2 }));

        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.source().toString(), containsString(LoggerMessageFormat.format(null, testCase.aggFunctionTemplate, "314.0/100")));
        assertEquals(
            s_expr.child(),
            testCase.replacementForConstant.apply(new Div(EMPTY, new Literal(EMPTY, 314.0, DOUBLE), new Literal(EMPTY, 100, INTEGER)))
        );
        assertEquals(s_expr.child().fold(FoldContext.small()), testCase.aggSingleValue.apply(3.14));

        var s_null = as(exprs.get(2), Alias.class);
        assertThat(s_null.source().toString(), containsString(LoggerMessageFormat.format(null, testCase.aggFunctionTemplate, "null")));
        assertEquals(s_null.child(), testCase.replacementForConstant.apply(NULL));
        // Cannot just fold as there may be no evaluator for the NULL datatype;
        // instead we emulate how the optimizer would fold the null value:
        // it transforms up from the leaves; c.f. FoldNull.
        assertTrue(oneLeaveIsNull(s_null));
    }

    private static void assertSubstitutionChain(Expression e, List<Class<? extends Expression>> substitutionChain) {
        var currentExpression = e;

        for (Class<? extends Expression> currentSubstitution : substitutionChain.subList(0, substitutionChain.size() - 1)) {
            assertThat(currentExpression, instanceOf(currentSubstitution));
            assertEquals(currentExpression.children().size(), 1);
            currentExpression = currentExpression.children().get(0);
        }

        assertThat(currentExpression, instanceOf(substitutionChain.get(substitutionChain.size() - 1)));
    }

    private static boolean oneLeaveIsNull(Expression e) {
        Holder<Boolean> result = new Holder<>(false);

        e.forEachUp(node -> {
            if (node.children().size() == 0) {
                result.set(result.get() || Expressions.isGuaranteedNull(node));
            }
        });

        return result.get();
    }

    public void testEmptyMappingIndex() {
        EsIndex empty = EsIndexGenerator.esIndex("empty_test");
        var analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(empty),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test")));
        as(plan, LocalRelation.class);
        assertThat(plan.output(), equalTo(NO_FIELDS));

        plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test metadata _id | eval x = 1")));
        as(plan, LocalRelation.class);
        assertThat(Expressions.names(plan.output()), contains("_id", "x"));

        plan = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement("from empty_test metadata _id, _version | limit 5")));
        as(plan, LocalRelation.class);
        assertThat(Expressions.names(plan.output()), contains("_id", "_version"));

        plan = logicalOptimizer.optimize(
            analyzer.analyze(parser.createStatement("from empty_test | eval x = \"abc\" | enrich languages_idx on x"))
        );
        LocalRelation local = as(plan, LocalRelation.class);
        assertThat(Expressions.names(local.output()), contains(NO_FIELDS.get(0).name(), "x", "language_code", "language_name"));
    }

    public void testPlanSanityCheck() throws Exception {
        var plan = optimizedPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        // emulate a rule that adds an invalid field
        var invalidPlan = new OrderBy(
            limit.source(),
            limit,
            asList(new Order(limit.source(), salary, Order.OrderDirection.ASC, Order.NullsPosition.FIRST))
        );

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> logicalOptimizer.optimize(invalidPlan));
        assertThat(e.getMessage(), containsString("Plan [OrderBy[[Order[salary"));
        assertThat(e.getMessage(), containsString(" optimized incorrectly due to missing references [salary"));
    }

    /**
     * Before we alter the plan to make it invalid, we expect
     *
     * <pre>{@code
     * Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *          languages{f}#9 AS language_code#4, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_name{f}#18]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#9],[languages{f}#9],[language_code{f}#17]]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#17, language_name{f}#18]
     * }</pre>
     */
    public void testPlanSanityCheckWithBinaryPlans() {
        var plan = optimizedPlan("""
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """);

        var project = as(plan, Project.class);
        var upperLimit = asLimit(project.child(), null, true);
        var join = as(upperLimit.child(), Join.class);

        var joinWithInvalidLeftPlan = join.replaceChildren(join.right(), join.right());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> logicalOptimizer.optimize(joinWithInvalidLeftPlan));
        assertThat(e.getMessage(), containsString(" optimized incorrectly due to missing references from left hand side [languages"));

        var joinWithInvalidRightPlan = join.replaceChildren(join.left(), join.left());
        e = expectThrows(IllegalStateException.class, () -> logicalOptimizer.optimize(joinWithInvalidRightPlan));
        assertThat(e.getMessage(), containsString(" optimized incorrectly due to missing references from right hand side [language_code"));
    }

    /**
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[languages{f}#8],[language_code{f}#16],languages{f}#8 == language_code{f}#16 AND language_name{f}#17 == English
     * [KEYWORD]]
     *   |_Limit[1000[INTEGER],false]
     *   | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#16, language_name{f}#17]
     * }</pre>
     */
    public void testLookupJoinRightFilter() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN languages_lookup ON languages == language_code and language_name == "English"
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertEquals("ON languages == language_code and language_name == \"English\"", join.config().joinOnConditions().toString());
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    public void testLookupJoinRightFilterMatch() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN languages_lookup ON languages == language_code and MATCH(language_name,"English")
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertEquals("ON languages == language_code and MATCH(language_name,\"English\")", join.config().joinOnConditions().toString());
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_Filter[LIKE(language_name{f}#18, "French*", false)]
     *   \_Join[LEFT,[languages{f}#9],[language_code{f}#17],languages{f}#9 == language_code{f}#17 AND MATCH(language_name{f}#18,
     * English[KEYWORD])]
     *     |_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *     \_Filter[LIKE(language_name{f}#18, "French*", false)]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#17, language_name{f}#18]
     */
    public void testLookupJoinRightFilterMatchWithWhereClause() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN languages_lookup ON languages == language_code and MATCH(language_name,"English")
            | WHERE language_name LIKE "French*"
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, false);
        var topFilter = as(upperLimit.child(), Filter.class);
        var join = as(topFilter.child(), Join.class);
        assertEquals("ON languages == language_code and MATCH(language_name,\"English\")", join.config().joinOnConditions().toString());

        // Check that the LIKE condition is pushed down as a right pre-join filter
        var rightFilter = as(join.right(), Filter.class);
        var likeCondition = as(rightFilter.condition(), WildcardLike.class);
        var field = as(likeCondition.field(), FieldAttribute.class);
        assertEquals("language_name", field.name());
        assertEquals("French*", likeCondition.pattern().pattern());

        as(rightFilter.child(), EsRelation.class);
    }

    // https://github.com/elastic/elasticsearch/issues/104995
    public void testNoWrongIsNotNullPruning() {
        var plan = optimizedPlan("""
              ROW a = 5, b = [ 1, 2 ]
              | EVAL sum = a + b
              | LIMIT 1
              | WHERE sum IS NOT NULL
            """);

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
        assertWarnings(
            "Line 2:16: evaluation of [a + b] failed, treating result as null. Only first 20 failures recorded.",
            "Line 2:16: java.lang.IllegalArgumentException: single-value function encountered multi-value"
        );
    }

    /**
     * Pushing down EVAL/GROK/DISSECT/ENRICH must not accidentally shadow attributes required by SORT.
     * <p>
     * For DISSECT expects the following; the others are similar.
     *
     * <pre>{@code
     * Project[[first_name{f}#37, emp_no{r}#30, salary{r}#31]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#46,ASC,LAST], Order[$$order_by$temp_name$1{r}#47,DESC,FIRST]],3[INTEGER]]
     *   \_Dissect[first_name{f}#37,Parser[pattern=%{emp_no} %{salary}, appendSeparator=,
     *   parser=org.elasticsearch.dissect.DissectParser@87f460f],[emp_no{r}#30, salary{r}#31]]
     *     \_Eval[[emp_no{f}#36 + salary{f}#41 * 13[INTEGER] AS $$order_by$temp_name$0, NEG(salary{f}#41) AS $$order_by$temp_name$1]]
     *       \_EsRelation[test][_meta_field{f}#42, emp_no{f}#36, first_name{f}#37, ..]
     * }</pre>
     */
    public void testPushdownWithOverwrittenName() {
        List<String> overwritingCommands = List.of(
            "EVAL emp_no = 3*emp_no, salary = -2*emp_no-salary",
            "DISSECT first_name \"%{emp_no} %{salary}\"",
            "GROK first_name \"%{WORD:emp_no} %{WORD:salary}\"",
            "ENRICH languages_idx ON first_name WITH emp_no = language_code, salary = language_code"
        );

        String queryTemplateKeepAfter = """
            FROM test
            | SORT emp_no ASC nulls first, salary DESC nulls last, emp_no
            | {}
            | KEEP first_name, emp_no, salary
            | LIMIT 3
            """;
        // Equivalent but with KEEP first - ensures that attributes in the final projection are correct after pushdown rules were applied.
        String queryTemplateKeepFirst = """
            FROM test
            | KEEP emp_no, salary, first_name
            | SORT emp_no ASC nulls first, salary DESC nulls last, emp_no
            | {}
            | LIMIT 3
            """;

        for (String overwritingCommand : overwritingCommands) {
            String queryTemplate = randomBoolean() ? queryTemplateKeepFirst : queryTemplateKeepAfter;
            var plan = optimizedPlan(LoggerMessageFormat.format(null, queryTemplate, overwritingCommand));

            var project = as(plan, Project.class);
            var projections = project.projections();
            assertThat(projections.size(), equalTo(3));
            assertThat(projections.get(0).name(), equalTo("first_name"));
            assertThat(projections.get(1).name(), equalTo("emp_no"));
            assertThat(projections.get(2).name(), equalTo("salary"));

            var topN = as(project.child(), TopN.class);
            assertThat(topN.order().size(), is(3));

            var firstOrder = as(topN.order().get(0), Order.class);
            assertThat(firstOrder.direction(), equalTo(Order.OrderDirection.ASC));
            assertThat(firstOrder.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
            var renamed_emp_no = as(firstOrder.child(), ReferenceAttribute.class);
            assertThat(renamed_emp_no.toString(), startsWith("$$emp_no$temp_name"));

            var secondOrder = as(topN.order().get(1), Order.class);
            assertThat(secondOrder.direction(), equalTo(Order.OrderDirection.DESC));
            assertThat(secondOrder.nullsPosition(), equalTo(Order.NullsPosition.LAST));
            var renamed_salary = as(secondOrder.child(), ReferenceAttribute.class);
            assertThat(renamed_salary.toString(), startsWith("$$salary$temp_name"));

            var thirdOrder = as(topN.order().get(2), Order.class);
            assertThat(thirdOrder.direction(), equalTo(Order.OrderDirection.ASC));
            assertThat(thirdOrder.nullsPosition(), equalTo(Order.NullsPosition.LAST));
            var renamed_emp_no2 = as(thirdOrder.child(), ReferenceAttribute.class);
            assertThat(renamed_emp_no2.toString(), startsWith("$$emp_no$temp_name"));

            assert (renamed_emp_no2.semanticEquals(renamed_emp_no) && renamed_emp_no2.equals(renamed_emp_no));

            Eval renamingEval = null;
            if (overwritingCommand.startsWith("EVAL")) {
                // Multiple EVALs should be merged, so there's only one.
                renamingEval = as(topN.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("DISSECT")) {
                var dissect = as(topN.child(), Dissect.class);
                renamingEval = as(dissect.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("GROK")) {
                var grok = as(topN.child(), Grok.class);
                renamingEval = as(grok.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("ENRICH")) {
                var enrich = as(topN.child(), Enrich.class);
                renamingEval = as(enrich.child(), Eval.class);
            }

            var attributesCreatedInEval = AttributeSet.builder();
            for (Alias field : renamingEval.fields()) {
                attributesCreatedInEval.add(field.toAttribute());
            }
            assertThat(attributesCreatedInEval.build(), allOf(hasItem(renamed_emp_no), hasItem(renamed_salary), hasItem(renamed_emp_no2)));

            assertThat(renamingEval.fields().size(), anyOf(equalTo(2), equalTo(4))); // 4 for EVAL, 3 for the other overwritingCommands
            // emp_no ASC nulls first
            Alias empNoAsc = renamingEval.fields().get(0);
            assertThat(empNoAsc.toAttribute(), equalTo(renamed_emp_no));
            var emp_no = as(empNoAsc.child(), FieldAttribute.class);
            assertThat(emp_no.name(), equalTo("emp_no"));

            // salary DESC nulls last
            Alias salaryDesc = renamingEval.fields().get(1);
            assertThat(salaryDesc.toAttribute(), equalTo(renamed_salary));
            var salary_desc = as(salaryDesc.child(), FieldAttribute.class);
            assertThat(salary_desc.name(), equalTo("salary"));

            assertThat(renamingEval.child(), instanceOf(EsRelation.class));
        }
    }

    record PushdownShadowingGeneratingPlanTestCase(
        BiFunction<LogicalPlan, Attribute, LogicalPlan> applyLogicalPlan,
        OptimizerRules.OptimizerRule<? extends LogicalPlan> rule
    ) {};

    static PushdownShadowingGeneratingPlanTestCase[] PUSHDOWN_SHADOWING_GENERATING_PLAN_TEST_CASES = {
        // | EVAL y = to_integer(x), y = y + 1
        new PushdownShadowingGeneratingPlanTestCase((plan, attr) -> {
            Alias y1 = new Alias(EMPTY, "y", new ToInteger(EMPTY, attr));
            Alias y2 = new Alias(EMPTY, "y", new Add(EMPTY, y1.toAttribute(), new Literal(EMPTY, 1, INTEGER)));
            return new Eval(EMPTY, plan, List.of(y1, y2));
        }, new PushDownEval()),
        // | DISSECT x "%{y} %{y}"
        new PushdownShadowingGeneratingPlanTestCase(
            (plan, attr) -> new Dissect(
                EMPTY,
                plan,
                attr,
                new Dissect.Parser("%{y} %{y}", ",", new DissectParser("%{y} %{y}", ",")),
                List.of(new ReferenceAttribute(EMPTY, "y", KEYWORD), new ReferenceAttribute(EMPTY, "y", KEYWORD))
            ),
            new PushDownRegexExtract()
        ),
        // | GROK x "%{WORD:y} %{WORD:y}"
        new PushdownShadowingGeneratingPlanTestCase(
            (plan, attr) -> new Grok(EMPTY, plan, attr, Grok.pattern(EMPTY, "%{WORD:y} %{WORD:y}")),
            new PushDownRegexExtract()
        ),
        // | ENRICH some_policy ON x WITH y = some_enrich_idx_field, y = some_other_enrich_idx_field
        new PushdownShadowingGeneratingPlanTestCase(
            (plan, attr) -> new Enrich(
                EMPTY,
                plan,
                Enrich.Mode.ANY,
                Literal.keyword(EMPTY, "some_policy"),
                attr,
                null,
                Map.of(),
                List.of(
                    new Alias(EMPTY, "y", new ReferenceAttribute(EMPTY, "some_enrich_idx_field", KEYWORD)),
                    new Alias(EMPTY, "y", new ReferenceAttribute(EMPTY, "some_other_enrich_idx_field", KEYWORD))
                )
            ),
            new PushDownEnrich()
        ),
        // | COMPLETION y =CONCAT(some text, x) WITH { "inference_id" : "inferenceID" }
        new PushdownShadowingGeneratingPlanTestCase(
            (plan, attr) -> new Completion(
                EMPTY,
                plan,
                randomLiteral(TEXT),
                new Concat(EMPTY, randomLiteral(TEXT), List.of(attr)),
                new ReferenceAttribute(EMPTY, "y", KEYWORD)
            ),
            new PushDownInferencePlan()
        ),
        // | RERANK "some text" ON x INTO y WITH { "inference_id" : "inferenceID" }
        new PushdownShadowingGeneratingPlanTestCase(
            (plan, attr) -> new Rerank(
                EMPTY,
                plan,
                randomLiteral(TEXT),
                randomLiteral(TEXT),
                List.of(new Alias(EMPTY, attr.name(), attr)),
                new ReferenceAttribute(EMPTY, "y", KEYWORD)
            ),
            new PushDownInferencePlan()
        ), };

    /**
     * Consider
     *
     * <pre>{@code
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Project[[y{r}#3, x{r}#2]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     * }</pre>
     *
     * We can freely push down the Eval without renaming, but need to update the Project's references.
     *
     * <pre>{@code
     * Project[[x{r}#2, y{r}#6 AS y]]
     * \_Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     * }</pre>
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastProject() {
        Alias x = new Alias(EMPTY, "x", Literal.keyword(EMPTY, "1"));
        Alias y = new Alias(EMPTY, "y", Literal.keyword(EMPTY, "2"));
        LogicalPlan initialRow = new Row(EMPTY, List.of(x, y));
        LogicalPlan initialProject = new Project(EMPTY, initialRow, List.of(y.toAttribute(), x.toAttribute()));

        for (PushdownShadowingGeneratingPlanTestCase testCase : PUSHDOWN_SHADOWING_GENERATING_PLAN_TEST_CASES) {
            LogicalPlan initialPlan = testCase.applyLogicalPlan.apply(initialProject, x.toAttribute());
            @SuppressWarnings("unchecked")
            List<Attribute> initialGeneratedExprs = ((GeneratingPlan) initialPlan).generatedAttributes();
            LogicalPlan optimizedPlan = testCase.rule.apply(initialPlan);

            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan, initialPlan.output());
            assertFalse(inconsistencies.hasFailures());

            Project project = as(optimizedPlan, Project.class);
            LogicalPlan pushedDownGeneratingPlan = project.child();

            List<? extends NamedExpression> projections = project.projections();
            @SuppressWarnings("unchecked")
            List<Attribute> newGeneratedExprs = ((GeneratingPlan) pushedDownGeneratingPlan).generatedAttributes();
            assertEquals(newGeneratedExprs, initialGeneratedExprs);
            // The rightmost generated attribute makes it into the final output as "y".
            Attribute rightmostGenerated = newGeneratedExprs.get(newGeneratedExprs.size() - 1);

            assertThat(Expressions.names(projections), contains("x", "y"));
            assertThat(projections, everyItem(instanceOf(ReferenceAttribute.class)));
            ReferenceAttribute yShadowed = as(projections.get(1), ReferenceAttribute.class);
            assertTrue(yShadowed.semanticEquals(rightmostGenerated));
        }
    }

    /**
     * Consider
     *
     * <pre>{@code
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Project[[x{r}#2, y{r}#3, y{r}#3 AS z]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     * }</pre>
     *
     * To push down the Eval, we must not shadow the reference y{r}#3, so we rename.
     *
     * <pre>{@code
     * Project[[x{r}#2, y{r}#3 AS z, $$y$temp_name$10{r}#12 AS y]]
     * Eval[[TO_INTEGER(x{r}#2) AS $$y$temp_name$10, $$y$temp_name$10{r}#11 + 1[INTEGER] AS $$y$temp_name$10]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     * }</pre>
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastRenamingProject() {
        Alias x = new Alias(EMPTY, "x", Literal.keyword(EMPTY, "1"));
        Alias y = new Alias(EMPTY, "y", Literal.keyword(EMPTY, "2"));
        LogicalPlan initialRow = new Row(EMPTY, List.of(x, y));
        LogicalPlan initialProject = new Project(
            EMPTY,
            initialRow,
            List.of(x.toAttribute(), y.toAttribute(), new Alias(EMPTY, "z", y.toAttribute()))
        );

        for (PushdownShadowingGeneratingPlanTestCase testCase : PUSHDOWN_SHADOWING_GENERATING_PLAN_TEST_CASES) {
            LogicalPlan initialPlan = testCase.applyLogicalPlan.apply(initialProject, x.toAttribute());
            @SuppressWarnings("unchecked")
            List<Attribute> initialGeneratedExprs = ((GeneratingPlan) initialPlan).generatedAttributes();
            LogicalPlan optimizedPlan = testCase.rule.apply(initialPlan);

            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan, initialPlan.output());
            assertFalse(inconsistencies.hasFailures());

            Project project = as(optimizedPlan, Project.class);
            LogicalPlan pushedDownGeneratingPlan = project.child();

            List<? extends NamedExpression> projections = project.projections();
            @SuppressWarnings("unchecked")
            List<Attribute> newGeneratedExprs = ((GeneratingPlan) pushedDownGeneratingPlan).generatedAttributes();
            List<String> newNames = Expressions.names(newGeneratedExprs);
            assertThat(newNames.size(), equalTo(initialGeneratedExprs.size()));
            assertThat(newNames, everyItem(startsWith("$$y$temp_name$")));
            // The rightmost generated attribute makes it into the final output as "y".
            Attribute rightmostGeneratedWithNewName = newGeneratedExprs.get(newGeneratedExprs.size() - 1);

            assertThat(Expressions.names(projections), contains("x", "z", "y"));
            assertThat(projections.get(0), instanceOf(ReferenceAttribute.class));
            Alias zAlias = as(projections.get(1), Alias.class);
            ReferenceAttribute yRenamed = as(zAlias.child(), ReferenceAttribute.class);
            assertEquals(yRenamed.name(), "y");
            Alias yAlias = as(projections.get(2), Alias.class);
            ReferenceAttribute yTempRenamed = as(yAlias.child(), ReferenceAttribute.class);
            assertTrue(yTempRenamed.semanticEquals(rightmostGeneratedWithNewName));
        }
    }

    /**
     * Consider
     *
     * <pre>{@code
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#3 + 1[INTEGER] AS y]]
     * \_Project[[y{r}#1, y{r}#1 AS x]]
     * \_Row[[2[INTEGER] AS y]]
     * }</pre>
     *
     * To push down the Eval, we must not shadow the reference y{r}#1, so we rename.
     * Additionally, the rename "y AS x" needs to be propagated into the Eval.
     *
     * <pre>{@code
     * Project[[y{r}#1 AS x, $$y$temp_name$10{r}#12 AS y]]
     * Eval[[TO_INTEGER(y{r}#1) AS $$y$temp_name$10, $$y$temp_name$10{r}#11 + 1[INTEGER] AS $$y$temp_name$10]]
     * \_Row[[2[INTEGER] AS y]]
     * }</pre>
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastRenamingProjectWithResolution() {
        Alias y = new Alias(EMPTY, "y", Literal.keyword(EMPTY, "2"));
        Alias yAliased = new Alias(EMPTY, "x", y.toAttribute());
        LogicalPlan initialRow = new Row(EMPTY, List.of(y));
        LogicalPlan initialProject = new Project(EMPTY, initialRow, List.of(y.toAttribute(), yAliased));

        for (PushdownShadowingGeneratingPlanTestCase testCase : PUSHDOWN_SHADOWING_GENERATING_PLAN_TEST_CASES) {
            LogicalPlan initialPlan = testCase.applyLogicalPlan.apply(initialProject, yAliased.toAttribute());
            @SuppressWarnings("unchecked")
            List<Attribute> initialGeneratedExprs = ((GeneratingPlan) initialPlan).generatedAttributes();
            LogicalPlan optimizedPlan = testCase.rule.apply(initialPlan);

            // This ensures that our generating plan doesn't use invalid references, resp. that any rename from the Project has
            // been propagated into the generating plan.
            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan, initialPlan.output());
            assertFalse(inconsistencies.hasFailures());

            Project project = as(optimizedPlan, Project.class);
            LogicalPlan pushedDownGeneratingPlan = project.child();

            List<? extends NamedExpression> projections = project.projections();
            @SuppressWarnings("unchecked")
            List<Attribute> newGeneratedExprs = ((GeneratingPlan) pushedDownGeneratingPlan).generatedAttributes();
            List<String> newNames = Expressions.names(newGeneratedExprs);
            assertThat(newNames.size(), equalTo(initialGeneratedExprs.size()));
            assertThat(newNames, everyItem(startsWith("$$y$temp_name$")));
            // The rightmost generated attribute makes it into the final output as "y".
            Attribute rightmostGeneratedWithNewName = newGeneratedExprs.get(newGeneratedExprs.size() - 1);

            assertThat(Expressions.names(projections), contains("x", "y"));
            Alias yRenamed = as(projections.get(0), Alias.class);
            assertTrue(yRenamed.child().semanticEquals(y.toAttribute()));
            Alias yTempRenamed = as(projections.get(1), Alias.class);
            ReferenceAttribute yTemp = as(yTempRenamed.child(), ReferenceAttribute.class);
            assertTrue(yTemp.semanticEquals(rightmostGeneratedWithNewName));
        }
    }

    /*
     * Test for: https://github.com/elastic/elasticsearch/issues/134407
     *
     * Input:
     * OrderBy[[Order[a{f}#2,ASC,ANY]]]
     * \_Project[[aTemp{r}#3 AS a#2]]
     *   \_Eval[[null[INTEGER] AS aTemp#3]]
     *     \_EsRelation[uYiPqAFD][LOOKUP][a{f}#2]
     *
     * Output:
     * Project[[aTemp{r}#3 AS a#2]]
     * \_OrderBy[[Order[aTemp{r}#3,ASC,ANY]]]
     *   \_Eval[[null[INTEGER] AS aTemp#3]]
     *     \_EsRelation[uYiPqAFD][LOOKUP][a{f}#2]
     */
    public void testPushDownOrderByPastRename() {
        FieldAttribute a = getFieldAttribute("a");
        EsRelation relation = relation().withAttributes(List.of(a));

        Alias aTemp = new Alias(EMPTY, "aTemp", new Literal(EMPTY, null, a.dataType()));
        Eval eval = new Eval(EMPTY, relation, List.of(aTemp));

        // Rename the null literal to "a" so that the OrderBy can refer to it. Requires re-using the id of original "a" attribute.
        Alias aliasA = new Alias(EMPTY, "a", aTemp.toAttribute(), a.id());
        Project project = new Project(EMPTY, eval, List.of(aliasA));

        // OrderBy sorts on original `a` attribute; after pushing down it should sort on aTemp.
        OrderBy orderBy = new OrderBy(EMPTY, project, List.of(new Order(EMPTY, a, Order.OrderDirection.ASC, Order.NullsPosition.ANY)));

        LogicalPlan optimized = new PushDownAndCombineOrderBy().apply(orderBy);

        var projectOut = as(optimized, Project.class);
        assertThat(projectOut.projections(), equalTo(project.projections()));
        var orderByOutput = as(projectOut.child(), OrderBy.class);
        var orderAttr = as(orderByOutput.order().getFirst().child(), ReferenceAttribute.class);

        // the actual fix test
        assertThat(orderAttr.name(), equalTo("aTemp"));
        assertThat(orderAttr.id(), equalTo(aTemp.id()));

        var evalOutput = as(orderByOutput.child(), Eval.class);
        assertThat(evalOutput.fields(), equalTo(eval.fields()));
        assertThat(evalOutput.child(), equalTo(relation));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[min{r}#4, languages{f}#11]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#18,ASC,LAST]],1000[INTEGER]]
     *   \_Eval[[min{r}#4 + languages{f}#11 AS $$order_by$temp_name$0]]
     *     \_Aggregate[[languages{f}#11],[MIN(salary{f}#13) AS min, languages{f}#11]]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     * }</pre>
     */
    public void testReplaceSortByExpressionsWithStats() {
        var plan = optimizedPlan("""
            from test
            | stats min = min(salary) by languages
            | sort min + languages
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("min", "languages"));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));

        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        var expression = as(order.child(), ReferenceAttribute.class);
        assertThat(expression.toString(), startsWith("$$order_by$0$"));

        var eval = as(topN.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.attribute(fields.get(0)), is(Expressions.attribute(expression)));
        var aggregate = as(eval.child(), Aggregate.class);
        var aggregates = aggregate.aggregates();
        assertThat(Expressions.names(aggregates), contains("min", "languages"));
        var unwrapped = Alias.unwrap(aggregates.get(0));
        var min = as(unwrapped, Min.class);
        as(aggregate.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false]
     * \_InlineJoin[LEFT,[emp_no % 2{r}#6],[emp_no % 2{r}#6],[emp_no % 2{r}#6]]
     *   |_Eval[[emp_no{f}#7 % 2[INTEGER] AS emp_no % 2#6]]
     *   | \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   \_Aggregate[[emp_no % 2{r}#6],[COUNT(salary{f}#12,true[BOOLEAN]) AS c#4, emp_no % 2{r}#6]]
     *     \_StubRelation[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, lang
     *          uages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, emp_no % 2{r}#6]]
     */
    public void testInlineStatsNestedExpressionsInGroups() {
        var query = """
            FROM test
            | INLINE STATS c = COUNT(salary) by emp_no % 2
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class); // TODO: this needs to go
        var inline = as(limit.child(), InlineJoin.class);
        var eval = as(inline.left(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("emp_no % 2")));
        var relation = as(eval.child(), EsRelation.class);
        var agg = as(inline.right(), Aggregate.class);
        var groupings = agg.groupings();
        var ref = as(groupings.get(0), ReferenceAttribute.class);
        var aggs = agg.aggregates();
        assertThat(aggs.get(1), is(ref));
        assertThat(eval.fields().get(0).toAttribute(), is(ref));
        assertThat(eval.fields().get(0).name(), is("emp_no % 2"));
        var stub = as(agg.child(), StubRelation.class);
    }

    // if non-null, the `query` must have "INLINE STATS" capitalized
    public static boolean releaseBuildForInlineStats(@Nullable String query) {
        if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled() == false) {
            if (query != null) {
                var e = expectThrows(ParsingException.class, () -> analyze(query));
                assertThat(e.getMessage(), containsString("mismatched input 'INLINE' expecting"));
            }
            return true;
        }
        return false;
    }

    /*
     * Project[[emp_no{f}#12 AS x#8, emp_no{f}#12]]
     * \_TopN[[Order[emp_no{f}#12,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testInlinestatsGetsPrunedEntirely() {
        var query = """
            FROM employees
            | INLINE STATS x = avg(salary) BY emp_no
            | EVAL x = emp_no
            | SORT x
            | KEEP x, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "emp_no")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var relation = as(topN.child(), EsRelation.class);
    }

    /*
     * EsqlProject[[emp_no{f}#16, count{r}#7]]
     * \_TopN[[Order[emp_no{f}#16,ASC,LAST]],5[INTEGER]]
     *   \_InlineJoin[LEFT,[salaryK{r}#5],[salaryK{r}#5],[salaryK{r}#5]]
     *     |_Eval[[salary{f}#21 / 1000[INTEGER] AS salaryK#5]]
     *     | \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_Aggregate[[salaryK{r}#5],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count#7, salaryK{r}#5]]
     *       \_StubRelation[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *              languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21, salaryK{r}#5]]
     */
    public void testDoubleInlineStatsWithEvalGetsPrunedEntirely() {
        var query = """
            FROM employees
            | SORT languages DESC
            | EVAL salaryK = salary/1000
            | INLINE STATS count = COUNT(*) BY salaryK
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | KEEP emp_no, count
            | SORT emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "count")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        var ref = as(order.child(), FieldAttribute.class);
        assertThat(ref.name(), is("emp_no"));
        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("salaryK")));
        // Left
        var eval = as(inlineJoin.left(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("salaryK")));
        var relation = as(eval.child(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("salaryK")));
        assertThat(Expressions.names(agg.aggregates()), is(List.of("count", "salaryK")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#19 AS x#15, emp_no{f}#19]]
     * \_TopN[[Order[emp_no{f}#19,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     */
    public void testDoubleInlineStatsGetsPrunedEntirely() {
        var query = """
            FROM employees
            | INLINE STATS x = avg(salary) BY emp_no
            | INLINE STATS y = avg(salary) BY languages
            | EVAL y = emp_no
            | EVAL x = y
            | SORT x
            | KEEP x, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "emp_no")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var relation = as(topN.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#15 AS x#11, a{r}#7, emp_no{f}#15]]
     * \_Limit[1[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#15],[emp_no{f}#15],[emp_no{r}#15]]
     *     |_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *     \_Aggregate[[emp_no{f}#15],[COUNTDISTINCT(languages{f}#18,true[BOOLEAN]) AS a#7, emp_no{f}#15]]
     *       \_StubRelation[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, l
     *          anguages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     */
    public void testInlineStatsGetsPrunedPartially() {
        var query = """
            FROM employees
            | INLINE STATS x = AVG(salary), a = COUNT_DISTINCT(languages) BY emp_no
            | EVAL x = emp_no
            | KEEP x, a, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "a", "emp_no")));
        var upperLimit = asLimit(project.child(), 1, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("a", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    // same as above
    public void testTripleInlineStatsGetsPrunedPartially() {
        var query = """
            FROM employees
            | INLINE STATS x = AVG(salary), a = COUNT_DISTINCT(languages) BY emp_no
            | INLINE STATS y = AVG(salary), b = COUNT_DISTINCT(languages) BY emp_no
            | EVAL x = emp_no
            | INLINE STATS z = AVG(salary), c = COUNT_DISTINCT(languages), d = AVG(languages) BY last_name
            | KEEP x, a, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "a", "emp_no")));
        var upperLimit = asLimit(project.child(), 1, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("a", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * EsqlProject[[emp_no{f}#26, salaryK{r}#4, count{r}#6, min{r}#19]]
     * \_TopN[[Order[emp_no{f}#26,ASC,LAST]],5[INTEGER]]
     *   \_InlineJoin[LEFT,[salaryK{r}#4],[salaryK{r}#4]]
     *     |_EsqlProject[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34, job.raw{f}#35,
     *              languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, count{r}#6, salaryK{r}#4, hire_date_string{r}#10,
     *              date{r}#15]]
     *     | \_Dissect[hire_date_string{r}#10,Parser[pattern=%{date}, appendSeparator=,
     *            parser=org.elasticsearch.dissect.DissectParser@77d1afc3],[date{r}#15]] <-- TODO: Dissect & Eval could/should be dropped
     *     |   \_Eval[[TOSTRING(hire_date{f}#33) AS hire_date_string#10]]
     *     |     \_InlineJoin[LEFT,[salaryK{r}#4],[salaryK{r}#4]]
     *     |       |_Eval[[salary{f}#31 / 10000[INTEGER] AS salaryK#4]]
     *     |       | \_EsRelation[test][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     *     |       \_Aggregate[[salaryK{r}#4],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count#6, salaryK{r}#4]]
     *     |         \_StubRelation[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34,
     *                      job.raw{f}#35, languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, salaryK{r}#4]]
     *     \_Aggregate[[salaryK{r}#4],[MIN($$MV_COUNT(langua>$MIN$0{r$}#37,true[BOOLEAN]) AS min#19, salaryK{r}#4]]
     *       \_Eval[[MVCOUNT(languages{f}#29) AS $$MV_COUNT(langua>$MIN$0#37]]
     *         \_StubRelation[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34, job.raw{f}#35,
     *              languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, count{r}#6, salaryK{r}#4, sum{r}#13,
     *              hire_date_string{r}#10, date{r}#15, $$MV_COUNT(langua>$MIN$0{r$}#37]]
     */
    public void testTripleInlineStatsMultipleAssignmentsGetsPrunedPartially() {
        // TODO: reenable 1st sort, pull the 2nd further up when #132417 is in
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | DISSECT hire_date_string "%{date}"
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no, salaryK, count, min
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var employeesFields = List.of(
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
        );

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "salaryK", "count", "min")));
        var topN = as(project.child(), TopN.class);
        var outerinline = as(topN.child(), InlineJoin.class);
        //
        var expectedOutterOutput = new ArrayList<>(employeesFields);
        expectedOutterOutput.addAll(List.of("count", "hire_date_string", "date", "min", "salaryK"));
        assertThat(Expressions.names(outerinline.output()), is(expectedOutterOutput));
        // outer left
        var outerProject = as(outerinline.left(), Project.class);
        var dissect = as(outerProject.child(), Dissect.class);
        var eval = as(dissect.child(), Eval.class);
        var innerinline = as(eval.child(), InlineJoin.class);
        var expectedInnerOutput = new ArrayList<>(employeesFields);
        expectedInnerOutput.addAll(List.of("count", "salaryK"));
        assertThat(Expressions.names(innerinline.output()), is(expectedInnerOutput));
        // inner left
        eval = as(innerinline.left(), Eval.class);
        var relation = as(eval.child(), EsRelation.class);
        // inner right
        var agg = as(innerinline.right(), Aggregate.class);
        var stub = as(agg.child(), StubRelation.class);
        // outer right
        agg = as(outerinline.right(), Aggregate.class);
        eval = as(agg.child(), Eval.class);
        stub = as(eval.child(), StubRelation.class);
    }

    /*
     * EsqlProject[[emp_no{f}#917]]
     * \_TopN[[Order[emp_no{f}#917,ASC,LAST]],5[INTEGER]]
     *   \_Dissect[hire_date_string{r}#898,Parser[pattern=%{date}, appendSeparator=,
     *          parser=org.elasticsearch.dissect.DissectParser@46132aa7],[date{r}#903]] <-- TODO: Dissect & Eval could/should be dropped
     *     \_Eval[[TOSTRING(hire_date{f}#918) AS hire_date_string#898]]
     *       \_EsRelation[employees][emp_no{f}#917, hire_date{f}#918, languages{f}#913, ..]
     */
    public void testTripleInlineStatsMultipleAssignmentsGetsPrunedEntirely() {
        // same as the above query, but only keep emp_no
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | DISSECT hire_date_string "%{date}"
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));
        var topN = as(project.child(), TopN.class);
        var dissect = as(topN.child(), Dissect.class);
        var eval = as(dissect.child(), Eval.class);
        var relation = as(eval.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#1556]]
     * \_TopN[[Order[emp_no{f}#1556,ASC,LAST]],5[INTEGER]]
     *   \_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1561]]
     *     |_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1560]]
     *     | |_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1559]]
     *     | | |_EsRelation[employees][emp_no{f}#1556, hire_date{f}#1557, languages{f}#155..]
     *     | | \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1559]
     *     | \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1560]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1561]
     */
    public void testTripleInlineStatsWithLookupJoinGetsPrunedEntirely() {
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));
        var topN = as(project.child(), TopN.class);

        var outterjoin = as(topN.child(), Join.class);
        var middlejoin = as(outterjoin.left(), Join.class);
        var innerjoin = as(middlejoin.left(), Join.class);

        var innerJoinLeftRelation = as(innerjoin.left(), EsRelation.class);
        var innerJoinRightRelation = as(innerjoin.right(), EsRelation.class);

        var middleJoinRightRelation = as(middlejoin.right(), EsRelation.class);
        var outerJoinRightRelation = as(outterjoin.right(), EsRelation.class);
    }

    /*
     * Project[[avg{r}#14, decades{r}#10]]
     * \_Eval[[$$SUM$avg$0{r$}#35 / $$COUNT$avg$1{r$}#36 AS avg#14]]
     *   \_Limit[1000[INTEGER],false]
     *     \_Aggregate[[decades{r}#10],[SUM(salary{f}#29,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$avg$0#35, COUNT(salary{f}#29,
     *              true[BOOLEAN]) AS $$COUNT$avg$1#36, decades{r}#10]]
     *       \_Eval[[DATEDIFF(years[KEYWORD],hire_date{f}#31,1755625790494[DATETIME]) AS age#4, age{r}#4 / 10[INTEGER] AS decades#7,
     *                   decades{r}#7 * 10[INTEGER] AS decades#10]]
     *         \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     */
    public void testInlineStatsWithAggGetsPrunedEntirely() {
        var query = """
            FROM employees
            | EVAL age = DATE_DIFF("years", hire_date, NOW())
            | EVAL decades = age / 10, decades = decades * 10
            | STATS avg = AVG(salary) BY decades
            | EVAL idecades = decades / 2
            | INLINE STATS iavg = AVG(avg) BY idecades
            | KEEP avg, decades
            """;

        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "decades")));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var aggregate = as(limit.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        var source = as(eval.child(), EsRelation.class);
    }

    /*
     * EsqlProject[[avg{r}#1053, decades{r}#1049, avgavg{r}#1063]]
     * \_Limit[1000[INTEGER],true]
     *   \_InlineJoin[LEFT,[],[],[]]
     *     |_Project[[avg{r}#1053, decades{r}#1049, idecades{r}#1056]]
     *     | \_Eval[[$$SUM$avg$0{r$}#1073 / $$COUNT$avg$1{r$}#1074 AS avg#1053, decades{r}#1049 / 2[INTEGER] AS idecades#1056]]
     *     |   \_Aggregate[[decades{r}#1049],[SUM(salary{f}#1072,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$avg$0#1073,
     *                      COUNT(salary{f}#1072,true[BOOLEAN]) AS $$COUNT$avg$1#1074, decades{r}#1049]]
     *     |     \_Eval[[DATEDIFF(years[KEYWORD],birth_date{f}#1071,1755626308505[DATETIME]) AS age#1043, age{r}#1043 / 10[INTEGER] AS
     *                          decades#1046, decades{r}#1046 * 10[INTEGER] AS decades#1049]]
     *     |       \_EsRelation[employees][birth_date{f}#1071, salary{f}#1072]
     *     \_Project[[avgavg{r}#1063]]
     *       \_Eval[[$$SUM$avgavg$0{r$}#1077 / $$COUNT$avgavg$1{r$}#1078 AS avgavg#1063]]
     *         \_Aggregate[[],[SUM(avg{r}#1053,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$avgavg$0#1077,
     *                      COUNT(avg{r}#1053,true[BOOLEAN]) AS $$COUNT$avgavg$1#1078]]
     *           \_StubRelation[[avg{r}#1053, decades{r}#1049, iavg{r}#1059, idecades{r}#1056]]
     */
    public void testInlineStatsWithAggAndInlineStatsGetsPruned() {
        var query = """
            FROM employees
            | EVAL age = DATE_DIFF("years", hire_date, NOW())
            | EVAL decades = age / 10, decades = decades * 10
            | STATS avg = AVG(salary) BY decades
            | EVAL idecades = decades / 2
            | INLINE STATS iavg = AVG(avg) BY idecades
            | INLINE STATS avgavg = AVG(avg)
            | KEEP avg, decades, avgavg
            """;

        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "decades", "avgavg")));
        var limit = asLimit(project.child(), 1000, false);
        var inlineJoin = as(limit.child(), InlineJoin.class);

        // Left branch: Project with avg, decades, idecades
        var leftProject = as(inlineJoin.left(), Project.class);
        assertThat(Expressions.names(leftProject.projections()), is(List.of("avg", "decades", "idecades")));
        var leftEval = as(leftProject.child(), Eval.class);
        var leftAggregate = as(leftEval.child(), Aggregate.class);
        assertThat(Expressions.names(leftAggregate.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "decades")));
        var leftEval2 = as(leftAggregate.child(), Eval.class);
        var leftRelation = as(leftEval2.child(), EsRelation.class);

        // Right branch: Project with avgavg
        var rightProject = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(rightProject.projections()), is(List.of("avgavg")));
        var rightEval = as(rightProject.child(), Eval.class);
        var rightAggregate = as(rightEval.child(), Aggregate.class);
        assertThat(Expressions.names(rightAggregate.output()), is(List.of("$$SUM$avgavg$0", "$$COUNT$avgavg$1")));
        assertThat(rightAggregate.groupings(), empty());
        var rightStub = as(rightAggregate.child(), StubRelation.class);
    }

    /*
     * Project[[abbrev{f}#19, scalerank{f}#21 AS backup_scalerank#4, language_name{f}#28 AS scalerank#11]]
     * \_TopN[[Order[abbrev{f}#19,DESC,FIRST]],5[INTEGER]]
     *   \_Join[LEFT,[scalerank{f}#21],[scalerank{f}#21],[language_code{f}#27]]
     *     |_EsRelation[airports][abbrev{f}#19, city{f}#25, city_location{f}#26, coun..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#27, language_name{f}#28]
     */
    public void testInlineStatsWithLookupJoin() {
        var query = """
            FROM airports
            | EVAL backup_scalerank = scalerank
            | RENAME scalerank AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name as scalerank
            | DROP language_code
            | INLINE STATS count=COUNT(*) BY scalerank
            | SORT abbrev DESC
            | KEEP abbrev, *scalerank
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }

        var plan = planAirports(query);
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("abbrev", "backup_scalerank", "scalerank")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), equalTo("abbrev"));
        var join = as(topN.child(), Join.class);
        assertThat(Expressions.names(join.config().leftFields()), is(List.of("scalerank")));
        var left = as(join.left(), EsRelation.class);
        assertThat(left.concreteIndices(), is(Set.of("airports")));
        var right = as(join.right(), EsRelation.class);
        assertThat(right.concreteIndices(), is(Set.of("languages_lookup")));
    }

    /*
     * EsqlProject[[avg{r}#4, emp_no{f}#9, first_name{f}#10]]
     * \_Limit[10[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#9],[emp_no{f}#9],[emp_no{r}#9]]
     *     |_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_Project[[avg{r}#4, emp_no{f}#9]]
     *       \_Eval[[$$SUM$avg$0{r$}#20 / $$COUNT$avg$1{r$}#21 AS avg#4]]
     *         \_Aggregate[[emp_no{f}#9],[SUM(salary{f}#14,true[BOOLEAN]) AS $$SUM$avg$0#20, COUNT(salary{f}#14,true[BOOLEAN]) AS $$COUNT$
     *              avg$1#21, emp_no{f}#9]]
     *           \_StubRelation[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     */
    public void testInlineStatsWithAvg() {
        var query = """
            FROM employees
            | INLINE STATS avg = AVG(salary) BY emp_no
            | KEEP avg, emp_no, first_name
            | LIMIT 10
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var esqlProject = as(plan, EsqlProject.class);
        assertThat(Expressions.names(esqlProject.projections()), is(List.of("avg", "emp_no", "first_name")));
        var upperLimit = asLimit(esqlProject.child(), 10, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var project = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), contains("avg", "emp_no"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * EsqlProject[[emp_no{r}#5]]
     * \_Limit[1000[INTEGER],false]
     *   \_LocalRelation[[salary{r}#3, emp_no{r}#5, gender{r}#7],
     *      org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@9d5b596d]
     */
    public void testInlineStatsWithRow() {
        var query = """
            ROW salary = 12300, emp_no = 5, gender = "F"
            | EVAL salaryK = salary/1000
            | INLINE STATS sum = SUM(salaryK) BY gender
            | KEEP emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var esqlProject = as(plan, Project.class);
        assertThat(Expressions.names(esqlProject.projections()), is(List.of("emp_no")));
        var limit = asLimit(esqlProject.child(), 1000, false);
        var localRelation = as(limit.child(), LocalRelation.class);
        assertThat(
            localRelation.output(),
            containsIgnoringIds(
                new ReferenceAttribute(EMPTY, "salary", INTEGER),
                new ReferenceAttribute(EMPTY, "emp_no", INTEGER),
                new ReferenceAttribute(EMPTY, "gender", KEYWORD)
            )
        );
    }

    /*
     * EsqlProject[[a{r}#4]]
     * \_Limit[2[INTEGER],false]
     *   \_InlineJoin[LEFT,[],[],[]]
     *     |_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *     \_Project[[a{r}#4]]
     *       \_Eval[[$$SUM$a$0{r$}#17 / $$COUNT$a$1{r$}#18 AS a#4]]
     *         \_Aggregate[[],[SUM(salary{f}#11,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$a$0#17,
     *                  COUNT(salary{f}#11,true[BOOLEAN]) AS $$COUNT$a$1#18]]
     *           \_StubRelation[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                      languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     */
    public void testInlineStatsWithLimit() {
        var query = """
            FROM employees
            | INLINE STATS a = AVG(salary)
            | LIMIT 2
            | KEEP a
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), is(List.of("a")));
        var limit = asLimit(project.child(), 2, false);
        var inlineJoin = as(limit.child(), InlineJoin.class);
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var rightProject = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(rightProject.projections()), contains("a"));
        var eval = as(rightProject.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("a")));
        var agg = as(eval.child(), Aggregate.class);
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Limit[10000[INTEGER],false]
     * \_Aggregate[[],[VALUES(max_lang{r}#7,true[BOOLEAN]) AS v#11]]
     *   \_Limit[1[INTEGER],false]
     *     \_InlineJoin[LEFT,[gender{f}#14],[gender{f}#14],[gender{r}#14]]
     *       |_EsqlProject[[emp_no{f}#12, languages{f}#15, gender{f}#14]]
     *       | \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *       \_Aggregate[[gender{f}#14],[MAX(languages{f}#15,true[BOOLEAN]) AS max_lang#7, gender{f}#14]]
     *         \_StubRelation[[emp_no{f}#12, languages{f}#15, gender{f}#14]]
     */
    public void testInlineStatsWithLimitAndAgg() {
        var query = """
            FROM employees
            | KEEP emp_no, languages, gender
            | INLINE STATS max_lang = MAX(languages) BY gender
            | LIMIT 1
            | STATS v = VALUES(max_lang)
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var limit = asLimit(plan, 10000, false);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), is(List.of("v")));
        var innerLimit = asLimit(aggregate.child(), 1, false);
        var inlineJoin = as(innerLimit.child(), InlineJoin.class);
        // Left
        var project = as(inlineJoin.left(), EsqlProject.class);
        var relation = as(project.child(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * EsqlProject[[c{r}#7, b{r}#5, a{r}#14]]
     * \_Eval[[[KEYWORD] AS a#14]]
     *   \_Limit[1000[INTEGER],false]
     *     \_LocalRelation[[a{r}#3, b{r}#5, c{r}#7],Page{blocks=[ConstantNullBlock[positions=1], IntVectorBlock[vector=ConstantIntVector[p
     *          ositions=1, value=0]], BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[]]]]}]
     */
    public void testInlineStatsWithShadowedOutput() {
        var query = """
            ROW a = null, b = 0, c = ""
            | INLINE STATS a = MAX(c) BY b
            | EVAL a = c
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("c", "b", "a")));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var localRelation = as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * <pre>{@code
     * Project[[salary{f}#19, languages{f}#17, emp_no{f}#14]]
     * \_TopN[[Order[$$order_by$0$0{r}#24,ASC,LAST], Order[emp_no{f}#14,DESC,FIRST]],1000[INTEGER]]
     *   \_Eval[[salary{f}#19 / 10000[INTEGER] + languages{f}#17 AS $$order_by$0$0]]
     *     \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }</pre>
     */
    public void testReplaceSortByExpressionsMultipleSorts() {
        var plan = optimizedPlan("""
            from test
            | sort salary/10000 + languages, emp_no desc
            | eval d = emp_no
            | sort salary/10000 + languages, d desc
            | keep salary, languages, emp_no
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("salary", "languages", "emp_no"));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(2));

        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        ReferenceAttribute expression = as(order.child(), ReferenceAttribute.class);
        assertThat(expression.toString(), startsWith("$$order_by$0$"));

        order = as(topN.order().get(1), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        FieldAttribute empNo = as(order.child(), FieldAttribute.class);
        assertThat(empNo.name(), equalTo("emp_no"));

        var eval = as(topN.child(), Eval.class);
        var fields = eval.fields();
        assertThat(fields.size(), equalTo(1));
        assertThat(Expressions.attribute(fields.get(0)), is(Expressions.attribute(expression)));
        Alias salaryAddLanguages = eval.fields().get(0);
        var add = as(salaryAddLanguages.child(), Add.class);
        var div = as(add.left(), Div.class);
        var salary = as(div.left(), FieldAttribute.class);
        assertThat(salary.name(), equalTo("salary"));
        var _10000 = as(div.right(), Literal.class);
        assertThat(_10000.value(), equalTo(10000));
        var languages = as(add.right(), FieldAttribute.class);
        assertThat(languages.name(), equalTo("languages"));

        as(eval.child(), EsRelation.class);
    }

    /**
     * For DISSECT expects the following; the others are similar.
     *
     * <pre>{@code
     * Project[[first_name{f}#37, emp_no{r}#30, salary{r}#31]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#46,ASC,LAST], Order[$$order_by$temp_name$1{r}#47,DESC,FIRST]],3[INTEGER]]
     *   \_Dissect[first_name{f}#37,Parser[pattern=%{emp_no} %{salary}, appendSeparator=,
     *   parser=org.elasticsearch.dissect.DissectParser@87f460f],[emp_no{r}#30, salary{r}#31]]
     *     \_Eval[[emp_no{f}#36 + salary{f}#41 * 13[INTEGER] AS $$order_by$temp_name$0, NEG(salary{f}#41) AS $$order_by$temp_name$1]]
     *       \_EsRelation[test][_meta_field{f}#42, emp_no{f}#36, first_name{f}#37, ..]
     * }</pre>
     */
    public void testReplaceSortByExpressions() {
        List<String> overwritingCommands = List.of(
            "EVAL emp_no = 3*emp_no, salary = -2*emp_no-salary",
            "DISSECT first_name \"%{emp_no} %{salary}\"",
            "GROK first_name \"%{WORD:emp_no} %{WORD:salary}\"",
            "ENRICH languages_idx ON first_name WITH emp_no = language_code, salary = language_code"
        );

        String queryTemplateKeepAfter = """
            FROM test
            | SORT 13*(emp_no+salary) ASC, -salary DESC
            | {}
            | KEEP first_name, emp_no, salary
            | LIMIT 3
            """;
        // Equivalent but with KEEP first - ensures that attributes in the final projection are correct after pushdown rules were applied.
        String queryTemplateKeepFirst = """
            FROM test
            | KEEP emp_no, salary, first_name
            | SORT 13*(emp_no+salary) ASC, -salary DESC
            | {}
            | LIMIT 3
            """;

        for (String overwritingCommand : overwritingCommands) {
            String queryTemplate = randomBoolean() ? queryTemplateKeepFirst : queryTemplateKeepAfter;
            var plan = optimizedPlan(LoggerMessageFormat.format(null, queryTemplate, overwritingCommand));

            var project = as(plan, Project.class);
            var projections = project.projections();
            assertThat(projections.size(), equalTo(3));
            assertThat(projections.get(0).name(), equalTo("first_name"));
            assertThat(projections.get(1).name(), equalTo("emp_no"));
            assertThat(projections.get(2).name(), equalTo("salary"));

            var topN = as(project.child(), TopN.class);
            assertThat(topN.order().size(), is(2));

            var firstOrderExpr = as(topN.order().get(0), Order.class);
            assertThat(firstOrderExpr.direction(), equalTo(Order.OrderDirection.ASC));
            assertThat(firstOrderExpr.nullsPosition(), equalTo(Order.NullsPosition.LAST));
            var renamedEmpNoSalaryExpression = as(firstOrderExpr.child(), ReferenceAttribute.class);
            assertThat(renamedEmpNoSalaryExpression.toString(), startsWith("$$order_by$0$"));

            var secondOrderExpr = as(topN.order().get(1), Order.class);
            assertThat(secondOrderExpr.direction(), equalTo(Order.OrderDirection.DESC));
            assertThat(secondOrderExpr.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
            var renamedNegatedSalaryExpression = as(secondOrderExpr.child(), ReferenceAttribute.class);
            assertThat(renamedNegatedSalaryExpression.toString(), startsWith("$$order_by$1$"));

            Eval renamingEval = null;
            if (overwritingCommand.startsWith("EVAL")) {
                // Multiple EVALs should be merged, so there's only one.
                renamingEval = as(topN.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("DISSECT")) {
                var dissect = as(topN.child(), Dissect.class);
                renamingEval = as(dissect.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("GROK")) {
                var grok = as(topN.child(), Grok.class);
                renamingEval = as(grok.child(), Eval.class);
            }
            if (overwritingCommand.startsWith("ENRICH")) {
                var enrich = as(topN.child(), Enrich.class);
                renamingEval = as(enrich.child(), Eval.class);
            }

            assertThat(renamingEval.fields().size(), anyOf(equalTo(2), equalTo(4))); // 4 for EVAL, 2 for the other overwritingCommands

            // 13*(emp_no+salary)
            Alias _13empNoSalary = renamingEval.fields().get(0);
            assertThat(_13empNoSalary.toAttribute(), equalTo(renamedEmpNoSalaryExpression));
            var mul = as(_13empNoSalary.child(), Mul.class);
            var add = as(mul.left(), Add.class);
            var emp_no = as(add.left(), FieldAttribute.class);
            assertThat(emp_no.name(), equalTo("emp_no"));
            var salary = as(add.right(), FieldAttribute.class);
            assertThat(salary.name(), equalTo("salary"));
            var _13 = as(mul.right(), Literal.class);
            assertThat(_13.value(), equalTo(13));

            // -salary
            Alias negatedSalary = renamingEval.fields().get(1);
            assertThat(negatedSalary.toAttribute(), equalTo(renamedNegatedSalaryExpression));
            var neg = as(negatedSalary.child(), Neg.class);
            assertThat(neg.field(), equalTo(salary));

            assertThat(renamingEval.child(), instanceOf(EsRelation.class));
        }
    }

    public void testPartiallyFoldCase() {
        var plan = optimizedPlan("""
              FROM test
            | EVAL c = CASE(true, emp_no, salary)
            """);

        var eval = as(plan, Eval.class);
        var languages = as(Alias.unwrap(eval.expressions().get(0)), FieldAttribute.class);
        assertThat(languages.name(), is("emp_no"));
    }

    private EsqlBinaryComparison extractPlannedBinaryComparison(String expression) {
        LogicalPlan plan = planTypes("FROM types | WHERE " + expression);

        return extractPlannedBinaryComparison(plan);
    }

    private static EsqlBinaryComparison extractPlannedBinaryComparison(LogicalPlan plan) {
        assertTrue("Expected unary plan, found [" + plan + "]", plan instanceof UnaryPlan);
        UnaryPlan unaryPlan = (UnaryPlan) plan;
        assertTrue("Epxected top level Filter, foung [" + unaryPlan.child().toString() + "]", unaryPlan.child() instanceof Filter);
        Filter filter = (Filter) unaryPlan.child();
        assertTrue(
            "Expected filter condition to be a binary comparison but found [" + filter.condition() + "]",
            filter.condition() instanceof EsqlBinaryComparison
        );
        return (EsqlBinaryComparison) filter.condition();
    }

    private void doTestSimplifyComparisonArithmetics(
        String expression,
        String fieldName,
        EsqlBinaryComparison.BinaryComparisonOperation opType,
        Object bound
    ) {
        EsqlBinaryComparison bc = extractPlannedBinaryComparison(expression);
        assertEquals(opType, bc.getFunctionType());

        assertTrue(
            "Expected left side of comparison to be a field attribute but found [" + bc.left() + "]",
            bc.left() instanceof FieldAttribute
        );
        FieldAttribute attribute = (FieldAttribute) bc.left();
        assertEquals(fieldName, attribute.name());

        assertTrue("Expected right side of comparison to be a literal but found [" + bc.right() + "]", bc.right() instanceof Literal);
        Literal literal = (Literal) bc.right();
        assertEquals(bound, literal.value());
    }

    private void assertSemanticMatching(String expected, String provided) {
        BinaryComparison bc = extractPlannedBinaryComparison(provided);
        LogicalPlan exp = analyzerTypes.analyze(parser.createStatement("FROM types | WHERE " + expected));
        // Exp is created separately, so the IDs will be different - ignore them for the comparison.
        assertSemanticMatching((BinaryComparison) ignoreIds(bc), (EsqlBinaryComparison) ignoreIds(extractPlannedBinaryComparison(exp)));
    }

    private static void assertSemanticMatching(Expression fieldAttributeExp, Expression unresolvedAttributeExp) {
        Expression unresolvedUpdated = unresolvedAttributeExp.transformUp(
            LITERALS_ON_THE_RIGHT.expressionToken(),
            be -> LITERALS_ON_THE_RIGHT.rule(be, logicalOptimizerCtx)
        ).transformUp(x -> x.foldable() ? new Literal(x.source(), x.fold(FoldContext.small()), x.dataType()) : x);

        List<Expression> resolvedFields = fieldAttributeExp.collectFirstChildren(x -> x instanceof FieldAttribute);
        for (Expression field : resolvedFields) {
            FieldAttribute fa = (FieldAttribute) field;
            unresolvedUpdated = unresolvedUpdated.transformDown(UnresolvedAttribute.class, x -> x.name().equals(fa.name()) ? fa : x);
        }

        assertTrue(unresolvedUpdated.semanticEquals(fieldAttributeExp));
    }

    private Expression getComparisonFromLogicalPlan(LogicalPlan plan) {
        List<Expression> expressions = new ArrayList<>();
        plan.forEachExpression(Expression.class, expressions::add);
        return expressions.get(0);
    }

    private void assertNotSimplified(String comparison) {
        String query = "FROM types | WHERE " + comparison;
        Expression optimized = getComparisonFromLogicalPlan(planTypes(query));
        Expression raw = getComparisonFromLogicalPlan(analyzerTypes.analyze(parser.createStatement(query)));

        assertTrue(raw.semanticEquals(optimized));
    }

    private static String randomBinaryComparison() {
        return randomFrom(EsqlBinaryComparison.BinaryComparisonOperation.values()).symbol();
    }

    public void testSimplifyComparisonArithmeticCommutativeVsNonCommutativeOps() {
        doTestSimplifyComparisonArithmetics("integer + 2 > 3", "integer", GT, 1);
        doTestSimplifyComparisonArithmetics("2 + integer > 3", "integer", GT, 1);
        doTestSimplifyComparisonArithmetics("integer - 2 > 3", "integer", GT, 5);
        doTestSimplifyComparisonArithmetics("2 - integer > 3", "integer", LT, -1);
        doTestSimplifyComparisonArithmetics("integer * 2 > 4", "integer", GT, 2);
        doTestSimplifyComparisonArithmetics("2 * integer > 4", "integer", GT, 2);

    }

    public void testSimplifyComparisonArithmeticsWithFloatingPoints() {
        doTestSimplifyComparisonArithmetics("float / 2 > 4", "float", GT, 8d);
    }

    public void testAssertSemanticMatching() {
        // This test is just to verify that the complicated assert logic is working on a known-good case
        assertSemanticMatching("integer > 1", "integer + 2 > 3");
    }

    public void testSimplyComparisonArithmeticWithUnfoldedProd() {
        assertSemanticMatching("integer * integer >= 3", "((integer * integer + 1) * 2 - 4) * 4 >= 16");
    }

    public void testSimplifyComparisonArithmeticWithMultipleOps() {
        // i >= 3
        doTestSimplifyComparisonArithmetics("((integer + 1) * 2 - 4) * 4 >= 16", "integer", GTE, 3);
    }

    public void testSimplifyComparisonArithmeticWithFieldNegation() {
        doTestSimplifyComparisonArithmetics("12 * (-integer - 5) >= -120", "integer", LTE, 5);
    }

    public void testSimplifyComparisonArithmeticWithFieldDoubleNegation() {
        doTestSimplifyComparisonArithmetics("12 * -(-integer - 5) <= 120", "integer", LTE, 5);
    }

    public void testSimplifyComparisonArithmeticWithConjunction() {
        doTestSimplifyComparisonArithmetics("12 * (-integer - 5) == -120 AND integer < 6 ", "integer", EQ, 5);
    }

    public void testSimplifyComparisonArithmeticWithDisjunction() {
        doTestSimplifyComparisonArithmetics("12 * (-integer - 5) >= -120 OR integer < 5", "integer", LTE, 5);
    }

    public void testSimplifyComparisonArithmeticWithFloatsAndDirectionChange() {
        doTestSimplifyComparisonArithmetics("float / -2 < 4", "float", GT, -8d);
        doTestSimplifyComparisonArithmetics("float * -2 < 4", "float", GT, -2d);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/108519")
    public void testSimplifyComparisonArithmeticSkippedOnIntegerArithmeticalOverflow() {
        assertNotSimplified("integer - 1 " + randomBinaryComparison() + " " + Long.MAX_VALUE);
        assertNotSimplified("1 - integer " + randomBinaryComparison() + " " + Long.MIN_VALUE);
        assertNotSimplified("integer - 1 " + randomBinaryComparison() + " " + Integer.MAX_VALUE);
        assertNotSimplified("1 - integer " + randomBinaryComparison() + " " + Integer.MIN_VALUE);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/108519")
    public void testSimplifyComparisonArithmeticSkippedOnNegatingOverflow() {
        assertNotSimplified("-integer " + randomBinaryComparison() + " " + Long.MIN_VALUE);
        assertNotSimplified("-integer " + randomBinaryComparison() + " " + Integer.MIN_VALUE);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/108519")
    public void testSimplifyComparisonArithmeticSkippedOnDateOverflow() {
        assertNotSimplified("date - 999999999 years > to_datetime(\"2010-01-01T01:01:01\")");
        assertNotSimplified("date + -999999999 years > to_datetime(\"2010-01-01T01:01:01\")");
    }

    public void testSimplifyComparisonArithmeticSkippedOnMulDivByZero() {
        assertNotSimplified("float / 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("float * 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("integer / 0 " + randomBinaryComparison() + " 1");
        assertNotSimplified("integer * 0 " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnDiv() {
        assertNotSimplified("integer / 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 / integer " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnResultingFloatLiteral() {
        assertNotSimplified("integer * 2 " + randomBinaryComparison() + " 3");
        assertNotSimplified("float * 4.0 " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnFloatFieldWithPlusMinus() {
        assertNotSimplified("float + 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 + float " + randomBinaryComparison() + " 1");
        assertNotSimplified("float - 4 " + randomBinaryComparison() + " 1");
        assertNotSimplified("4 - float " + randomBinaryComparison() + " 1");
    }

    public void testSimplifyComparisonArithmeticSkippedOnFloats() {
        for (String field : List.of("integer", "float")) {
            for (Tuple<? extends Number, ? extends Number> nr : List.of(new Tuple<>(.4, 1), new Tuple<>(1, .4))) {
                assertNotSimplified(field + " + " + nr.v1() + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(field + " - " + nr.v1() + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(nr.v1() + " + " + field + " " + randomBinaryComparison() + " " + nr.v2());
                assertNotSimplified(nr.v1() + " - " + field + " " + randomBinaryComparison() + " " + nr.v2());
            }
        }
    }

    public void testReplaceStringCasingWithInsensitiveEqualsUpperFalse() {
        var plan = optimizedPlan("FROM test | WHERE TO_UPPER(first_name) == \"VALÜe\"");
        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    public void testReplaceStringCasingWithInsensitiveEqualsUpperTrue() {
        var plan = optimizedPlan("FROM test | WHERE TO_UPPER(first_name) != \"VALÜe\"");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var isNotNull = as(filter.condition(), IsNotNull.class);
        assertThat(Expressions.name(isNotNull.field()), is("first_name"));
        as(filter.child(), EsRelation.class);
    }

    public void testReplaceStringCasingWithInsensitiveEqualsLowerFalse() {
        var plan = optimizedPlan("FROM test | WHERE TO_LOWER(first_name) == \"VALÜe\"");
        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    public void testReplaceStringCasingWithInsensitiveEqualsLowerTrue() {
        var plan = optimizedPlan("FROM test | WHERE TO_LOWER(first_name) != \"VALÜe\"");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(IsNotNull.class));
        as(filter.child(), EsRelation.class);
    }

    public void testReplaceStringCasingWithInsensitiveEqualsEquals() {
        for (var fn : List.of("TO_LOWER", "TO_UPPER")) {
            var value = fn.equals("TO_LOWER") ? fn.toLowerCase(Locale.ROOT) : fn.toUpperCase(Locale.ROOT);
            value += "🐔✈🔥🎉"; // these should not cause folding, they're not in the upper/lower char class
            var plan = optimizedPlan("FROM test | WHERE " + fn + "(first_name) == \"" + value + "\"");
            var limit = as(plan, Limit.class);
            var filter = as(limit.child(), Filter.class);
            var insensitive = as(filter.condition(), InsensitiveEquals.class);
            as(insensitive.left(), FieldAttribute.class);
            var bRef = as(insensitive.right().fold(FoldContext.small()), BytesRef.class);
            assertThat(bRef.utf8ToString(), is(value));
            as(filter.child(), EsRelation.class);
        }
    }

    public void testReplaceStringCasingWithInsensitiveEqualsNotEquals() {
        for (var fn : List.of("TO_LOWER", "TO_UPPER")) {
            var value = fn.equals("TO_LOWER") ? fn.toLowerCase(Locale.ROOT) : fn.toUpperCase(Locale.ROOT);
            value += "🐔✈🔥🎉"; // these should not cause folding, they're not in the upper/lower char class
            var plan = optimizedPlan("FROM test | WHERE " + fn + "(first_name) != \"" + value + "\"");
            var limit = as(plan, Limit.class);
            var filter = as(limit.child(), Filter.class);
            var not = as(filter.condition(), Not.class);
            var insensitive = as(not.field(), InsensitiveEquals.class);
            as(insensitive.left(), FieldAttribute.class);
            var bRef = as(insensitive.right().fold(FoldContext.small()), BytesRef.class);
            assertThat(bRef.utf8ToString(), is(value));
            as(filter.child(), EsRelation.class);
        }
    }

    public void testReplaceStringCasingWithInsensitiveEqualsUnwrap() {
        var plan = optimizedPlan("FROM test | WHERE TO_UPPER(TO_LOWER(TO_UPPER(first_name))) == \"VALÜ\"");
        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var insensitive = as(filter.condition(), InsensitiveEquals.class);
        var field = as(insensitive.left(), FieldAttribute.class);
        assertThat(field.fieldName().string(), is("first_name"));
        var bRef = as(insensitive.right().fold(FoldContext.small()), BytesRef.class);
        assertThat(bRef.utf8ToString(), is("VALÜ"));
        as(filter.child(), EsRelation.class);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    //
    // Lookup
    //

    /**
     * Expects
     * <pre>{@code
     * Join[JoinConfig[type=LEFT OUTER, matchFields=[int{r}#4], conditions=[LOOKUP int_number_names ON int]]]
     * |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, job{f}#13, job.raw{f}#14, languages{f}#9 AS int
     * , last_name{f}#10, long_noidx{f}#15, salary{f}#11]]
     * | \_Limit[1000[INTEGER]]
     * |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * \_LocalRelation[[int{f}#16, name{f}#17],[IntVectorBlock[vector=IntArrayVector[positions=10, values=[0, 1, 2, 3, 4, 5, 6, 7, 8,
     * 9]]], BytesRefVectorBlock[vector=BytesRefArrayVector[positions=10]]]]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "lookup functionality is not yet implemented")
    public void testLookupSimple() {
        String query = """
              FROM test
            | RENAME languages AS int
            | LOOKUP_?? int_number_names ON int""";
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:3: mismatched input 'LOOKUP' expecting {"));
            return;
        }
        var plan = optimizedPlan(query);
        var join = as(plan, Join.class);

        // Right is the lookup table
        var right = as(join.right(), LocalRelation.class);
        assertMap(
            right.output().stream().map(Object::toString).sorted().toList(),
            matchesList().item(containsString("int{f}")).item(containsString("name{f}"))
        );

        // Left is the rest of the query
        var left = as(join.left(), EsqlProject.class);
        assertThat(left.output().toString(), containsString("int{r}"));
        var limit = as(left.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        assertThat(join.config().leftFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(join.config().leftFields().size(), equalTo(1));
        assertThat(join.config().rightFields().size(), equalTo(1));
        Attribute lhs = join.config().leftFields().get(0);
        Attribute rhs = join.config().rightFields().get(0);
        assertThat(lhs.toString(), startsWith("int{r}"));
        assertThat(rhs.toString(), startsWith("int{r}"));
        assertTrue(join.children().get(0).outputSet() + " contains " + lhs, join.children().get(0).outputSet().contains(lhs));
        assertTrue(join.children().get(1).outputSet() + " contains " + rhs, join.children().get(1).outputSet().contains(rhs));

        // TODO: this needs to be fixed
        // Join's output looks sensible too
        assertMap(
            join.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                // TODO prune unused columns down through the join
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                /*
                 * Int is a reference here because we renamed it in project.
                 * If we hadn't it'd be a field and that'd be fine.
                 */
                .item(containsString("int{r}"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
                /*
                 * It's important that name is returned as a *reference* here
                 * instead of a field. If it were a field we'd use SearchStats
                 * on it and discover that it doesn't exist in the index. It doesn't!
                 * We don't expect it to. It exists only in the lookup table.
                 */
                .item(containsString("name"))
        );
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[name{r}#20],[MIN(emp_no{f}#9) AS MIN(emp_no), name{r}#20]]
     *   \_Join[JoinConfig[type=LEFT OUTER, matchFields=[int{r}#4], conditions=[LOOKUP int_number_names ON int]]]
     *     |_EsqlProject[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, job{f}#16, job.raw{f}#17, languages{f}#12 AS
     * int, last_name{f}#13, long_noidx{f}#18, salary{f}#14]]
     *     | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_LocalRelation[[int{f}#19, name{f}#20],[IntVectorBlock[vector=IntArrayVector[positions=10, values=[0, 1, 2, 3, 4, 5, 6, 7, 8,
     * 9]]], BytesRefVectorBlock[vector=BytesRefArrayVector[positions=10]]]]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "lookup functionality is not yet implemented")
    public void testLookupStats() {
        String query = """
              FROM test
            | RENAME languages AS int
            | LOOKUP_?? int_number_names ON int
            | STATS MIN(emp_no) BY name""";
        if (Build.current().isSnapshot() == false) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:3: mismatched input 'LOOKUP' expecting {"));
            return;
        }
        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        var agg = as(limit.child(), Aggregate.class);
        assertMap(
            agg.aggregates().stream().map(Object::toString).sorted().toList(),
            matchesList().item(startsWith("MIN(emp_no)")).item(startsWith("name"))
        );
        assertMap(agg.groupings().stream().map(Object::toString).toList(), matchesList().item(startsWith("name")));

        var join = as(agg.child(), Join.class);
        // Right is the lookup table
        var right = as(join.right(), LocalRelation.class);
        assertMap(
            right.output().stream().map(Object::toString).toList(),
            matchesList().item(containsString("int{f}")).item(containsString("name{f}"))
        );

        // Left is the rest of the query
        var left = as(join.left(), EsqlProject.class);
        assertThat(left.output().toString(), containsString("int{r}"));
        as(left.child(), EsRelation.class);

        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        assertThat(join.config().leftFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(join.config().leftFields().size(), equalTo(1));
        assertThat(join.config().rightFields().size(), equalTo(1));
        Attribute lhs = join.config().leftFields().get(0);
        Attribute rhs = join.config().rightFields().get(0);
        assertThat(lhs.toString(), startsWith("int{r}"));
        assertThat(rhs.toString(), startsWith("int{r}"));

        // TODO: fixme
        // Join's output looks sensible too
        assertMap(
            join.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                // TODO prune unused columns down through the join
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                /*
                 * Int is a reference here because we renamed it in project.
                 * If we hadn't it'd be a field and that'd be fine.
                 */
                .item(containsString("int{r}"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
                /*
                 * It's important that name is returned as a *reference* here
                 * instead of a field. If it were a field we'd use SearchStats
                 * on it and discover that it doesn't exist in the index. It doesn't!
                 * We don't expect it to. It exists only in the lookup table.
                 */
                .item(containsString("name"))
        );
    }

    //
    // Lookup JOIN
    //

    /**
     * Filter on join keys should be pushed down
     * <p>
     * Expects
     *
     * <pre>{@code
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *          languages{f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18]]
     *     |_Limit[1000[INTEGER],false]
     *     | \_Filter[languages{f}#10 &gt; 1[INTEGER]]
     *     |   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     * }</pre>
     */
    public void testLookupJoinPushDownFilterOnJoinKeyWithRename() {
        String query = """
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_code > 1
            """;
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var upperLimit = asLimit(project.child(), 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        var limit = asLimit(join.left(), 1000, false);
        var filter = as(limit.child(), Filter.class);
        // assert that the rename has been undone
        var op = as(filter.condition(), GreaterThan.class);
        var field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("languages"));

        var literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(1));

        var leftRel = as(filter.child(), EsRelation.class);
        var rightRel = as(join.right(), EsRelation.class);
    }

    /**
     * Filter on on left side fields (outside the join key) should be pushed down
     * Expects
     *
     * <pre>{@code
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *          languages{f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18]]
     *     |_Limit[1000[INTEGER],false]
     *     | \_Filter[emp_no{f}#7 > 1[INTEGER]]
     *     |   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     *
     * }</pre>
     */
    public void testLookupJoinPushDownFilterOnLeftSideField() {
        String query = """
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE emp_no > 1
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var upperLimit = asLimit(project.child(), 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        var limit = asLimit(join.left(), 1000, false);
        var filter = as(limit.child(), Filter.class);
        var op = as(filter.condition(), GreaterThan.class);
        var field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("emp_no"));

        var literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(1));

        var leftRel = as(filter.child(), EsRelation.class);
        var rightRel = as(join.right(), EsRelation.class);
    }

    /**
     * Filter works on the right side fields and thus cannot be pushed down
     * <p>
     * Expects
     *
     * <pre>{@code
     * Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, languages
     * {f}#10 AS language_code#4, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_name{f}#19]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[language_name{f}#19 == English[KEYWORD]]
     *     \_Join[LEFT,[languages{f}#10],[languages{f}#10],[language_code{f}#18],false]
     *       |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *       \_Filter[language_name{f}#19 == English[KEYWORD]]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18, language_name{f}#19]
     * }</pre>
     */
    public void testLookupJoinPushDownDisabledForLookupField() {
        String query = """
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 1000, false);

        var filter = as(limit.child(), Filter.class);
        var op = as(filter.condition(), Equals.class);
        var field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("language_name"));
        var literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(new BytesRef("English")));

        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        var leftRel = as(join.left(), EsRelation.class);
        var rightFilter = as(join.right(), Filter.class);
        assertEquals("language_name == \"English\"", rightFilter.condition().toString());
        var joinRightEsRelation = as(rightFilter.child(), EsRelation.class);

    }

    /**
     * Split the conjunction into pushable and non pushable filters.
     * <p>
     * Expects
     *
     * <pre>{@code
     * Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17, languages
     * {f}#11 AS language_code#4, last_name{f}#12, long_noidx{f}#18, salary{f}#13, language_name{f}#20]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[language_name{f}#20 == English[KEYWORD]]
     *     \_Join[LEFT,[languages{f}#11],[languages{f}#11],[language_code{f}#19],false]
     *       |_Filter[emp_no{f}#8 > 1[INTEGER]]
     *       | \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *       \_Filter[language_name{f}#20 == English[KEYWORD]]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     * }</pre>
     */
    public void testLookupJoinPushDownSeparatedForConjunctionBetweenLeftAndRightField() {
        String query = """
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English" AND emp_no > 1
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 1000, false);
        // filter kept in place, working on the right side
        var filter = as(limit.child(), Filter.class);
        EsqlBinaryComparison op = as(filter.condition(), Equals.class);
        var field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("language_name"));
        var literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(new BytesRef("English")));

        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));
        // filter pushed down
        filter = as(join.left(), Filter.class);
        op = as(filter.condition(), GreaterThan.class);
        field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("emp_no"));

        literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(1));

        var leftRel = as(filter.child(), EsRelation.class);
        var rightFilter = as(join.right(), Filter.class);
        assertEquals("language_name == \"English\"", rightFilter.condition().toString());
        var rightRel = as(rightFilter.child(), EsRelation.class);
    }

    /**
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[emp_no{f}#5],[id{f}#16],emp_no{f}#5 == id{f}#16 AND SPATIALINTERSECTS([1 3 0 0 0 1 0 0 0 5 0 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0
     * f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0][GEO_SHAPE],shape{f}#19)]
     *   |_Limit[1000[INTEGER],false]
     *   | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsRelation[spatial_lookup][LOOKUP][contains{f}#18, id{f}#16, intersects{f}#17, shape{f..]
     */
    public void testLookupJoinRightFilterSpatialIntersects() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN spatial_lookup ON emp_no == id AND ST_INTERSECTS(TO_GEOSHAPE("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"), shape)
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertEquals(
            "ON emp_no == id AND ST_INTERSECTS(TO_GEOSHAPE(\"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))\"), shape)",
            join.config().joinOnConditions().toString()
        );
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[emp_no{f}#5],[id{f}#16],emp_no{f}#5 == id{f}#16 AND SPATIALINTERSECTS([1 3 0 0 0 1 0 0 0 5 0 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0
     * f0 3f 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0][GEO_SHAPE],shape{f}#19)]
     *   |_Limit[1000[INTEGER],false]
     *   | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsRelation[spatial_lookup][LOOKUP][contains{f}#18, id{f}#16, intersects{f}#17, shape{f..]
     */
    public void testLookupJoinRightFilterSpatialWithin() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN spatial_lookup ON emp_no == id AND ST_WITHIN(shape, TO_GEOSHAPE("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertEquals(
            "ON emp_no == id AND ST_WITHIN(shape, TO_GEOSHAPE(\"POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))\"))",
            join.config().joinOnConditions().toString()
        );
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[emp_no{f}#5],[id{f}#16],emp_no{f}#5 == id{f}#16 AND SPATIALCONTAINS(shape{f}#19,[1 3 0 0 0 1 0 0 0 5 0 0 0
     * 0 0 0 0 0 0 e0 3f 0 0 0 0 0 0 e0 3f 0 0 0 0 0 0 f8 3f 0 0 0 0 0 0 e0 3f 0 0 0 0 0 0 f8 3f 0 0 0 0 0 0 f8 3f 0 0 0 0 0 0
     * e0 3f 0 0 0 0 0 0 f8 3f 0 0 0 0 0 0 e0 3f 0 0 0 0 0 0 e0 3f][GEO_SHAPE])]
     *   |_Limit[1000[INTEGER],false]
     *   | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsRelation[spatial_lookup][LOOKUP][contains{f}#18, id{f}#16, intersects{f}#17, shape{f..]
     */
    public void testLookupJoinRightFilterSpatialContains() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        var plan = optimizedPlan("""
              FROM test
            | LOOKUP JOIN spatial_lookup
            ON emp_no == id AND ST_CONTAINS(shape, TO_GEOSHAPE("POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"))
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        var upperLimit = asLimit(plan, 1000, true);
        var join = as(upperLimit.child(), Join.class);
        assertEquals(
            "ON emp_no == id AND ST_CONTAINS(shape, TO_GEOSHAPE(\"POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))\"))",
            join.config().joinOnConditions().toString()
        );
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Disjunctions however keep the filter in place, even on pushable fields
     * <p>
     * Expects
     *
     * <pre>{@code
     * Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *          languages{f}#11 AS language_code#4, last_name{f}#12, long_noidx{f}#18, salary{f}#13, language_name{f}#20]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[language_name{f}#20 == [45 6e 67 6c 69 73 68][KEYWORD] OR emp_no{f}#8 > 1[INTEGER]]
     *     \_Join[LEFT,[languages{f}#11],[languages{f}#11],[language_code{f}#19]]
     *       |_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#19, language_name{f}#20]
     * }</pre>
     */
    public void testLookupJoinPushDownDisabledForDisjunctionBetweenLeftAndRightField() {
        String query = """
              FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English" OR emp_no > 1
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        var filter = as(limit.child(), Filter.class);
        var or = as(filter.condition(), Or.class);
        EsqlBinaryComparison op = as(or.left(), Equals.class);
        // OR left side
        var field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("language_name"));
        var literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(new BytesRef("English")));
        // OR right side
        op = as(or.right(), GreaterThan.class);
        field = as(op.left(), FieldAttribute.class);
        assertThat(field.name(), equalTo("emp_no"));
        literal = as(op.right(), Literal.class);
        assertThat(literal.value(), equalTo(1));

        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        var leftRel = as(join.left(), EsRelation.class);
        var rightRel = as(join.right(), EsRelation.class);
    }

    /**
     * When dropping lookup fields, the lookup relation shouldn't include them.
     * At least until we can implement InsertFieldExtract there.
     * <p>
     * Expects
     * <pre>{@code
     * EsqlProject[[languages{f}#21]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[language_code{r}#4],[language_code{r}#4],[language_code{f}#29]]
     *     |_Project[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27, l
     * anguages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, languages{f}#21 AS language_code]]
     *     | \_Limit[1000[INTEGER],false]
     *     |   \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#29]
     * }</pre>
     */
    public void testLookupJoinKeepNoLookupFields() {
        String commandDiscardingFields = randomBoolean() ? "| KEEP languages" : """
            | DROP _meta_field, emp_no, first_name, gender, language_code,
                   language_name, last_name, salary, hire_date, job, job.raw, long_noidx
            """;

        String query = """
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            """ + commandDiscardingFields;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(project.projections().get(0).name(), equalTo("languages"));

        var limit = asLimit(project.child(), 1000, true);

        var join = as(limit.child(), Join.class);
        var joinRightRelation = as(join.right(), EsRelation.class);

        assertThat(joinRightRelation.output().size(), equalTo(1));
        assertThat(joinRightRelation.output().get(0).name(), equalTo("language_code"));
    }

    /**
     * Ensure a JOIN shadowed by another JOIN doesn't request the shadowed fields.
     * <p>
     * Expected
     * <pre>{@code
     * Limit[1000[INTEGER],true]
     * \_Join[LEFT,[language_code{r}#4],[language_code{r}#4],[language_code{f}#20]]
     *   |_Limit[1000[INTEGER],true]
     *   | \_Join[LEFT,[language_code{r}#4],[language_code{r}#4],[language_code{f}#18]]
     *   |   |_Eval[[languages{f}#10 AS language_code]]
     *   |   | \_Limit[1000[INTEGER],false]
     *   |   |   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#18]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#20, language_name{f}#21]
     * }</pre>
     */
    public void testMultipleLookupShadowing() {
        String query = """
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """;

        var plan = optimizedPlan(query);

        var limit1 = asLimit(plan, 1000, true);

        var finalJoin = as(limit1.child(), Join.class);
        var finalJoinRightRelation = as(finalJoin.right(), EsRelation.class);

        assertThat(finalJoinRightRelation.output().size(), equalTo(2));
        assertThat(finalJoinRightRelation.output().get(0).name(), equalTo("language_code"));
        assertThat(finalJoinRightRelation.output().get(1).name(), equalTo("language_name"));

        var limit2 = asLimit(finalJoin.left(), 1000, true);

        var initialJoin = as(limit2.child(), Join.class);
        var initialJoinRightRelation = as(initialJoin.right(), EsRelation.class);

        assertThat(initialJoinRightRelation.output().size(), equalTo(1));
        assertThat(initialJoinRightRelation.output().get(0).name(), equalTo("language_code"));

        var eval = as(initialJoin.left(), Eval.class);
        var limit3 = asLimit(eval.child(), 1000, false);
    }

    public void testTranslateMetricsWithoutGrouping() {
        var query = "TS k8s | STATS max(rate(network.total_bytes_in))";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs, not(instanceOf(TimeSeriesAggregate.class)));
        TimeSeriesAggregate aggsByTsid = as(finalAggs.child(), TimeSeriesAggregate.class);
        assertNull(aggsByTsid.timeBucket());
        as(aggsByTsid.child(), EsRelation.class);

        assertThat(finalAggs.aggregates(), hasSize(1));
        Max max = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        assertThat(Expressions.attribute(max.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAggs.groupings(), empty());

        assertThat(aggsByTsid.aggregates(), hasSize(1)); // _tsid is dropped
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
    }

    public void testTranslateMixedAggsWithoutGrouping() {
        var query = "TS k8s | STATS max(rate(network.total_bytes_in)), max(network.cost)";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs, not(instanceOf(TimeSeriesAggregate.class)));
        TimeSeriesAggregate aggsByTsid = as(finalAggs.child(), TimeSeriesAggregate.class);
        assertNull(aggsByTsid.timeBucket());
        as(aggsByTsid.child(), EsRelation.class);

        assertThat(finalAggs.aggregates(), hasSize(2));
        Max maxRate = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        Max maxCost = as(Alias.unwrap(finalAggs.aggregates().get(1)), Max.class);
        assertThat(Expressions.attribute(maxRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(maxCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(finalAggs.groupings(), empty());

        assertThat(aggsByTsid.aggregates(), hasSize(2));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        LastOverTime lastCost = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), LastOverTime.class);
        assertThat(Expressions.attribute(lastCost.field()).name(), equalTo("network.cost"));
    }

    public void testTranslateMixedAggsWithMathWithoutGrouping() {
        var query = "TS k8s | STATS max(rate(network.total_bytes_in)), max(network.cost + 0.2) * 1.1";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        Eval mulEval = as(project.child(), Eval.class);
        assertThat(mulEval.fields(), hasSize(1));
        Mul mul = as(Alias.unwrap(mulEval.fields().get(0)), Mul.class);
        Limit limit = as(mulEval.child(), Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAggs.aggregates(), hasSize(2));
        TimeSeriesAggregate aggsByTsid = as(finalAggs.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2));
        assertNull(aggsByTsid.timeBucket());
        Eval addEval = as(aggsByTsid.child(), Eval.class);
        assertThat(addEval.fields(), hasSize(1));
        Add add = as(Alias.unwrap(addEval.fields().get(0)), Add.class);
        EsRelation relation = as(addEval.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        assertThat(Expressions.attribute(mul.left()).id(), equalTo(finalAggs.aggregates().get(1).id()));
        assertThat(mul.right().fold(FoldContext.small()), equalTo(1.1));

        Max maxRate = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        Max maxCost = as(Alias.unwrap(finalAggs.aggregates().get(1)), Max.class);
        assertThat(Expressions.attribute(maxRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(maxCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(finalAggs.groupings(), empty());

        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        LastOverTime lastCost = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), LastOverTime.class);
        assertThat(Expressions.attribute(lastCost.field()).id(), equalTo(addEval.fields().get(0).id()));
        assertThat(Expressions.attribute(add.left()).name(), equalTo("network.cost"));
        assertThat(add.right().fold(FoldContext.small()), equalTo(0.2));
    }

    public void testTranslateMetricsGroupedByOneDimension() {
        var query = "TS k8s | STATS sum(rate(network.total_bytes_in)) BY cluster | SORT cluster | LIMIT 10";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval unpack = as(topN.child(), Eval.class);
        Aggregate aggsByCluster = as(unpack.child(), Aggregate.class);
        assertThat(aggsByCluster, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(aggsByCluster.aggregates(), hasSize(2));
        Eval pack = as(aggsByCluster.child(), Eval.class);
        TimeSeriesAggregate aggsByTsid = as(pack.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        assertNull(aggsByTsid.timeBucket());
        EsRelation relation = as(aggsByTsid.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        Sum sum = as(Alias.unwrap(aggsByCluster.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(aggsByCluster.groupings(), hasSize(1));

        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        DimensionValues values = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), DimensionValues.class);
        assertThat(Expressions.attribute(values.field()).name(), equalTo("cluster"));
    }

    public void testTranslateMetricsGroupedByTwoDimension() {
        var query = "TS k8s | STATS avg(rate(network.total_bytes_in)) BY cluster, pod";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        Limit limit = as(eval.child(), Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAggs.aggregates(), hasSize(4));
        Eval pack = as(finalAggs.child(), Eval.class);
        TimeSeriesAggregate aggsByTsid = as(pack.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // _tsid is dropped
        assertNull(aggsByTsid.timeBucket());
        EsRelation relation = as(aggsByTsid.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        var unpackCluster = as(Alias.unwrap(eval.fields().get(0)), UnpackDimension.class);
        assertThat(Expressions.name(unpackCluster), equalTo("cluster"));
        var unpackPod = as(Alias.unwrap(eval.fields().get(1)), UnpackDimension.class);
        assertThat(Expressions.name(unpackPod), equalTo("pod"));
        Div div = as(Alias.unwrap(eval.fields().get(2)), Div.class);

        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAggs.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAggs.aggregates().get(1).id()));

        Sum sum = as(Alias.unwrap(finalAggs.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        Count count = as(Alias.unwrap(finalAggs.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(count.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAggs.groupings(), hasSize(2));

        assertThat(aggsByTsid.aggregates(), hasSize(3)); // rates, values(cluster), values(pod)
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        DimensionValues values1 = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), DimensionValues.class);
        assertThat(Expressions.attribute(values1.field()).name(), equalTo("cluster"));
        DimensionValues values2 = as(Alias.unwrap(aggsByTsid.aggregates().get(2)), DimensionValues.class);
        assertThat(Expressions.attribute(values2.field()).name(), equalTo("pod"));
    }

    public void testTranslateMetricsGroupedByTimeBucket() {
        var query = "TS k8s | STATS sum(rate(network.total_bytes_in)) BY bucket(@timestamp, 1h)";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAgg.aggregates(), hasSize(2));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
        Eval eval = as(aggsByTsid.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(eval.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(eval.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testTranslateMetricsGroupedByTimeBucketAndDimensions() {
        var query = """
            TS k8s
            | STATS avg(rate(network.total_bytes_in)) BY pod, bucket(@timestamp, 5 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        Div div = as(Alias.unwrap(eval.fields().get(2)), Div.class);
        Aggregate finalAgg = as(eval.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        Eval packDimensions = as(finalAgg.child(), Eval.class);
        TimeSeriesAggregate aggsByTsid = as(packDimensions.child(), TimeSeriesAggregate.class);
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
        Eval bucket = as(aggsByTsid.child(), Eval.class);
        EsRelation relation = as(bucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));
        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAgg.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAgg.aggregates().get(1).id()));

        assertThat(finalAgg.aggregates(), hasSize(5)); // sum, count, pod, bucket, cluster
        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count count = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(count.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(3));

        assertThat(aggsByTsid.aggregates(), hasSize(4)); // rate, values(pod), values(cluster), bucket
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        DimensionValues podValues = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), DimensionValues.class);
        assertThat(Expressions.attribute(podValues.field()).name(), equalTo("pod"));
    }

    public void testTranslateSumOfTwoRates() {
        var query = """
            TS k8s
            | STATS max(rate(network.total_bytes_in) + rate(network.total_bytes_out)) BY pod, bucket(@timestamp, 5 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project projection = as(plan, Project.class);
        TopN topN = as(projection.child(), TopN.class);
        Eval unpack = as(topN.child(), Eval.class);
        assertThat(unpack.fields(), hasSize(2));
        var unpack1 = as(Alias.unwrap(unpack.fields().get(0)), UnpackDimension.class);
        var unpack2 = as(Alias.unwrap(unpack.fields().get(1)), UnpackDimension.class);
        Aggregate finalAgg = as(unpack.child(), Aggregate.class);
        Eval eval = as(finalAgg.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        var pack1 = as(Alias.unwrap(eval.fields().get(0)), PackDimension.class);
        var pack2 = as(Alias.unwrap(eval.fields().get(1)), PackDimension.class);
        Add sum = as(Alias.unwrap(eval.fields().get(2)), Add.class);
        assertThat(Expressions.name(sum.left()), equalTo("RATE_$1"));
        assertThat(Expressions.name(sum.right()), equalTo("RATE_$2"));
        TimeSeriesAggregate aggsByTsid = as(eval.child(), TimeSeriesAggregate.class);
        assertThat(Expressions.name(aggsByTsid.aggregates().get(0)), equalTo("RATE_$1"));
        assertThat(Expressions.name(aggsByTsid.aggregates().get(1)), equalTo("RATE_$2"));
    }

    public void testTranslateMixedAggsGroupedByTimeBucketAndDimensions() {
        var query = """
            TS k8s
            | STATS avg(rate(network.total_bytes_in)), avg(network.cost) BY bucket(@timestamp, 5 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        Div div = as(Alias.unwrap(eval.fields().get(2)), Div.class);
        Aggregate finalAgg = as(eval.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        Eval packDimensions = as(finalAgg.child(), Eval.class);
        TimeSeriesAggregate aggsByTsid = as(packDimensions.child(), TimeSeriesAggregate.class);
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
        Eval bucket = as(aggsByTsid.child(), Eval.class);
        EsRelation relation = as(bucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        assertThat(finalAgg.aggregates(), hasSize(6)); // sum, count, sum, count, bucket, cluster
        Sum sumRate = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count countRate = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(sumRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(countRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));

        Sum sumCost = as(Alias.unwrap(finalAgg.aggregates().get(2)), Sum.class);
        Count countCost = as(Alias.unwrap(finalAgg.aggregates().get(3)), Count.class);
        assertThat(Expressions.attribute(sumCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(Expressions.attribute(countCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        assertThat(finalAgg.groupings(), hasSize(2));

        assertThat(aggsByTsid.aggregates(), hasSize(4)); // rate, last_over_time, values(cluster), bucket
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        LastOverTime lastSum = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), LastOverTime.class);
        assertThat(Expressions.attribute(lastSum.field()).name(), equalTo("network.cost"));
    }

    public void testAdjustMetricsRateBeforeFinalAgg() {
        var query = """
            TS k8s
            | STATS avg(round(1.05 * rate(network.total_bytes_in))) BY bucket(@timestamp, 1 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval evalDiv = as(topN.child(), Eval.class);
        assertThat(evalDiv.fields(), hasSize(2));
        as(Alias.unwrap(evalDiv.fields().get(0)), UnpackDimension.class);
        Div div = as(Alias.unwrap(evalDiv.fields().get(1)), Div.class);

        Aggregate finalAgg = as(evalDiv.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAgg.aggregates(), hasSize(4)); // sum, count, bucket, cluster
        assertThat(finalAgg.groupings(), hasSize(2));

        Eval evalRound = as(finalAgg.child(), Eval.class);
        Round round = as(Alias.unwrap(evalRound.fields().get(1)), Round.class);
        Mul mul = as(round.field(), Mul.class);

        TimeSeriesAggregate aggsByTsid = as(evalRound.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // rate, cluster, bucket
        assertThat(aggsByTsid.groupings(), hasSize(2));
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));

        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        EsRelation relation = as(evalBucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAgg.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAgg.aggregates().get(1).id()));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count count = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);

        assertThat(
            Expressions.attribute(finalAgg.groupings().get(0)).id(),
            equalTo(Expressions.attribute(aggsByTsid.groupings().get(1)).id())
        );

        assertThat(Expressions.attribute(mul.left()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(mul.right().fold(FoldContext.small()), equalTo(1.05));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        DimensionValues values = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), DimensionValues.class);
        assertThat(Expressions.attribute(values.field()).name(), equalTo("cluster"));
    }

    public void testTranslateMaxOverTime() {
        var query = "TS k8s | STATS sum(max_over_time(network.bytes_in)) BY bucket(@timestamp, 1h)";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAgg.aggregates(), hasSize(2));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
        Eval eval = as(aggsByTsid.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        Max max = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Max.class);
        assertThat(Expressions.attribute(max.field()).name(), equalTo("network.bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(eval.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(eval.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testTranslateAvgOverTime() {
        var query = "TS k8s | STATS sum(avg_over_time(network.bytes_in)) BY bucket(@timestamp, 1h)";
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAgg.aggregates(), hasSize(2));
        Eval evalAvg = as(finalAgg.child(), Eval.class);
        TimeSeriesAggregate aggsByTsid = as(evalAvg.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // _tsid is dropped
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        EsRelation relation = as(evalBucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(evalAvg.fields().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(2).id()));

        Sum sumTs = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Sum.class);
        assertThat(sumTs.summationMode(), equalTo(SummationMode.LOSSY_LITERAL));
        assertThat(Expressions.attribute(sumTs.field()).name(), equalTo("network.bytes_in"));
        Count countTs = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(countTs.field()).name(), equalTo("network.bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(evalBucket.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testTranslateLastOverTime() {
        var query = """
            TS k8s | STATS avg(last_over_time(network.bytes_in)) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        EsRelation relation = as(evalBucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));

        as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);

        LastOverTime lastOverTime = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), LastOverTime.class);
        assertThat(Expressions.attribute(lastOverTime.field()).name(), equalTo("network.bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(evalBucket.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testTranslateWithInlineFilter() {
        var query = """
            TS k8s | STATS sum(last_over_time(network.bytes_in)) WHERE cluster == "prod" BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        var limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        EsRelation relation = as(evalBucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));

        var sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertFalse(sum.hasFilter());

        LastOverTime lastOverTime = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), LastOverTime.class);
        assertThat(Expressions.attribute(lastOverTime.field()).name(), equalTo("network.bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(evalBucket.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
        assertTrue(lastOverTime.hasFilter());
        assertThat(lastOverTime.filter(), instanceOf(Equals.class));
    }

    public void testTranslateWithInlineFilterWithImplicitLastOverTime() {
        var query = """
            TS k8s | STATS avg(network.bytes_in) WHERE cluster == "prod" BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        EsRelation relation = as(evalBucket.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));

        var sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertFalse(sum.hasFilter());
        var count = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertFalse(count.hasFilter());

        LastOverTime lastOverTime = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), LastOverTime.class);
        assertThat(Expressions.attribute(lastOverTime.field()).name(), equalTo("network.bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(evalBucket.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
        assertTrue(lastOverTime.hasFilter());
        assertThat(lastOverTime.filter(), instanceOf(Equals.class));
    }

    public void testTranslateOverTimeWithWindow() {
        {
            int window = between(1, 20);
            var query = String.format(Locale.ROOT, """
                TS k8s
                | STATS avg(last_over_time(network.bytes_in, %s minute)) BY TBUCKET(1 minute)
                | LIMIT 10
                """, window);
            var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            Holder<LastOverTime> holder = new Holder<>();
            plan.forEachExpressionDown(LastOverTime.class, holder::set);
            assertNotNull(holder.get());
            assertTrue(holder.get().hasWindow());
            assertThat(holder.get().window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(window)));
        }
        {
            int window = between(1, 3);
            int bucket = randomFrom(5, 10, 15, 20, 30, 60);
            var query = String.format(Locale.ROOT, """
                TS k8s
                | STATS avg(last_over_time(network.bytes_in, %s hour)) BY TBUCKET(%s minute)
                | LIMIT 10
                """, window, bucket);
            var plan = logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            Holder<LastOverTime> holder = new Holder<>();
            plan.forEachExpressionDown(LastOverTime.class, holder::set);
            assertNotNull(holder.get());
            assertTrue(holder.get().hasWindow());
            assertThat(holder.get().window().fold(FoldContext.small()), equalTo(Duration.ofHours(window)));
        }
        // smaller window
        {
            var query = """
                TS k8s
                | STATS sum(rate(network.total_bytes_in, 1m)) BY TBUCKET(5m)
                | LIMIT 10
                """;
            var error = expectThrows(EsqlIllegalArgumentException.class, () -> {
                logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            });
            assertThat(
                error.getMessage(),
                equalTo(
                    "Unsupported window [1m] for aggregate function [rate(network.total_bytes_in, 1m)]; "
                        + "the window must be larger than the time bucket [TBUCKET(5m)] and an exact multiple of it"
                )
            );
        }
        // not supported
        {
            int window = randomValueOtherThanMany(n -> n % 5 == 0, () -> between(1, 30));
            var query = String.format(Locale.ROOT, """
                TS k8s
                | STATS avg(last_over_time(network.bytes_in, %s minute)) BY tbucket(5 minute)
                | LIMIT 10
                """, window);
            var error = expectThrows(EsqlIllegalArgumentException.class, () -> {
                logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            });
            assertThat(
                error.getMessage(),
                containsString("the window must be larger than the time bucket [tbucket(5 minute)] and an exact multiple of it")
            );
        }
        // no time bucket
        {
            int window = randomIntBetween(1, 50);
            var query = String.format(Locale.ROOT, """
                TS k8s
                | STATS avg(last_over_time(network.bytes_in, %s minute))
                | LIMIT 10
                """, window);
            var error = expectThrows(EsqlIllegalArgumentException.class, () -> {
                logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            });
            assertThat(
                error.getMessage(),
                equalTo(
                    String.format(
                        Locale.ROOT,
                        "Using a window in aggregation [STATS avg(last_over_time(network.bytes_in, %s minute))] "
                            + "requires a time bucket in groupings",
                        window
                    )
                )
            );
        }
    }

    public void testMvSortInvalidOrder() {
        VerificationException e = expectThrows(VerificationException.class, () -> plan("""
            from test
            | EVAL sd = mv_sort(salary, "ABC")
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        assertEquals(
            "2:29: Invalid order value in [mv_sort(salary, \"ABC\")], expected one of [ASC, DESC] but got [ABC]",
            e.getMessage().substring(header.length())
        );

        e = expectThrows(VerificationException.class, () -> plan("""
            from test
            | EVAL order = "ABC", sd = mv_sort(salary, order)
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        assertEquals(
            "2:16: Invalid order value in [mv_sort(salary, order)], expected one of [ASC, DESC] but got [ABC]",
            e.getMessage().substring(header.length())
        );

        e = expectThrows(VerificationException.class, () -> plan("""
            from test
            | EVAL order = concat("d", "sc"), sd = mv_sort(salary, order)
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        assertEquals(
            "2:16: Invalid order value in [mv_sort(salary, order)], expected one of [ASC, DESC] but got [dsc]",
            e.getMessage().substring(header.length())
        );

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> plan("""
            row v = [1, 2, 3] | EVAL sd = mv_sort(v, "dsc")
            """));
        assertEquals("Invalid order value in [mv_sort(v, \"dsc\")], expected one of [ASC, DESC] but got [dsc]", iae.getMessage());

        iae = expectThrows(IllegalArgumentException.class, () -> plan("""
            row v = [1, 2, 3], o = concat("d", "sc") | EVAL sd = mv_sort(v, o)
            """));
        assertEquals("Invalid order value in [mv_sort(v, o)], expected one of [ASC, DESC] but got [dsc]", iae.getMessage());
    }

    public void testToDatePeriodTimeDurationInvalidIntervals() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types | EVAL interval = "3 dys", x = date + interval::date_period"""));
        assertEquals(
            "Invalid interval value in [interval::date_period], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [3 dys]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types | EVAL interval = "- 3 days", x = date + interval::date_period"""));
        assertEquals(
            "Invalid interval value in [interval::date_period], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [- 3 days]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "3 dys", x = date - to_dateperiod(interval)"""));
        assertEquals(
            "Invalid interval value in [to_dateperiod(interval)], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [3 dys]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "- 3 days", x = date - to_dateperiod(interval)"""));
        assertEquals(
            "Invalid interval value in [to_dateperiod(interval)], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [- 3 days]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "3 ours", x = date + interval::time_duration"""));
        assertEquals(
            "Invalid interval value in [interval::time_duration], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3 ours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "- 3 hours", x = date + interval::time_duration"""));
        assertEquals(
            "Invalid interval value in [interval::time_duration], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [- 3 hours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "3 ours", x = date - to_timeduration(interval)"""));
        assertEquals(
            "Invalid interval value in [to_timeduration(interval)], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3 ours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "- 3 hours", x = date - to_timeduration(interval)"""));
        assertEquals(
            "Invalid interval value in [to_timeduration(interval)], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [- 3 hours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            from types  | EVAL interval = "3.5 hours", x = date - to_timeduration(interval)"""));
        assertEquals(
            "Invalid interval value in [to_timeduration(interval)], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3.5 hours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            row x = "2024-01-01"::datetime | eval y = x + "3 dys"::date_period"""));
        assertEquals(
            "Invalid interval value in [\"3 dys\"::date_period], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [3 dys]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            row x = "2024-01-01"::datetime | eval y = x - to_dateperiod("3 dys")"""));
        assertEquals(
            "Invalid interval value in [to_dateperiod(\"3 dys\")], expected integer followed by one of "
                + "[DAY, DAYS, D, WEEK, WEEKS, W, MONTH, MONTHS, MO, QUARTER, QUARTERS, Q, YEAR, YEARS, YR, Y] but got [3 dys]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            row x = "2024-01-01"::datetime | eval y = x + "3 ours"::time_duration"""));
        assertEquals(
            "Invalid interval value in [\"3 ours\"::time_duration], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3 ours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            row x = "2024-01-01"::datetime | eval y = x - to_timeduration("3 ours")"""));
        assertEquals(
            "Invalid interval value in [to_timeduration(\"3 ours\")], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3 ours]",
            e.getMessage()
        );

        e = expectThrows(IllegalArgumentException.class, () -> planTypes("""
            row x = "2024-01-01"::datetime | eval y = x - to_timeduration("3.5 hours")"""));
        assertEquals(
            "Invalid interval value in [to_timeduration(\"3.5 hours\")], expected integer followed by one of "
                + "[MILLISECOND, MILLISECONDS, MS, SECOND, SECONDS, SEC, S, MINUTE, MINUTES, MIN, M, HOUR, HOURS, H] but got [3.5 hours]",
            e.getMessage()
        );
    }

    public void testToDatePeriodToTimeDurationWithField() {
        final String header = "Found 1 problem\nline ";
        VerificationException e = expectThrows(VerificationException.class, () -> planTypes("""
            from types | EVAL x = date + keyword::date_period"""));
        assertTrue(e.getMessage().startsWith("Found "));
        assertEquals(
            "1:30: argument of [keyword::date_period] must be a constant, received [keyword]",
            e.getMessage().substring(header.length())
        );

        e = expectThrows(VerificationException.class, () -> planTypes("""
            from types  | EVAL x = date - to_timeduration(keyword)"""));
        assertEquals(
            "1:47: argument of [to_timeduration(keyword)] must be a constant, received [keyword]",
            e.getMessage().substring(header.length())
        );

        e = expectThrows(VerificationException.class, () -> planTypes("""
            from types | EVAL x = keyword, y = date + x::date_period"""));
        assertTrue(e.getMessage().startsWith("Found "));
        assertEquals("1:43: argument of [x::date_period] must be a constant, received [x]", e.getMessage().substring(header.length()));

        e = expectThrows(VerificationException.class, () -> planTypes("""
            from types  | EVAL x = keyword, y = date - to_timeduration(x)"""));
        assertEquals("1:60: argument of [to_timeduration(x)] must be a constant, received [x]", e.getMessage().substring(header.length()));
    }

    public void testWhereNull() {
        var plan = plan("""
            from test
            | sort salary
            | rename emp_no as e, first_name as f
            | keep salary, e, f
            | where null
            | LIMIT 12
            """);
        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(EmptyLocalSupplier.EMPTY));
    }

    public void testFunctionNamedParamsAsFunctionArgument() {
        var query = """
            from test
            | WHERE MATCH(first_name, "Anna Smith", {"minimum_should_match": 2.0})
            """;
        var plan = optimizedPlan(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match match = as(filter.condition(), Match.class);
        MapExpression me = as(match.options(), MapExpression.class);
        assertEquals(1, me.entryExpressions().size());
        EntryExpression ee = as(me.entryExpressions().get(0), EntryExpression.class);
        BytesRef key = as(ee.key().fold(FoldContext.small()), BytesRef.class);
        assertEquals("minimum_should_match", key.utf8ToString());
        assertEquals(new Literal(EMPTY, 2.0, DataType.DOUBLE), ee.value());
        assertEquals(DataType.DOUBLE, ee.dataType());
    }

    public void testFunctionNamedParamsAsFunctionArgument1() {
        var query = """
            from test
            | WHERE MULTI_MATCH("Anna Smith", first_name, last_name, {"minimum_should_match": 2.0})
            """;
        var plan = optimizedPlan(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        MultiMatch match = as(filter.condition(), MultiMatch.class);
        MapExpression me = as(match.options(), MapExpression.class);
        assertEquals(1, me.entryExpressions().size());
        EntryExpression ee = as(me.entryExpressions().get(0), EntryExpression.class);
        BytesRef key = as(ee.key().fold(FoldContext.small()), BytesRef.class);
        assertEquals("minimum_should_match", key.utf8ToString());
        assertEquals(new Literal(EMPTY, 2.0, DataType.DOUBLE), ee.value());
        assertEquals(DataType.DOUBLE, ee.dataType());
    }

    /**
     * <pre>{@code
     * Project[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20,
     *          languages{f}#14 AS language_code#5, last_name{f}#15, long_noidx{f}#21, salary{f}#16, foo{r}#7, language_name{f}#23]]
     * \_TopN[[Order[emp_no{f}#11,ASC,LAST]],1000[INTEGER]]
     *   \_Join[LEFT,[languages{f}#14],[languages{f}#14],[language_code{f}#22]]
     *     |_Eval[[[62 61 72][KEYWORD] AS foo#7]]
     *     | \_Filter[languages{f}#14 > 1[INTEGER]]
     *     |   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#22, language_name{f}#23]
     * }</pre>
     */
    public void testRedundantSortOnJoin() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | RENAME languages AS language_code
            | EVAL foo = "bar"
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_code > 1
            | SORT emp_no
            """);

        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        var join = as(topN.child(), Join.class);
        var eval = as(join.left(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        as(filter.child(), EsRelation.class);

        assertThat(Expressions.names(topN.order()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#9,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#9 > 1[INTEGER]]
     *   \_MvExpand[languages{f}#12,languages{r}#20,null]
     *     \_Eval[[[62 61 72][KEYWORD] AS foo]]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testRedundantSortOnMvExpand() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | EVAL foo = "bar"
            | MV_EXPAND languages
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var eval = as(mvExpand.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#11,ASC,LAST]],1000[INTEGER]]
     * \_Join[LEFT,[language_code{r}#5],[language_code{r}#5],[language_code{f}#22]]
     *   |_Filter[emp_no{f}#11 > 1[INTEGER]]
     *   | \_MvExpand[languages{f}#14,languages{r}#24,null]
     *   |   \_Eval[[languages{f}#14 AS language_code]]
     *   |     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#22, language_name{f}#23]
     * }</pre>
     */
    public void testRedundantSortOnMvExpandAndJoin() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | EVAL language_code = languages
            | MV_EXPAND languages
            | WHERE emp_no > 1
            | LOOKUP JOIN languages_lookup ON language_code
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var join = as(topN.child(), Join.class);
        var filter = as(join.left(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var eval = as(mvExpand.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#12,ASC,LAST]],1000[INTEGER]]
     * \_Join[LEFT,[language_code{r}#5],[language_code{r}#5],[language_code{f}#23]]
     *   |_Filter[emp_no{f}#12 > 1[INTEGER]]
     *   | \_MvExpand[languages{f}#15,languages{r}#25,null]
     *   |   \_Eval[[languages{f}#15 AS language_code]]
     *   |     \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#23, language_name{f}#24]
     * }</pre>
     */
    public void testMultlipleRedundantSortOnMvExpandAndJoin() {
        var plan = optimizedPlan("""
              FROM test
            | SORT first_name
            | EVAL language_code = languages
            | MV_EXPAND languages
            | sort last_name
            | WHERE emp_no > 1
            | LOOKUP JOIN languages_lookup ON language_code
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var join = as(topN.child(), Join.class);
        var filter = as(join.left(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var eval = as(mvExpand.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#16,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#16 > 1[INTEGER]]
     *   \_MvExpand[languages{f}#19,languages{r}#31]
     *     \_Dissect[foo{r}#5,Parser[pattern=%{z}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@26f2cab],[z{r}#10
     * ]]
     *       \_Grok[foo{r}#5,Parser[pattern=%{WORD:y}, grok=org.elasticsearch.grok.Grok@6ea44ccd],[y{r}#9]]
     *         \_Enrich[ANY,[6c 61 6e 67 75 61 67 65 73 5f 69 64 78][KEYWORD],foo{r}#5,{"match":{"indices":[],"match_field":"id","enrich_
     * fields":["language_code","language_name"]}},{=languages_idx},[language_code{r}#29, language_name{r}#30]]
     *           \_Eval[[TOSTRING(languages{f}#19) AS foo]]
     *             \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    public void testRedundantSortOnMvExpandEnrichGrokDissect() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | EVAL foo = to_string(languages)
            | ENRICH languages_idx on foo
            | GROK foo "%{WORD:y}"
            | DISSECT foo "%{z}"
            | MV_EXPAND languages
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var dissect = as(mvExpand.child(), Dissect.class);
        var grok = as(dissect.child(), Grok.class);
        var enrich = as(grok.child(), Enrich.class);
        var eval = as(enrich.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#20,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#20 > 1[INTEGER]]
     *   \_MvExpand[languages{f}#23,languages{r}#37]
     *     \_Dissect[foo{r}#5,Parser[pattern=%{z}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@3e922db0],[z{r}#1
     * 4]]
     *       \_Grok[foo{r}#5,Parser[pattern=%{WORD:y}, grok=org.elasticsearch.grok.Grok@4d6ad024],[y{r}#13]]
     *         \_Enrich[ANY,[6c 61 6e 67 75 61 67 65 73 5f 69 64 78][KEYWORD],foo{r}#5,{"match":{"indices":[],"match_field":"id","enrich_
     * fields":["language_code","language_name"]}},{=languages_idx},[language_code{r}#35, language_name{r}#36]]
     *           \_Join[LEFT,[language_code{r}#8],[language_code{r}#8],[language_code{f}#31]]
     *             |_Eval[[TOSTRING(languages{f}#23) AS foo, languages{f}#23 AS language_code]]
     *             | \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     *             \_EsRelation[languages_lookup][LOOKUP][language_code{f}#31]
     * }</pre>
     */
    public void testRedundantSortOnMvExpandJoinEnrichGrokDissect() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | EVAL foo = to_string(languages), language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | ENRICH languages_idx on foo
            | GROK foo "%{WORD:y}"
            | DISSECT foo "%{z}"
            | MV_EXPAND languages
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var dissect = as(mvExpand.child(), Dissect.class);
        var grok = as(dissect.child(), Grok.class);
        var enrich = as(grok.child(), Enrich.class);
        var join = as(enrich.child(), Join.class);
        var eval = as(join.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * <pre>{@code
     * TopN[[Order[emp_no{f}#23,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#23 > 1[INTEGER]]
     *   \_MvExpand[languages{f}#26,languages{r}#36]
     *     \_Project[[language_name{f}#35, foo{r}#5 AS bar#18, languages{f}#26, emp_no{f}#23]]
     *       \_Join[LEFT,[languages{f}#26],[languages{f}#26],[language_code{f}#34]]
     *         |_Eval[[TOSTRING(languages{f}#26) AS foo#5]]
     *         | \_EsRelation[test][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#34, language_name{f}#35]
     * }</pre>
     */
    public void testRedundantSortOnMvExpandJoinKeepDropRename() {
        var plan = optimizedPlan("""
              FROM test
            | SORT languages
            | EVAL foo = to_string(languages), language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP language_name, language_code, foo, languages, emp_no
            | DROP language_code
            | RENAME foo AS bar
            | MV_EXPAND languages
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var project = as(mvExpand.child(), Project.class);
        var join = as(project.child(), Join.class);
        var eval = as(join.left(), Eval.class);
        as(eval.child(), EsRelation.class);

        assertThat(Expressions.names(topN.order()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{f}#15,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#15 > 1[INTEGER]]
     *   \_MvExpand[foo{r}#10,foo{r}#29]
     *     \_Eval[[CONCAT(language_name{r}#28,[66 6f 6f][KEYWORD]) AS foo]]
     *       \_MvExpand[language_name{f}#27,language_name{r}#28]
     *         \_Join[LEFT,[language_code{r}#3],[language_code{r}#3],[language_code{f}#26]]
     *           |_Eval[[1[INTEGER] AS language_code]]
     *           | \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *           \_EsRelation[languages_lookup][LOOKUP][language_code{f}#26, language_name{f}#27]
     * }</pre>
     */
    public void testEvalLookupMultipleSorts() {
        var plan = optimizedPlan("""
              FROM test
            | EVAL language_code = 1
            | LOOKUP JOIN languages_lookup ON language_code
            | SORT language_name
            | MV_EXPAND language_name
            | EVAL foo = concat(language_name, "foo")
            | MV_EXPAND foo
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        var topN = as(plan, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var mvExpand = as(filter.child(), MvExpand.class);
        var eval = as(mvExpand.child(), Eval.class);
        mvExpand = as(eval.child(), MvExpand.class);
        var join = as(mvExpand.child(), Join.class);
        eval = as(join.left(), Eval.class);
        as(eval.child(), EsRelation.class);

    }

    public void testUnboundedSortSimple() {
        var query = """
              ROW x = [1,2,3], y = 1
              | SORT y
              | MV_EXPAND x
              | WHERE x > 2
            """;

        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertThat(e.getMessage(), containsString("line 2:5: Unbounded SORT not supported yet [SORT y] please add a LIMIT"));
    }

    public void testUnboundedSortJoin() {
        var query = """
              ROW x = [1,2,3], y = 2, language_code = 1
              | SORT y
              | LOOKUP JOIN languages_lookup ON language_code
              | WHERE language_name == "foo"
            """;

        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertThat(e.getMessage(), containsString("line 2:5: Unbounded SORT not supported yet [SORT y] please add a LIMIT"));
    }

    public void testUnboundedSortWithMvExpandAndFilter() {
        var query = """
              FROM test
            | EVAL language_code = 1
            | LOOKUP JOIN languages_lookup ON language_code
            | SORT language_name
            | EVAL foo = concat(language_name, "foo")
            | MV_EXPAND foo
            | WHERE foo == "foo"
            """;

        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertThat(e.getMessage(), containsString("line 4:3: Unbounded SORT not supported yet [SORT language_name] please add a LIMIT"));
    }

    public void testUnboundedSortWithLookupJoinAndFilter() {
        var query = """
              FROM test
            | EVAL language_code = 1
            | EVAL foo = concat(language_code::string, "foo")
            | MV_EXPAND foo
            | SORT foo
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "foo"
            """;

        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertThat(e.getMessage(), containsString("line 5:3: Unbounded SORT not supported yet [SORT foo] please add a LIMIT"));
    }

    public void testUnboundedSortExpandFilter() {
        var query = """
              ROW x = [1,2,3], y = 1
              | SORT x
              | MV_EXPAND x
              | WHERE x > 2
            """;

        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertThat(e.getMessage(), containsString("line 2:5: Unbounded SORT not supported yet [SORT x] please add a LIMIT"));
    }

    public void testPruneRedundantOrderBy() {
        var rule = new PruneRedundantOrderBy();

        var query = """
            row x = [1,2,3], y = 1
            | sort x
            | mv_expand x
            | sort x
            | mv_expand x
            | sort y
            """;
        LogicalPlan analyzed = analyzer.analyze(parser.createStatement(query));
        LogicalPlan optimized = rule.apply(analyzed);

        // check that all the redundant SORTs are removed in a single run
        var limit = as(optimized, Limit.class);
        var orderBy = as(limit.child(), OrderBy.class);
        var mvExpand = as(orderBy.child(), MvExpand.class);
        var mvExpand2 = as(mvExpand.child(), MvExpand.class);
        as(mvExpand2.child(), Row.class);
    }

    /**
     * <pre>{@code
     * Eval[[1[INTEGER] AS irrelevant1, 2[INTEGER] AS irrelevant2]]
     *    \_Limit[1000[INTEGER],false]
     *      \_Sample[0.015[DOUBLE],15[INTEGER]]
     *        \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testSampleMerged() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var query = """
            FROM test
            | SAMPLE .3
            | EVAL irrelevant1 = 1
            | SAMPLE .5
            | EVAL irrelevant2 = 2
            | SAMPLE .1
            """;
        var optimized = optimizedPlan(query);

        var eval = as(optimized, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var sample = as(limit.child(), Sample.class);
        var source = as(sample.child(), EsRelation.class);

        assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.015));
    }

    public void testSamplePushDown() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        for (var command : List.of(
            "ENRICH languages_idx on first_name",
            "EVAL x = 1",
            // "INSIST emp_no", // TODO
            "KEEP emp_no",
            "DROP emp_no",
            "RENAME emp_no AS x",
            "GROK first_name \"%{WORD:bar}\"",
            "DISSECT first_name \"%{z}\""
        )) {
            var query = "FROM test | " + command + " | SAMPLE .5";
            var optimized = optimizedPlan(query);

            var unary = as(optimized, UnaryPlan.class);
            var limit = as(unary.child(), Limit.class);
            var sample = as(limit.child(), Sample.class);
            var source = as(sample.child(), EsRelation.class);

            assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.5));
        }
    }

    public void testSamplePushDown_sort() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var query = "FROM test | WHERE emp_no > 0 | SAMPLE 0.5 | LIMIT 100";
        var optimized = optimizedPlan(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var sample = as(filter.child(), Sample.class);
        var source = as(sample.child(), EsRelation.class);

        assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.5));
    }

    public void testSamplePushDown_where() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var query = "FROM test | SORT emp_no | SAMPLE 0.5 | LIMIT 100";
        var optimized = optimizedPlan(query);

        var topN = as(optimized, TopN.class);
        var sample = as(topN.child(), Sample.class);
        var source = as(sample.child(), EsRelation.class);

        assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.5));
    }

    public void testSampleNoPushDown() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        for (var command : List.of("LIMIT 100", "MV_EXPAND languages", "STATS COUNT()")) {
            var query = "FROM test | " + command + " | SAMPLE .5";
            var optimized = optimizedPlan(query);

            var limit = as(optimized, Limit.class);
            var sample = as(limit.child(), Sample.class);
            var unary = as(sample.child(), UnaryPlan.class);
            var source = as(unary.child(), EsRelation.class);
        }
    }

    /**
     * <pre>{@code
     *    Limit[1000[INTEGER],false]
     *    \_Sample[0.5[DOUBLE],null]
     *      \_Join[LEFT,[language_code{r}#4],[language_code{r}#4],[language_code{f}#17]]
     *        |_Eval[[emp_no{f}#6 AS language_code]]
     *        | \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *        \_EsRelation[languages_lookup][LOOKUP][language_code{f}#17, language_name{f}#18]
     * }</pre>
     */
    public void testSampleNoPushDownLookupJoin() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var query = """
            FROM test
            | EVAL language_code = emp_no
            | LOOKUP JOIN languages_lookup ON language_code
            | SAMPLE .5
            """;
        var optimized = optimizedPlan(query);

        var limit = as(optimized, Limit.class);
        var sample = as(limit.child(), Sample.class);
        var join = as(sample.child(), Join.class);
        var eval = as(join.left(), Eval.class);
        var source = as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     *    Limit[1000[INTEGER],false]
     *    \_Sample[0.5[DOUBLE],null]
     *      \_Limit[1000[INTEGER],false]
     *        \_ChangePoint[emp_no{f}#6,hire_date{f}#13,type{r}#4,pvalue{r}#5]
     *          \_TopN[[Order[hire_date{f}#13,ASC,ANY]],1001[INTEGER]]
     *            \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testSampleNoPushDownChangePoint() {
        assumeTrue("sample must be enabled", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());

        var query = """
            FROM test
            | CHANGE_POINT emp_no ON hire_date
            | SAMPLE .5
            """;
        var optimized = optimizedPlan(query);

        var limit = as(optimized, Limit.class);
        var sample = as(limit.child(), Sample.class);
        limit = as(sample.child(), Limit.class);
        var changePoint = as(limit.child(), ChangePoint.class);
        var topN = as(changePoint.child(), TopN.class);
        var source = as(topN.child(), EsRelation.class);
    }

    public void testPushDownConjunctionsToKnnPrefilter() {
        var query = """
            from types
            | where knn(dense_vector, [0, 1, 2]) and integer > 10
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        var knn = as(and.left(), Knn.class);
        List<Expression> filterExpressions = knn.filterExpressions();
        assertThat(filterExpressions.size(), equalTo(1));
        var prefilter = as(filterExpressions.get(0), GreaterThan.class);
        assertThat(and.right(), equalTo(prefilter));
        var esRelation = as(filter.child(), EsRelation.class);
    }

    public void testPushDownMultipleFiltersToKnnPrefilter() {
        var query = """
            from types
            | where knn(dense_vector, [0, 1, 2])
            | where integer > 10
            | where keyword == "test"
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var firstAnd = as(filter.condition(), And.class);
        var knn = as(firstAnd.left(), Knn.class);
        var prefilterAnd = as(firstAnd.right(), And.class);
        as(prefilterAnd.left(), GreaterThan.class);
        as(prefilterAnd.right(), Equals.class);
        List<Expression> filterExpressions = knn.filterExpressions();
        assertThat(filterExpressions.size(), equalTo(1));
        assertThat(prefilterAnd, equalTo(filterExpressions.get(0)));
    }

    public void testNotPushDownDisjunctionsToKnnPrefilter() {
        var query = """
            from types
            | where knn(dense_vector, [0, 1, 2]) or integer > 10
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var or = as(filter.condition(), Or.class);
        var knn = as(or.left(), Knn.class);
        List<Expression> filterExpressions = knn.filterExpressions();
        assertThat(filterExpressions.size(), equalTo(0));
    }

    public void testPushDownConjunctionsAndNotDisjunctionsToKnnPrefilter() {
        /*
            and
                and
                    or
                        knn(dense_vector, [0, 1, 2], 10)
                        integer > 10
                    keyword == "test"
                 or
                     short < 5
                     double > 5.0
         */
        // Both conjunctions are pushed down to knn prefilters, disjunctions are not
        var query = """
            from types
            | where
                 ((knn(dense_vector, [0, 1, 2]) or integer > 10) and keyword == "test") and ((short < 5) or (double > 5.0))
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        var leftAnd = as(and.left(), And.class);
        var rightOr = as(and.right(), Or.class);
        var leftOr = as(leftAnd.left(), Or.class);
        var knn = as(leftOr.left(), Knn.class);
        var rightOrPrefilter = as(knn.filterExpressions().get(0), Or.class);
        assertThat(rightOr, equalTo(rightOrPrefilter));
        var leftAndPrefilter = as(knn.filterExpressions().get(1), Equals.class);
        assertThat(leftAnd.right(), equalTo(leftAndPrefilter));
    }

    public void testMorePushDownConjunctionsAndNotDisjunctionsToKnnPrefilter() {
        /*
            or
                or
                    and
                        knn(dense_vector, [0, 1, 2], 10)
                        integer > 10
                    keyword == "test"
                 and
                     short < 5
                     double > 5.0
         */
        // Just the conjunction is pushed down to knn prefilters, disjunctions are not
        var query = """
            from types
            | where
                 ((knn(dense_vector, [0, 1, 2]) and integer > 10) or keyword == "test") or ((short < 5) and (double > 5.0))
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var or = as(filter.condition(), Or.class);
        var leftOr = as(or.left(), Or.class);
        var leftAnd = as(leftOr.left(), And.class);
        var knn = as(leftAnd.left(), Knn.class);
        var rightAndPrefilter = as(knn.filterExpressions().get(0), GreaterThan.class);
        assertThat(leftAnd.right(), equalTo(rightAndPrefilter));
    }

    public void testMultipleKnnQueriesInPrefilters() {
        /*
            and
                or
                    knn(dense_vector, [0, 1, 2], 10)
                    integer > 10
                or
                    keyword == "test"
                    knn(dense_vector, [4, 5, 6], 10)
         */
        var query = """
            from types
            | where ((knn(dense_vector, [0, 1, 2]) or integer > 10) and ((keyword == "test") or knn(dense_vector, [4, 5, 6])))
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var and = as(filter.condition(), And.class);

        // First OR (knn1 OR integer > 10)
        var firstOr = as(and.left(), Or.class);
        var firstKnn = as(firstOr.left(), Knn.class);
        var integerGt = as(firstOr.right(), GreaterThan.class);

        // Second OR (keyword == "test" OR knn2)
        var secondOr = as(and.right(), Or.class);
        as(secondOr.left(), Equals.class);
        var secondKnn = as(secondOr.right(), Knn.class);

        // First KNN should have the second OR as its filter
        List<Expression> firstKnnFilters = firstKnn.filterExpressions();
        assertThat(firstKnnFilters.size(), equalTo(1));
        assertTrue(firstKnnFilters.contains(secondOr.left()));

        // Second KNN should have the first OR as its filter
        List<Expression> secondKnnFilters = secondKnn.filterExpressions();
        assertThat(secondKnnFilters.size(), equalTo(1));
        assertTrue(secondKnnFilters.contains(firstOr.right()));
    }

    public void testKnnImplicitLimit() {
        var query = """
            from types
            | where knn(dense_vector, [0, 1, 2])
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(1000));
    }

    public void testKnnWithLimit() {
        var query = """
            from types
            | where knn(dense_vector, [0, 1, 2])
            | limit 10
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(10));
    }

    public void testKnnWithTopN() {
        var query = """
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2])
            | sort _score desc
            | limit 10
            """;
        var optimized = planTypes(query);

        var topN = as(optimized, TopN.class);
        var filter = as(topN.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(10));
    }

    public void testKnnWithMultipleLimitsAfterTopN() {
        var query = """
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2])
            | limit 20
            | sort _score desc
            | limit 10
            """;
        var optimized = planTypes(query);

        var topN = as(optimized, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        var limit = as(topN.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(20));
    }

    public void testKnnWithMultipleLimitsCombined() {
        var query = """
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2])
            | limit 20
            | limit 10
            """;
        var optimized = planTypes(query);

        var limit = as(optimized, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(10));
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(10));
    }

    public void testKnnWithMultipleClauses() {
        var query = """
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2]) and match(keyword, "test")
            | where knn(dense_vector, [1, 2, 3])
            | sort _score
            | limit 10
            """;
        var optimized = planTypes(query);

        var topN = as(optimized, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10));
        var filter = as(topN.child(), Filter.class);
        var firstAnd = as(filter.condition(), And.class);
        var fistKnn = as(firstAnd.right(), Knn.class);
        assertThat(((Literal) fistKnn.query()).value(), is(List.of(1.0f, 2.0f, 3.0f)));
        var secondAnd = as(firstAnd.left(), And.class);
        var secondKnn = as(secondAnd.left(), Knn.class);
        assertThat(((Literal) secondKnn.query()).value(), is(List.of(0.0f, 1.0f, 2.0f)));
    }

    public void testKnnWithStats() {
        assertThat(
            typesError("from types | where knn(dense_vector, [0, 1, 2]) | stats c = count(*)"),
            containsString("Knn function must be used with a LIMIT clause")
        );
    }

    public void testKnnWithRerankAmdTopN() {
        assertThat(typesError("""
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2])
            | rerank "some text" on text with { "inference_id" : "reranking-inference-id" }
            | sort _score desc
            | limit 10
            """), containsString("Knn function must be used with a LIMIT clause"));
    }

    public void testKnnWithRerankAmdLimit() {
        var query = """
            from types metadata _score
            | where knn(dense_vector, [0, 1, 2])
            | rerank "some text" on text with { "inference_id" : "reranking-inference-id" }
            | limit 100
            """;

        var optimized = planTypes(query);

        var rerank = as(optimized, Rerank.class);
        var limit = as(rerank.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(100));
        var filter = as(limit.child(), Filter.class);
        var knn = as(filter.condition(), Knn.class);
        assertThat(knn.implicitK(), equalTo(100));
    }

    private LogicalPlanOptimizer getCustomRulesLogicalPlanOptimizer(
        List<RuleExecutor.Batch<LogicalPlan>> batches,
        TransportVersion minimumVersion
    ) {
        LogicalOptimizerContext context = new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), minimumVersion);
        LogicalPlanOptimizer customOptimizer = new LogicalPlanOptimizer(context) {
            @Override
            protected List<Batch<LogicalPlan>> batches() {
                return batches;
            }
        };
        return customOptimizer;
    }

    public void testVerifierOnAdditionalAttributeAdded() throws Exception {
        var plan = optimizedPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        Holder<Integer> appliedCount = new Holder<>(0);
        // use a custom rule that adds another output attribute
        var customRuleBatch = new RuleExecutor.Batch<>(
            "CustomRuleBatch",
            RuleExecutor.Limiter.ONCE,
            new OptimizerRules.ParameterizedOptimizerRule<Aggregate, LogicalOptimizerContext>(UP) {
                @Override
                protected LogicalPlan rule(Aggregate plan, LogicalOptimizerContext context) {
                    // This rule adds a missing attribute to the plan output
                    // We only want to apply it once, so we use a static counter
                    if (appliedCount.get() == 0) {
                        appliedCount.set(appliedCount.get() + 1);
                        Literal additionalLiteral = new Literal(Source.EMPTY, "additional literal", INTEGER);
                        return new Eval(plan.source(), plan, List.of(new Alias(Source.EMPTY, "additionalAttribute", additionalLiteral)));
                    }
                    return plan;
                }

            }
        );
        LogicalPlanOptimizer customRulesLogicalPlanOptimizer = getCustomRulesLogicalPlanOptimizer(
            List.of(customRuleBatch),
            logicalOptimizer.context().minimumVersion()
        );
        Exception e = expectThrows(VerificationException.class, () -> customRulesLogicalPlanOptimizer.optimize(plan));
        assertThat(e.getMessage(), containsString("Output has changed from"));
        assertThat(e.getMessage(), containsString("additionalAttribute"));
    }

    public void testVerifierOnAttributeDatatypeChanged() {
        var plan = optimizedPlan("""
            from test
            | stats a = min(salary) by emp_no
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var min = as(Alias.unwrap(aggregate.aggregates().get(0)), Min.class);
        var salary = as(min.field(), NamedExpression.class);
        assertThat(salary.name(), is("salary"));
        Holder<Integer> appliedCount = new Holder<>(0);
        // use a custom rule that changes the datatype of an output attribute
        var customRuleBatch = new RuleExecutor.Batch<>(
            "CustomRuleBatch",
            RuleExecutor.Limiter.ONCE,
            new OptimizerRules.ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext>(DOWN) {
                @Override
                protected LogicalPlan rule(LogicalPlan plan, LogicalOptimizerContext context) {
                    // We only want to apply it once, so we use a static counter
                    if (appliedCount.get() == 0) {
                        appliedCount.set(appliedCount.get() + 1);
                        Limit limit = as(plan, Limit.class);
                        Limit newLimit = new Limit(plan.source(), limit.limit(), limit.child()) {
                            @Override
                            public List<Attribute> output() {
                                List<Attribute> oldOutput = super.output();
                                List<Attribute> newOutput = new ArrayList<>(oldOutput);
                                newOutput.set(0, oldOutput.get(0).withDataType(DataType.DATETIME));
                                return newOutput;
                            }
                        };
                        return newLimit;
                    }
                    return plan;
                }

            }
        );
        LogicalPlanOptimizer customRulesLogicalPlanOptimizer = getCustomRulesLogicalPlanOptimizer(
            List.of(customRuleBatch),
            logicalOptimizerCtx.minimumVersion()
        );
        Exception e = expectThrows(VerificationException.class, () -> customRulesLogicalPlanOptimizer.optimize(plan));
        assertThat(e.getMessage(), containsString("Output has changed from"));
    }

    public void testTranslateDataGroupedByTBucket() {
        assumeTrue("requires TBUCKET capability enabled", EsqlCapabilities.Cap.TBUCKET.isEnabled());
        var query = """
            FROM sample_data
            | STATS min = MIN(@timestamp), max = MAX(@timestamp) BY bucket = TBUCKET(1 hour)
            | SORT min
            """;

        var plan = planSample(query);
        var topN = as(plan, TopN.class);

        Aggregate aggregate = as(topN.child(), Aggregate.class);
        assertThat(aggregate, not(instanceOf(TimeSeriesAggregate.class)));

        assertThat(aggregate.groupings(), hasSize(1));
        assertThat(aggregate.groupings().get(0), instanceOf(ReferenceAttribute.class));
        assertThat(as(aggregate.groupings().getFirst(), ReferenceAttribute.class).name(), equalTo("bucket"));

        assertThat(aggregate.aggregates(), hasSize(3));
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertThat(aggregates, hasSize(3));
        Alias a = as(aggregates.get(0), Alias.class);
        assertEquals("min", a.name());
        Min min = as(a.child(), Min.class);
        FieldAttribute fa = as(min.field(), FieldAttribute.class);
        assertEquals("@timestamp", fa.name());
        a = as(aggregates.get(1), Alias.class);
        assertEquals("max", a.name());
        Max max = as(a.child(), Max.class);
        fa = as(max.field(), FieldAttribute.class);
        assertEquals("@timestamp", fa.name());
        ReferenceAttribute ra = as(aggregates.get(2), ReferenceAttribute.class);
        assertEquals("bucket", ra.name());
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(aggregate.aggregates().get(2).id()));
        Eval eval = as(aggregate.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        Alias bucketAlias = eval.fields().get(0);
        assertThat(bucketAlias.child(), instanceOf(Bucket.class));
        assertThat(Expressions.attribute(bucketAlias).id(), equalTo(aggregate.aggregates().get(2).id()));
        Bucket bucket = as(Alias.unwrap(bucketAlias), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
        assertThat(bucket.children().get(0), instanceOf(FieldAttribute.class));
        assertThat(((FieldAttribute) bucket.children().get(0)).name(), equalTo("@timestamp"));
        assertThat(bucket.children().get(1), instanceOf(Literal.class));
        assertThat(((Literal) bucket.children().get(1)).value(), equalTo(Duration.ofHours(1)));
    }

    public void testTranslateMetricsGroupedByTBucketInTSMode() {
        var query = "TS k8s | STATS sum(rate(network.total_bytes_in)) BY tbucket(1h)";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg, not(instanceOf(TimeSeriesAggregate.class)));
        assertThat(finalAgg.aggregates(), hasSize(2));
        TimeSeriesAggregate aggsByTsid = as(finalAgg.child(), TimeSeriesAggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        assertNotNull(aggsByTsid.timeBucket());
        assertThat(aggsByTsid.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
        Eval eval = as(aggsByTsid.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertThat(relation.indexMode(), equalTo(IndexMode.TIME_SERIES));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(eval.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(eval.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testDecayOriginMustBeLiteral() {
        var query = """
            FROM employees
            | EVAL decay_result = decay(salary, salary, 10, {"offset": 5, "decay": 0.5, "type": "linear"})
            | KEEP decay_result
            | LIMIT 5""";

        Exception e = expectThrows(
            VerificationException.class,
            () -> logicalOptimizer.optimize(defaultAnalyzer().analyze(parser.createStatement(query)))
        );
        assertThat(e.getMessage(), containsString("has non-literal value [origin]"));
    }

    public void testDecayScaleMustBeLiteral() {
        var query = """
            FROM employees
            | EVAL decay_result = decay(salary, 10, salary, {"offset": 5, "decay": 0.5, "type": "linear"})
            | KEEP decay_result
            | LIMIT 5""";

        Exception e = expectThrows(
            VerificationException.class,
            () -> logicalOptimizer.optimize(defaultAnalyzer().analyze(parser.createStatement(query)))
        );
        assertThat(e.getMessage(), containsString("has non-literal value [scale]"));
    }

    /**
     *
     * Project[[languages{f}#8, language_code{f}#16, language_name{f}#17]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#8, language_code{f}#16],[languages{f}#8],[language_code{f}#16],languages{f}#8 == language_code{
     * f}#16]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#16, language_name{f}#17]
     *
     */
    public void testLookupJoinExpressionSwapped() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        LogicalPlan plan = optimizedPlan("""
            from test
            | keep languages
            | lookup join languages_lookup ON language_code == languages
            """);
        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 1000, true);
        var join = as(limit.child(), Join.class);
        assertEquals("language_code == languages", join.config().joinOnConditions().toString());
        var equals = as(join.config().joinOnConditions(), Equals.class);
        // we expect left and right to be swapped
        var left = as(equals.left(), Attribute.class);
        var right = as(equals.right(), Attribute.class);
        assertEquals("language_code", right.name());
        assertEquals("languages", left.name());
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     *
     * Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18, la
     * nguages{f}#12 AS language_code_left#4, last_name{f}#13, long_noidx{f}#19, salary{f}#14, language_code{f}#20, language_name{f}#21]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#12, languages{f}#12],[language_code{f}#20, language_code{f}#20],languages{f}#12 != language_co
     * de{f}#20 AND languages{f}#12 > language_code{f}#20]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#20, language_name{f}#21]
     *
     */
    public void testLookupJoinExpressionSameAttrsDifferentConditions() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        String query = """
            from test
            | rename languages as language_code_left
            | lookup join languages_lookup ON language_code_left != language_code and language_code_left > language_code
            """;

        LogicalPlan plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var limit = asLimit(project.child(), 1000, true);
        var join = as(limit.child(), Join.class);

        // Verify the join conditions contain both != and > operators
        var joinConditions = join.config().joinOnConditions();
        assertThat(joinConditions, instanceOf(And.class));
        var and = as(joinConditions, And.class);

        // Check the left condition (should be !=)
        var notEquals = as(and.left(), NotEquals.class);
        var leftAttr = as(notEquals.left(), Attribute.class);
        var rightAttr = as(notEquals.right(), Attribute.class);
        assertEquals("languages", leftAttr.name());
        assertEquals("language_code", rightAttr.name());

        // Check the right condition (should be >)
        var greaterThan = as(and.right(), GreaterThan.class);
        var leftAttrGT = as(greaterThan.left(), Attribute.class);
        var rightAttrGT = as(greaterThan.right(), Attribute.class);
        assertEquals("languages", leftAttrGT.name());
        assertEquals("language_code", rightAttrGT.name());

        // Verify the left side of join has Limit then EsRelation
        var limitPastJoin = asLimit(join.left(), 1000, false);
        as(limitPastJoin.child(), EsRelation.class);

        // Verify the right side of join is EsRelation with LOOKUP
        as(join.right(), EsRelation.class);
    }

    /**
     * Project[[_meta_field{f}#16, emp_no{f}#10, first_name{f}#11 AS language_name#4, gender{f}#12, hire_date{f}#17, job{f}#1
     * 8, job.raw{f}#19, languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15, language_code{f}#21]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[NOT(language_code{f}#21 >= 50[INTEGER])]
     *     \_Join[LEFT,[first_name{f}#11],[language_name{f}#22],null]
     *       |_Filter[first_name{f}#11 == [KEYWORD]]
     *       | \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *       \_Filter[language_code{f}#21 &lt; 50[INTEGER]]
     *         \_EsRelation[languages_lookup][LOOKUP][language_code{f}#21, language_name{f}#22]
     */
    public void LookupJoinSemanticFilterDeupPushdown() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as language_name
            | lookup join languages_lookup on language_name
            | where NOT language_code >= 50 OR language_name == ""
            | where language_code < 50
            | where language_name == ""
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);

        // Verify the top-level filter is NOT(language_code >= 50)
        var not = as(filter.condition(), Not.class);
        var gte = as(not.field(), GreaterThanOrEqual.class);
        assertThat(Expressions.name(gte.left()), equalTo("language_code"));
        assertThat(gte.right().fold(FoldContext.small()), equalTo(50));

        // Verify the join structure
        var join = as(filter.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinTypes.LEFT));

        // Verify left side has filter for language_name == ""
        var leftFilter = as(join.left(), Filter.class);
        var leftEquals = as(leftFilter.condition(), Equals.class);
        assertThat(Expressions.name(leftEquals.left()), equalTo("first_name"));
        assertThat(leftEquals.right().fold(FoldContext.small()), equalTo(new BytesRef("")));

        var leftRelation = as(leftFilter.child(), EsRelation.class);

        // Verify right side has filter for language_code < 50
        var rightFilter = as(join.right(), Filter.class);
        var rightLt = as(rightFilter.condition(), LessThan.class);
        assertThat(Expressions.name(rightLt.left()), equalTo("language_code"));
        assertThat(rightLt.right().fold(FoldContext.small()), equalTo(50));

        var rightRelation = as(rightFilter.child(), EsRelation.class);
    }

    /**
     * EsqlProject[[@timestamp{r}#3]]
     * \_Eval[[1715300259000[DATETIME] AS @timestamp#3]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[k8s][@timestamp{f}#5, client.ip{f}#9, cluster{f}#6, ...]
     */
    public void testTranslateTRangeFoldsToLiteralWhenTimestampInsideRange() {
        String timestampValue = "2024-05-10T00:17:39.000Z";

        String query = String.format(Locale.ROOT, """
            TS k8s
            | EVAL @timestamp = to_datetime("%s")
            | WHERE TRANGE("2024-05-10T00:17:14.000Z", "2024-05-10T00:18:33.000Z")
            | KEEP @timestamp
            """, timestampValue);

        LogicalPlan statement = parser.createStatement(query);
        LogicalPlan analyze = metricsAnalyzer.analyze(statement);
        LogicalPlan plan = logicalOptimizerWithLatestVersion.optimize(analyze);

        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));

        Literal timestampLiteral = as(Alias.unwrap(eval.fields().getFirst()), Literal.class);
        long expectedTimestamp = DateUtils.asDateTimeWithNanos(timestampValue, DateUtils.UTC).toInstant().toEpochMilli();
        assertThat(timestampLiteral.fold(FoldContext.small()), equalTo(expectedTimestamp));

        Limit limit = asLimit(eval.child(), 1000, false);
        assertThat(limit.children(), hasSize(1));

        EsRelation relation = as(limit.child(), EsRelation.class);
        assertThat(relation.children(), hasSize(0));
    }

    /**
     * LocalRelation[[@timestamp{r}#3],EMPTY]
     */
    public void testTranslateTRangeFoldsToLiteralWhenTimestampOutsideRange() {
        String timestampValue = "2024-05-10T00:15:39.000Z";

        String query = String.format(Locale.ROOT, """
            TS k8s
            | EVAL @timestamp = to_datetime("%s")
            | WHERE TRANGE("2024-05-10T00:17:14.000Z", "2024-05-10T00:18:33.000Z")
            | KEEP @timestamp
            """, timestampValue);

        LogicalPlan statement = parser.createStatement(query);
        LogicalPlan analyze = metricsAnalyzer.analyze(statement);
        LogicalPlan plan = logicalOptimizerWithLatestVersion.optimize(analyze);

        LocalRelation relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), hasSize(1));
        assertThat(relation.children(), hasSize(0));
    }

    /**
     * LocalRelation[[@timestamp{r}#3],EMPTY]
     */
    public void testTranslateTRangeFoldsToLocalRelation() {
        LogicalPlan statement = parser.createStatement("""
            TS k8s
            | EVAL @timestamp = null::datetime
            | WHERE TRANGE("2024-05-10T00:17:14.000Z", "2024-05-10T00:18:33.000Z")
            | KEEP @timestamp
            """);
        LogicalPlan analyze = metricsAnalyzer.analyze(statement);
        LogicalPlan plan = logicalOptimizerWithLatestVersion.optimize(analyze);

        LocalRelation relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), hasSize(1));
        assertThat(relation.children(), hasSize(0));

        Attribute attribute = relation.output().get(0);
        assertThat(attribute.name(), equalTo("@timestamp"));
    }

    /*
     * Nested subqueries are not supported yet.
     */
    public void testNestedSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM test, (FROM test, (FROM languages
                                                      | WHERE language_code > 0))
            | WHERE emp_no > 10000
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        assertEquals("1:18: Nested subqueries are not supported", e.getMessage().substring(header.length()));
    }

    /*
     * FORK inside subquery is not supported yet.
     */
    public void testForkInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM test, (FROM languages
                                 | WHERE language_code > 0
                                 | FORK (WHERE language_name == "a") (WHERE language_name == "b")
                                 )
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        assertEquals("3:24: FORK inside subquery is not supported", e.getMessage().substring(header.length()));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[MATCH(last_name{f}#8,Doe[KEYWORD])]
     *   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     */
    public void testFullTextFunctionOnNull() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = optimizedPlan(String.format(Locale.ROOT, """
            from test
            | where %s(null, "John") or %s(last_name, "Doe")
            """, functionName, functionName));

        // Limit[1000[INTEGER],false,false]
        var limit = as(plan, Limit.class);

        // Filter has a single match on last_name only
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        FieldAttribute lastName = as(fullTextFunction.field(), FieldAttribute.class);
        assertEquals("last_name", lastName.name());
        Literal queryLiteral = as(fullTextFunction.query(), Literal.class);
        assertEquals(new BytesRef("Doe"), queryLiteral.value());

        // EsRelation[test]
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    public void testFullTextFunctionOnEvalNull() {
        String functionName = randomFrom("match", "match_phrase");
        var plan = optimizedPlan(String.format(Locale.ROOT, """
            from test
            | eval some_field = null
            | where %s(some_field, "John") or %s(last_name, "Doe")
            """, functionName, functionName));

        // Eval null
        var eval = as(plan, Eval.class);
        assertThat(eval.fields().getFirst().child(), is(NULL));

        var limit = as(eval.child(), Limit.class);

        // Filter has a single match on last_name only
        var filter = as(limit.child(), Filter.class);
        var fullTextFunction = as(filter.condition(), SingleFieldFullTextFunction.class);
        FieldAttribute lastName = as(fullTextFunction.field(), FieldAttribute.class);
        assertEquals("last_name", lastName.name());
        Literal queryLiteral = as(fullTextFunction.query(), Literal.class);
        assertEquals(new BytesRef("Doe"), queryLiteral.value());

        // EsRelation[test]
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * Renaming or shadowing the @timestamp field prior to running stats with TS command is not allowed.
     */
    public void testTranslateMetricsAfterRenamingTimestamp() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement("""
                    TS k8s |
                    EVAL @timestamp = region |
                    STATS max(network.cost), count(network.eth0.rx)
                    """)))
            ).getMessage(),
            containsString("""
                Functions [count(network.eth0.rx), max(network.cost)] require a @timestamp field of type date or date_nanos \
                to be present when run with the TS command, but it was not present.""")
        );

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement("""
                    TS k8s |
                    DISSECT event "%{@timestamp} %{network.total_bytes_in}" |
                    STATS ohxqxpSqEZ = avg(network.eth0.currently_connected_clients)
                    """)))
            ).getMessage(),
            containsString("""
                Function [avg(network.eth0.currently_connected_clients)] requires a @timestamp field of type date or date_nanos \
                to be present when run with the TS command, but it was not present.""")
        );

        // we may want to allow this later
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer.analyze(parser.createStatement("""
                    TS k8s |
                    EVAL `@timestamp` = @timestamp + 1day |
                    STATS std_dev(network.eth0.currently_connected_clients)
                    """)))
            ).getMessage(),
            containsString("""
                Function [std_dev(network.eth0.currently_connected_clients)] requires a @timestamp field of type date or date_nanos \
                to be present when run with the TS command, but it was not present.""")
        );
    }
}
