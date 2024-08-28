/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.Build;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.rules.LiteralsOnTheRight;
import org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownAndCombineFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownEnrich;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownEval;
import org.elasticsearch.xpack.esql.optimizer.rules.PushDownRegexExtract;
import org.elasticsearch.xpack.esql.optimizer.rules.SplitInWithFoldableValue;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.junit.BeforeClass;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.localSource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.EQ;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.GT;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.GTE;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.LT;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation.LTE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LogicalPlanOptimizerTests extends ESTestCase {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);
    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static Map<String, EsField> mapping;
    private static Map<String, EsField> mappingAirports;
    private static Map<String, EsField> mappingTypes;
    private static Analyzer analyzerAirports;
    private static Analyzer analyzerTypes;
    private static Map<String, EsField> mappingExtra;
    private static Analyzer analyzerExtra;
    private static EnrichResolution enrichResolution;
    private static final LiteralsOnTheRight LITERALS_ON_THE_RIGHT = new LiteralsOnTheRight();

    private static Map<String, EsField> metricMapping;
    private static Analyzer metricsAnalyzer;

    private static class SubstitutionOnlyOptimizer extends LogicalPlanOptimizer {
        static SubstitutionOnlyOptimizer INSTANCE = new SubstitutionOnlyOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));

        SubstitutionOnlyOptimizer(LogicalOptimizerContext optimizerContext) {
            super(optimizerContext);
        }

        @Override
        protected List<Batch<LogicalPlan>> batches() {
            return List.of(substitutions());
        }
    }

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");

        // Most tests used data from the test index, so we load it here, and use it in the plan() function.
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, enrichResolution),
            TEST_VERIFIER
        );

        // Some tests use data from the airports index, so we load it here, and use it in the plan_airports() function.
        mappingAirports = loadMapping("mapping-airports.json");
        EsIndex airports = new EsIndex("airports", mappingAirports, Set.of("airports"));
        IndexResolution getIndexResultAirports = IndexResolution.valid(airports);
        analyzerAirports = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultAirports, enrichResolution),
            TEST_VERIFIER
        );

        // Some tests need additional types, so we load that index here and use it in the plan_types() function.
        mappingTypes = loadMapping("mapping-all-types.json");
        EsIndex types = new EsIndex("types", mappingTypes, Set.of("types"));
        IndexResolution getIndexResultTypes = IndexResolution.valid(types);
        analyzerTypes = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultTypes, enrichResolution),
            TEST_VERIFIER
        );

        // Some tests use mappings from mapping-extra.json to be able to test more types so we load it here
        mappingExtra = loadMapping("mapping-extra.json");
        EsIndex extra = new EsIndex("extra", mappingExtra, Set.of("extra"));
        IndexResolution getIndexResultExtra = IndexResolution.valid(extra);
        analyzerExtra = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultExtra, enrichResolution),
            TEST_VERIFIER
        );

        metricMapping = loadMapping("k8s-mappings.json");
        var metricsIndex = IndexResolution.valid(new EsIndex("k8s", metricMapping, Set.of("k8s")));
        metricsAnalyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), metricsIndex, enrichResolution),
            TEST_VERIFIER
        );
    }

    public void testEmptyProjections() {
        var plan = plan("""
            from test
            | keep salary
            | drop salary
            """);

        var relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), is(empty()));
        assertThat(relation.supplier().get(), emptyArray());
    }

    public void testEmptyProjectionInStat() {
        var plan = plan("""
            from test
            | stats c = count(salary)
            | drop c
            """);

        var relation = as(plan, LocalRelation.class);
        assertThat(relation.output(), is(empty()));
        assertThat(relation.supplier().get(), emptyArray());
    }

    /**
     * Expects
     *
     * EsqlProject[[x{r}#6]]
     * \_Eval[[1[INTEGER] AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_LocalRelation[[{e}#18],[ConstantNullBlock[positions=1]]]
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
        assertThat(singleRow.length, equalTo(1));
        assertThat(singleRow[0].getPositionCount(), equalTo(1));

        var exprs = eval.fields();
        assertThat(exprs.size(), equalTo(1));
        var alias = as(exprs.get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));
        assertThat(alias.child().fold(), equalTo(1));
    }

    /**
     * Expects
     *
     * EsqlProject[[x{r}#8]]
     * \_Eval[[1[INTEGER] AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[emp_no{f}#15],[emp_no{f}#15]]
     *       \_Filter[languages{f}#18 > 1[INTEGER]]
     *         \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
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
        assertThat(Expressions.names(agg.aggregates()), contains("emp_no"));

        var exprs = eval.fields();
        assertThat(exprs.size(), equalTo(1));
        var alias = as(exprs.get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));
        assertThat(alias.child().fold(), equalTo(1));

        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("languages"));
        assertThat(filterCondition.right().fold(), equalTo(1));
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
     * Project[[languages{f}#12 AS f2]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
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
     * Project[[last_name{f}#26, languages{f}#25 AS f2, f4{r}#13]]
     * \_Eval[[languages{f}#25 + 3[INTEGER] AS f4]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[last_name{f}#23, first_name{f}#20],[SUM(salary{f}#24) AS s, last_name{f}#23, first_name{f}#20, first_name{f}#2
     * 0 AS k]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
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
     * TopN[[Order[x{r}#10,ASC,LAST]],1000[INTEGER]]
     * \_Aggregate[[languages{f}#16],[MAX(emp_no{f}#13) AS x, languages{f}#16]]
     *   \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
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
    }

    // expected stats b by b (grouping overrides the rest of the aggs)

    /**
     * Expects
     * TopN[[Order[b{r}#10,ASC,LAST]],1000[INTEGER]]
     * \_Aggregate[[b{r}#10],[languages{f}#16 AS b]]
     *   \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
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
    }

    /**
     * Project[[s{r}#4 AS d, s{r}#4, last_name{f}#21, first_name{f}#18]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[last_name{f}#21, first_name{f}#18],[SUM(salary{f}#22) AS s, last_name{f}#21, first_name{f}#18]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testCombineProjectionWithDuplicateAggregation() {
        var plan = plan("""
            from test
            | stats s = sum(salary), d = sum(salary), c = sum(salary) by last_name, first_name
            | keep d, s, last_name, first_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("d", "s", "last_name", "first_name"));
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name"));
        assertThat(Alias.unwrap(agg.aggregates().get(0)), instanceOf(Sum.class));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#12],[COUNT(salary{f}#16) AS count(salary), first_name{f}#12 AS x]]
     *   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#16],[SUM(emp_no{f}#15) AS s, COUNT(first_name{f}#16) AS c, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[first_name{f}#16],[SUM(emp_no{f}#15) AS s, first_name{f}#16 AS f]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
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
     * EsqlProject[[x{r}#3, y{r}#6]]
     * \_Eval[[emp_no{f}#9 + 2[INTEGER] AS x, salary{f}#14 + 3[INTEGER] AS y]]
     *   \_Limit[10000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
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
            new PushDownAndCombineLimits().rule(anotherLimit)
        );
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
        assertEquals(
            new Limit(EMPTY, L(minimum), relation),
            new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG)).optimize(plan)
        );
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, randomZone());
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, randomZone());
    }

    public void testCombineFilters() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
    }

    public void testCombineFiltersLikeRLike() {
        EsRelation relation = relation();
        RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        Filter fb = new Filter(EMPTY, fa, conditionB);

        assertEquals(new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)), new PushDownAndCombineFilters().apply(fb));
    }

    public void testPushDownFilter() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
    }

    public void testPushDownLikeRlikeFilter() {
        EsRelation relation = relation();
        org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike conditionA = rlike(getFieldAttribute("a"), "foo");
        WildcardLike conditionB = wildcardLike(getFieldAttribute("b"), "bar");

        Filter fa = new Filter(EMPTY, relation, conditionA);
        List<FieldAttribute> projections = singletonList(getFieldAttribute("b"));
        EsqlProject keep = new EsqlProject(EMPTY, fa, projections);
        Filter fb = new Filter(EMPTY, keep, conditionB);

        Filter combinedFilter = new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB));
        assertEquals(new EsqlProject(EMPTY, combinedFilter, projections), new PushDownAndCombineFilters().apply(fb));
    }

    // from ... | where a > 1 | stats count(1) by b | where count(1) >= 3 and b < 2
    // => ... | where a > 1 and b < 2 | stats count(1) by b | where count(1) >= 3
    public void testSelectivelyPushDownFilterPastFunctionAgg() {
        EsRelation relation = relation();
        GreaterThan conditionA = greaterThanOf(getFieldAttribute("a"), ONE);
        LessThan conditionB = lessThanOf(getFieldAttribute("b"), TWO);
        GreaterThanOrEqual aggregateCondition = greaterThanOrEqualOf(new Count(EMPTY, ONE), THREE);

        Filter fa = new Filter(EMPTY, relation, conditionA);
        // invalid aggregate but that's fine cause its properties are not used by this rule
        Aggregate aggregate = new Aggregate(
            EMPTY,
            fa,
            Aggregate.AggregateType.STANDARD,
            singletonList(getFieldAttribute("b")),
            emptyList()
        );
        Filter fb = new Filter(EMPTY, aggregate, new And(EMPTY, aggregateCondition, conditionB));

        // expected
        Filter expected = new Filter(
            EMPTY,
            new Aggregate(
                EMPTY,
                new Filter(EMPTY, relation, new And(EMPTY, conditionA, conditionB)),
                Aggregate.AggregateType.STANDARD,
                singletonList(getFieldAttribute("b")),
                emptyList()
            ),
            aggregateCondition
        );
        assertEquals(expected, new PushDownAndCombineFilters().apply(fb));
    }

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
            contains(
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
        assertThat(dissect.extractedFields(), contains(referenceAttribute("y", DataType.KEYWORD)));
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
        assertThat(grok.extractedFields(), contains(referenceAttribute("y", DataType.KEYWORD)));
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
     * TopN[[Order[first_name{f}#170,ASC,LAST]],1000[INTEGER]]
     *  \_MvExpand[first_name{f}#170]
     *    \_TopN[[Order[emp_no{f}#169,ASC,LAST]],1000[INTEGER]]
     *      \_EsRelation[test][avg_worked_seconds{f}#167, birth_date{f}#168, emp_n..]
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
        topN = as(mvExpand.child(), TopN.class);
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     *  \_MvExpand[x{r}#159]
     *    \_EsqlProject[[first_name{f}#162 AS x]]
     *      \_Limit[1000[INTEGER]]
     *        \_EsRelation[test][first_name{f}#162]
     */
    public void testCopyDefaultLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | rename first_name as x
            | keep x
            | mv_expand x
            """);

        var limit = as(plan, Limit.class);
        var mvExpand = as(limit.child(), MvExpand.class);
        var keep = as(mvExpand.child(), EsqlProject.class);
        var limitPastMvExpand = as(keep.child(), Limit.class);
        assertThat(limitPastMvExpand.limit(), equalTo(limit.limit()));
        as(limitPastMvExpand.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[10[INTEGER]]
     *  \_MvExpand[first_name{f}#155]
     *    \_EsqlProject[[first_name{f}#155, last_name{f}#156]]
     *      \_Limit[1[INTEGER]]
     *        \_EsRelation[test][first_name{f}#155, last_name{f}#156]
     */
    public void testDontPushDownLimitPastMvExpand() {
        LogicalPlan plan = optimizedPlan("""
            from test
            | limit 1
            | keep first_name, last_name
            | mv_expand first_name
            | limit 10""");

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(10));
        var mvExpand = as(limit.child(), MvExpand.class);
        var project = as(mvExpand.child(), EsqlProject.class);
        limit = as(project.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(1));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#141, first_name{f}#142, languages{f}#143, lll{r}#132, salary{f}#147]]
     *  \_TopN[[Order[salary{f}#147,DESC,FIRST], Order[first_name{f}#142,ASC,LAST]],5[INTEGER]]
     *    \_Limit[5[INTEGER]]
     *      \_MvExpand[salary{f}#147]
     *        \_Eval[[languages{f}#143 + 5[INTEGER] AS lll]]
     *          \_Filter[languages{f}#143 > 1[INTEGER]]
     *            \_Limit[10[INTEGER]]
     *              \_MvExpand[first_name{f}#142]
     *                \_TopN[[Order[emp_no{f}#141,DESC,FIRST]],10[INTEGER]]
     *                  \_Filter[emp_no{f}#141 &lt; 10006[INTEGER]]
     *                    \_EsRelation[test][emp_no{f}#141, first_name{f}#142, languages{f}#1..]
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
            | sort salary desc""");

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var limit = as(topN.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(5));
        var mvExp = as(limit.child(), MvExpand.class);
        var eval = as(mvExp.child(), Eval.class);
        var filter = as(eval.child(), Filter.class);
        limit = as(filter.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(10));
        mvExp = as(limit.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10));
        filter = as(topN.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#350, first_name{f}#351, salary{f}#352]]
     *  \_TopN[[Order[salary{f}#352,ASC,LAST], Order[first_name{f}#351,ASC,LAST]],5[INTEGER]]
     *    \_MvExpand[first_name{f}#351]
     *      \_TopN[[Order[emp_no{f}#350,ASC,LAST]],10000[INTEGER]]
     *        \_EsRelation[employees][emp_no{f}#350, first_name{f}#351, salary{f}#352]
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
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var mvExp = as(topN.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#361, first_name{f}#362, salary{f}#363]]
     *  \_TopN[[Order[first_name{f}#362,ASC,LAST]],5[INTEGER]]
     *    \_TopN[[Order[salary{f}#363,ASC,LAST]],5[INTEGER]]
     *      \_MvExpand[first_name{f}#362]
     *        \_TopN[[Order[emp_no{f}#361,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#361, first_name{f}#362, salary{f}#363]
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
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("first_name"));
        topN = as(topN.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var mvExp = as(topN.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[5[INTEGER]]
     *  \_Aggregate[[first_name{f}#232],[MAX(salary{f}#233) AS max_s, first_name{f}#232]]
     *    \_Filter[ISNOTNULL(first_name{f}#232)]
     *      \_MvExpand[first_name{f}#232]
     *        \_TopN[[Order[emp_no{f}#231,ASC,LAST]],50[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#231, first_name{f}#232, salary{f}#233]
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
        assertThat(limit.limit().fold(), equalTo(5));
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        var mvExp = as(filter.child(), MvExpand.class);
        var topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(50));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * Limit[5[INTEGER]]
     *  \_Aggregate[[first_name{f}#262],[MAX(salary{f}#263) AS max_s, first_name{f}#262]]
     *    \_Filter[ISNOTNULL(first_name{f}#262)]
     *      \_Limit[50[INTEGER]]
     *        \_MvExpand[first_name{f}#262]
     *          \_Limit[50[INTEGER]]
     *            \_EsRelation[employees][emp_no{f}#261, first_name{f}#262, salary{f}#263]
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

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(5));
        var agg = as(limit.child(), Aggregate.class);
        var filter = as(agg.child(), Filter.class);
        limit = as(filter.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(50));
        var mvExp = as(limit.child(), MvExpand.class);
        limit = as(mvExp.child(), Limit.class);
        assertThat(limit.limit().fold(), equalTo(50));
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[first_name{f}#11, emp_no{f}#10, salary{f}#12, b{r}#4]]
     *  \_TopN[[Order[salary{f}#12,ASC,LAST]],5[INTEGER]]
     *    \_Eval[[100[INTEGER] AS b]]
     *      \_MvExpand[first_name{f}#11]
     *        \_TopN[[Order[first_name{f}#11,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#10, first_name{f}#11, salary{f}#12]
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
        assertThat(topN.limit().fold(), equalTo(5));
        assertThat(orderNames(topN), contains("salary"));
        var eval = as(topN.child(), Eval.class);
        var mvExp = as(eval.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("first_name"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#104, first_name{f}#105, salary{f}#106]]
     *  \_TopN[[Order[salary{f}#106,ASC,LAST], Order[first_name{f}#105,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#215 == [46][KEYWORD] AND WILDCARDLIKE(first_name{f}#105)]
     *      \_MvExpand[first_name{f}#105]
     *        \_TopN[[Order[emp_no{f}#104,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#104, first_name{f}#105, salary{f}#106]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilterOnExpandedField() {
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
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filter acts on first_name (the one used in mv_expand), so the limit 15 is not pushed down past mv_expand
        // instead the default limit is added
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#104, first_name{f}#105, salary{f}#106]]
     *  \_TopN[[Order[salary{f}#106,ASC,LAST], Order[first_name{f}#105,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#215 == [46][KEYWORD] AND salary{f}#106 > 60000[INTEGER]]
     *      \_MvExpand[first_name{f}#105]
     *        \_TopN[[Order[emp_no{f}#104,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#104, first_name{f}#105, salary{f}#106]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilter_NOT_OnExpandedField() {
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
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filters after mv_expand do not act on the expanded field values, as such the limit 15 is the one being pushed down
        // otherwise that limit wouldn't have pushed down and the default limit was instead being added by default before mv_expanded
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("emp_no"));
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expected
     * EsqlProject[[emp_no{f}#116, first_name{f}#117 AS x, salary{f}#119]]
     *  \_TopN[[Order[salary{f}#119,ASC,LAST], Order[first_name{f}#117,ASC,LAST]],15[INTEGER]]
     *    \_Filter[gender{f}#118 == [46][KEYWORD] AND WILDCARDLIKE(first_name{f}#117)]
     *      \_MvExpand[first_name{f}#117]
     *        \_TopN[[Order[gender{f}#118,ASC,LAST]],10000[INTEGER]]
     *          \_EsRelation[employees][emp_no{f}#116, first_name{f}#117, gender{f}#118, sa..]
     */
    public void testAddDefaultLimit_BeforeMvExpand_WithFilterOnExpandedFieldAlias() {
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

        var keep = as(plan, EsqlProject.class);
        var topN = as(keep.child(), TopN.class);
        assertThat(topN.limit().fold(), equalTo(15));
        assertThat(orderNames(topN), contains("salary", "first_name"));
        var filter = as(topN.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(And.class));
        var mvExp = as(filter.child(), MvExpand.class);
        topN = as(mvExp.child(), TopN.class);
        // the filter uses an alias ("x") to the expanded field ("first_name"), so the default limit is used and not the one provided
        assertThat(topN.limit().fold(), equalTo(10000));
        assertThat(orderNames(topN), contains("gender"));
        as(topN.child(), EsRelation.class);
    }

    private static List<String> orderNames(TopN topN) {
        return topN.order().stream().map(o -> as(o.child(), NamedExpression.class).name()).toList();
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
            contains(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, "e", INTEGER, Nullability.TRUE, null, false),
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
            contains(
                new Order(
                    EMPTY,
                    new ReferenceAttribute(EMPTY, "e", INTEGER, Nullability.TRUE, null, false),
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
            contains(
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
            contains(
                new Order(
                    EMPTY,
                    new FieldAttribute(EMPTY, "emp_no", mapping.get("emp_no")),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.FIRST
                )
            )
        );
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
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold());
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
        assertEquals(BytesRefs.toBytesRef("foo"), equals.right().fold());
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
        assertThat(local.supplier(), is(LocalSupplier.EMPTY));
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
        assertThat(enrich.policyName().fold(), is(BytesRefs.toBytesRef("languages_idx")));
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
        assertThat(enrich.policyName().fold(), is(BytesRefs.toBytesRef("languages_idx")));
        var eval = as(enrich.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * EsqlProject[[a{r}#3, last_name{f}#9]]
     * \_Eval[[__a_SUM_123{r}#12 / __a_COUNT_150{r}#13 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#9],[SUM(salary{f}#10) AS __a_SUM_123, COUNT(salary{f}#10) AS __a_COUNT_150, last_nam
     * e{f}#9]]
     *       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
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
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / c{r}#6 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS c, SUM(salary{f}#16) AS s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
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
     * EsqlProject[[a{r}#3, c{r}#6, s{r}#9, last_name{f}#15]]
     * \_Eval[[s{r}#9 / __a_COUNT@xxx{r}#18 AS a]]
     *   \_Limit[10000[INTEGER]]
     *     \_Aggregate[[last_name{f}#15],[COUNT(salary{f}#16) AS __a_COUNT@xxx, COUNT(languages{f}#14) AS c, SUM(salary{f}#16) AS
     *  s, last_name{f}#15]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
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
     * Limit[10000[INTEGER]]
     * \_Aggregate[[last_name{f}#9],[PERCENTILE(salary{f}#10,50[INTEGER]) AS m, last_name{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
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
        assertThat((int) QuantileStates.MEDIAN, is(literal.fold()));

        assertThat(Expressions.names(agg.groupings()), contains("last_name"));
    }

    public void testSplittingInWithFoldableValue() {
        FieldAttribute fa = getFieldAttribute("foo");
        In in = new In(EMPTY, ONE, List.of(TWO, THREE, fa, L(null)));
        Or expected = new Or(EMPTY, new In(EMPTY, ONE, List.of(TWO, THREE)), new In(EMPTY, ONE, List.of(fa, L(null))));
        assertThat(new SplitInWithFoldableValue().rule(in), equalTo(expected));
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

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#1345) AS c]]
     *   \_EsRelation[test][_meta_field{f}#1346, emp_no{f}#1340, first_name{f}#..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#19) AS x]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
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
     * Project[[c{r}#342]]
     * \_Limit[1000[INTEGER]]
     *   \_Filter[min{r}#348 > 10[INTEGER]]
     *     \_Aggregate[[],[COUNT(salary{f}#367) AS c, MIN(salary{f}#367) AS min]]
     *       \_EsRelation[test][_meta_field{f}#368, emp_no{f}#362, first_name{f}#36..]
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
     * Eval[[max{r}#6 + min{r}#9 + c{r}#3 AS x, min{r}#9 AS y, c{r}#3 AS z]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[COUNT(salary{f}#26) AS c, MAX(salary{f}#26) AS max, MIN(salary{f}#26) AS min]]
     *     \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
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
     * Project[[y{r}#6 AS z]]
     * \_Eval[[emp_no{f}#11 + 1[INTEGER] AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
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
     * Project[[salary{f}#20 AS x, emp_no{f}#15 AS y]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#24) AS cx, COUNT(emp_no{f}#19) AS cy]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[x{r}#6],[COUNT(emp_no{f}#17) AS cy, salary{f}#22 AS x]]
     *   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(emp_no{f}#20) AS cy, MIN(salary{f}#25) AS cx, gender{f}#22]]
     *   \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#21],[COUNT(emp_no{f}#19) AS cy, MIN(salary{f}#24) AS cx, gender{f}#21]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#19],[COUNT(x{r}#3) AS cy, MIN(x{r}#3) AS cx, gender{f}#19]]
     *   \_Eval[[emp_no{f}#17 + 1[INTEGER] AS x]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#22],[COUNT(z{r}#9) AS cy, MIN(x{r}#3) AS cx, gender{f}#22]]
     *   \_Eval[[emp_no{f}#20 + 1[INTEGER] AS x, x{r}#3 + 1[INTEGER] AS z]]
     *     \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#14],[COUNT(salary{f}#17) AS cy, MIN(emp_no{f}#12) AS cx, gender{f}#14]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
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
     * Expects
     * Project[[c1{r}#2, c2{r}#4, cs{r}#6, cm{r}#8, cexp{r}#10]]
     * \_Eval[[c1{r}#2 AS c2, c1{r}#2 AS cs, c1{r}#2 AS cm, c1{r}#2 AS cexp]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsCountAll() {
        var plan = plan("""
              from test
            | stats c1 = count(1), c2 = count(2), cs = count(*), cm = count(), cexp = count("123")
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "c2", "cs", "cm", "cexp"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("c2", "cs", "cm", "cexp"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[c1{r}#7, cx{r}#10, cs{r}#12, cy{r}#15]]
     * \_Eval[[c1{r}#7 AS cx, c1{r}#7 AS cs, c1{r}#7 AS cy]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsWithAliasedFields() {
        var plan = plan("""
              from test
            | eval x = 1
            | eval y = x
            | stats c1 = count(1), cx = count(x), cs = count(*), cy = count(y)
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "cx", "cs", "cy"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("cx", "cs", "cy"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Project[[min{r}#1385, max{r}#1388, min{r}#1385 AS min2, max{r}#1388 AS max2, gender{f}#1398]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[gender{f}#1398],[MIN(salary{f}#1401) AS min, MAX(salary{f}#1401) AS max, gender{f}#1398]]
     *     \_EsRelation[test][_meta_field{f}#1402, emp_no{f}#1396, first_name{f}#..]
     */
    public void testEliminateDuplicateAggsMixed() {
        var plan = plan("""
              from test
            | stats min = min(salary), max = max(salary), min2 = min(salary), max2 = max(salary) by gender
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("min", "max", "min2", "max2", "gender"));
        as(projections.get(0), ReferenceAttribute.class);
        as(projections.get(1), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("min"));
        assertThat(Expressions.name(aliased(projections.get(3), ReferenceAttribute.class)), is("max"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("min", "max", "gender"));
        aggFieldName(aggs.get(0), Min.class, "salary");
        aggFieldName(aggs.get(1), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * EsqlProject[[a{r}#5, c{r}#8]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggWithNull() {
        var plan = plan("""
              from test
            | eval x = null + 1
            | stats a = avg(x), c = count(x)
            """);
        fail("Awaits fix");
    }

    /**
     * Expects
     * Project[[max(x){r}#11, max(x){r}#11 AS max(y), max(x){r}#11 AS max(z)]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[MAX(salary{f}#21) AS max(x)]]
     *     \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testEliminateDuplicateAggsNonCount() {
        var plan = plan("""
            from test
            | eval x = salary
            | eval y = x
            | eval z = y
            | stats max(x), max(y), max(z)
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("max(x)", "max(y)", "max(z)"));
        as(projections.get(0), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(1), ReferenceAttribute.class)), is("max(x)"));
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("max(x)"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("max(x)"));
        aggFieldName(aggs.get(0), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#12],[salary{f}#12, salary{f}#12 AS x]]
     *   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testEliminateDuplicateRenamedGroupings() {
        var plan = plan("""
            from test
            | eval x = salary
            | stats by salary, x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var relation = as(agg.child(), EsRelation.class);

        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("salary", "x"));
    }

    /**
     * Expected
     * Limit[2[INTEGER]]
     * \_Filter[a{r}#6 > 2[INTEGER]]
     *   \_MvExpand[a{r}#2,a{r}#6]
     *     \_Row[[[1, 2, 3][INTEGER] AS a]]
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
        var row = as(expand.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#2],[COUNT([2a][KEYWORD]) AS bar]]
     *   \_Row[[1[INTEGER] AS a]]
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
        var row = as(agg.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[a{r}#3, b{r}#5],[COUNT([2a][KEYWORD]) AS baz, b{r}#5 AS bar]]
     *   \_Row[[1[INTEGER] AS a, 2[INTEGER] AS b]]
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
        var row = as(agg.child(), Row.class);
    }

    /**
     * Expected
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#14, gender{f}#16],[MAX(salary{f}#19) AS baz, gender{f}#16 AS bar]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
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

    private <T> T aliased(Expression exp, Class<T> clazz) {
        var alias = as(exp, Alias.class);
        return as(alias.child(), clazz);
    }

    private <T extends AggregateFunction> void aggFieldName(Expression exp, Class<T> aggType, String fieldName) {
        var alias = as(exp, Alias.class);
        var af = as(alias.child(), aggType);
        var field = af.field();
        var name = field.foldable() ? BytesRefs.toString(field.fold()) : Expressions.name(field);
        assertThat(name, is(fieldName));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#4) AS sum(emp_no)]]
     *   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#1185],[SUM(salary{f}#1185) AS sum(salary), salary{f}#1185]]
     *   \_EsRelation[test][_meta_field{f}#1186, emp_no{f}#1180, first_name{f}#..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#13],[SUM(salary{f}#13) AS sum(salary), salary{f}#13 AS x]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#13],[SUM(emp_no{f}#8) AS sum(x), salary{f}#13]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SUM(emp_no{f}#8) AS a, MIN(salary{f}#13) AS b]]
     *   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[gender{f}#11],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, gender{f}#11]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#9],[SUM(emp_no{f}#9) AS a, MIN(salary{f}#14) AS b, emp_no{f}#9]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[w{r}#14, g{r}#16],[COUNT(b{r}#24) AS c, w{r}#14, gender{f}#32 AS g]]
     *   \_Eval[[emp_no{f}#30 / 10[INTEGER] AS x, x{r}#4 + salary{f}#35 AS y, y{r}#8 / 4[INTEGER] AS z, z{r}#11 * 2[INTEGER] +
     *  3[INTEGER] AS w, salary{f}#35 + 4[INTEGER] / 2[INTEGER] AS a, a{r}#21 + 3[INTEGER] AS b]]
     *     \_EsRelation[test][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     *   \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]
     */
    public void testSpatialTypesAndStatsUseDocValues() {
        var plan = planAirports("""
            from test
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     *   \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]
     */
    public void testSpatialTypesAndStatsUseDocValuesWithEval() {
        var plan = planAirports("""
            from test
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
     * Eval[[types.type{f}#5 AS new_types.type]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
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
            var plan = planExtra("from test | eval new_" + field + " = " + func + "(" + field + ")");
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#6],[COUNT(salary{f}#12) AS c, emp_no%2{r}#6]]
     *   \_Eval[[emp_no{f}#7 % 2[INTEGER] AS emp_no%2]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no{f}#6],[COUNT(__c_COUNT@1bd45f36{r}#16) AS c, emp_no{f}#6]]
     *   \_Eval[[salary{f}#11 + 1[INTEGER] AS __c_COUNT@1bd45f36]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no%2{r}#7],[COUNT(__c_COUNT@fb7855b0{r}#18) AS c, emp_no%2{r}#7]]
     *   \_Eval[[emp_no{f}#8 % 2[INTEGER] AS emp_no%2, 100[INTEGER] / languages{f}#11 + salary{f}#13 + 1[INTEGER] AS __c_COUNT
     * @fb7855b0]]
     *     \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
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
     * Limit[1000[INTEGER]]
     * \_Aggregate[[g{r}#8],[COUNT($$emp_no_%_2_+_la>$COUNT$0{r}#20) AS c, g{r}#8]]
     *   \_Eval[[emp_no{f}#10 % 2[INTEGER] AS g, languages{f}#13 + emp_no{f}#10 % 2[INTEGER] AS $$emp_no_%_2_+_la>$COUNT$0]]
     *     \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
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
        assertThat(math.right().fold(), is(2));
        // languages + emp_no % 2
        var add = as(Alias.unwrap(fields.get(1).canonical()), Add.class);
        if (add.left() instanceof Mod mod) {
            add = add.swapLeftAndRight();
        }
        assertThat(Expressions.name(add.left()), is("languages"));
        var mod = as(add.right().canonical(), Mod.class);
        assertThat(Expressions.name(mod.left()), is("emp_no"));
        assertThat(mod.right().fold(), is(2));
    }

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_Aggregate[[emp_no % 2{r}#12, languages + salary{r}#15],[MAX(languages + salary{r}#15) AS m, COUNT($$languages_+_sal>$COUN
     * T$0{r}#28) AS c, emp_no % 2{r}#12, languages + salary{r}#15]]
     *   \_Eval[[emp_no{f}#18 % 2[INTEGER] AS emp_no % 2, languages{f}#21 + salary{f}#23 AS languages + salary, languages{f}#2
     * 1 + salary{f}#23 + emp_no{f}#18 % 2[INTEGER] AS $$languages_+_sal>$COUNT$0]]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
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
        assertThat(math.right().fold(), is(2));
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
        assertThat(mod.right().fold(), is(2));
    }

    /**
     * Expects
     * Project[[e{r}#5, languages + emp_no{r}#8]]
     * \_Eval[[$$MAX$max(languages_+>$0{r}#20 + 1[INTEGER] AS e]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[languages + emp_no{r}#8],[MAX(emp_no{f}#10 + languages{f}#13) AS $$MAX$max(languages_+>$0, languages + emp_no{
     * r}#8]]
     *       \_Eval[[languages{f}#13 + emp_no{f}#10 AS languages + emp_no]]
     *         \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
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

    /*
     * Project[[bucket(salary, 1000.) + 1{r}#3, bucket(salary, 1000.){r}#5]]
        \_Eval[[bucket(salary, 1000.){r}#5 + 1[INTEGER] AS bucket(salary, 1000.) + 1]]
          \_Limit[1000[INTEGER]]
            \_Aggregate[[bucket(salary, 1000.){r}#5],[bucket(salary, 1000.){r}#5]]
              \_Eval[[BUCKET(salary{f}#12,1000.0[DOUBLE]) AS bucket(salary, 1000.)]]
                \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
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
     * Project[[x{r}#5]]
     * \_Eval[[____x_AVG@9efc3cf3_SUM@daf9f221{r}#18 / ____x_AVG@9efc3cf3_COUNT@53cd08ed{r}#19 AS __x_AVG@9efc3cf3, __x_AVG@
     * 9efc3cf3{r}#16 / 2[INTEGER] + __x_MAX@475d0e4d{r}#17 AS x]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[SUM(salary{f}#11) AS ____x_AVG@9efc3cf3_SUM@daf9f221, COUNT(salary{f}#11) AS ____x_AVG@9efc3cf3_COUNT@53cd0
     * 8ed, MAX(salary{f}#11) AS __x_MAX@475d0e4d]]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
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
     * Project[[a{r}#5, b{r}#9, $$max(salary)_+_3>$COUNT$2{r}#46 AS d, $$count(salary)_->$MIN$3{r}#47 AS e, $$avg(salary)_+_m
     * >$MAX$1{r}#45 AS g]]
     * \_Eval[[$$$$avg(salary)_+_m>$AVG$0$SUM$0{r}#48 / $$max(salary)_+_3>$COUNT$2{r}#46 AS $$avg(salary)_+_m>$AVG$0, $$avg(
     * salary)_+_m>$AVG$0{r}#44 + $$avg(salary)_+_m>$MAX$1{r}#45 AS a, $$avg(salary)_+_m>$MAX$1{r}#45 + 3[INTEGER] +
     * 3.141592653589793[DOUBLE] + $$max(salary)_+_3>$COUNT$2{r}#46 AS b]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[w{r}#28],[SUM(salary{f}#39) AS $$$$avg(salary)_+_m>$AVG$0$SUM$0, MAX(salary{f}#39) AS $$avg(salary)_+_m>$MAX$1
     * , COUNT(salary{f}#39) AS $$max(salary)_+_3>$COUNT$2, MIN(salary{f}#39) AS $$count(salary)_->$MIN$3]]
     *       \_Eval[[languages{f}#37 % 2[INTEGER] AS w]]
     *         \_EsRelation[test][_meta_field{f}#40, emp_no{f}#34, first_name{f}#35, ..]
     */
    public void testStatsExpOverAggsWithScalarAndDuplicateAggs() {
        var plan = optimizedPlan("""
            from test
            | stats a = avg(salary) + max(salary),
                    b = max(salary) + 3 + PI() + count(salary),
                    c = count(salary) - min(salary),
                    d = count(salary),
                    e = min(salary),
                    f = max(salary),
                    g = max(salary)
                    by w = languages % 2
            | keep a, b, d, e, g
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("a", "b", "d", "e", "g"));
        var refA = Alias.unwrap(projections.get(0));
        var refB = Alias.unwrap(projections.get(1));
        var refD = Alias.unwrap(projections.get(2));
        var refE = Alias.unwrap(projections.get(3));
        var refG = Alias.unwrap(projections.get(4));

        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // avg + max
        assertThat(Expressions.name(fields.get(1)), is("a"));
        var add = as(Alias.unwrap(fields.get(1)), Add.class);
        var max_salary = add.right();
        assertThat(Expressions.attribute(fields.get(1)), is(Expressions.attribute(refA)));

        assertThat(Expressions.name(fields.get(2)), is("b"));
        assertThat(Expressions.attribute(fields.get(2)), is(Expressions.attribute(refB)));

        add = as(Alias.unwrap(fields.get(2)), Add.class);
        add = as(add.left(), Add.class);
        add = as(add.left(), Add.class);
        assertThat(Expressions.attribute(max_salary), is(Expressions.attribute(add.left())));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);

        assertThat(Expressions.attribute(aggs.get(1)), is(Expressions.attribute(max_salary)));
        var max = as(Alias.unwrap(aggs.get(1)), Max.class);
        var count = as(Alias.unwrap(aggs.get(2)), Count.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("w"));
    }

    /**
     * Expects
     * Project[[a{r}#5, a{r}#5 AS b, w{r}#12]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[w{r}#12],[SUM($$salary_/_2_+_la>$SUM$0{r}#26) AS a, w{r}#12]]
     *     \_Eval[[emp_no{f}#16 % 2[INTEGER] AS w, salary{f}#21 / 2[INTEGER] + languages{f}#19 AS $$salary_/_2_+_la>$SUM$0]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
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
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *     \_Eval[[COALESCE(MVCOUNT([1, 2][INTEGER]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s, COALESCE(MVCOUNT(314.0[DOUBLE] / 100[
     * INTEGER]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s_expr, COALESCE(MVCOUNT(null[NULL]),0[INTEGER]) * $$COUNT$s$0{r}#26 AS s_null]]
     *       \_Aggregate[[w{r}#10],[COUNT(*[KEYWORD]) AS $$COUNT$s$0, w{r}#10]]
     *         \_Eval[[emp_no{f}#16 % 2[INTEGER] AS w]]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testCountOfLiteral() {
        var plan = plan("""
            from test
            | stats s = count([1,2]),
                    s_expr = count(314.0/100),
                    s_null = count(null)
                    by w = emp_no % 2
            | keep s, s_expr, s_null, w
            """, SubstitutionOnlyOptimizer.INSTANCE);

        var limit = as(plan, Limit.class);
        var esqlProject = as(limit.child(), EsqlProject.class);
        var project = as(esqlProject.child(), Project.class);
        var eval = as(project.child(), Eval.class);
        var agg = as(eval.child(), Aggregate.class);

        assertThat(Expressions.names(agg.aggregates()), contains("$$COUNT$s$0", "w"));
        var countAggLiteral = as(as(Alias.unwrap(agg.aggregates().get(0)), Count.class).field(), Literal.class);
        assertTrue(countAggLiteral.semanticEquals(new Literal(EMPTY, StringUtils.WILDCARD, DataType.KEYWORD)));

        var exprs = eval.fields();
        // s == mv_count([1,2]) * count(*)
        var s = as(exprs.get(0), Alias.class);
        assertThat(s.name(), equalTo("s"));
        var mul = as(s.child(), Mul.class);
        var mvCoalesce = as(mul.left(), Coalesce.class);
        assertThat(mvCoalesce.children().size(), equalTo(2));
        var mvCount = as(mvCoalesce.children().get(0), MvCount.class);
        assertThat(mvCount.fold(), equalTo(2));
        assertThat(mvCoalesce.children().get(1).fold(), equalTo(0));
        var count = as(mul.right(), ReferenceAttribute.class);
        assertThat(count.name(), equalTo("$$COUNT$s$0"));

        // s_expr == mv_count(314.0/100) * count(*)
        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.name(), equalTo("s_expr"));
        var mul_expr = as(s_expr.child(), Mul.class);
        var mvCoalesce_expr = as(mul_expr.left(), Coalesce.class);
        assertThat(mvCoalesce_expr.children().size(), equalTo(2));
        var mvCount_expr = as(mvCoalesce_expr.children().get(0), MvCount.class);
        assertThat(mvCount_expr.fold(), equalTo(1));
        assertThat(mvCoalesce_expr.children().get(1).fold(), equalTo(0));
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
        assertThat(mvCoalesce_null.children().get(1).fold(), equalTo(0));
        var count_null = as(mul_null.right(), ReferenceAttribute.class);
        assertThat(count_null.name(), equalTo("$$COUNT$s$0"));
    }

    /**
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, w{r}#10]]
     *     \_Eval[[MVSUM([1, 2][INTEGER]) * $$COUNT$s$0{r}#25 AS s, MVSUM(314.0[DOUBLE] / 100[INTEGER]) * $$COUNT$s$0{r}#25 AS s
     * _expr, MVSUM(null[NULL]) * $$COUNT$s$0{r}#25 AS s_null]]
     *       \_Aggregate[[w{r}#10],[COUNT(*[KEYWORD]) AS $$COUNT$s$0, w{r}#10]]
     *         \_Eval[[emp_no{f}#15 % 2[INTEGER] AS w]]
     *           \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testSumOfLiteral() {
        var plan = plan("""
            from test
            | stats s = sum([1,2]),
                    s_expr = sum(314.0/100),
                    s_null = sum(null)
                    by w = emp_no % 2
            | keep s, s_expr, s_null, w
            """, SubstitutionOnlyOptimizer.INSTANCE);

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
        assertThat(mvSum.fold(), equalTo(3));
        var count = as(mul.right(), ReferenceAttribute.class);
        assertThat(count.name(), equalTo("$$COUNT$s$0"));

        // s_expr == mv_sum(314.0/100) * count(*)
        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.name(), equalTo("s_expr"));
        var mul_expr = as(s_expr.child(), Mul.class);
        var mvSum_expr = as(mul_expr.left(), MvSum.class);
        assertThat(mvSum_expr.fold(), equalTo(3.14));
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
     *
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7]]
     *     \_Eval[[MVAVG([1, 2][INTEGER]) AS s, MVAVG(314.0[DOUBLE] / 100[INTEGER]) AS s_expr, MVAVG(null[NULL]) AS s_null]]
     *       \_LocalRelation[[{e}#21],[ConstantNullBlock[positions=1]]]
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

            var plan = plan(query, SubstitutionOnlyOptimizer.INSTANCE);

            var limit = as(plan, Limit.class);
            var esqlProject = as(limit.child(), EsqlProject.class);
            var project = as(esqlProject.child(), Project.class);
            var eval = as(project.child(), Eval.class);
            var singleRowRelation = as(eval.child(), LocalRelation.class);
            var singleRow = singleRowRelation.supplier().get();
            assertThat(singleRow.length, equalTo(1));
            assertThat(singleRow[0].getPositionCount(), equalTo(1));

            assertAggOfConstExprs(testCase, eval.fields());
        }
    }

    /**
     * Like {@link LogicalPlanOptimizerTests#testAggOfLiteral()} but with a grouping key.
     *
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * Limit[1000[INTEGER]]
     * \_EsqlProject[[s{r}#3, s_expr{r}#5, s_null{r}#7, emp_no{f}#13]]
     *   \_Project[[s{r}#3, s_expr{r}#5, s_null{r}#7, emp_no{f}#13]]
     *     \_Eval[[MVAVG([1, 2][INTEGER]) AS s, MVAVG(314.0[DOUBLE] / 100[INTEGER]) AS s_expr, MVAVG(null[NULL]) AS s_null]]
     *       \_Aggregate[[emp_no{f}#13],[emp_no{f}#13]]
     *         \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
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

            var plan = plan(query, SubstitutionOnlyOptimizer.INSTANCE);

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
        assertEquals(s.child().fold(), testCase.aggMultiValue.apply(new int[] { 1, 2 }));

        var s_expr = as(exprs.get(1), Alias.class);
        assertThat(s_expr.source().toString(), containsString(LoggerMessageFormat.format(null, testCase.aggFunctionTemplate, "314.0/100")));
        assertEquals(
            s_expr.child(),
            testCase.replacementForConstant.apply(new Div(EMPTY, new Literal(EMPTY, 314.0, DOUBLE), new Literal(EMPTY, 100, INTEGER)))
        );
        assertEquals(s_expr.child().fold(), testCase.aggSingleValue.apply(3.14));

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
                result.set(result.get() || Expressions.isNull(node));
            }
        });

        return result.get();
    }

    public void testEmptyMappingIndex() {
        EsIndex empty = new EsIndex("empty_test", emptyMap(), emptySet());
        IndexResolution getIndexResultAirports = IndexResolution.valid(empty);
        var analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResultAirports, enrichResolution),
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

    // https://github.com/elastic/elasticsearch/issues/104995
    public void testNoWrongIsNotNullPruning() {
        var plan = optimizedPlan("""
              ROW a = 5, b = [ 1, 2 ]
              | EVAL sum = a + b
              | LIMIT 1
              | WHERE sum IS NOT NULL
            """);

        var local = as(plan, LocalRelation.class);
        assertThat(local.supplier(), equalTo(LocalSupplier.EMPTY));
        assertWarnings(
            "Line 2:16: evaluation of [a + b] failed, treating result as null. Only first 20 failures recorded.",
            "Line 2:16: java.lang.IllegalArgumentException: single-value function encountered multi-value"
        );
    }

    /**
     * Pushing down EVAL/GROK/DISSECT/ENRICH must not accidentally shadow attributes required by SORT.
     *
     * For DISSECT expects the following; the others are similar.
     *
     * Project[[first_name{f}#37, emp_no{r}#30, salary{r}#31]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#46,ASC,LAST], Order[$$order_by$temp_name$1{r}#47,DESC,FIRST]],3[INTEGER]]
     *   \_Dissect[first_name{f}#37,Parser[pattern=%{emp_no} %{salary}, appendSeparator=,
     *   parser=org.elasticsearch.dissect.DissectParser@87f460f],[emp_no{r}#30, salary{r}#31]]
     *     \_Eval[[emp_no{f}#36 + salary{f}#41 * 13[INTEGER] AS $$order_by$temp_name$0, NEG(salary{f}#41) AS $$order_by$temp_name$1]]
     *       \_EsRelation[test][_meta_field{f}#42, emp_no{f}#36, first_name{f}#37, ..]
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

            AttributeSet attributesCreatedInEval = new AttributeSet();
            for (Alias field : renamingEval.fields()) {
                attributesCreatedInEval.add(field.toAttribute());
            }
            assertThat(attributesCreatedInEval, allOf(hasItem(renamed_emp_no), hasItem(renamed_salary), hasItem(renamed_emp_no2)));

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
                new Literal(EMPTY, "some_policy", KEYWORD),
                attr,
                null,
                Map.of(),
                List.of(
                    new Alias(EMPTY, "y", new ReferenceAttribute(EMPTY, "some_enrich_idx_field", KEYWORD)),
                    new Alias(EMPTY, "y", new ReferenceAttribute(EMPTY, "some_other_enrich_idx_field", KEYWORD))
                )
            ),
            new PushDownEnrich()
        ) };

    /**
     * Consider
     *
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Project[[y{r}#3, x{r}#2]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     *
     * We can freely push down the Eval without renaming, but need to update the Project's references.
     *
     * Project[[x{r}#2, y{r}#6 AS y]]
     * \_Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastProject() {
        Alias x = new Alias(EMPTY, "x", new Literal(EMPTY, "1", KEYWORD));
        Alias y = new Alias(EMPTY, "y", new Literal(EMPTY, "2", KEYWORD));
        LogicalPlan initialRow = new Row(EMPTY, List.of(x, y));
        LogicalPlan initialProject = new Project(EMPTY, initialRow, List.of(y.toAttribute(), x.toAttribute()));

        for (PushdownShadowingGeneratingPlanTestCase testCase : PUSHDOWN_SHADOWING_GENERATING_PLAN_TEST_CASES) {
            LogicalPlan initialPlan = testCase.applyLogicalPlan.apply(initialProject, x.toAttribute());
            @SuppressWarnings("unchecked")
            List<Attribute> initialGeneratedExprs = ((GeneratingPlan) initialPlan).generatedAttributes();
            LogicalPlan optimizedPlan = testCase.rule.apply(initialPlan);

            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan);
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
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#4 + 1[INTEGER] AS y]]
     * \_Project[[x{r}#2, y{r}#3, y{r}#3 AS z]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     *
     * To push down the Eval, we must not shadow the reference y{r}#3, so we rename.
     *
     * Project[[x{r}#2, y{r}#3 AS z, $$y$temp_name$10{r}#12 AS y]]
     * Eval[[TO_INTEGER(x{r}#2) AS $$y$temp_name$10, $$y$temp_name$10{r}#11 + 1[INTEGER] AS $$y$temp_name$10]]
     * \_Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastRenamingProject() {
        Alias x = new Alias(EMPTY, "x", new Literal(EMPTY, "1", KEYWORD));
        Alias y = new Alias(EMPTY, "y", new Literal(EMPTY, "2", KEYWORD));
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

            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan);
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
     * Eval[[TO_INTEGER(x{r}#2) AS y, y{r}#3 + 1[INTEGER] AS y]]
     * \_Project[[y{r}#1, y{r}#1 AS x]]
     * \_Row[[2[INTEGER] AS y]]
     *
     * To push down the Eval, we must not shadow the reference y{r}#1, so we rename.
     * Additionally, the rename "y AS x" needs to be propagated into the Eval.
     *
     * Project[[y{r}#1 AS x, $$y$temp_name$10{r}#12 AS y]]
     * Eval[[TO_INTEGER(y{r}#1) AS $$y$temp_name$10, $$y$temp_name$10{r}#11 + 1[INTEGER] AS $$y$temp_name$10]]
     * \_Row[[2[INTEGER] AS y]]
     *
     * And similarly for dissect, grok and enrich.
     */
    public void testPushShadowingGeneratingPlanPastRenamingProjectWithResolution() {
        Alias y = new Alias(EMPTY, "y", new Literal(EMPTY, "2", KEYWORD));
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
            Failures inconsistencies = LogicalVerifier.INSTANCE.verify(optimizedPlan);
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

    /**
     * Expects
     * Project[[min{r}#4, languages{f}#11]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#18,ASC,LAST]],1000[INTEGER]]
     *   \_Eval[[min{r}#4 + languages{f}#11 AS $$order_by$temp_name$0]]
     *     \_Aggregate[[languages{f}#11],[MIN(salary{f}#13) AS min, languages{f}#11]]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
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

    /**
     * Expects
     * Limit[1000[INTEGER]]
     * \_InlineStats[[emp_no % 2{r}#6],[COUNT(salary{f}#12) AS c, emp_no % 2{r}#6]]
     *   \_Eval[[emp_no{f}#7 % 2[INTEGER] AS emp_no % 2]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testInlinestatsNestedExpressionsInGroups() {
        var plan = optimizedPlan("""
            FROM test
            | INLINESTATS c = COUNT(salary) by emp_no % 2
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), InlineStats.class);
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
     *
     * Project[[salary{f}#19, languages{f}#17, emp_no{f}#14]]
     * \_TopN[[Order[$$order_by$0$0{r}#24,ASC,LAST], Order[emp_no{f}#14,DESC,FIRST]],1000[INTEGER]]
     *   \_Eval[[salary{f}#19 / 10000[INTEGER] + languages{f}#17 AS $$order_by$0$0]]
     *     \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
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
     * Project[[first_name{f}#37, emp_no{r}#30, salary{r}#31]]
     * \_TopN[[Order[$$order_by$temp_name$0{r}#46,ASC,LAST], Order[$$order_by$temp_name$1{r}#47,DESC,FIRST]],3[INTEGER]]
     *   \_Dissect[first_name{f}#37,Parser[pattern=%{emp_no} %{salary}, appendSeparator=,
     *   parser=org.elasticsearch.dissect.DissectParser@87f460f],[emp_no{r}#30, salary{r}#31]]
     *     \_Eval[[emp_no{f}#36 + salary{f}#41 * 13[INTEGER] AS $$order_by$temp_name$0, NEG(salary{f}#41) AS $$order_by$temp_name$1]]
     *       \_EsRelation[test][_meta_field{f}#42, emp_no{f}#36, first_name{f}#37, ..]
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

    private LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    private LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    private LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = optimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    private LogicalPlan planAirports(String query) {
        var analyzed = analyzerAirports.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    private LogicalPlan planExtra(String query) {
        var analyzed = analyzerExtra.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    private LogicalPlan planTypes(String query) {
        return logicalOptimizer.optimize(analyzerTypes.analyze(parser.createStatement(query)));
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
        assertSemanticMatching(bc, extractPlannedBinaryComparison(exp));
    }

    private static void assertSemanticMatching(Expression fieldAttributeExp, Expression unresolvedAttributeExp) {
        Expression unresolvedUpdated = unresolvedAttributeExp.transformUp(
            LITERALS_ON_THE_RIGHT.expressionToken(),
            LITERALS_ON_THE_RIGHT::rule
        ).transformUp(x -> x.foldable() ? new Literal(x.source(), x.fold(), x.dataType()) : x);

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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/108388")
    public void testSimplifyComparisonArithmeticWithFloatsAndDirectionChange() {
        doTestSimplifyComparisonArithmetics("float / -2 < 4", "float", GT, -8d);
        doTestSimplifyComparisonArithmetics("float * -2 < 4", "float", GT, -2d);
    }

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
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

    public static WildcardLike wildcardLike(Expression left, String exp) {
        return new WildcardLike(EMPTY, left, new WildcardPattern(exp));
    }

    public static RLike rlike(Expression left, String exp) {
        return new RLike(EMPTY, left, new RLikePattern(exp));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    // Null folding

    public void testBasicNullFolding() {
        FoldNull rule = new FoldNull();
        assertNullLiteral(rule.rule(new Add(EMPTY, L(randomInt()), Literal.NULL)));
        assertNullLiteral(rule.rule(new Round(EMPTY, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new Pow(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateFormat(EMPTY, Literal.NULL, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new DateParse(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateTrunc(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new Substring(EMPTY, Literal.NULL, Literal.NULL, Literal.NULL)));
    }

    public void testNullFoldingIsNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNull(EMPTY, NULL)).fold());
        assertEquals(false, foldNull.rule(new IsNull(EMPTY, TRUE)).fold());
    }

    public void testNullFoldingIsNotNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNotNull(EMPTY, TRUE)).fold());
        assertEquals(false, foldNull.rule(new IsNotNull(EMPTY, NULL)).fold());
    }

    public void testGenericNullableExpression() {
        FoldNull rule = new FoldNull();
        // arithmetic
        assertNullLiteral(rule.rule(new Add(EMPTY, getFieldAttribute("a"), NULL)));
        // comparison
        assertNullLiteral(rule.rule(greaterThanOf(getFieldAttribute("a"), NULL)));
        // regex
        assertNullLiteral(rule.rule(new RLike(EMPTY, NULL, new RLikePattern("123"))));
        // date functions
        assertNullLiteral(rule.rule(new DateExtract(EMPTY, NULL, NULL, configuration(""))));
        // math functions
        assertNullLiteral(rule.rule(new Cos(EMPTY, NULL)));
        // string functions
        assertNullLiteral(rule.rule(new LTrim(EMPTY, NULL)));
        // spatial
        assertNullLiteral(rule.rule(new SpatialCentroid(EMPTY, NULL)));
        // ip
        assertNullLiteral(rule.rule(new CIDRMatch(EMPTY, NULL, List.of(NULL))));
        // conversion
        assertNullLiteral(rule.rule(new ToString(EMPTY, NULL)));
    }

    public void testNullFoldingDoesNotApplyOnLogicalExpressions() {
        FoldNull rule = new FoldNull();

        Or or = new Or(EMPTY, NULL, TRUE);
        assertEquals(or, rule.rule(or));
        or = new Or(EMPTY, NULL, NULL);
        assertEquals(or, rule.rule(or));

        And and = new And(EMPTY, NULL, TRUE);
        assertEquals(and, rule.rule(and));
        and = new And(EMPTY, NULL, NULL);
        assertEquals(and, rule.rule(and));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnAbstractMultivalueFunction() throws Exception {
        FoldNull rule = new FoldNull();

        List<Class<? extends AbstractMultivalueFunction>> items = List.of(
            MvDedupe.class,
            MvFirst.class,
            MvLast.class,
            MvMax.class,
            MvMedian.class,
            MvMin.class,
            MvSum.class
        );
        for (Class<? extends AbstractMultivalueFunction> clazz : items) {
            Constructor<? extends AbstractMultivalueFunction> ctor = clazz.getConstructor(Source.class, Expression.class);
            AbstractMultivalueFunction conditionalFunction = ctor.newInstance(EMPTY, getFieldAttribute("a"));
            assertEquals(conditionalFunction, rule.rule(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, rule.rule(conditionalFunction));
        }

        // avg and count ar different just because they know the return type in advance (all the others infer the type from the input)
        MvAvg avg = new MvAvg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, rule.rule(avg));
        avg = new MvAvg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(avg));

        MvCount count = new MvCount(EMPTY, getFieldAttribute("a"));
        assertEquals(count, rule.rule(count));
        count = new MvCount(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, INTEGER), rule.rule(count));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnAggregate() throws Exception {
        FoldNull rule = new FoldNull();

        List<Class<? extends AggregateFunction>> items = List.of(Max.class, Min.class);
        for (Class<? extends AggregateFunction> clazz : items) {
            Constructor<? extends AggregateFunction> ctor = clazz.getConstructor(Source.class, Expression.class);
            AggregateFunction conditionalFunction = ctor.newInstance(EMPTY, getFieldAttribute("a"));
            assertEquals(conditionalFunction, rule.rule(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, rule.rule(conditionalFunction));
        }

        Avg avg = new Avg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, rule.rule(avg));
        avg = new Avg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(avg));

        Count count = new Count(EMPTY, getFieldAttribute("a"));
        assertEquals(count, rule.rule(count));
        count = new Count(EMPTY, NULL);
        assertEquals(count, rule.rule(count));

        CountDistinct countd = new CountDistinct(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(countd, rule.rule(countd));
        countd = new CountDistinct(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, LONG), rule.rule(countd));

        Median median = new Median(EMPTY, getFieldAttribute("a"));
        assertEquals(median, rule.rule(median));
        median = new Median(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(median));

        MedianAbsoluteDeviation medianad = new MedianAbsoluteDeviation(EMPTY, getFieldAttribute("a"));
        assertEquals(medianad, rule.rule(medianad));
        medianad = new MedianAbsoluteDeviation(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(medianad));

        Percentile percentile = new Percentile(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(percentile, rule.rule(percentile));
        percentile = new Percentile(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(percentile));

        Sum sum = new Sum(EMPTY, getFieldAttribute("a"));
        assertEquals(sum, rule.rule(sum));
        sum = new Sum(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(sum));

    }

    public void testNullFoldableDoesNotApplyToIsNullAndNotNull() {
        FoldNull rule = new FoldNull();

        DataType numericType = randomFrom(INTEGER, LONG, DOUBLE);
        DataType genericType = randomFrom(INTEGER, LONG, DOUBLE, UNSIGNED_LONG, KEYWORD, TEXT, GEO_POINT, GEO_SHAPE, VERSION, IP);
        List<Expression> items = List.of(
            new Add(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Add(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Sub(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Sub(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Mul(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Mul(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Div(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Div(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),

            new GreaterThan(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new GreaterThan(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new GreaterThanOrEqual(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new GreaterThanOrEqual(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new LessThan(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new LessThan(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new LessThanOrEqual(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new LessThanOrEqual(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new NotEquals(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new NotEquals(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),

            new Equals(EMPTY, getFieldAttribute("a", genericType), getFieldAttribute("b", genericType)),
            new Equals(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER))
        );
        for (Expression item : items) {
            Expression isNull = new IsNull(EMPTY, item);
            Expression transformed = rule.rule(isNull);
            assertEquals(isNull, transformed);

            IsNotNull isNotNull = new IsNotNull(EMPTY, item);
            transformed = rule.rule(isNotNull);
            assertEquals(isNotNull, transformed);
        }

    }

    //
    // Propagate nullability (IS NULL / IS NOT NULL)
    //

    // a IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNull() throws Exception {
        FieldAttribute fa = getFieldAttribute("a");

        And and = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        assertEquals(FALSE, new PropagateNullable().rule(and));
    }

    // a IS NULL AND b IS NOT NULL AND c IS NULL AND d IS NOT NULL AND e IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNullMultiField() throws Exception {
        FieldAttribute fa = getFieldAttribute("a");

        And andOne = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, getFieldAttribute("b")));
        And andTwo = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute("c")), new IsNotNull(EMPTY, getFieldAttribute("d")));
        And andThree = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute("e")), new IsNotNull(EMPTY, fa));

        And and = new And(EMPTY, andOne, new And(EMPTY, andTwo, andThree));

        assertEquals(FALSE, new PropagateNullable().rule(and));
    }

    // a IS NULL AND a > 1 => a IS NULL AND NULL
    public void testIsNullAndComparison() {
        FieldAttribute fa = getFieldAttribute("a");
        IsNull isNull = new IsNull(EMPTY, fa);

        And and = new And(EMPTY, isNull, greaterThanOf(fa, ONE));
        assertEquals(new And(EMPTY, isNull, nullOf(BOOLEAN)), new PropagateNullable().rule(and));
    }

    // a IS NULL AND b < 1 AND c < 1 AND a < 1 => a IS NULL AND b < 1 AND c < 1 AND NULL
    public void testIsNullAndMultipleComparison() {
        FieldAttribute fa = getFieldAttribute("a");
        IsNull aIsNull = new IsNull(EMPTY, fa);

        And bLT1_AND_cLT1 = new And(EMPTY, lessThanOf(getFieldAttribute("b"), ONE), lessThanOf(getFieldAttribute("c"), ONE));
        And aIsNull_AND_bLT1_AND_cLT1 = new And(EMPTY, aIsNull, bLT1_AND_cLT1);
        And aIsNull_AND_bLT1_AND_cLT1_AND_aLT1 = new And(EMPTY, aIsNull_AND_bLT1_AND_cLT1, lessThanOf(fa, ONE));

        Expression optimized = new PropagateNullable().rule(aIsNull_AND_bLT1_AND_cLT1_AND_aLT1);
        Expression aIsNull_AND_bLT1_AND_cLT1_AND_NULL = new And(EMPTY, aIsNull_AND_bLT1_AND_cLT1, nullOf(BOOLEAN));
        assertEquals(Predicates.splitAnd(aIsNull_AND_bLT1_AND_cLT1_AND_NULL), Predicates.splitAnd(optimized));
    }

    public void testDoNotOptimizeIsNullAndMultipleComparisonWithConstants() {
        Literal a = ONE;
        Literal b = ONE;
        IsNull aIsNull = new IsNull(EMPTY, a);

        And bLT1_AND_cLT1 = new And(EMPTY, lessThanOf(b, ONE), lessThanOf(getFieldAttribute("c"), ONE));
        And aIsNull_AND_bLT1_AND_cLT1 = new And(EMPTY, aIsNull, bLT1_AND_cLT1);
        And aIsNull_AND_bLT1_AND_cLT1_AND_aLT1 = new And(EMPTY, aIsNull_AND_bLT1_AND_cLT1, lessThanOf(a, ONE));

        Expression optimized = new PropagateNullable().rule(aIsNull_AND_bLT1_AND_cLT1_AND_aLT1);
        Literal nullLiteral = new Literal(EMPTY, null, BOOLEAN);
        assertEquals(asList(aIsNull, nullLiteral, nullLiteral, nullLiteral), Predicates.splitAnd(optimized));
    }

    // ((a+1)/2) > 1 AND a + 2 AND a IS NULL AND b < 3 => NULL AND NULL AND a IS NULL AND b < 3
    public void testIsNullAndDeeplyNestedExpression() throws Exception {
        FieldAttribute fa = getFieldAttribute("a");
        IsNull isNull = new IsNull(EMPTY, fa);

        Expression nullified = new And(
            EMPTY,
            greaterThanOf(new Div(EMPTY, new Add(EMPTY, fa, ONE), TWO), ONE),
            greaterThanOf(new Add(EMPTY, fa, TWO), ONE)
        );
        Expression kept = new And(EMPTY, isNull, lessThanOf(getFieldAttribute("b"), THREE));
        And and = new And(EMPTY, nullified, kept);

        Expression optimized = new PropagateNullable().rule(and);
        Expression expected = new And(EMPTY, new And(EMPTY, nullOf(BOOLEAN), nullOf(BOOLEAN)), kept);

        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // a IS NULL OR a IS NOT NULL => no change
    // a IS NULL OR a > 1 => no change
    public void testIsNullInDisjunction() throws Exception {
        FieldAttribute fa = getFieldAttribute("a");

        Or or = new Or(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        Filter dummy = new Filter(EMPTY, relation(), or);
        LogicalPlan transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());

        or = new Or(EMPTY, new IsNull(EMPTY, fa), greaterThanOf(fa, ONE));
        dummy = new Filter(EMPTY, relation(), or);
        transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());
    }

    // a + 1 AND (a IS NULL OR a > 3) => no change
    public void testIsNullDisjunction() throws Exception {
        FieldAttribute fa = getFieldAttribute("a");
        IsNull isNull = new IsNull(EMPTY, fa);

        Or or = new Or(EMPTY, isNull, greaterThanOf(fa, THREE));
        And and = new And(EMPTY, new Add(EMPTY, fa, ONE), or);

        assertEquals(and, new PropagateNullable().rule(and));
    }

    //
    // Lookup
    //

    /**
     * Expects
     * {@code
     * Join[JoinConfig[type=LEFT OUTER, matchFields=[int{r}#4], conditions=[LOOKUP int_number_names ON int]]]
     * |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, job{f}#13, job.raw{f}#14, languages{f}#9 AS int
     * , last_name{f}#10, long_noidx{f}#15, salary{f}#11]]
     * | \_Limit[1000[INTEGER]]
     * |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * \_LocalRelation[[int{f}#16, name{f}#17],[IntVectorBlock[vector=IntArrayVector[positions=10, values=[0, 1, 2, 3, 4, 5, 6, 7, 8,
     * 9]]], BytesRefVectorBlock[vector=BytesRefArrayVector[positions=10]]]]
     * }
     */
    public void testLookupSimple() {
        String query = """
              FROM test
            | RENAME languages AS int
            | LOOKUP int_number_names ON int""";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
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
        assertThat(limit.limit().fold(), equalTo(1000));

        assertThat(join.config().type(), equalTo(JoinType.LEFT));
        assertThat(join.config().matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(join.config().leftFields().size(), equalTo(1));
        assertThat(join.config().rightFields().size(), equalTo(1));
        Attribute lhs = join.config().leftFields().get(0);
        Attribute rhs = join.config().rightFields().get(0);
        assertThat(lhs.toString(), startsWith("int{r}"));
        assertThat(rhs.toString(), startsWith("int{r}"));
        assertTrue(join.children().get(0).outputSet() + " contains " + lhs, join.children().get(0).outputSet().contains(lhs));
        assertTrue(join.children().get(1).outputSet() + " contains " + rhs, join.children().get(1).outputSet().contains(rhs));

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
                .item(containsString("name{r}"))
        );
    }

    /**
     * Expects
     * {@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[name{r}#20],[MIN(emp_no{f}#9) AS MIN(emp_no), name{r}#20]]
     *   \_Join[JoinConfig[type=LEFT OUTER, matchFields=[int{r}#4], conditions=[LOOKUP int_number_names ON int]]]
     *     |_EsqlProject[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, job{f}#16, job.raw{f}#17, languages{f}#12 AS
     * int, last_name{f}#13, long_noidx{f}#18, salary{f}#14]]
     *     | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_LocalRelation[[int{f}#19, name{f}#20],[IntVectorBlock[vector=IntArrayVector[positions=10, values=[0, 1, 2, 3, 4, 5, 6, 7, 8,
     * 9]]], BytesRefVectorBlock[vector=BytesRefArrayVector[positions=10]]]]
     * }
     */
    public void testLookupStats() {
        String query = """
              FROM test
            | RENAME languages AS int
            | LOOKUP int_number_names ON int
            | STATS MIN(emp_no) BY name""";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(), equalTo(1000));

        var agg = as(limit.child(), Aggregate.class);
        assertMap(
            agg.aggregates().stream().map(Object::toString).sorted().toList(),
            matchesList().item(startsWith("MIN(emp_no)")).item(startsWith("name{r}"))
        );
        assertMap(agg.groupings().stream().map(Object::toString).toList(), matchesList().item(startsWith("name{r}")));

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

        assertThat(join.config().type(), equalTo(JoinType.LEFT));
        assertThat(join.config().matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertThat(join.config().leftFields().size(), equalTo(1));
        assertThat(join.config().rightFields().size(), equalTo(1));
        Attribute lhs = join.config().leftFields().get(0);
        Attribute rhs = join.config().rightFields().get(0);
        assertThat(lhs.toString(), startsWith("int{r}"));
        assertThat(rhs.toString(), startsWith("int{r}"));

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
                .item(containsString("name{r}"))
        );
    }

    public void testTranslateMetricsWithoutGrouping() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s max(rate(network.total_bytes_in))";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        Aggregate aggsByTsid = as(finalAggs.child(), Aggregate.class);
        as(aggsByTsid.child(), EsRelation.class);

        assertThat(finalAggs.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        assertThat(finalAggs.aggregates(), hasSize(1));
        Max max = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        assertThat(Expressions.attribute(max.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAggs.groupings(), empty());

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        assertThat(aggsByTsid.aggregates(), hasSize(1)); // _tsid is dropped
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
    }

    public void testTranslateMixedAggsWithoutGrouping() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s max(rate(network.total_bytes_in)), max(network.cost)";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        Aggregate aggsByTsid = as(finalAggs.child(), Aggregate.class);
        as(aggsByTsid.child(), EsRelation.class);

        assertThat(finalAggs.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        assertThat(finalAggs.aggregates(), hasSize(2));
        Max maxRate = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        FromPartial maxCost = as(Alias.unwrap(finalAggs.aggregates().get(1)), FromPartial.class);
        assertThat(Expressions.attribute(maxRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(maxCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(finalAggs.groupings(), empty());

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        assertThat(aggsByTsid.aggregates(), hasSize(2));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        ToPartial toPartialMaxCost = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), ToPartial.class);
        assertThat(Expressions.attribute(toPartialMaxCost.field()).name(), equalTo("network.cost"));
    }

    public void testTranslateMixedAggsWithMathWithoutGrouping() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s max(rate(network.total_bytes_in)), max(network.cost + 0.2) * 1.1";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        Eval mulEval = as(project.child(), Eval.class);
        assertThat(mulEval.fields(), hasSize(1));
        Mul mul = as(Alias.unwrap(mulEval.fields().get(0)), Mul.class);
        Limit limit = as(mulEval.child(), Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs.aggregates(), hasSize(2));
        Aggregate aggsByTsid = as(finalAggs.child(), Aggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2));
        Eval addEval = as(aggsByTsid.child(), Eval.class);
        assertThat(addEval.fields(), hasSize(1));
        Add add = as(Alias.unwrap(addEval.fields().get(0)), Add.class);
        as(addEval.child(), EsRelation.class);

        assertThat(Expressions.attribute(mul.left()).id(), equalTo(finalAggs.aggregates().get(1).id()));
        assertThat(mul.right().fold(), equalTo(1.1));

        assertThat(finalAggs.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        Max maxRate = as(Alias.unwrap(finalAggs.aggregates().get(0)), Max.class);
        FromPartial maxCost = as(Alias.unwrap(finalAggs.aggregates().get(1)), FromPartial.class);
        assertThat(Expressions.attribute(maxRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(maxCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(finalAggs.groupings(), empty());

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        ToPartial toPartialMaxCost = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), ToPartial.class);
        assertThat(Expressions.attribute(toPartialMaxCost.field()).id(), equalTo(addEval.fields().get(0).id()));
        assertThat(Expressions.attribute(add.left()).name(), equalTo("network.cost"));
        assertThat(add.right().fold(), equalTo(0.2));
    }

    public void testTranslateMetricsGroupedByOneDimension() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s sum(rate(network.total_bytes_in)) BY cluster | SORT cluster | LIMIT 10";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        TopN topN = as(plan, TopN.class);
        Aggregate aggsByCluster = as(topN.child(), Aggregate.class);
        assertThat(aggsByCluster.aggregates(), hasSize(2));
        Aggregate aggsByTsid = as(aggsByCluster.child(), Aggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        as(aggsByTsid.child(), EsRelation.class);

        assertThat(aggsByCluster.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        Sum sum = as(Alias.unwrap(aggsByCluster.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(aggsByCluster.groupings(), hasSize(1));
        assertThat(Expressions.attribute(aggsByCluster.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        Values values = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), Values.class);
        assertThat(Expressions.attribute(values.field()).name(), equalTo("cluster"));
    }

    public void testTranslateMetricsGroupedByTwoDimension() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s avg(rate(network.total_bytes_in)) BY cluster, pod";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        Limit limit = as(eval.child(), Limit.class);
        Aggregate finalAggs = as(limit.child(), Aggregate.class);
        assertThat(finalAggs.aggregates(), hasSize(4));
        Aggregate aggsByTsid = as(finalAggs.child(), Aggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // _tsid is dropped
        as(aggsByTsid.child(), EsRelation.class);

        Div div = as(Alias.unwrap(eval.fields().get(0)), Div.class);
        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAggs.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAggs.aggregates().get(1).id()));

        assertThat(finalAggs.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        Sum sum = as(Alias.unwrap(finalAggs.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        Count count = as(Alias.unwrap(finalAggs.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(count.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAggs.groupings(), hasSize(2));
        assertThat(Expressions.attribute(finalAggs.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(Expressions.attribute(finalAggs.groupings().get(1)).id(), equalTo(aggsByTsid.aggregates().get(2).id()));

        assertThat(finalAggs.groupings(), hasSize(2));

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // rates, values(cluster), values(pod)
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        Values values1 = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), Values.class);
        assertThat(Expressions.attribute(values1.field()).name(), equalTo("cluster"));
        Values values2 = as(Alias.unwrap(aggsByTsid.aggregates().get(2)), Values.class);
        assertThat(Expressions.attribute(values2.field()).name(), equalTo("pod"));
    }

    public void testTranslateMetricsGroupedByTimeBucket() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = "METRICS k8s sum(rate(network.total_bytes_in)) BY bucket(@timestamp, 1h)";
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Limit limit = as(plan, Limit.class);
        Aggregate finalAgg = as(limit.child(), Aggregate.class);
        assertThat(finalAgg.aggregates(), hasSize(2));
        Aggregate aggsByTsid = as(finalAgg.child(), Aggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(2)); // _tsid is dropped
        Eval eval = as(aggsByTsid.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        as(eval.child(), EsRelation.class);

        assertThat(finalAgg.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        assertThat(Expressions.attribute(aggsByTsid.groupings().get(1)).id(), equalTo(eval.fields().get(0).id()));
        Bucket bucket = as(Alias.unwrap(eval.fields().get(0)), Bucket.class);
        assertThat(Expressions.attribute(bucket.field()).name(), equalTo("@timestamp"));
    }

    public void testTranslateMetricsGroupedByTimeBucketAndDimensions() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = """
            METRICS k8s avg(rate(network.total_bytes_in)) BY pod, bucket(@timestamp, 5 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        Div div = as(Alias.unwrap(eval.fields().get(0)), Div.class);
        Aggregate finalAgg = as(eval.child(), Aggregate.class);
        Aggregate aggsByTsid = as(finalAgg.child(), Aggregate.class);
        Eval bucket = as(aggsByTsid.child(), Eval.class);
        as(bucket.child(), EsRelation.class);
        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAgg.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAgg.aggregates().get(1).id()));

        assertThat(finalAgg.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        assertThat(finalAgg.aggregates(), hasSize(5)); // sum, count, pod, bucket, cluster
        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count count = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(count.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(finalAgg.groupings(), hasSize(3));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        assertThat(aggsByTsid.aggregates(), hasSize(4)); // rate, values(pod), values(cluster), bucket
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        Values podValues = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), Values.class);
        assertThat(Expressions.attribute(podValues.field()).name(), equalTo("pod"));
        Values clusterValues = as(Alias.unwrap(aggsByTsid.aggregates().get(3)), Values.class);
        assertThat(Expressions.attribute(clusterValues.field()).name(), equalTo("cluster"));
    }

    public void testTranslateMixedAggsGroupedByTimeBucketAndDimensions() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = """
            METRICS k8s avg(rate(network.total_bytes_in)), avg(network.cost) BY bucket(@timestamp, 5 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval eval = as(topN.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));
        Div div = as(Alias.unwrap(eval.fields().get(0)), Div.class);
        Aggregate finalAgg = as(eval.child(), Aggregate.class);
        Aggregate aggsByTsid = as(finalAgg.child(), Aggregate.class);
        Eval bucket = as(aggsByTsid.child(), Eval.class);
        as(bucket.child(), EsRelation.class);
        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAgg.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAgg.aggregates().get(1).id()));

        assertThat(finalAgg.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
        assertThat(finalAgg.aggregates(), hasSize(6)); // sum, count, sum, count, bucket, cluster
        Sum sumRate = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count countRate = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(sumRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(Expressions.attribute(countRate.field()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));

        FromPartial sumCost = as(Alias.unwrap(finalAgg.aggregates().get(2)), FromPartial.class);
        FromPartial countCost = as(Alias.unwrap(finalAgg.aggregates().get(3)), FromPartial.class);
        assertThat(Expressions.attribute(sumCost.field()).id(), equalTo(aggsByTsid.aggregates().get(1).id()));
        assertThat(Expressions.attribute(countCost.field()).id(), equalTo(aggsByTsid.aggregates().get(2).id()));

        assertThat(finalAgg.groupings(), hasSize(2));
        assertThat(Expressions.attribute(finalAgg.groupings().get(0)).id(), equalTo(aggsByTsid.aggregates().get(3).id()));

        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        assertThat(aggsByTsid.aggregates(), hasSize(5)); // rate, to_partial(sum(cost)), to_partial(count(cost)), values(cluster), bucket
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        ToPartial toPartialSum = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), ToPartial.class);
        assertThat(toPartialSum.function(), instanceOf(Sum.class));
        assertThat(Expressions.attribute(toPartialSum.field()).name(), equalTo("network.cost"));
        ToPartial toPartialCount = as(Alias.unwrap(aggsByTsid.aggregates().get(2)), ToPartial.class);
        assertThat(toPartialCount.function(), instanceOf(Count.class));
        assertThat(Expressions.attribute(toPartialCount.field()).name(), equalTo("network.cost"));
        Values clusterValues = as(Alias.unwrap(aggsByTsid.aggregates().get(4)), Values.class);
        assertThat(Expressions.attribute(clusterValues.field()).name(), equalTo("cluster"));
    }

    public void testAdjustMetricsRateBeforeFinalAgg() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = """
            METRICS k8s avg(round(1.05 * rate(network.total_bytes_in))) BY bucket(@timestamp, 1 minute), cluster
            | SORT cluster
            | LIMIT 10
            """;
        var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval evalDiv = as(topN.child(), Eval.class);
        assertThat(evalDiv.fields(), hasSize(1));
        Div div = as(Alias.unwrap(evalDiv.fields().get(0)), Div.class);

        Aggregate finalAgg = as(evalDiv.child(), Aggregate.class);
        assertThat(finalAgg.aggregates(), hasSize(4)); // sum, count, bucket, cluster
        assertThat(finalAgg.groupings(), hasSize(2));

        Eval evalRound = as(finalAgg.child(), Eval.class);
        Round round = as(Alias.unwrap(evalRound.fields().get(0)), Round.class);
        Mul mul = as(round.field(), Mul.class);

        Aggregate aggsByTsid = as(evalRound.child(), Aggregate.class);
        assertThat(aggsByTsid.aggregates(), hasSize(3)); // rate, cluster, bucket
        assertThat(aggsByTsid.groupings(), hasSize(2));

        Eval evalBucket = as(aggsByTsid.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        Bucket bucket = as(Alias.unwrap(evalBucket.fields().get(0)), Bucket.class);
        as(evalBucket.child(), EsRelation.class);

        assertThat(Expressions.attribute(div.left()).id(), equalTo(finalAgg.aggregates().get(0).id()));
        assertThat(Expressions.attribute(div.right()).id(), equalTo(finalAgg.aggregates().get(1).id()));

        assertThat(finalAgg.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));

        Sum sum = as(Alias.unwrap(finalAgg.aggregates().get(0)), Sum.class);
        Count count = as(Alias.unwrap(finalAgg.aggregates().get(1)), Count.class);
        assertThat(Expressions.attribute(sum.field()).id(), equalTo(evalRound.fields().get(0).id()));
        assertThat(Expressions.attribute(count.field()).id(), equalTo(evalRound.fields().get(0).id()));

        assertThat(
            Expressions.attribute(finalAgg.groupings().get(0)).id(),
            equalTo(Expressions.attribute(aggsByTsid.groupings().get(1)).id())
        );
        assertThat(Expressions.attribute(finalAgg.groupings().get(1)).id(), equalTo(aggsByTsid.aggregates().get(1).id()));

        assertThat(Expressions.attribute(mul.left()).id(), equalTo(aggsByTsid.aggregates().get(0).id()));
        assertThat(mul.right().fold(), equalTo(1.05));
        assertThat(aggsByTsid.aggregateType(), equalTo(Aggregate.AggregateType.METRICS));
        Rate rate = as(Alias.unwrap(aggsByTsid.aggregates().get(0)), Rate.class);
        assertThat(Expressions.attribute(rate.field()).name(), equalTo("network.total_bytes_in"));
        Values values = as(Alias.unwrap(aggsByTsid.aggregates().get(1)), Values.class);
        assertThat(Expressions.attribute(values.field()).name(), equalTo("cluster"));
    }

    public void testMetricsWithoutRate() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        List<String> queries = List.of("""
            METRICS k8s count(to_long(network.total_bytes_in)) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """, """
            METRICS k8s | STATS count(to_long(network.total_bytes_in)) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """, """
            FROM k8s | STATS count(to_long(network.total_bytes_in)) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """);
        List<LogicalPlan> plans = new ArrayList<>();
        for (String query : queries) {
            var plan = logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)));
            plans.add(plan);
        }
        for (LogicalPlan plan : plans) {
            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            assertThat(aggregate.aggregateType(), equalTo(Aggregate.AggregateType.STANDARD));
            assertThat(aggregate.aggregates(), hasSize(2));
            assertThat(aggregate.groupings(), hasSize(1));
            Eval eval = as(aggregate.child(), Eval.class);
            assertThat(eval.fields(), hasSize(2));
            assertThat(Alias.unwrap(eval.fields().get(0)), instanceOf(Bucket.class));
            assertThat(Alias.unwrap(eval.fields().get(1)), instanceOf(ToLong.class));
            EsRelation relation = as(eval.child(), EsRelation.class);
            assertThat(relation.indexMode(), equalTo(IndexMode.STANDARD));
        }
        // TODO: Unmute this part
        // https://github.com/elastic/elasticsearch/issues/110827
        // for (int i = 1; i < plans.size(); i++) {
        // assertThat(plans.get(i), equalTo(plans.get(0)));
        // }
    }

    public void testRateInStats() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        var query = """
            METRICS k8s | STATS max(rate(network.total_bytes_in)) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        VerificationException error = expectThrows(
            VerificationException.class,
            () -> logicalOptimizer.optimize(metricsAnalyzer.analyze(parser.createStatement(query)))
        );
        assertThat(error.getMessage(), equalTo("""
            Found 1 problem
            line 1:25: the rate aggregate[rate(network.total_bytes_in)] can only be used within the metrics command"""));
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

    private Literal nullOf(DataType dataType) {
        return new Literal(Source.EMPTY, null, dataType);
    }

    public static EsRelation relation() {
        return new EsRelation(EMPTY, new EsIndex(randomAlphaOfLength(8), emptyMap()), randomFrom(IndexMode.values()), randomBoolean());
    }
}
