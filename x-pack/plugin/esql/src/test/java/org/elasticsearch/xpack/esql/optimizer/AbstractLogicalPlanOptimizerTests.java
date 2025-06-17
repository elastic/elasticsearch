/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.LiteralsOnTheRight;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneRedundantOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownCompletion;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEnrich;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownRegexExtract;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SplitInWithFoldableValue;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
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
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.junit.BeforeClass;

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
import static java.util.Collections.emptyMap;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptySource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.localSource;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public abstract class AbstractLogicalPlanOptimizerTests extends ESTestCase {
    protected static EsqlParser parser;
    protected static LogicalOptimizerContext logicalOptimizerCtx;
    protected static LogicalPlanOptimizer logicalOptimizer;

    protected static Map<String, EsField> mapping;
    protected static Analyzer analyzer;
    protected static Map<String, EsField> mappingAirports;
    protected static Analyzer analyzerAirports;
    protected static Map<String, EsField> mappingTypes;
    protected static Analyzer analyzerTypes;
    protected static Map<String, EsField> mappingExtra;
    protected static Analyzer analyzerExtra;
    protected static Map<String, EsField> metricMapping;
    protected static Analyzer metricsAnalyzer;
    protected static Analyzer multiIndexAnalyzer;

    protected static EnrichResolution enrichResolution;

    public static class SubstitutionOnlyOptimizer extends LogicalPlanOptimizer {
        public static SubstitutionOnlyOptimizer INSTANCE = new SubstitutionOnlyOptimizer(unboundLogicalOptimizerContext());

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
        logicalOptimizerCtx = unboundLogicalOptimizerContext();
        logicalOptimizer = new LogicalPlanOptimizer(logicalOptimizerCtx);
        enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");

        // Most tests used data from the test index, so we load it here, and use it in the plan() function.
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use data from the airports index, so we load it here, and use it in the plan_airports() function.
        mappingAirports = loadMapping("mapping-airports.json");
        EsIndex airports = new EsIndex("airports", mappingAirports, Map.of("airports", IndexMode.STANDARD));
        IndexResolution getIndexResultAirports = IndexResolution.valid(airports);
        analyzerAirports = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultAirports,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests need additional types, so we load that index here and use it in the plan_types() function.
        mappingTypes = loadMapping("mapping-all-types.json");
        EsIndex types = new EsIndex("types", mappingTypes, Map.of("types", IndexMode.STANDARD));
        IndexResolution getIndexResultTypes = IndexResolution.valid(types);
        analyzerTypes = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultTypes,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use mappings from mapping-extra.json to be able to test more types so we load it here
        mappingExtra = loadMapping("mapping-extra.json");
        EsIndex extra = new EsIndex("extra", mappingExtra, Map.of("extra", IndexMode.STANDARD));
        IndexResolution getIndexResultExtra = IndexResolution.valid(extra);
        analyzerExtra = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultExtra,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        metricMapping = loadMapping("k8s-mappings.json");
        var metricsIndex = IndexResolution.valid(new EsIndex("k8s", metricMapping, Map.of("k8s", IndexMode.TIME_SERIES)));
        metricsAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                metricsIndex,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var multiIndexMapping = loadMapping("mapping-basic.json");
        multiIndexMapping.put("partial_type_keyword", new EsField("partial_type_keyword", KEYWORD, emptyMap(), true));
        var multiIndex = IndexResolution.valid(
            new EsIndex(
                "multi_index",
                multiIndexMapping,
                Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD),
                Set.of("partial_type_keyword")
            )
        );
        multiIndexAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                multiIndex,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    protected LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    protected LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        var analyzed = analyzer.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = optimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planAirports(String query) {
        var analyzed = analyzerAirports.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planExtra(String query) {
        var analyzed = analyzerExtra.analyze(parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planTypes(String query) {
        return logicalOptimizer.optimize(analyzerTypes.analyze(parser.createStatement(query)));
    }

    protected LogicalPlan planMultiIndex(String query) {
        return logicalOptimizer.optimize(multiIndexAnalyzer.analyze(parser.createStatement(query)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
