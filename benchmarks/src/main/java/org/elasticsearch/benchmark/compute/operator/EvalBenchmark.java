/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceDateTruncBucketWithRoundTo;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class EvalBenchmark {
    public enum Operation {
        ABS("abs") {
            @Override
            ExpressionEvaluator evaluator() {
                return absEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkAbsExpected(this, actual);
            }
        },
        ADD("add") {
            @Override
            ExpressionEvaluator evaluator() {
                return addLongEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkAddExpected(this, actual);
            }
        },
        ADD_DOUBLE("add_double") {
            @Override
            ExpressionEvaluator evaluator() {
                return addDoubleEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkAddDoubleExpected(this, actual);
            }
        },
        CASE_1_EAGER("case_1_eager") {
            @Override
            ExpressionEvaluator evaluator() {
                return caseEvaluator(false);
            }

            @Override
            void checkExpected(Page actual) {
                checkCaseExpected(this, actual, false);
            }
        },
        CASE_1_LAZY("case_1_lazy") {
            @Override
            ExpressionEvaluator evaluator() {
                return caseEvaluator(true);
            }

            @Override
            void checkExpected(Page actual) {
                checkCaseExpected(this, actual, true);
            }
        },
        COALESCE_2_NOOP("coalesce_2_noop") {
            @Override
            ExpressionEvaluator evaluator() {
                return coalesceEvaluator(false);
            }

            @Override
            void checkExpected(Page actual) {
                checkCoalesceNoopExpected(this, actual);
            }
        },
        COALESCE_2_EAGER("coalesce_2_eager") {
            @Override
            ExpressionEvaluator evaluator() {
                return coalesceEvaluator(false);
            }

            @Override
            void checkExpected(Page actual) {
                checkCoalesceEagerExpected(this, actual);
            }
        },
        COALESCE_2_LAZY("coalesce_2_lazy") {
            @Override
            ExpressionEvaluator evaluator() {
                return coalesceEvaluator(true);
            }

            @Override
            void checkExpected(Page actual) {
                checkCoalesceLazyExpected(this, actual);
            }
        },
        DATE_TRUNC("date_trunc") {
            @Override
            ExpressionEvaluator evaluator() {
                return dateTruncEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkDateTruncExpected(this, actual);
            }
        },
        EQUAL_TO_CONST("equal_to_const") {
            @Override
            ExpressionEvaluator evaluator() {
                return equalToConstEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkEqualToConstExpected(this, actual);
            }
        },
        JSON_EXTRACT("json_extract") {
            @Override
            ExpressionEvaluator evaluator() {
                return jsonExtractEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkJsonExtractExpected(this, actual);
            }
        },
        JSON_EXTRACT_OBJECT("json_extract_object") {
            @Override
            ExpressionEvaluator evaluator() {
                return jsonExtractObjectEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkJsonExtractObjectExpected(this, actual);
            }
        },
        JSON_EXTRACT_VAR("json_extract_var") {
            @Override
            ExpressionEvaluator evaluator() {
                return jsonExtractVarEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkJsonExtractVarExpected(this, actual);
            }
        },
        LONG_EQUAL_TO_LONG("long_equal_to_long") {
            @Override
            ExpressionEvaluator evaluator() {
                return longEqualToLongEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkLongEqualsExpected(this, actual);
            }
        },
        LONG_EQUAL_TO_INT("long_equal_to_int") {
            @Override
            ExpressionEvaluator evaluator() {
                return longEqualToIntEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkLongEqualsExpected(this, actual);
            }
        },
        MOD_LONG_LONG("mod_long_long") {
            @Override
            ExpressionEvaluator evaluator() {
                return modLongLongEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkModLongLongExpected(this, actual);
            }
        },
        MOD_LONG_CONST_60("mod_long_const_60") {
            @Override
            ExpressionEvaluator evaluator() {
                return modLongConst60Evaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkModLongConst60Expected(this, actual);
            }
        },
        DIV_LONG_LONG("div_long_long") {
            @Override
            ExpressionEvaluator evaluator() {
                return divLongLongEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkDivLongLongExpected(this, actual);
            }
        },
        DIV_LONG_CONST_60("div_long_const_60") {
            @Override
            ExpressionEvaluator evaluator() {
                return divLongConst60Evaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkDivLongConst60Expected(this, actual);
            }
        },
        REPLACE_CONST("replace_const") {
            @Override
            ExpressionEvaluator evaluator() {
                return replaceConstEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkReplaceConstExpected(this, actual);
            }
        },
        STARTS_WITH_CONST("starts_with_const") {
            @Override
            ExpressionEvaluator evaluator() {
                return startsWithConstEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkFooRowsBooleanExpected(this, actual);
            }
        },
        STARTS_WITH_VAR("starts_with_var") {
            @Override
            ExpressionEvaluator evaluator() {
                return startsWithVarEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkStartsOrEndsWithVarExpected(this, actual);
            }
        },
        ENDS_WITH_CONST("ends_with_const") {
            @Override
            ExpressionEvaluator evaluator() {
                return endsWithConstEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkFooRowsBooleanExpected(this, actual);
            }
        },
        ENDS_WITH_VAR("ends_with_var") {
            @Override
            ExpressionEvaluator evaluator() {
                return endsWithVarEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkStartsOrEndsWithVarExpected(this, actual);
            }
        },
        RLIKE_LONG_PATTERN("rlike_long_pattern") {
            @Override
            ExpressionEvaluator evaluator() {
                return rlikeLongPatternEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkFooRowsBooleanExpected(this, actual);
            }
        },
        MV_MIN("mv_min") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvMinEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkMvMinExpected(this, actual);
            }
        },
        MV_MIN_ASCENDING("mv_min_ascending") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvMinEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkMvMinExpected(this, actual);
            }
        },
        ROUND_TO_4_VIA_CASE("round_to_4_via_case") {
            @Override
            ExpressionEvaluator evaluator() {
                return roundTo4ViaCaseEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkRoundTo4Expected(this, actual);
            }
        },
        ROUND_TO_2("round_to_2") {
            @Override
            ExpressionEvaluator evaluator() {
                return roundTo2Evaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkRoundTo2Expected(this, actual);
            }
        },
        ROUND_TO_3("round_to_3") {
            @Override
            ExpressionEvaluator evaluator() {
                return roundTo3Evaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkRoundTo3Expected(this, actual);
            }
        },
        ROUND_TO_4("round_to_4") {
            @Override
            ExpressionEvaluator evaluator() {
                return roundTo4Evaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkRoundTo4Expected(this, actual);
            }
        },
        RLIKE("rlike") {
            @Override
            ExpressionEvaluator evaluator() {
                return rlikeEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkRlikeExpected(this, actual);
            }
        },
        MV_LIKE_PREFIX_SINGLE("mv_like_prefix_single") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvLikeEvaluator("f*");
            }

            @Override
            void checkExpected(Page actual) {
                checkMvLikeExpected(this, actual);
            }
        },
        MV_LIKE_PREFIX_MULTI("mv_like_prefix_multi") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvLikeEvaluator("f*");
            }

            @Override
            void checkExpected(Page actual) {
                checkMvLikeExpected(this, actual);
            }
        },
        MV_LIKE_GENERAL_SINGLE("mv_like_general_single") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvLikeEvaluator("f?o*");
            }

            @Override
            void checkExpected(Page actual) {
                checkMvLikeExpected(this, actual);
            }
        },
        MV_LIKE_GENERAL_MULTI("mv_like_general_multi") {
            @Override
            ExpressionEvaluator evaluator() {
                return mvLikeEvaluator("f?o*");
            }

            @Override
            void checkExpected(Page actual) {
                checkMvLikeExpected(this, actual);
            }
        },
        TO_LOWER("to_lower") {
            @Override
            ExpressionEvaluator evaluator() {
                return toLowerEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkToLowerExpected(this, actual, false);
            }
        },
        TO_LOWER_ORDS("to_lower_ords") {
            @Override
            ExpressionEvaluator evaluator() {
                return toLowerEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkToLowerExpected(this, actual, true);
            }
        },
        TO_UPPER("to_upper") {
            @Override
            ExpressionEvaluator evaluator() {
                return toUpperEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkToUpperExpected(this, actual, false);
            }
        },
        TO_UPPER_ORDS("to_upper_ords") {
            @Override
            ExpressionEvaluator evaluator() {
                return toUpperEvaluator();
            }

            @Override
            void checkExpected(Page actual) {
                checkToUpperExpected(this, actual, true);
            }
        },
        TSTEP_5_EQUAL("tstep(5)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tStepEvaluator(5);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_5_EQUAL("tbucket(5)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tBucketEvaluator(5);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TSTEP_7_EQUAL("tstep(7)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tStepEvaluator(7);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TSTEP_99_EQUAL("tstep(99)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tStepEvaluator(99);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_99_EQUAL("tbucket(99)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tBucketEvaluator(99);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TSTEP_64_EQUAL("tstep(64)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tStepEvaluator(64);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_64_EQUAL("tbucket(64)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tBucketEvaluator(64);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TSTEP_1024_EQUAL("tstep(1024)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tStepEvaluator(1024);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_1024_EQUAL("tbucket(1024)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tBucketEvaluator(1024);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_7_EQUAL("tbucket(7)") {
            @Override
            ExpressionEvaluator evaluator() {
                return tBucketEvaluator(7);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_5_NONEQUAL("tbucket_nonequal(5)") {
            @Override
            ExpressionEvaluator evaluator() {
                return nonUniformBucketEvaluator(5);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_7_NONEQUAL("tbucket_nonequal(7)") {
            @Override
            ExpressionEvaluator evaluator() {
                return nonUniformBucketEvaluator(7);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_99_NONEQUAL("tbucket_nonequal(99)") {
            @Override
            ExpressionEvaluator evaluator() {
                return nonUniformBucketEvaluator(99);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        },
        TBUCKET_1024_NONEQUAL("tbucket_nonequal(1024)") {
            @Override
            ExpressionEvaluator evaluator() {
                return nonUniformBucketEvaluator(1024);
            }

            @Override
            void checkExpected(Page actual) {
                checkTimeGroupingExpected(this, actual);
            }
        };

        private final String id;

        Operation(String id) {
            this.id = id;
        }

        String id() {
            return id;
        }

        abstract ExpressionEvaluator evaluator();

        abstract void checkExpected(Page actual);
    }

    // Initialize logging before any BlockFactory / DriverContext field touches LogManager — otherwise
    // BlockFactory.<clinit> fires before the LogConfigurator SPI is set up and NPEs.
    // Matches the AggregatorBenchmark pattern.
    static {
        Utils.configureBenchmarkLogging();
        // EvalBenchmark constructs a fresh evaluator per invocation and discards it. With
        // admission threshold=2 (production default), each invocation would start a fresh
        // admission cycle and route through the Standard (non-JIT-folded) path — defeating
        // the measurement of the JIT-folded steady-state performance. Set threshold=1 here
        // so the bench measures what production sees AFTER admission is met (which is the
        // case for every query past the first one for a given constant). The admission
        // filter's protection against high-cardinality workloads is measured separately
        // by the dedicated stress harness (sweep/AdmissionStress.java).
        org.elasticsearch.compute.operator.ConstantMethodResultSpecializer.SHARED.setAdmissionThreshold(1);
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private static final int BLOCK_LENGTH = 8 * 1024;
    private static final long TBUCKET_RANGE_START_MILLIS = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long TBUCKET_RANGE_END_MILLIS = Instant.parse("2024-01-03T00:00:00Z").toEpochMilli();
    private static final ReplaceDateTruncBucketWithRoundTo REWRITE_RULE = new ReplaceDateTruncBucketWithRoundTo();

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod
            selfTest();
        }
    }

    static void selfTest() {
        Logger log = LogManager.getLogger(EvalBenchmark.class);
        for (Operation operation : Operation.values()) {
            log.info("self testing {}", operation);
            run(operation);
        }
    }

    @Param
    public Operation operation;

    private static Operator operator(Operation operation) {
        return new EvalOperator(driverContext, evaluator(operation));
    }

    private static ExpressionEvaluator evaluator(Operation operation) {
        return operation.evaluator();
    }

    private static ExpressionEvaluator absEvaluator() {
        FieldAttribute longField = longField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new Abs(Source.EMPTY, longField), layout(longField)).get(driverContext);
    }

    private static ExpressionEvaluator addLongEvaluator() {
        FieldAttribute longField = longField();
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Add(Source.EMPTY, longField, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration()),
            layout(longField)
        ).get(driverContext);
    }

    private static ExpressionEvaluator addDoubleEvaluator() {
        FieldAttribute doubleField = doubleField();
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Add(Source.EMPTY, doubleField, new Literal(Source.EMPTY, 1D, DataType.DOUBLE), configuration()),
            layout(doubleField)
        ).get(driverContext);
    }

    private static ExpressionEvaluator caseEvaluator(boolean lazy) {
        FieldAttribute f1 = longField();
        FieldAttribute f2 = longField();
        Expression condition = new Equals(Source.EMPTY, f1, new Literal(Source.EMPTY, 1L, DataType.LONG));
        Expression lhs = f1;
        Expression rhs = f2;
        if (lazy) {
            lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
            rhs = new Add(Source.EMPTY, rhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
        }
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Case(Source.EMPTY, condition, List.of(lhs, rhs)),
            layout(f1, f2)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, lazy ? "CaseLazyEvaluator" : "CaseEagerEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator coalesceEvaluator(boolean lazy) {
        FieldAttribute f1 = longField();
        FieldAttribute f2 = longField();
        Expression lhs = f1;
        if (lazy) {
            lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
        }
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, new Coalesce(Source.EMPTY, lhs, List.of(f2)), layout(f1, f2))
            .get(driverContext);
        assertEvaluatorContains(evaluator, lazy ? "CoalesceLongLazyEvaluator" : "CoalesceLongEagerEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator dateTruncEvaluator() {
        FieldAttribute timestamp = new FieldAttribute(
            Source.EMPTY,
            "timestamp",
            new EsField("timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new DateTrunc(
                Source.EMPTY,
                new Literal(Source.EMPTY, Duration.ofHours(24), DataType.TIME_DURATION),
                timestamp,
                configuration()
            ),
            layout(timestamp)
        ).get(driverContext);
    }

    private static ExpressionEvaluator equalToConstEvaluator() {
        FieldAttribute longField = longField();
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Equals(Source.EMPTY, longField, new Literal(Source.EMPTY, 100_000L, DataType.LONG)),
            layout(longField)
        ).get(driverContext);
    }

    private static ExpressionEvaluator jsonExtractEvaluator() {
        FieldAttribute keywordField = keywordField();
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new JsonExtract(Source.EMPTY, keywordField, new Literal(Source.EMPTY, new BytesRef("user.name"), DataType.KEYWORD)),
            layout(keywordField)
        ).get(driverContext);
    }

    private static ExpressionEvaluator jsonExtractObjectEvaluator() {
        FieldAttribute keywordField = keywordField();
        return EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new JsonExtract(Source.EMPTY, keywordField, new Literal(Source.EMPTY, new BytesRef("user"), DataType.KEYWORD)),
            layout(keywordField)
        ).get(driverContext);
    }

    private static ExpressionEvaluator jsonExtractVarEvaluator() {
        FieldAttribute json = keywordField();
        FieldAttribute path = keywordField("path");
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, new JsonExtract(Source.EMPTY, json, path), layout(json, path))
            .get(driverContext);
        if (evaluator.toString().contains("JsonExtractEvaluator") == false || evaluator.toString().contains("JsonExtractConstant")) {
            throw new IllegalArgumentException(
                "Evaluator was [" + evaluator + "] but expected one containing [JsonExtractEvaluator] (non-constant)"
            );
        }
        return evaluator;
    }

    private static ExpressionEvaluator longEqualToLongEvaluator() {
        FieldAttribute lhs = longField();
        FieldAttribute rhs = longField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
    }

    private static ExpressionEvaluator longEqualToIntEvaluator() {
        FieldAttribute lhs = longField();
        FieldAttribute rhs = intField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
    }

    private static ExpressionEvaluator modLongLongEvaluator() {
        FieldAttribute lhs = longField();
        FieldAttribute rhs = longField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new Mod(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
    }

    private static ExpressionEvaluator modLongConst60Evaluator() {
        FieldAttribute lhs = longField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Mod(Source.EMPTY, lhs, new Literal(Source.EMPTY, 60L, DataType.LONG)),
            layout(lhs)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "ModLongsByConstantEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator divLongLongEvaluator() {
        FieldAttribute lhs = longField();
        FieldAttribute rhs = longField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new Div(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
    }

    private static ExpressionEvaluator divLongConst60Evaluator() {
        FieldAttribute lhs = longField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Div(Source.EMPTY, lhs, new Literal(Source.EMPTY, 60L, DataType.LONG)),
            layout(lhs)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "DivLongsByConstantEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator replaceConstEvaluator() {
        FieldAttribute keywordField = keywordField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Replace(
                Source.EMPTY,
                keywordField,
                new Literal(Source.EMPTY, new BytesRef("foo"), DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef("X"), DataType.KEYWORD)
            ),
            layout(keywordField)
        ).get(driverContext);
        // Upstream main may select ReplaceConstantOrdinalEvaluator (ordinal fast path) for
        // BytesRef inputs; either is a "constant" path. Accept both shapes.
        assertEvaluatorContains(evaluator, "ReplaceConstant");
        return evaluator;
    }

    private static ExpressionEvaluator mvMinEvaluator() {
        FieldAttribute longField = longField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, new MvMin(Source.EMPTY, longField), layout(longField)).get(driverContext);
    }

    private static ExpressionEvaluator rlikeEvaluator() {
        FieldAttribute keywordField = keywordField();
        RLike rlike = new RLike(Source.EMPTY, keywordField, new RLikePattern(".ar"));
        return EvalMapper.toEvaluator(FOLD_CONTEXT, rlike, layout(keywordField)).get(driverContext);
    }

    private static ExpressionEvaluator mvLikeEvaluator(String pattern) {
        FieldAttribute keywordField = keywordField();
        MvLike mvLike = new MvLike(Source.EMPTY, keywordField, new Literal(Source.EMPTY, new BytesRef(pattern), DataType.KEYWORD));
        return EvalMapper.toEvaluator(FOLD_CONTEXT, mvLike, layout(keywordField)).get(driverContext);
    }

    private static void checkMvLikeExpected(Operation operation, Page actual) {
        // Both the single- and multi-valued blocks are built so that the pattern matches at even positions.
        BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            boolean expected = i % 2 == 0;
            if (v.getBoolean(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
            }
        }
    }

    private static ExpressionEvaluator rlikeLongPatternEvaluator() {
        FieldAttribute keywordField = keywordField();
        // More complex pattern — exercises a larger DFA than the existing "rlike" case.
        RLike rlike = new RLike(Source.EMPTY, keywordField, new RLikePattern("[a-z]oo"));
        return EvalMapper.toEvaluator(FOLD_CONTEXT, rlike, layout(keywordField)).get(driverContext);
    }

    private static ExpressionEvaluator startsWithConstEvaluator() {
        FieldAttribute keywordField = keywordField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new StartsWith(Source.EMPTY, keywordField, new Literal(Source.EMPTY, new BytesRef("fo"), DataType.KEYWORD)),
            layout(keywordField)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "StartsWithConstantEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator endsWithConstEvaluator() {
        FieldAttribute keywordField = keywordField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new EndsWith(Source.EMPTY, keywordField, new Literal(Source.EMPTY, new BytesRef("oo"), DataType.KEYWORD)),
            layout(keywordField)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "EndsWithConstantEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator startsWithVarEvaluator() {
        FieldAttribute str = keywordField();
        FieldAttribute prefix = keywordField("prefix");
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, new StartsWith(Source.EMPTY, str, prefix), layout(str, prefix))
            .get(driverContext);
        if (evaluator.toString().contains("StartsWithEvaluator") == false || evaluator.toString().contains("StartsWithConstant")) {
            throw new IllegalArgumentException(
                "Evaluator was [" + evaluator + "] but expected one containing [StartsWithEvaluator] (non-constant)"
            );
        }
        return evaluator;
    }

    private static ExpressionEvaluator endsWithVarEvaluator() {
        FieldAttribute str = keywordField();
        FieldAttribute suffix = keywordField("suffix");
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, new EndsWith(Source.EMPTY, str, suffix), layout(str, suffix))
            .get(driverContext);
        if (evaluator.toString().contains("EndsWithEvaluator") == false || evaluator.toString().contains("EndsWithConstant")) {
            throw new IllegalArgumentException(
                "Evaluator was [" + evaluator + "] but expected one containing [EndsWithEvaluator] (non-constant)"
            );
        }
        return evaluator;
    }

    private static ExpressionEvaluator roundTo4ViaCaseEvaluator() {
        FieldAttribute f = longField();
        Expression ltkb = new LessThan(Source.EMPTY, f, kb());
        Expression ltmb = new LessThan(Source.EMPTY, f, mb());
        Expression ltgb = new LessThan(Source.EMPTY, f, gb());
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new Case(Source.EMPTY, ltkb, List.of(b(), ltmb, kb(), ltgb, mb(), gb())),
            layout(f)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "CaseLazyEvaluator");
        return evaluator;
    }

    private static ExpressionEvaluator roundTo2Evaluator() {
        FieldAttribute f = longField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FOLD_CONTEXT, new RoundTo(Source.EMPTY, f, List.of(b(), kb())), layout(f))
            .get(driverContext);
        assertEvaluatorContains(evaluator, "RoundToLong2");
        return evaluator;
    }

    private static ExpressionEvaluator roundTo3Evaluator() {
        FieldAttribute f = longField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new RoundTo(Source.EMPTY, f, List.of(b(), kb(), mb())),
            layout(f)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "RoundToLong3");
        return evaluator;
    }

    private static ExpressionEvaluator roundTo4Evaluator() {
        FieldAttribute f = longField();
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new RoundTo(Source.EMPTY, f, List.of(b(), kb(), mb(), gb())),
            layout(f)
        ).get(driverContext);
        assertEvaluatorContains(evaluator, "RoundToLong4");
        return evaluator;
    }

    private static ExpressionEvaluator toLowerEvaluator() {
        FieldAttribute keywordField = keywordField();
        ToLower toLower = new ToLower(Source.EMPTY, keywordField, configuration());
        return EvalMapper.toEvaluator(FOLD_CONTEXT, toLower, layout(keywordField)).get(driverContext);
    }

    private static ExpressionEvaluator toUpperEvaluator() {
        FieldAttribute keywordField = keywordField();
        ToUpper toUpper = new ToUpper(Source.EMPTY, keywordField, configuration());
        return EvalMapper.toEvaluator(FOLD_CONTEXT, toUpper, layout(keywordField)).get(driverContext);
    }

    private static void assertEvaluatorContains(ExpressionEvaluator evaluator, String requiredSubstring) {
        if (evaluator.toString().contains(requiredSubstring) == false) {
            throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + requiredSubstring + "]");
        }
    }

    private static ExpressionEvaluator tStepEvaluator(int bucketCount) {
        FieldAttribute ts = timestampField();
        Expression surrogate = new TStep(
            Source.EMPTY,
            new Literal(Source.EMPTY, bucketCount, DataType.INTEGER),
            new Literal(Source.EMPTY, TBUCKET_RANGE_START_MILLIS, DataType.DATETIME),
            new Literal(Source.EMPTY, TBUCKET_RANGE_END_MILLIS, DataType.DATETIME),
            ts,
            configuration()
        ).surrogate();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, rewriteWithRule(surrogate, ts), layout(ts)).get(driverContext);
    }

    private static ExpressionEvaluator tBucketEvaluator(int bucketCount) {
        FieldAttribute ts = timestampField();
        Expression surrogate = new TBucket(
            Source.EMPTY,
            new Literal(Source.EMPTY, bucketCount, DataType.INTEGER),
            new Literal(Source.EMPTY, TBUCKET_RANGE_START_MILLIS, DataType.DATETIME),
            new Literal(Source.EMPTY, TBUCKET_RANGE_END_MILLIS, DataType.DATETIME),
            ts,
            configuration()
        ).surrogate();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, rewriteWithRule(surrogate, ts), layout(ts)).get(driverContext);
    }

    private static ExpressionEvaluator nonUniformBucketEvaluator(int bucketCount) {
        FieldAttribute ts = timestampField();
        return EvalMapper.toEvaluator(FOLD_CONTEXT, nonUniformRoundTo(ts, bucketCount), layout(ts)).get(driverContext);
    }

    /**
     * Creates a RoundTo with quadratically-spaced points — guaranteed non-uniform so neither branch
     * uses the fixed-interval fast path.
     */
    private static RoundTo nonUniformRoundTo(FieldAttribute ts, int bucketCount) {
        long range = TBUCKET_RANGE_END_MILLIS - TBUCKET_RANGE_START_MILLIS;
        List<Expression> literals = new ArrayList<>(bucketCount + 1);
        for (int i = 0; i <= bucketCount; i++) {
            // Quadratic distribution: denser near start, sparser near end
            double fraction = (double) i * i / ((double) bucketCount * bucketCount);
            long t = TBUCKET_RANGE_START_MILLIS + (long) (fraction * range);
            literals.add(new Literal(Source.EMPTY, t, DataType.DATETIME));
        }
        return new RoundTo(Source.EMPTY, ts, literals);
    }

    private static Expression rewriteWithRule(Expression expression, FieldAttribute ts) {
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(ts.toAttribute()), EmptyLocalSupplier.EMPTY);
        Eval eval = new Eval(Source.EMPTY, child, List.of(new Alias(Source.EMPTY, "group_key", expression)));
        LogicalPlan rewritten = REWRITE_RULE.apply(
            eval,
            new LocalLogicalOptimizerContext(configuration(), FOLD_CONTEXT, searchStats(ts.fieldName()))
        );
        if ((rewritten instanceof Eval) == false) {
            throw new IllegalStateException("expected Eval plan after rewrite but got [" + rewritten.getClass().getSimpleName() + "]");
        }
        Eval rewrittenEval = (Eval) rewritten;
        return rewrittenEval.fields().get(0).child();
    }

    private static SearchStats searchStats(FieldAttribute.FieldName fieldName) {
        return new SearchStats.UnsupportedSearchStats() {
            @Override
            public Object min(FieldAttribute.FieldName field) {
                return field.equals(fieldName) ? TBUCKET_RANGE_START_MILLIS : null;
            }

            @Override
            public Object max(FieldAttribute.FieldName field) {
                return field.equals(fieldName) ? TBUCKET_RANGE_END_MILLIS : null;
            }
        };
    }

    private static FieldAttribute longField() {
        return new FieldAttribute(
            Source.EMPTY,
            "long",
            new EsField("long", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute doubleField() {
        return new FieldAttribute(
            Source.EMPTY,
            "double",
            new EsField("double", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute intField() {
        return new FieldAttribute(
            Source.EMPTY,
            "int",
            new EsField("int", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute keywordField() {
        return keywordField("keyword");
    }

    private static FieldAttribute keywordField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute timestampField() {
        return new FieldAttribute(
            Source.EMPTY,
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static Configuration configuration() {
        return new Configuration(
            Instant.now(),
            Locale.ROOT,
            null,
            null,
            QueryPragmas.EMPTY,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY),
            null,
            false,
            Map.of(),
            0,
            false,
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            ResolvedSettings.EMPTY,
            Map.of()
        );
    }

    private static Layout layout(FieldAttribute... fields) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(Arrays.asList(fields));
        return layout.build();
    }

    private static void checkExpected(Operation operation, Page actual) {
        operation.checkExpected(actual);
    }

    private static void checkTimeGroupingExpected(Operation operation, Page actual) {
        LongVector result = actual.<LongBlock>getBlock(1).asVector();
        if (result.getPositionCount() != BLOCK_LENGTH) {
            throw new AssertionError("[" + operation + "] expected " + BLOCK_LENGTH + " positions");
        }
    }

    private static void checkAbsExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getLong(i) != i * 100_000) {
                throw new AssertionError("[" + operation + "] expected [" + (i * 100_000) + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkAddExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getLong(i) != i * 100_000 + 1) {
                throw new AssertionError("[" + operation + "] expected [" + (i * 100_000 + 1) + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkAddDoubleExpected(Operation operation, Page actual) {
        DoubleVector v = actual.<DoubleBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getDouble(i) != i * 100_000 + 1D) {
                throw new AssertionError("[" + operation + "] expected [" + (i * 100_000 + 1D) + "] but was [" + v.getDouble(i) + "]");
            }
        }
    }

    private static void checkCaseExpected(Operation operation, Page actual, boolean lazy) {
        LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
        LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
        LongVector result = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = f1.getLong(i) == 1 ? f1.getLong(i) : f2.getLong(i);
            if (lazy) {
                expected += 1;
            }
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkCoalesceNoopExpected(Operation operation, Page actual) {
        LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
        LongVector result = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = f1.getLong(i);
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkCoalesceEagerExpected(Operation operation, Page actual) {
        LongBlock f1 = actual.getBlock(0);
        LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
        LongVector result = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = i % 5 == 0 ? f2.getLong(i) : f1.getLong(f1.getFirstValueIndex(i));
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkCoalesceLazyExpected(Operation operation, Page actual) {
        LongBlock f1 = actual.getBlock(0);
        LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
        LongVector result = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = i % 5 == 0 ? f2.getLong(i) : f1.getLong(f1.getFirstValueIndex(i)) + 1;
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkDateTruncExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        long oneDay = TimeValue.timeValueHours(24).millis();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = i * 100_000;
            expected -= expected % oneDay;
            if (v.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkEqualToConstExpected(Operation operation, Page actual) {
        BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            boolean expected = i == 1;
            if (v.getBoolean(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
            }
        }
    }

    private static void checkLongEqualsExpected(Operation operation, Page actual) {
        BooleanVector v = actual.<BooleanBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getBoolean(i) == false) {
                throw new AssertionError("[" + operation + "] expected [true] but was [false]");
            }
        }
    }

    private static void checkModLongLongExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = (i * 100_000L) % ((i % 60) + 1);
            if (v.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkModLongConst60Expected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = (i * 100_000L) % 60L;
            if (v.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkDivLongLongExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = (i * 100_000L) / ((i % 60) + 1);
            if (v.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkDivLongConst60Expected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = (i * 100_000L) / 60L;
            if (v.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkMvMinExpected(Operation operation, Page actual) {
        LongVector v = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getLong(i) != i) {
                throw new AssertionError("[" + operation + "] expected [" + i + "] but was [" + v.getLong(i) + "]");
            }
        }
    }

    private static void checkReplaceConstExpected(Operation operation, Page actual) {
        BytesRef expected0 = new BytesRef("X");
        BytesRef expected1 = new BytesRef("bar");
        checkBytes(operation.id(), actual, false, new BytesRef[] { expected0, expected1 });
    }

    private static void checkRlikeExpected(Operation operation, Page actual) {
        BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            boolean expected = i % 2 == 1;
            if (v.getBoolean(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
            }
        }
    }

    private static void checkFooRowsBooleanExpected(Operation operation, Page actual) {
        BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            boolean expected = i % 2 == 0;
            if (v.getBoolean(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
            }
        }
    }

    private static void checkStartsOrEndsWithVarExpected(Operation operation, Page actual) {
        BooleanVector v = actual.<BooleanBlock>getBlock(2).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            boolean expected = i % 2 == 0;
            if (v.getBoolean(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
            }
        }
    }

    private static void checkRoundTo4Expected(Operation operation, Page actual) {
        long b = 1;
        long kb = ByteSizeUnit.KB.toBytes(1);
        long mb = ByteSizeUnit.MB.toBytes(1);
        long gb = ByteSizeUnit.GB.toBytes(1);
        LongVector f = actual.<LongBlock>getBlock(0).asVector();
        LongVector result = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = f.getLong(i);
            if (expected < kb) {
                expected = b;
            } else if (expected < mb) {
                expected = kb;
            } else if (expected < gb) {
                expected = mb;
            } else {
                expected = gb;
            }
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkRoundTo3Expected(Operation operation, Page actual) {
        long b = 1;
        long kb = ByteSizeUnit.KB.toBytes(1);
        long mb = ByteSizeUnit.MB.toBytes(1);
        LongVector f = actual.<LongBlock>getBlock(0).asVector();
        LongVector result = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = f.getLong(i);
            if (expected < kb) {
                expected = b;
            } else if (expected < mb) {
                expected = kb;
            } else {
                expected = mb;
            }
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkRoundTo2Expected(Operation operation, Page actual) {
        long b = 1;
        long kb = ByteSizeUnit.KB.toBytes(1);
        LongVector f = actual.<LongBlock>getBlock(0).asVector();
        LongVector result = actual.<LongBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            long expected = f.getLong(i);
            if (expected < kb) {
                expected = b;
            } else {
                expected = kb;
            }
            if (result.getLong(i) != expected) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
            }
        }
    }

    private static void checkJsonExtractExpected(Operation operation, Page actual) {
        BytesRef expected = new BytesRef("John");
        checkBytes(operation.id(), actual, false, new BytesRef[] { expected, expected });
    }

    private static void checkJsonExtractVarExpected(Operation operation, Page actual) {
        BytesRef expected = new BytesRef("John");
        BytesRefVector v = actual.<BytesRefBlock>getBlock(2).asVector();
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            if (v.getBytesRef(i, scratch).equals(expected) == false) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + scratch + "]");
            }
        }
    }

    private static void checkJsonExtractObjectExpected(Operation operation, Page actual) {
        BytesRef expected = new BytesRef("{\"name\":\"John\",\"age\":30}");
        checkBytes(operation.id(), actual, false, new BytesRef[] { expected, expected });
    }

    private static void checkToLowerExpected(Operation operation, Page actual, boolean expectOrds) {
        checkBytes(operation.id(), actual, expectOrds, new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") });
    }

    private static void checkToUpperExpected(Operation operation, Page actual, boolean expectOrds) {
        checkBytes(operation.id(), actual, expectOrds, new BytesRef[] { new BytesRef("FOO"), new BytesRef("BAR") });
    }

    private static void checkBytes(String operation, Page actual, boolean expectOrds, BytesRef[] expectedVals) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = expectedVals[i % 2];
            BytesRef b = v.getBytesRef(i, scratch);
            if (b.equals(expected) == false) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + b + "]");
            }
        }
        if (expectOrds) {
            if (v.asOrdinals() == null) {
                throw new IllegalArgumentException("expected ords but got " + v);
            }
        } else {
            if (v.asOrdinals() != null) {
                throw new IllegalArgumentException("expected non-ords but got " + v);
            }
        }
    }

    private static Page page(Operation operation) {
        return switch (operation) {
            case TSTEP_5_EQUAL, TSTEP_7_EQUAL, TBUCKET_5_EQUAL, TSTEP_99_EQUAL, TBUCKET_99_EQUAL, TSTEP_64_EQUAL, TBUCKET_64_EQUAL,
                TSTEP_1024_EQUAL, TBUCKET_1024_EQUAL, TBUCKET_7_EQUAL, TBUCKET_5_NONEQUAL, TBUCKET_7_NONEQUAL, TBUCKET_99_NONEQUAL,
                TBUCKET_1024_NONEQUAL -> bucketPage();
            case ABS, ADD, DATE_TRUNC, EQUAL_TO_CONST, MOD_LONG_CONST_60, DIV_LONG_CONST_60, ROUND_TO_4_VIA_CASE, ROUND_TO_2, ROUND_TO_3,
                ROUND_TO_4 -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i * 100_000);
                }
                yield new Page(builder.build());
            }
            case ADD_DOUBLE -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendDouble(i * 100_000D);
                }
                yield new Page(builder.build());
            }
            case CASE_1_EAGER, CASE_1_LAZY, COALESCE_2_NOOP -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    f1.appendLong(i);
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case COALESCE_2_EAGER, COALESCE_2_LAZY -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (i % 5 == 0) {
                        f1.appendNull();
                    } else {
                        f1.appendLong(i);
                    }
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case JSON_EXTRACT, JSON_EXTRACT_OBJECT -> {
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef json = new BytesRef("{\"user\":{\"name\":\"John\",\"age\":30},\"active\":true}");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(json);
                }
                yield new Page(builder.build().asBlock());
            }
            case JSON_EXTRACT_VAR -> {
                var jsonCol = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                var pathCol = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef json = new BytesRef("{\"user\":{\"name\":\"John\",\"age\":30},\"active\":true}");
                BytesRef path = new BytesRef("user.name");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    jsonCol.appendBytesRef(json);
                    pathCol.appendBytesRef(path);
                }
                yield new Page(jsonCol.build().asBlock(), pathCol.build().asBlock());
            }
            case LONG_EQUAL_TO_LONG -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000);
                    rhs.appendLong(i * 100_000);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case MOD_LONG_LONG, DIV_LONG_LONG -> {
                // lhs varies widely; rhs is always 1..60 (never zero) and varies per row so HotSpot
                // can't speculate it stable. This isolates the per-row IDIV cost from the constant
                // fast path.
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000);
                    rhs.appendLong((i % 60) + 1);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case LONG_EQUAL_TO_INT -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000);
                    rhs.appendInt(i * 100_000);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case MV_MIN -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.beginPositionEntry();
                    builder.appendLong(i);
                    builder.appendLong(i + 1);
                    builder.appendLong(i + 2);
                    builder.endPositionEntry();
                }
                yield new Page(builder.build());
            }
            case MV_MIN_ASCENDING -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.beginPositionEntry();
                    builder.appendLong(i);
                    builder.appendLong(i + 1);
                    builder.appendLong(i + 2);
                    builder.endPositionEntry();
                }
                yield new Page(builder.build());
            }
            case STARTS_WITH_VAR -> {
                var str = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                var prefix = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef[] values = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                BytesRef constPrefix = new BytesRef("fo");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    str.appendBytesRef(values[i % 2]);
                    prefix.appendBytesRef(constPrefix);
                }
                yield new Page(str.build().asBlock(), prefix.build().asBlock());
            }
            case ENDS_WITH_VAR -> {
                var str = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                var suffix = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef[] values = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                BytesRef constSuffix = new BytesRef("oo");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    str.appendBytesRef(values[i % 2]);
                    suffix.appendBytesRef(constSuffix);
                }
                yield new Page(str.build().asBlock(), suffix.build().asBlock());
            }
            case RLIKE, RLIKE_LONG_PATTERN, TO_LOWER, TO_UPPER, REPLACE_CONST, STARTS_WITH_CONST, ENDS_WITH_CONST -> {
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef[] values = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(values[i % 2]);
                }
                yield new Page(builder.build().asBlock());
            }
            case MV_LIKE_PREFIX_SINGLE, MV_LIKE_GENERAL_SINGLE -> {
                // Single-valued, vector-backed keyword block: the pattern ("f*" / "f?o*") matches "foo" at even positions.
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef[] values = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(values[i % 2]);
                }
                yield new Page(builder.build().asBlock());
            }
            case MV_LIKE_PREFIX_MULTI, MV_LIKE_GENERAL_MULTI -> {
                // Multi-valued keyword block: two values per position, the matching value present only at even positions.
                var builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                BytesRef[] first = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                BytesRef second = new BytesRef("zzz");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.beginPositionEntry();
                    builder.appendBytesRef(first[i % 2]);
                    builder.appendBytesRef(second);
                    builder.endPositionEntry();
                }
                yield new Page(builder.build());
            }
            case TO_LOWER_ORDS, TO_UPPER_ORDS -> {
                var bytes = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                bytes.appendBytesRef(new BytesRef("foo"));
                bytes.appendBytesRef(new BytesRef("bar"));
                var ordinals = blockFactory.newIntVectorFixedBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    ordinals.appendInt(i % 2);
                }
                yield new Page(new OrdinalBytesRefVector(ordinals.build(), bytes.build()).asBlock());
            }
        };
    }

    private static Page bucketPage() {
        long span = TBUCKET_RANGE_END_MILLIS - TBUCKET_RANGE_START_MILLIS;
        var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            // These evaluators are built from bounded range metadata, so benchmark rows should model that same range filter.
            long value = switch (i & 3) {
                case 0 -> TBUCKET_RANGE_START_MILLIS + (((long) i * 31) % span);
                case 1 -> TBUCKET_RANGE_START_MILLIS + (((long) i * 17) % span);
                case 2 -> TBUCKET_RANGE_END_MILLIS - (((long) i * 23) % span);
                case 3 -> TBUCKET_RANGE_START_MILLIS + (((long) i * 61) % span);
                default -> throw new IllegalStateException("unreachable");
            };
            builder.appendLong(value);
        }
        return new Page(builder.build());
    }

    private static Literal b() {
        return lit(1L);
    }

    private static Literal kb() {
        return lit(ByteSizeUnit.KB.toBytes(1));
    }

    private static Literal mb() {
        return lit(ByteSizeUnit.MB.toBytes(1));
    }

    private static Literal gb() {
        return lit(ByteSizeUnit.GB.toBytes(1));
    }

    private static Literal lit(long v) {
        return new Literal(Source.EMPTY, v, DataType.LONG);
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run(Blackhole bh) {
        bh.consume(run(operation));
    }

    private static Object run(Operation operation) {
        try (var operator = operator(operation)) {
            Page page = page(operation);
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            // We only check the last one
            checkExpected(operation, output);

            return output;
        }
    }
}
