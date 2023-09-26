/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class LocalPhysicalPlanOptimizerTests extends ESTestCase {

    private static EsqlParser parser;
    private static Analyzer analyzer;

    private static final Set<String> LONG_DOUBLE = Set.of("long", "double");
    private static final Set<String> INT_LONG_DOUBLE = Set.of("integer", "long", "double");

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        Map<String, EsField> mapping = loadMapping("mapping-alltypes.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);

        analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, EsqlTestUtils.emptyPolicyResolution()),
            new Verifier(new Metrics())
        );
    }

    private record ImplicitCastCase(String datatype, String expression) {}

    /**
     * Expects no filter pushdown if Lucene cannot handle an implicit conversion, for example:
     * LimitExec[500[INTEGER]]
     * \_FilterExec[integer{f}#2 > 1000000000000[LONG]]
     *   \_FieldExtractExec[integer{f}#2]
     *     \_EsQueryExec[test], query[][_doc{f}#7], limit[], sort[] estimatedRowSize[null]
     */
    public void testNoFilterPushdownForUnsupportedImplicitCast() {
        String sign = randomBoolean() ? "" : "-";
        String op = randomBinaryComparisonOperatorSymbol();

        List<ImplicitCastCase> casts = List.of(
            // Exact numerical types
            // Bytes/Shorts are treated as ints by Lucene, so out of range = out of int range
            new ImplicitCastCase("byte", toLongOrDouble() + "(" + sign + "1000000000000)"),
            new ImplicitCastCase("short", toLongOrDouble() + "(" + sign + "1000000000000)"),
            new ImplicitCastCase("integer", toLongOrDouble() + "(" + sign + "1000000000000)"),
            // Floating point types
            // TODO: We treat float and half_float as double, so we cannot properly decide if we can push down or not.
            // https://github.com/elastic/elasticsearch/issues/100130
            // new ImplicitCastCase("half_float", toLongOrDouble() + "(" + sign + "1000000000000)"),
            // new ImplicitCastCase("float", "to_double(" + sign + "1.797693134862315) * pow(10.0, 307)"),
            // Other types
            new ImplicitCastCase("text", "\"foo\"")
        );

        for (ImplicitCastCase c : casts) {
            var plan = plan("from test | where " + c.datatype + op + c.expression);

            var optimizedPlan = localPhysicalPlan(plan);

            var limit = as(optimizedPlan, LimitExec.class);
            var filter = as(limit.child(), FilterExec.class);
            var extract = as(filter.child(), FieldExtractExec.class);
            var queryExec = as(extract.child(), EsQueryExec.class);
            assertNull(queryExec.query());
        }
    }

    /**
     * Expects a single {@link EsQueryExec} containing the filter, like the following:
     * EsQueryExec[test], query[{"esql_single_value":{"field":"long","next":{"range":{"long":{"gt":1,"boost":1.0}}}}}][_doc{f}#7],
     *  limit[500], sort[] estimatedRowSize[null]
     */
    public void testFilterPushdownForSupportedImplicitCast() {
        String sign = randomBoolean() ? "" : "-";
        String op = randomBinaryComparisonOperatorSymbol();

        List<ImplicitCastCase> casts = List.of(
            // Exact numerical types
            new ImplicitCastCase("byte", toIntLongOrDouble() + "(2147483647)"),
            new ImplicitCastCase("byte", toIntLongOrDouble() + "(-2147483648)"),
            new ImplicitCastCase("short", toIntLongOrDouble() + "(2147483647)"),
            new ImplicitCastCase("short", toIntLongOrDouble() + "(-2147483648)"),
            new ImplicitCastCase("integer", toIntLongOrDouble() + "(2147483647)"),
            new ImplicitCastCase("integer", toIntLongOrDouble() + "(-2147483648)"),
            new ImplicitCastCase("long", toIntLongOrDouble() + "(" + sign + "1)"),
            new ImplicitCastCase("long", toIntLongOrDouble() + "(" + sign + "1)"),
            // Floating point types
            new ImplicitCastCase("half_float", toIntLongOrDouble() + "(" + sign + "65505)"),
            new ImplicitCastCase("float", toIntLongOrDouble() + "(" + sign + "1.0)"),
            new ImplicitCastCase("float", "to_double(" + sign + "3.4028235) * pow(10.0, 38)"),
            new ImplicitCastCase("double", toIntLongOrDouble() + "(" + sign + "1)"),
            // Other types
            new ImplicitCastCase("wildcard", "\"foo\"")
        );

        for (ImplicitCastCase c : casts) {
            var plan = plan("from test | where " + c.datatype + op + c.expression);

            var optimizedPlan = localPhysicalPlan(plan);

            var queryExec = as(optimizedPlan, EsQueryExec.class);
            assertNotNull(queryExec.query());
        }
    }

    private LogicalPlan plan(String query) {
        var analyzed = analyzer.analyze(parser.createStatement(query));

        return (new LogicalPlanOptimizer()).optimize(analyzed);
    }

    private PhysicalPlan localPhysicalPlan(LogicalPlan plan) {
        var localLogicalPlanMapper = new Mapper(true);

        // no logical optimization to have better control over plans in tests
        var localPhysicalPlan = localLogicalPlanMapper.map(plan);
        var localPhysicalContext = new LocalPhysicalOptimizerContext(TEST_CFG);
        return (new LocalPhysicalPlanOptimizer(localPhysicalContext)).localOptimize(localPhysicalPlan);
    }

    private String randomBinaryComparisonOperatorSymbol() {
        var operators = BinaryComparisonProcessor.BinaryComparisonOperation.values();

        int ordinal = randomNonNegativeInt() % operators.length;

        if (operators[ordinal] == BinaryComparisonProcessor.BinaryComparisonOperation.NULLEQ) {
            // <=> not supported in ESQL
            return BinaryComparisonProcessor.BinaryComparisonOperation.EQ.symbol();
        }

        return operators[ordinal].symbol();
    }

    private static String toIntLongOrDouble() {
        return "to_" + randomSubsetOf(1, INT_LONG_DOUBLE).get(0);
    }

    private static String toLongOrDouble() {
        return "to_" + randomSubsetOf(1, LONG_DOUBLE).get(0);
    }
}
