/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.Queries;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.ql.util.Queries.Clause.FILTER;
import static org.elasticsearch.xpack.ql.util.Queries.Clause.MUST;
import static org.elasticsearch.xpack.ql.util.Queries.Clause.SHOULD;
import static org.hamcrest.Matchers.nullValue;

public class FilterTests extends ESTestCase {

    // use a field that already exists in the mapping
    private static final String AT_TIMESTAMP = "emp_no";
    private static final String OTHER_FIELD = "salary";

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static PhysicalPlanOptimizer physicalPlanOptimizer;
    private static Map<String, EsField> mapping;
    private static Mapper mapper;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        mapper = new Mapper(false);

        analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, EsqlTestUtils.emptyPolicyResolution()),
            TEST_VERIFIER
        );
    }

    public void testTimestampRequestFilterNoQueryFilter() {
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > 10
            """, OTHER_FIELD), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        assertEquals(restFilter.toString(), filter.toString());
    }

    public void testTimestampNoRequestFilterQueryFilter() {
        var value = 10;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {}
            """, AT_TIMESTAMP, value), null);

        var filter = filterQueryForTransportNodes(plan);
        var expected = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(value), AT_TIMESTAMP);
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilter() {
        var value = 10;
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > 10
            """, AT_TIMESTAMP, value), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var queryFilter = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(value).includeUpper(false), AT_TIMESTAMP);
        var expected = Queries.combine(FILTER, asList(restFilter, queryFilter));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithConjunction() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} AND {} < {}
            """, AT_TIMESTAMP, lowValue, AT_TIMESTAMP, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var left = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(lowValue), AT_TIMESTAMP);
        var right = singleValueQuery(rangeQuery(AT_TIMESTAMP).lt(highValue), AT_TIMESTAMP);
        var must = Queries.combine(MUST, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, must));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithDisjunctionOnDifferentFields() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} OR {} < {}
            """, OTHER_FIELD, lowValue, AT_TIMESTAMP, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var expected = restFilter;
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithDisjunctionOnSameField() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} OR {} < {}
            """, AT_TIMESTAMP, lowValue, AT_TIMESTAMP, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var left = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(lowValue), AT_TIMESTAMP);
        var right = singleValueQuery(rangeQuery(AT_TIMESTAMP).lt(highValue), AT_TIMESTAMP);
        var should = Queries.combine(SHOULD, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, should));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithMultiConjunction() {
        var lowValue = 10;
        var highValue = 100;
        var eqValue = 1234;
        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} AND {} == {} AND {} < {}
            """, AT_TIMESTAMP, lowValue, OTHER_FIELD, eqValue, AT_TIMESTAMP, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var left = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(lowValue), AT_TIMESTAMP);
        var right = singleValueQuery(rangeQuery(AT_TIMESTAMP).lt(highValue), AT_TIMESTAMP);
        var must = Queries.combine(MUST, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, must));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryMultipleFilters() {
        var lowValue = 10;
        var eqValue = 1234;
        var highValue = 100;

        var restFilter = restFilterQuery(AT_TIMESTAMP);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {}
            |EVAL {} = {}
            |WHERE {} > {}
            """, AT_TIMESTAMP, lowValue, AT_TIMESTAMP, eqValue, AT_TIMESTAMP, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var queryFilter = singleValueQuery(rangeQuery(AT_TIMESTAMP).gt(lowValue), AT_TIMESTAMP);
        var expected = Queries.combine(FILTER, asList(restFilter, queryFilter));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampOverriddenFilterFilter() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |EVAL {} = {}
            |WHERE {} > {}
            """, AT_TIMESTAMP, OTHER_FIELD, AT_TIMESTAMP, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    public void testTimestampAsFunctionArgument() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE to_int(to_string({})) == {}
            """, AT_TIMESTAMP, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    public void testTimestampAsFunctionArgumentInsideExpression() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE to_int(to_string({})) + 987 == {}
            """, AT_TIMESTAMP, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    /**
     * Ugly hack to create a QueryBuilder for SingleValueQuery.
     * For some reason however the queryName is set to null on range queries when deserializing.
     */
    public static QueryBuilder singleValueQuery(QueryBuilder inner, String field) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // emulate SingleValueQuery writeTo
            out.writeFloat(AbstractQueryBuilder.DEFAULT_BOOST);
            out.writeOptionalString(null);
            out.writeNamedWriteable(inner);
            out.writeString(field);

            StreamInput in = new NamedWriteableAwareStreamInput(
                ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())),
                SerializationTestUtils.writableRegistry()
            );

            Object obj = SingleValueQuery.ENTRY.reader.read(in);
            return (QueryBuilder) obj;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PhysicalPlan plan(String query, QueryBuilder restFilter) {
        var logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        // System.out.println("physical\n" + physical);
        physical = physical.transformUp(
            FragmentExec.class,
            f -> new FragmentExec(f.source(), f.fragment(), restFilter, f.estimatedRowSize())
        );
        physical = physicalPlanOptimizer.optimize(physical);
        // System.out.println("optimized\n" + physical);
        assertSerialization(physical);
        return physical;
    }

    private QueryBuilder restFilterQuery(String field) {
        return rangeQuery(field).lt("2020-12-34");
    }

    private QueryBuilder filterQueryForTransportNodes(PhysicalPlan plan) {
        return PlannerUtils.detectFilter(plan, AT_TIMESTAMP);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
