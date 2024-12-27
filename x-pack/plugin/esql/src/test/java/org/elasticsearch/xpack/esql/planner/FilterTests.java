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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.FILTER;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.MUST;
import static org.elasticsearch.xpack.esql.core.util.Queries.Clause.SHOULD;
import static org.hamcrest.Matchers.nullValue;

public class FilterTests extends ESTestCase {

    // use a field that already exists in the mapping
    private static final String EMP_NO = "emp_no";
    private static final String OTHER_FIELD = "salary";

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static PhysicalPlanOptimizer physicalPlanOptimizer;
    private static Mapper mapper;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        mapper = new Mapper();

        analyzer = new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, EsqlTestUtils.emptyPolicyResolution()),
            TEST_VERIFIER
        );
    }

    public void testTimestampRequestFilterNoQueryFilter() {
        var restFilter = restFilterQuery(EMP_NO);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > 10
            """, OTHER_FIELD), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        assertEquals(restFilter.toString(), filter.toString());
    }

    public void testTimestampNoRequestFilterQueryFilter() {
        var value = 10;

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {}
            """, EMP_NO, value);
        var plan = plan(query, null);

        var filter = filterQueryForTransportNodes(plan);
        var expected = singleValueQuery(query, rangeQuery(EMP_NO).gt(value), EMP_NO, ((SingleValueQuery.Builder) filter).source());
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilter() {
        var value = 10;
        var restFilter = restFilterQuery(EMP_NO);

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > 10
            """, EMP_NO, value);
        var plan = plan(query, restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var builder = ((BoolQueryBuilder) filter).filter().get(1);
        var queryFilter = singleValueQuery(
            query,
            rangeQuery(EMP_NO).gt(value).includeUpper(false),
            EMP_NO,
            ((SingleValueQuery.Builder) builder).source()
        );
        var expected = Queries.combine(FILTER, asList(restFilter, queryFilter));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithConjunction() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(EMP_NO);

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} AND {} < {}
            """, EMP_NO, lowValue, EMP_NO, highValue);
        var plan = plan(query, restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var musts = ((BoolQueryBuilder) ((BoolQueryBuilder) filter).filter().get(1)).must();
        var left = singleValueQuery(query, rangeQuery(EMP_NO).gt(lowValue), EMP_NO, ((SingleValueQuery.Builder) musts.get(0)).source());
        var right = singleValueQuery(query, rangeQuery(EMP_NO).lt(highValue), EMP_NO, ((SingleValueQuery.Builder) musts.get(1)).source());
        var must = Queries.combine(MUST, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, must));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithDisjunctionOnDifferentFields() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(EMP_NO);

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} OR {} < {}
            """, OTHER_FIELD, lowValue, EMP_NO, highValue), restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var expected = restFilter;
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithDisjunctionOnSameField() {
        var lowValue = 10;
        var highValue = 100;
        var restFilter = restFilterQuery(EMP_NO);

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} OR {} < {}
            """, EMP_NO, lowValue, EMP_NO, highValue);
        var plan = plan(query, restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var shoulds = ((BoolQueryBuilder) ((BoolQueryBuilder) filter).filter().get(1)).should();
        var left = singleValueQuery(query, rangeQuery(EMP_NO).gt(lowValue), EMP_NO, ((SingleValueQuery.Builder) shoulds.get(0)).source());
        var right = singleValueQuery(query, rangeQuery(EMP_NO).lt(highValue), EMP_NO, ((SingleValueQuery.Builder) shoulds.get(1)).source());
        var should = Queries.combine(SHOULD, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, should));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryFilterWithMultiConjunction() {
        var lowValue = 10;
        var highValue = 100;
        var eqValue = 1234;
        var restFilter = restFilterQuery(EMP_NO);

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {} AND {} == {} AND {} < {}
            """, EMP_NO, lowValue, OTHER_FIELD, eqValue, EMP_NO, highValue);
        var plan = plan(query, restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var musts = ((BoolQueryBuilder) ((BoolQueryBuilder) filter).filter().get(1)).must();
        var left = singleValueQuery(query, rangeQuery(EMP_NO).gt(lowValue), EMP_NO, ((SingleValueQuery.Builder) musts.get(0)).source());
        var right = singleValueQuery(query, rangeQuery(EMP_NO).lt(highValue), EMP_NO, ((SingleValueQuery.Builder) musts.get(1)).source());
        var must = Queries.combine(MUST, asList(left, right));
        var expected = Queries.combine(FILTER, asList(restFilter, must));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampRequestFilterQueryMultipleFilters() {
        var lowValue = 10;
        var eqValue = 1234;
        var highValue = 100;

        var restFilter = restFilterQuery(EMP_NO);

        String query = LoggerMessageFormat.format(null, """
             FROM test
            |WHERE {} > {}
            |EVAL {} = {}
            |WHERE {} > {}
            """, EMP_NO, lowValue, EMP_NO, eqValue, EMP_NO, highValue);
        var plan = plan(query, restFilter);

        var filter = filterQueryForTransportNodes(plan);
        var builder = ((BoolQueryBuilder) filter).filter().get(1);
        var queryFilter = singleValueQuery(query, rangeQuery(EMP_NO).gt(lowValue), EMP_NO, ((SingleValueQuery.Builder) builder).source());
        var expected = Queries.combine(FILTER, asList(restFilter, queryFilter));
        assertEquals(expected.toString(), filter.toString());
    }

    public void testTimestampOverriddenFilterFilter() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |EVAL {} = {}
            |WHERE {} > {}
            """, EMP_NO, OTHER_FIELD, EMP_NO, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    public void testTimestampAsFunctionArgument() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE to_int(to_string({})) == {}
            """, EMP_NO, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    public void testTimestampAsFunctionArgumentInsideExpression() {
        var eqValue = 1234;

        var plan = plan(LoggerMessageFormat.format(null, """
             FROM test
            |WHERE to_int(to_string({})) + 987 == {}
            """, EMP_NO, eqValue), null);

        var filter = filterQueryForTransportNodes(plan);
        assertThat(filter, nullValue());
    }

    /**
     * Ugly hack to create a QueryBuilder for SingleValueQuery.
     * For some reason however the queryName is set to null on range queries when deserializing.
     */
    public static QueryBuilder singleValueQuery(String query, QueryBuilder inner, String field, Source source) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Configuration config = randomConfiguration(query, Map.of());

            // emulate SingleValueQuery writeTo
            out.writeFloat(AbstractQueryBuilder.DEFAULT_BOOST);
            out.writeOptionalString(null);
            out.writeNamedWriteable(inner);
            out.writeString(field);
            source.writeTo(new PlanStreamOutput(out, config));

            StreamInput in = new NamedWriteableAwareStreamInput(
                ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())),
                SerializationTestUtils.writableRegistry()
            );

            Object obj = SingleValueQuery.ENTRY.reader.read(new PlanStreamInput(in, in.namedWriteableRegistry(), config));
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
        return PlannerUtils.detectFilter(plan, EMP_NO);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
