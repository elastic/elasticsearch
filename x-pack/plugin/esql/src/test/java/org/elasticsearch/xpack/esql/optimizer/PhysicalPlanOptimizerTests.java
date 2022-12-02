/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class PhysicalPlanOptimizerTests extends ESTestCase {

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static PhysicalPlanOptimizer physicalPlanOptimizer;
    private static Mapper mapper;
    private static Map<String, EsField> mapping;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer();
        physicalPlanOptimizer = new PhysicalPlanOptimizer(TEST_CFG);
        mapper = new Mapper();

        analyzer = new Analyzer(getIndexResult, new EsqlFunctionRegistry(), new Verifier(), TEST_CFG);
    }

    public void testSingleFieldExtractor() {
        // using a function (round()) here and following tests to prevent the optimizer from pushing the
        // filter down to the source and thus change the shape of the expected physical tree.
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            """);

        var optimized = fieldExtractorRule(plan);
        var node = as(optimized, UnaryExec.class);
        var project = as(node.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var filter = as(restExtract.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);

        assertEquals(
            Sets.difference(mapping.keySet(), Set.of("emp_no")),
            Sets.newHashSet(Expressions.names(restExtract.attributesToExtract()))
        );
        assertEquals(Set.of("emp_no"), Sets.newHashSet(Expressions.names(extract.attributesToExtract())));
    }

    public void testExactlyOneExtractorPerFieldWithPruning() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = emp_no
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var eval = as(restExtract.child(), EvalExec.class);
        var filter = as(eval.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);

        assertEquals(
            Sets.difference(mapping.keySet(), Set.of("emp_no")),
            Sets.newHashSet(Expressions.names(restExtract.attributesToExtract()))
        );
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = as(extract.child(), EsQueryExec.class);
    }

    public void testDoubleExtractorPerFieldEvenWithAliasNoPruningDueToImplicitProjection() {
        var plan = physicalPlan("""
            from test
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(c)
            """);

        var optimized = fieldExtractorRule(plan);
        var aggregate = as(optimized, AggregateExec.class);
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        var eval = as(aggregate.child(), EvalExec.class);

        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("first_name"));

        var limit = as(extract.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);

        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = as(extract.child(), EsQueryExec.class);
    }

    public void testTripleExtractorPerField() {
        var plan = physicalPlan("""
            from test
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);

        var optimized = fieldExtractorRule(plan);
        var aggregate = as(optimized, AggregateExec.class);
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));

        var eval = as(extract.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("first_name"));

        var limit = as(extract.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);

        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = as(extract.child(), EsQueryExec.class);
    }

    public void testExtractorForField() {
        var plan = physicalPlan("""
            from test
            | sort languages
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);

        var optimized = fieldExtractorRule(plan);
        var aggregateFinal = as(optimized, AggregateExec.class);
        var aggregatePartial = as(aggregateFinal.child(), AggregateExec.class);

        var extract = as(aggregatePartial.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));

        var eval = as(extract.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("first_name"));

        var topNFinal = as(extract.child(), TopNExec.class);
        var exchange = as(topNFinal.child(), ExchangeExec.class);
        var topNPartial = as(exchange.child(), TopNExec.class);

        extract = as(topNPartial.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("languages"));

        var filter = as(extract.child(), FilterExec.class);

        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testExtractorMultiEvalWithDifferentNames() {
        var plan = physicalPlan("""
            from test
            | eval e = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            Expressions.names(extract.attributesToExtract()),
            contains("first_name", "gender", "languages", "last_name", "salary", "_meta_field")
        );

        var eval = as(extract.child(), EvalExec.class);
        eval = as(eval.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-internal/issues/403")
    public void testExtractorMultiEvalWithSameName() {
        var plan = physicalPlan("""
            from test
            | eval emp_no = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            Expressions.names(extract.attributesToExtract()),
            contains("first_name", "gender", "languages", "last_name", "salary", "_meta_field")
        );

        var eval = as(extract.child(), EvalExec.class);
        eval = as(eval.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testExtractorsOverridingFields() {
        var plan = physicalPlan("""
            from test
            | stats emp_no = avg(emp_no)
            """);

        var optimized = fieldExtractorRule(plan);
        var node = as(optimized, AggregateExec.class);
        var exchange = as(node.child(), ExchangeExec.class);
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithAggregation() {
        var plan = physicalPlan("""
            from test
            | stats avg(emp_no)
            """);

        var optimized = fieldExtractorRule(plan);
        var node = as(optimized, AggregateExec.class);
        var exchange = as(node.child(), ExchangeExec.class);
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testPushAndInequalitiesFilter() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0
            | where languages < 10
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = as(fieldExtract.child(), EsQueryExec.class);

        QueryBuilder query = source.query();
        assertTrue(query instanceof BoolQueryBuilder);
        List<QueryBuilder> mustClauses = ((BoolQueryBuilder) query).must();
        assertEquals(2, mustClauses.size());
        assertTrue(mustClauses.get(0) instanceof RangeQueryBuilder);
        assertThat(mustClauses.get(0).toString(), containsString("""
                "emp_no" : {
                  "gt" : -1,
            """));
        assertTrue(mustClauses.get(1) instanceof RangeQueryBuilder);
        assertThat(mustClauses.get(1).toString(), containsString("""
                "languages" : {
                  "lt" : 10,
            """));
    }

    public void testOnlyPushTranslatableConditionsInFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) + 1 > 0
            | where languages < 10
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var filter = as(extractRest.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);

        assertTrue(filter.condition() instanceof GreaterThan);
        assertTrue(((GreaterThan) filter.condition()).left() instanceof Round);

        QueryBuilder query = source.query();
        assertTrue(query instanceof RangeQueryBuilder);
        assertEquals(10, ((RangeQueryBuilder) query).to());
    }

    public void testNoPushDownNonFoldableInComparisonFilter() {
        var plan = physicalPlan("""
            from test
            | where emp_no > languages
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var filter = as(extractRest.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);

        assertThat(Expressions.names(filter.condition().collect(x -> x instanceof FieldAttribute)), contains("emp_no", "languages"));
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no", "languages"));
        assertNull(source.query());
    }

    public void testNoPushDownNonFieldAttributeInComparisonFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 0
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var filter = as(extractRest.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);

        assertTrue(filter.condition() instanceof BinaryComparison);
        assertTrue(((BinaryComparison) filter.condition()).left() instanceof Round);
        assertNull(source.query());
    }

    public void testCombineUserAndPhysicalFilters() {
        var plan = physicalPlan("""
            from test
            | where languages < 10
            """);
        var userFilter = new RangeQueryBuilder("emp_no").gt(-1);
        plan = plan.transformUp(EsQueryExec.class, node -> new EsQueryExec(node.source(), node.index(), userFilter));

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = as(fieldExtract.child(), EsQueryExec.class);

        QueryBuilder query = source.query();
        assertTrue(query instanceof BoolQueryBuilder);
        List<QueryBuilder> mustClauses = ((BoolQueryBuilder) query).must();
        assertEquals(2, mustClauses.size());
        assertTrue(mustClauses.get(0) instanceof RangeQueryBuilder);
        assertThat(mustClauses.get(0).toString(), containsString("""
                "emp_no" : {
                  "gt" : -1,
            """));
        assertTrue(mustClauses.get(1) instanceof RangeQueryBuilder);
        assertThat(mustClauses.get(1).toString(), containsString("""
                "languages" : {
                  "lt" : 10,
            """));
    }

    public void testPushBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or languages < 10
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = as(fieldExtract.child(), EsQueryExec.class);

        QueryBuilder query = source.query();
        assertTrue(query instanceof BoolQueryBuilder);
        List<QueryBuilder> shouldClauses = ((BoolQueryBuilder) query).should();
        assertEquals(2, shouldClauses.size());
        assertTrue(shouldClauses.get(0) instanceof RangeQueryBuilder);
        assertThat(shouldClauses.get(0).toString(), containsString("""
                "emp_no" : {
                  "gt" : -1,
            """));
        assertTrue(shouldClauses.get(1) instanceof RangeQueryBuilder);
        assertThat(shouldClauses.get(1).toString(), containsString("""
                "languages" : {
                  "lt" : 10,
            """));
    }

    public void testPushMultipleBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or languages < 10
            | where salary <= 10000 or salary >= 50000
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = as(fieldExtract.child(), EsQueryExec.class);

        QueryBuilder query = source.query();
        assertTrue(query instanceof BoolQueryBuilder);
        List<QueryBuilder> mustClauses = ((BoolQueryBuilder) query).must();
        assertEquals(2, mustClauses.size());

        assertTrue(mustClauses.get(0) instanceof BoolQueryBuilder);
        assertThat(mustClauses.get(0).toString(), containsString("""
            "emp_no" : {
                        "gt" : -1"""));
        assertThat(mustClauses.get(0).toString(), containsString("""
            "languages" : {
                        "lt" : 10"""));

        assertTrue(mustClauses.get(1) instanceof BoolQueryBuilder);
        assertThat(mustClauses.get(1).toString(), containsString("""
            "salary" : {
                        "lte" : 10000"""));
        assertThat(mustClauses.get(1).toString(), containsString("""
            "salary" : {
                        "gte" : 50000"""));
    }

    private static PhysicalPlan fieldExtractorRule(PhysicalPlan plan) {
        return physicalPlanOptimizer.optimize(plan);
    }

    private PhysicalPlan physicalPlan(String query) {
        return mapper.map(logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query))));
    }

}
