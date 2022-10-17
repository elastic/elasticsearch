/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analyzer.Analyzer;
import org.elasticsearch.xpack.esql.analyzer.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.junit.BeforeClass;

import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class PhysicalPlanOptimizerTests extends ESTestCase {

    private static EsqlParser parser;
    private static Analyzer analyzer;
    private static LogicalPlanOptimizer logicalOptimizer;
    private static PhysicalPlanOptimizer physicalPlanOptimizer;
    private static Mapper mapper;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();

        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer();
        physicalPlanOptimizer = new PhysicalPlanOptimizer(EsqlTestUtils.TEST_CFG);
        mapper = new Mapper();

        analyzer = new Analyzer(getIndexResult, new EsqlFunctionRegistry(), new Verifier(), EsqlTestUtils.TEST_CFG);
    }

    public void testSingleFieldExtractor() throws Exception {
        var plan = physicalPlan("""
            from test
            | where emp_no > 10
            """);

        var optimized = fieldExtractorRule(plan);
        var node = as(optimized, UnaryExec.class);
        var filter = as(node.child(), FilterExec.class);

        var extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testExactlyOneExtractorPerField() throws Exception {
        var plan = physicalPlan("""
            from test
            | where emp_no > 10
            | eval c = emp_no
            """);

        var optimized = fieldExtractorRule(plan);
        var exchange = as(optimized, ExchangeExec.class);
        var eval = as(exchange.child(), EvalExec.class);
        var filter = as(eval.child(), FilterExec.class);

        var extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = as(extract.child(), EsQueryExec.class);
    }

    public void testDoubleExtractorPerFieldEvenWithAlias() throws Exception {
        var plan = physicalPlan("""
            from test
            | limit 10
            | where emp_no > 10
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

    public void testTripleExtractorPerField() throws Exception {
        var plan = physicalPlan("""
            from test
            | limit 10
            | where emp_no > 10
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-internal/issues/296")
    public void testExtractorForField() throws Exception {
        var plan = physicalPlan("""
            from test
            | sort languages
            | limit 10
            | where emp_no > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);

        var optimized = fieldExtractorRule(plan);
        var aggregate = as(optimized, AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));

        var eval = as(extract.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("first_name"));

        var limit = as(extract.child(), LimitExec.class);
        var order = as(limit.child(), OrderExec.class);

        extract = as(order.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("languages"));

        var filter = as(extract.child(), FilterExec.class);

        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithAggregation() throws Exception {
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

    private static <T extends PhysicalPlan> T as(PhysicalPlan plan, Class<T> type) {
        assertThat(plan, instanceOf(type));
        return type.cast(plan);
    }

    private static PhysicalPlan fieldExtractorRule(PhysicalPlan plan) {
        return physicalPlanOptimizer.optimize(plan);

    }

    private PhysicalPlan physicalPlan(String query) {
        return mapper.map(logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query))));
    }

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(DefaultDataTypeRegistry.INSTANCE, name, null);
    }
}
