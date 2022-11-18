/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
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
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.contains;

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
        var plan = physicalPlan("""
            from test
            | where emp_no > 10
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
            | where emp_no > 10
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

    public void testTripleExtractorPerField() {
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

    public void testExtractorForField() {
        var plan = physicalPlan("""
            from test
            | sort languages
            | limit 10
            | where emp_no > 10
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

    private static PhysicalPlan fieldExtractorRule(PhysicalPlan plan) {
        return physicalPlanOptimizer.optimize(plan);
    }

    private PhysicalPlan physicalPlan(String query) {
        return mapper.map(logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query))));
    }

}
