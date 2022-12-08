/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PhysicalPlanOptimizerTests extends ESTestCase {

    private static final String PARAM_FORMATTING = "%1$s";

    private EsqlParser parser;
    private Analyzer analyzer;
    private LogicalPlanOptimizer logicalOptimizer;
    private PhysicalPlanOptimizer physicalPlanOptimizer;
    private Mapper mapper;
    private Map<String, EsField> mapping;

    private final EsqlConfiguration config;

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() {
        return settings().stream().map(t -> {
            var settings = Settings.builder().loadFromMap(t.v2()).build();
            return new Object[] {
                t.v1(),
                new EsqlConfiguration(
                    DateUtils.UTC,
                    null,
                    null,
                    settings,
                    EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(settings)
                ) };
        }).toList();
    }

    private static List<Tuple<String, Map<String, Object>>> settings() {
        return asList(
            new Tuple<>("default", Map.of()),
            new Tuple<>("parallelism above the query", Map.of(PhysicalPlanOptimizer.ADD_TASK_PARALLELISM_ABOVE_QUERY.getKey(), true))
        );
    }

    public PhysicalPlanOptimizerTests(String name, EsqlConfiguration config) {
        this.config = config;
    }

    @Before
    public void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer();
        physicalPlanOptimizer = new PhysicalPlanOptimizer(config);
        mapper = new Mapper();

        analyzer = new Analyzer(getIndexResult, new EsqlFunctionRegistry(), new Verifier(), config);
    }

    public void testSingleFieldExtractor() {
        // using a function (round()) here and following tests to prevent the optimizer from pushing the
        // filter down to the source and thus change the shape of the expected physical tree.
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(restExtract.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var eval = as(restExtract.child(), EvalExec.class);
        var limit = as(eval.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);

        assertEquals(
            Sets.difference(mapping.keySet(), Set.of("emp_no")),
            Sets.newHashSet(Expressions.names(restExtract.attributesToExtract()))
        );
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = source(extract.child());
    }

    public void testDoubleExtractorPerFieldEvenWithAliasNoPruningDueToImplicitProjection() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = languages
            | stats x = avg(c)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        var eval = as(aggregate.child(), EvalExec.class);

        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("languages"));

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = source(extract.child());
    }

    public void testTripleExtractorPerField() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));

        var eval = as(extract.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("first_name"));

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
        var source = source(extract.child());
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

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregateFinal = as(limit.child(), AggregateExec.class);
        var aggregatePartial = as(aggregateFinal.child(), AggregateExec.class);

        var eval = as(aggregatePartial.child(), EvalExec.class);
        var filter = as(eval.child(), FilterExec.class);

        var topNFinal = as(filter.child(), TopNExec.class);
        var exchange = as(topNFinal.child(), ExchangeExec.class);
        var topNPartial = as(exchange.child(), TopNExec.class);

        var project = as(topNPartial.child(), ProjectExec.class);
        assertThat(Expressions.names(project.projections()), contains("languages", "salary", "first_name", "emp_no"));
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("languages", "salary", "first_name", "emp_no"));
    }

    public void testExtractorMultiEvalWithDifferentNames() {
        var plan = physicalPlan("""
            from test
            | eval e = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
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

    public void testExtractorMultiEvalWithSameName() {
        var plan = physicalPlan("""
            from test
            | eval emp_no = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
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

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var node = as(limit.child(), AggregateExec.class);
        var exchange = as(node.child(), ExchangeExec.class);
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testDoNotExtractGroupingFields() {
        var plan = physicalPlan("""
            from test
            | stats x = avg(salary) by gender
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), equalTo(List.of("salary")));

        var source = source(extract.child());
        assertNotNull(source);
    }

    public void testQueryWithAggregation() {
        var plan = physicalPlan("""
            from test
            | stats avg(emp_no)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var node = as(limit.child(), AggregateExec.class);
        var exchange = as(node.child(), ExchangeExec.class);
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithAggAndEval() {
        var plan = physicalPlan("""
            from test
            | stats avg_emp = avg(emp_no)
            | eval x = avg_emp + 7
            """);

        var optimized = optimizedPlan(plan);
        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var agg = as(topLimit.child(), AggregateExec.class);
        var exchange = as(agg.child(), ExchangeExec.class);
        var aggregate = as(exchange.child(), AggregateExec.class);
        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithNull() {
        var plan = physicalPlan("""
            from test
            | eval nullsum = emp_no + null
            | sort emp_no
            | limit 1
            """);

        var optimized = optimizedPlan(plan);
    }

    public void testPushAndInequalitiesFilter() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0
            | where languages < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(fieldExtract.child(), LimitExec.class);
        var source = source(limit.child());

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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

        assertThat(Expressions.names(filter.condition().collect(x -> x instanceof FieldAttribute)), contains("emp_no", "languages"));
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no", "languages"));
        assertNull(source.query());
    }

    public void testNoPushDownNonFieldAttributeInComparisonFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 0
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(fieldExtract.child(), LimitExec.class);
        var source = source(limit.child());

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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(fieldExtract.child(), LimitExec.class);
        var source = source(limit.child());

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

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(fieldExtract.child(), LimitExec.class);
        var source = source(limit.child());

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

    public void testLimit() {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 10
            """));

        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(fieldExtract.child(), LimitExec.class);
        assertThat(limit.limit().fold(), is(10));
        source(limit.child());
    }

    public void testEvalNull() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval nullsum = emp_no + null
            | project *
            | sort nullsum
            | limit 1
            """));
        var topN = as(optimized, TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var topNLocal = as(exchange.child(), TopNExec.class);
        var project = as(topNLocal.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
    }

    private static EsQueryExec source(PhysicalPlan plan) {
        if (plan instanceof ExchangeExec exchange) {
            assertThat(exchange.getPartitioning(), is(ExchangeExec.Partitioning.FIXED_ARBITRARY_DISTRIBUTION));
            assertThat(exchange.getType(), is(ExchangeExec.Type.REPARTITION));

            plan = exchange.child();
        }
        return as(plan, EsQueryExec.class);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan) {
        var p = physicalPlanOptimizer.optimize(plan);
        // System.out.println(p);
        return p;
    }

    private PhysicalPlan physicalPlan(String query) {
        return mapper.map(logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query))));
    }

}
