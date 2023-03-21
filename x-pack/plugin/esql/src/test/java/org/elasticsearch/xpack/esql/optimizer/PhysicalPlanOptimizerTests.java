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
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
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
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
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
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

//@TestLogging(value = "org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer:TRACE", reason = "debug")
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
        return asList(new Tuple<>("default", Map.of()));
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
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        mapper = new Mapper(functionRegistry);

        analyzer = new Analyzer(new AnalyzerContext(config, functionRegistry, getIndexResult), new Verifier());
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
        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
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
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no"));

        var source = source(extract.child());
    }

    public void testDoubleExtractorPerFieldEvenWithAliasNoPruningDueToImplicitProjection() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = salary
            | stats x = avg(c)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        var eval = as(aggregate.child(), EvalExec.class);

        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));

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

    /**
     * Expected
     * LimitExec[10000[INTEGER]]
     * \_AggregateExec[[],[AVG(salary{f}#38) AS x],FINAL]
     *   \_AggregateExec[[],[AVG(salary{f}#38) AS x],PARTIAL]
     *     \_EvalExec[[first_name{f}#35 AS c]]
     *       \_FilterExec[ROUND(emp_no{f}#34) > 10[INTEGER]]
     *         \_TopNExec[[Order[last_name{f}#37,ASC,LAST]],10[INTEGER]]
     *           \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *             \_ProjectExec[[salary{f}#38, first_name{f}#35, emp_no{f}#34, last_name{f}#37]]     -- project away _doc
     *               \_FieldExtractExec[salary{f}#38, first_name{f}#35, emp_no{f}#34]                 -- local field extraction
     *                 \_TopNExec[[Order[last_name{f}#37,ASC,LAST]],10[INTEGER]]
     *                   \_FieldExtractExec[last_name{f}#37]
     *                     \_EsQueryExec[test], query[][_doc{f}#39], limit[]
     */
    public void testExtractorForField() {
        var plan = physicalPlan("""
            from test
            | sort last_name
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
        var topN = as(filter.child(), TopNExec.class);

        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary", "first_name", "emp_no"));
        var topNLocal = as(extract.child(), TopNExec.class);
        extract = as(topNLocal.child(), FieldExtractExec.class);

        assertThat(Expressions.names(extract.attributesToExtract()), contains("last_name"));
    }

    /**
     * Expected
     *
     * EvalExec[[emp_no{f}#538 + 1[INTEGER] AS emp_no]]
     * \_EvalExec[[emp_no{f}#538 + 1[INTEGER] AS e]]
     *   \_LimitExec[10000[INTEGER]]
     *     \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *       \_ProjectExec[[_meta_field{f}#537, emp_no{f}#538, first_name{f}#539, languages{f}#540, last_name{f}#541, salary{f}#542]]
     *         \_FieldExtractExec[_meta_field{f}#537, emp_no{f}#538, first_name{f}#53..]
     *           \_EsQueryExec[test], query[][_doc{f}#543], limit[10000]
     */
    public void testExtractorMultiEvalWithDifferentNames() {
        var plan = physicalPlan("""
            from test
            | eval e = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);
        var eval = as(optimized, EvalExec.class);
        eval = as(eval.child(), EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            Expressions.names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary")
        );
    }

    /**
     * Expected
     * EvalExec[[emp_no{r}#120 + 1[INTEGER] AS emp_no]]
     * \_EvalExec[[emp_no{f}#125 + 1[INTEGER] AS emp_no]]
     *   \_LimitExec[10000[INTEGER]]
     *     \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *       \_ProjectExec[[_meta_field{f}#124, emp_no{f}#125, first_name{f}#126, languages{f}#127, last_name{f}#128, salary{f}#129]]
     *         \_FieldExtractExec[_meta_field{f}#124, emp_no{f}#125, first_name{f}#12..]
     *           \_EsQueryExec[test], query[][_doc{f}#130], limit[10000]
     */
    public void testExtractorMultiEvalWithSameName() {
        var plan = physicalPlan("""
            from test
            | eval emp_no = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);
        var eval = as(optimized, EvalExec.class);
        eval = as(eval.child(), EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            Expressions.names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary")
        );
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
            | stats x = avg(salary) by first_name
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

    public void testExtractGroupingFieldsIfAggd() {
        var plan = physicalPlan("""
            from test
            | stats x = count(first_name) by first_name
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), equalTo(List.of("first_name")));

        var source = source(extract.child());
        assertNotNull(source);
    }

    public void testExtractGroupingFieldsIfAggdWithEval() {
        var plan = physicalPlan("""
            from test
            | eval g = first_name
            | stats x = count(first_name) by first_name
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        var exchange = as(aggregate.child(), ExchangeExec.class);
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var eval = as(aggregate.child(), EvalExec.class);
        assertThat(Expressions.names(eval.fields()), equalTo(List.of("g")));
        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), equalTo(List.of("first_name")));

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
            | where salary < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

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
                "salary" : {
                  "lt" : 10,
            """));
    }

    public void testOnlyPushTranslatableConditionsInFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) + 1 > 0
            | where salary < 10
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
            | where emp_no > salary
            """);

        assertThat("Expected to find an EsSourceExec found", plan.anyMatch(EsSourceExec.class::isInstance), is(true));

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

        assertThat(Expressions.names(filter.condition().collect(FieldAttribute.class::isInstance)), contains("emp_no", "salary"));
        assertThat(Expressions.names(extract.attributesToExtract()), contains("emp_no", "salary"));
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

    /**
     * Expected
     *
     * ProjectExec[[_meta_field{f}#417, emp_no{f}#418, first_name{f}#419, languages{f}#420, last_name{f}#421, salary{f}#422]]
     * \_LimitExec[10000[INTEGER]]
     *   \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *     \_ProjectExec[[_meta_field{f}#417, emp_no{f}#418, first_name{f}#419, languages{f}#420, last_name{f}#421, salary{f}#422]]
     *       \_FieldExtractExec[_meta_field{f}#417, emp_no{f}#418, first_name{f}#41..]
     *         \_EsQueryExec[test], query[{...}][_doc{f}#423], limit[10000]
     */
    public void testCombineUserAndPhysicalFilters() {
        var plan = physicalPlan("""
            from test
            | where salary < 10
            """);
        var userFilter = new RangeQueryBuilder("emp_no").gt(-1);
        plan = plan.transformUp(EsSourceExec.class, node -> new EsSourceExec(node.source(), node.index(), node.output(), userFilter));

        var optimized = optimizedPlan(plan);

        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

        var query = as(source.query(), BoolQueryBuilder.class);
        List<QueryBuilder> mustClauses = query.must();
        assertEquals(2, mustClauses.size());
        var mustClause = as(mustClauses.get(0), RangeQueryBuilder.class);
        assertThat(mustClause.toString(), containsString("""
                "emp_no" : {
                  "gt" : -1,
            """));
        mustClause = as(mustClauses.get(1), RangeQueryBuilder.class);
        assertThat(mustClause.toString(), containsString("""
                "salary" : {
                  "lt" : 10,
            """));
    }

    public void testPushBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or salary < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

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
                "salary" : {
                  "lt" : 10,
            """));
    }

    public void testPushMultipleBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or salary < 10
            | where salary <= 10000 or salary >= 50000
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

        QueryBuilder query = source.query();
        assertTrue(query instanceof BoolQueryBuilder);
        List<QueryBuilder> mustClauses = ((BoolQueryBuilder) query).must();
        assertEquals(2, mustClauses.size());

        assertTrue(mustClauses.get(0) instanceof BoolQueryBuilder);
        assertThat(mustClauses.get(0).toString(), containsString("""
            "emp_no" : {
                        "gt" : -1"""));
        assertThat(mustClauses.get(0).toString(), containsString("""
            "salary" : {
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
        var source = source(fieldExtract.child());
        assertThat(source.limit().fold(), is(10));
    }

    /**
     * ProjectExec[[_meta_field{f}#5, emp_no{f}#6, first_name{f}#7, languages{f}#8, last_name{f}#9, salary{f}#10, nullsum{r}#3]]
     * \_TopNExec[[Order[nullsum{r}#3,ASC,LAST]],1[INTEGER]]
     *   \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *     \_ProjectExec[[nullsum{r}#3, _meta_field{f}#5, emp_no{f}#6, first_name{f}#7, languages{f}#8, last_name{f}#9, salary{f}#10]]
     *       \_FieldExtractExec[_meta_field{f}#5, emp_no{f}#6, first_name{f}#7, lan..]
     *         \_TopNExec[[Order[nullsum{r}#3,ASC,LAST]],1[INTEGER]]
     *           \_EvalExec[[null[INTEGER] AS nullsum]]
     *             \_EsQueryExec[test], query[][_doc{f}#11], limit[]
     */
    public void testExtractorForEvalWithoutProject() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval nullsum = emp_no + null
            | sort nullsum
            | limit 1
            """));
        // var topProject = as(optimized, ProjectExec.class);
        var topN = as(optimized, TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topNLocal = as(extract.child(), TopNExec.class);
        var eval = as(topNLocal.child(), EvalExec.class);
    }

    public void testProjectAfterTopN() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | sort emp_no
            | project first_name
            | limit 2
            """));
        var topProject = as(optimized, ProjectExec.class);
        assertEquals(1, topProject.projections().size());
        assertEquals("first_name", topProject.projections().get(0).name());
        var topN = as(topProject.child(), TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        List<String> projectionNames = project.projections().stream().map(NamedExpression::name).collect(Collectors.toList());
        assertTrue(projectionNames.containsAll(List.of("first_name", "emp_no")));
        var extract = as(project.child(), FieldExtractExec.class);
        var topNLocal = as(extract.child(), TopNExec.class);
        var fieldExtract = as(topNLocal.child(), FieldExtractExec.class);
    }

    /**
     * Expected
     *
     * EvalExec[[emp_no{f}#248 * 10[INTEGER] AS emp_no_10]]
     * \_LimitExec[10[INTEGER]]
     *   \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *     \_ProjectExec[[_meta_field{f}#247, emp_no{f}#248, first_name{f}#249, languages{f}#250, last_name{f}#251, salary{f}#252]]
     *       \_FieldExtractExec[_meta_field{f}#247, emp_no{f}#248, first_name{f}#24..]
     *         \_EsQueryExec[test], query[][_doc{f}#253], limit[10]
     */
    public void testPushLimitToSource() {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval emp_no_10 = emp_no * 10
            | limit 10
            """));

        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var leaves = extract.collectLeaves();
        assertEquals(1, leaves.size());
        var source = as(leaves.get(0), EsQueryExec.class);
        assertThat(source.limit().fold(), is(10));
    }

    /**
     * Expected
     * EvalExec[[emp_no{f}#357 * 10[INTEGER] AS emp_no_10]]
     * \_LimitExec[10[INTEGER]]
     *   \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *     \_ProjectExec[[_meta_field{f}#356, emp_no{f}#357, first_name{f}#358, languages{f}#359, last_name{f}#360, salary{f}#361]]
     *       \_FieldExtractExec[_meta_field{f}#356, emp_no{f}#357, first_name{f}#35..]
     *         \_EsQueryExec[test], query[{"range":{"emp_no":{"gt":0,"boost":1.0}}}][_doc{f}#362], limit[10]
     */
    public void testPushLimitAndFilterToSource() {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval emp_no_10 = emp_no * 10
            | where emp_no > 0
            | limit 10
            """));

        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);

        assertThat(
            Expressions.names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary")
        );

        var source = source(extract.child());
        assertThat(source.limit().fold(), is(10));
        assertTrue(source.query() instanceof RangeQueryBuilder);
        assertThat(source.query().toString(), containsString("""
              "range" : {
                "emp_no" : {
                  "gt" : 0,
            """));
    }

    /**
     * Expected
     * TopNExec[[Order[emp_no{f}#422,ASC,LAST]],1[INTEGER]]
     * \_LimitExec[1[INTEGER]]
     *   \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *     \_ProjectExec[[_meta_field{f}#421, emp_no{f}#422, first_name{f}#423, languages{f}#424, last_name{f}#425, salary{f}#426]]
     *       \_FieldExtractExec[_meta_field{f}#421, emp_no{f}#422, first_name{f}#42..]
     *         \_EsQueryExec[test], query[][_doc{f}#427], limit[1]
     */
    public void testQueryWithLimitSort() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 1
            | sort emp_no
            """));

        var topN = as(optimized, TopNExec.class);
        var limit = as(topN.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
    }

    /**
     * Expected
     *
     * ProjectExec[[emp_no{f}#7, x{r}#4]]
     * \_TopNExec[[Order[emp_no{f}#7,ASC,LAST]],5[INTEGER]]
     *   \_ExchangeExec[]
     *     \_ProjectExec[[emp_no{f}#7, x{r}#4]]
     *       \_TopNExec[[Order[emp_no{f}#7,ASC,LAST]],5[INTEGER]]
     *         \_FieldExtractExec[emp_no{f}#7]
     *           \_EvalExec[[first_name{f}#8 AS x]]
     *             \_FieldExtractExec[first_name{f}#8]
     *               \_EsQueryExec[test], query[][_doc{f}#14], limit[]
     */
    public void testLocalProjectIncludeLocalAlias() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | sort emp_no
            | eval x = first_name
            | project emp_no, x
            | limit 5
            """));

        var project = as(optimized, ProjectExec.class);
        var topN = as(project.child(), TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);

        project = as(exchange.child(), ProjectExec.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "x"));
        topN = as(project.child(), TopNExec.class);
        var extract = as(topN.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
        extract = as(eval.child(), FieldExtractExec.class);
    }

    /**
     * Expected
     * ProjectExec[[languages{f}#10, salary{f}#12, x{r}#6]]
     * \_EvalExec[[languages{f}#10 + 1[INTEGER] AS x]]
     *   \_TopNExec[[Order[salary{f}#12,ASC,LAST]],1[INTEGER]]
     *     \_ExchangeExec[]
     *       \_ProjectExec[[languages{f}#10, salary{f}#12]]
     *         \_FieldExtractExec[languages{f}#10]
     *           \_TopNExec[[Order[salary{f}#12,ASC,LAST]],1[INTEGER]]
     *             \_FieldExtractExec[salary{f}#12]
     *               \_EsQueryExec[test], query[][_doc{f}#14], limit[]
     */
    public void testDoNotAliasesDefinedAfterTheExchange() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | sort salary
            | limit 1
            | project languages, salary
            | eval x = languages + 1
            """));

        var project = as(optimized, ProjectExec.class);
        var eval = as(project.child(), EvalExec.class);
        var topN = as(eval.child(), TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);

        project = as(exchange.child(), ProjectExec.class);
        assertThat(Expressions.names(project.projections()), contains("languages", "salary"));
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("languages"));

        topN = as(extract.child(), TopNExec.class);
        extract = as(topN.child(), FieldExtractExec.class);
        assertThat(Expressions.names(extract.attributesToExtract()), contains("salary"));
    }

    /**
     * Expected
     * TopNExec[[Order[emp_no{f}#299,ASC,LAST]],1[INTEGER]]
     * \_FilterExec[emp_no{f}#299 > 10[INTEGER]]
     *   \_LimitExec[1[INTEGER]]
     *     \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *       \_ProjectExec[[_meta_field{f}#298, emp_no{f}#299, first_name{f}#300, languages{f}#301, last_name{f}#302, salary{f}#303]]
     *         \_FieldExtractExec[_meta_field{f}#298, emp_no{f}#299, first_name{f}#30..]
     *           \_EsQueryExec[test], query[][_doc{f}#304], limit[1]
     */
    public void testQueryWithLimitWhereSort() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 1
            | where emp_no > 10
            | sort emp_no
            """));

        var topN = as(optimized, TopNExec.class);
        var filter = as(topN.child(), FilterExec.class);
        var limit = as(filter.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
    }

    /**
     * Expected
     * TopNExec[[Order[x{r}#462,ASC,LAST]],3[INTEGER]]
     * \_EvalExec[[emp_no{f}#465 AS x]]
     *   \_LimitExec[3[INTEGER]]
     *     \_ExchangeExec[GATHER,SINGLE_DISTRIBUTION]
     *       \_ProjectExec[[_meta_field{f}#464, emp_no{f}#465, first_name{f}#466, languages{f}#467, last_name{f}#468, salary{f}#469]]
     *         \_FieldExtractExec[_meta_field{f}#464, emp_no{f}#465, first_name{f}#46..]
     *           \_EsQueryExec[test], query[][_doc{f}#470], limit[3]
     */
    public void testQueryWithLimitWhereEvalSort() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 3
            | eval x = emp_no
            | sort x
            """));

        var topN = as(optimized, TopNExec.class);
        var eval = as(topN.child(), EvalExec.class);
        var limit = as(eval.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
    }

    public void testQueryJustWithLimit() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 3
            """));

        var limit = as(optimized, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
    }

    public void testPushDownDisjunction() {
        var plan = physicalPlan("""
            from test
            | where emp_no == 10010 or emp_no == 10011
            """);

        assertThat("Expected to find an EsSourceExec found", plan.anyMatch(EsSourceExec.class::isInstance), is(true));

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        QueryBuilder query = source.query();
        assertNotNull(query);
        List<QueryBuilder> shouldClauses = ((BoolQueryBuilder) query).should();
        assertEquals(2, shouldClauses.size());
        assertTrue(shouldClauses.get(0) instanceof TermQueryBuilder);
        assertThat(shouldClauses.get(0).toString(), containsString("""
                "emp_no" : {
                  "value" : 10010
            """));
        assertTrue(shouldClauses.get(1) instanceof TermQueryBuilder);
        assertThat(shouldClauses.get(1).toString(), containsString("""
                "emp_no" : {
                  "value" : 10011
            """));
    }

    private static EsQueryExec source(PhysicalPlan plan) {
        if (plan instanceof ExchangeExec exchange) {
            plan = exchange.child();
        }
        return as(plan, EsQueryExec.class);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan) {
        // System.out.println("Before\n" + plan);
        var p = physicalPlanOptimizer.optimize(plan);
        // System.out.println("After\n" + p);
        return p;
    }

    private PhysicalPlan physicalPlan(String query) {
        var logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        return mapper.map(logical);
    }

}
