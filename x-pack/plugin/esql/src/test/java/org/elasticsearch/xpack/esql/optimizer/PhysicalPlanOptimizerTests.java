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
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.FieldSort;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.PhysicalVerificationException;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.ql.expression.Expressions.name;
import static org.elasticsearch.xpack.ql.expression.Expressions.names;
import static org.elasticsearch.xpack.ql.expression.Order.OrderDirection.ASC;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
                    new QueryPragmas(settings),
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

        analyzer = new Analyzer(
            new AnalyzerContext(config, functionRegistry, getIndexResult, emptyPolicyResolution()),
            new Verifier(new Metrics())
        );
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
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(restExtract.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);

        assertEquals(Sets.difference(mapping.keySet(), Set.of("emp_no")), Sets.newHashSet(names(restExtract.attributesToExtract())));
        assertEquals(Set.of("emp_no"), Sets.newHashSet(names(extract.attributesToExtract())));
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
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var restExtract = as(project.child(), FieldExtractExec.class);
        var limit = as(restExtract.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);

        assertEquals(Sets.difference(mapping.keySet(), Set.of("emp_no")), Sets.newHashSet(names(restExtract.attributesToExtract())));
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var source = source(extract.child());
    }

    public void testDoubleExtractorPerFieldEvenWithAliasNoPruningDueToImplicitProjection() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = salary
            | stats x = sum(c)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        var eval = as(aggregate.child(), EvalExec.class);

        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("salary"));

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var source = source(extract.child());
    }

    public void testTripleExtractorPerField() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = sum(salary)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("salary"));

        var eval = as(extract.child(), EvalExec.class);

        extract = as(eval.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("first_name"));

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));
        var source = source(extract.child());
    }

    /**
     * Expected
     * LimitExec[10000[INTEGER]]
     * \_AggregateExec[[],[AVG(salary{f}#14) AS x],FINAL]
     *   \_AggregateExec[[],[AVG(salary{f}#14) AS x],PARTIAL]
     *     \_EvalExec[[first_name{f}#10 AS c]]
     *       \_FilterExec[ROUND(emp_no{f}#9) > 10[INTEGER]]
     *         \_TopNExec[[Order[last_name{f}#13,ASC,LAST]],10[INTEGER]]
     *           \_ExchangeExec[]
     *             \_ProjectExec[[salary{f}#14, first_name{f}#10, emp_no{f}#9, last_name{f}#13]]     -- project away _doc
     *               \_FieldExtractExec[salary{f}#14, first_name{f}#10, emp_no{f}#9, last_n..]       -- local field extraction
     *                 \_EsQueryExec[test], query[][_doc{f}#16], limit[10], sort[[last_name]]
     */
    public void testExtractorForField() {
        var plan = physicalPlan("""
            from test
            | sort last_name
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = sum(salary)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregateFinal = as(limit.child(), AggregateExec.class);
        var aggregatePartial = as(aggregateFinal.child(), AggregateExec.class);
        var eval = as(aggregatePartial.child(), EvalExec.class);
        var filter = as(eval.child(), FilterExec.class);
        var topN = as(filter.child(), TopNExec.class);

        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("salary", "first_name", "emp_no", "last_name"));
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.sorts(), is(sorts(topN.order())));

        assertThat(source.limit(), is(l(10)));
        assertThat(source.sorts().size(), is(1));
        FieldSort order = source.sorts().get(0);
        assertThat(order.direction(), is(ASC));
        assertThat(name(order.field()), is("last_name"));
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
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            names(extract.attributesToExtract()),
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
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary")
        );
    }

    public void testExtractorsOverridingFields() {
        var plan = physicalPlan("""
            from test
            | stats emp_no = sum(emp_no)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var node = as(limit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(node.child());
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testDoNotExtractGroupingFields() {
        var plan = physicalPlan("""
            from test
            | stats x = sum(salary) by first_name
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("salary")));

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
        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("first_name")));

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
        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));

        var eval = as(aggregate.child(), EvalExec.class);
        assertThat(names(eval.fields()), equalTo(List.of("g")));
        var extract = as(eval.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("first_name")));

        var source = source(extract.child());
        assertNotNull(source);
    }

    public void testQueryWithAggregation() {
        var plan = physicalPlan("""
            from test
            | stats sum(emp_no)
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var node = as(limit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(node.child());
        var aggregate = as(exchange.child(), AggregateExec.class);

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithAggAndEval() {
        var plan = physicalPlan("""
            from test
            | stats agg_emp = sum(emp_no)
            | eval x = agg_emp + 7
            """);

        var optimized = optimizedPlan(plan);
        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var agg = as(topLimit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(agg.child());
        var aggregate = as(exchange.child(), AggregateExec.class);
        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));
    }

    public void testQueryWithNull() {
        var plan = physicalPlan("""
            from test
            | eval nullsum = emp_no + null
            | sort emp_no
            | limit 1
            """);

        var optimized = optimizedPlan(plan);
        var topN = as(optimized, TopNExec.class);
        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topNLocal = as(extract.child(), TopNExec.class);
        var extractForEval = as(topNLocal.child(), FieldExtractExec.class);
        var eval = as(extractForEval.child(), EvalExec.class);
        var source = source(eval.child());
    }

    public void testPushAndInequalitiesFilter() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0
            | where salary < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

        var bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.must(), hasSize(2));
        var first = as(sv(bq.must().get(0), "emp_no"), RangeQueryBuilder.class);
        assertThat(first.fieldName(), equalTo("emp_no"));
        assertThat(first.from(), equalTo(-1));
        assertThat(first.includeLower(), equalTo(false));
        assertThat(first.to(), nullValue());
        var second = as(sv(bq.must().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(second.fieldName(), equalTo("salary"));
        assertThat(second.from(), nullValue());
        assertThat(second.to(), equalTo(10));
        assertThat(second.includeUpper(), equalTo(false));
    }

    public void testOnlyPushTranslatableConditionsInFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) + 1 > 0
            | where salary < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

        var gt = as(filter.condition(), GreaterThan.class);
        as(gt.left(), Round.class);

        var rq = as(sv(source.query(), "salary"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("salary"));
        assertThat(rq.to(), equalTo(10));
        assertThat(rq.includeLower(), equalTo(false));
        assertThat(rq.from(), nullValue());
    }

    public void testNoPushDownNonFoldableInComparisonFilter() {
        var plan = physicalPlan("""
            from test
            | where emp_no > salary
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

        assertThat(names(filter.condition().collect(FieldAttribute.class::isInstance)), contains("emp_no", "salary"));
        assertThat(names(extract.attributesToExtract()), contains("emp_no", "salary"));
        assertNull(source.query());
    }

    public void testNoPushDownNonFieldAttributeInComparisonFilter() {
        var plan = physicalPlan("""
            from test
            | where round(emp_no) > 0
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var extract = as(filter.child(), FieldExtractExec.class);
        var source = source(extract.child());

        var gt = as(filter.condition(), GreaterThan.class);
        as(gt.left(), Round.class);
        assertNull(source.query());
    }

    public void testPushBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or salary < 10
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

        BoolQueryBuilder bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.should(), hasSize(2));
        var rq = as(sv(bq.should().get(0), "emp_no"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("emp_no"));
        assertThat(rq.from(), equalTo(-1));
        assertThat(rq.includeLower(), equalTo(false));
        assertThat(rq.to(), nullValue());
        rq = as(sv(bq.should().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("salary"));
        assertThat(rq.from(), nullValue());
        assertThat(rq.to(), equalTo(10));
        assertThat(rq.includeUpper(), equalTo(false));
    }

    public void testPushMultipleBinaryLogicFilters() {
        var plan = physicalPlan("""
            from test
            | where emp_no + 1 > 0 or salary < 10
            | where salary <= 10000 or salary >= 50000
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());

        var top = as(source.query(), BoolQueryBuilder.class);
        assertThat(top.must(), hasSize(2));

        var first = as(top.must().get(0), BoolQueryBuilder.class);
        var rq = as(sv(first.should().get(0), "emp_no"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("emp_no"));
        assertThat(rq.from(), equalTo(-1));
        assertThat(rq.includeLower(), equalTo(false));
        assertThat(rq.to(), nullValue());
        rq = as(sv(first.should().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("salary"));
        assertThat(rq.from(), nullValue());
        assertThat(rq.to(), equalTo(10));
        assertThat(rq.includeUpper(), equalTo(false));

        var second = as(top.must().get(1), BoolQueryBuilder.class);
        rq = as(sv(second.should().get(0), "salary"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("salary"));
        assertThat(rq.from(), nullValue());
        assertThat(rq.to(), equalTo(10000));
        assertThat(rq.includeUpper(), equalTo(true));
        rq = as(sv(second.should().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("salary"));
        assertThat(rq.from(), equalTo(50000));
        assertThat(rq.includeLower(), equalTo(true));
        assertThat(rq.to(), nullValue());
    }

    public void testLimit() {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 10
            """));

        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());
        assertThat(source.limit().fold(), is(10));
    }

    /**
     * TopNExec[[Order[nullsum{r}#3,ASC,LAST]],1[INTEGER]]
     * \_ExchangeExec[]
     *   \_ProjectExec[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !gender, languages{f}#8, last_name{f}#9, salary{f}#10, nulls
     * um{r}#3]]
     *     \_FieldExtractExec[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     *       \_TopNExec[[Order[nullsum{r}#3,ASC,LAST]],1[INTEGER]]
     *         \_EvalExec[[null[INTEGER] AS nullsum]]
     *           \_EsQueryExec[test], query[][_doc{f}#12], limit[], sort[]
     */
    public void testExtractorForEvalWithoutProject() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval nullsum = emp_no + null
            | sort nullsum
            | limit 1
            """));
        var topN = as(optimized, TopNExec.class);
        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topNLocal = as(extract.child(), TopNExec.class);
        var eval = as(topNLocal.child(), EvalExec.class);
    }

    public void testProjectAfterTopN() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | sort emp_no
            | keep first_name
            | limit 2
            """));
        var topProject = as(optimized, ProjectExec.class);
        assertEquals(1, topProject.projections().size());
        assertEquals("first_name", topProject.projections().get(0).name());
        var topN = as(topProject.child(), TopNExec.class);
        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        List<String> projectionNames = project.projections().stream().map(NamedExpression::name).collect(Collectors.toList());
        assertTrue(projectionNames.containsAll(List.of("first_name", "emp_no")));
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.sorts(), is(sorts(topN.order())));
    }

    /**
     * Expected
     *
     * EvalExec[[emp_no{f}#248 * 10[INTEGER] AS emp_no_10]]
     * \_LimitExec[10[INTEGER]]
     *   \_ExchangeExec[]
     *     \_ProjectExec[[_meta_field{f}#247, emp_no{f}#248, first_name{f}#249, languages{f}#250, last_name{f}#251, salary{f}#252]]
     *       \_FieldExtractExec[_meta_field{f}#247, emp_no{f}#248, first_name{f}#24..]
     *         \_EsQueryExec[test], query[][_doc{f}#253], limit[10], sort[]
     */
    public void testPushLimitToSource() {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | eval emp_no_10 = emp_no * 10
            | limit 10
            """));

        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var leaves = extract.collectLeaves();
        assertEquals(1, leaves.size());
        var source = as(leaves.get(0), EsQueryExec.class);
        assertThat(source.limit().fold(), is(10));
    }

    /**
     * Expected
     * EvalExec[[emp_no{f}#5 * 10[INTEGER] AS emp_no_10]]
     * \_LimitExec[10[INTEGER]]
     *   \_ExchangeExec[]
     *     \_ProjectExec[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !gender, languages{f}#8, last_name{f}#9, salary{f}#10]]
     *       \_FieldExtractExec[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     *         \_EsQueryExec[test], query[{"range":{"emp_no":{"gt":0,"boost":1.0}}}][_doc{f}#12], limit[10], sort[]
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
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);

        assertThat(
            names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "languages", "last_name", "salary")
        );

        var source = source(extract.child());
        assertThat(source.limit().fold(), is(10));
        var rq = as(sv(source.query(), "emp_no"), RangeQueryBuilder.class);
        assertThat(rq.fieldName(), equalTo("emp_no"));
        assertThat(rq.from(), equalTo(0));
        assertThat(rq.includeLower(), equalTo(false));
        assertThat(rq.to(), nullValue());
    }

    /**
     * Expected
     * TopNExec[[Order[emp_no{f}#2,ASC,LAST]],1[INTEGER]]
     * \_LimitExec[1[INTEGER]]
     *   \_ExchangeExec[]
     *     \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !gender, languages{f}#5, last_name{f}#6, salary{f}#7]]
     *       \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !ge..]
     *         \_EsQueryExec[test], query[][_doc{f}#9], limit[1], sort[]
     */
    public void testQueryWithLimitSort() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | limit 1
            | sort emp_no
            """));

        var topN = as(optimized, TopNExec.class);
        var limit = as(topN.child(), LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
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
            | keep emp_no, x
            | limit 5
            """));

        var project = as(optimized, ProjectExec.class);
        var topN = as(project.child(), TopNExec.class);
        var exchange = asRemoteExchange(topN.child());

        project = as(exchange.child(), ProjectExec.class);
        assertThat(names(project.projections()), contains("emp_no", "x"));
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
     *           \_EsQueryExec[test], query[][_doc{f}#14], limit[1], sort[[salary]]
     */
    public void testDoNotAliasesDefinedAfterTheExchange() throws Exception {
        var optimized = optimizedPlan(physicalPlan("""
            from test
            | sort salary
            | limit 1
            | keep languages, salary
            | eval x = languages + 1
            """));

        var project = as(optimized, ProjectExec.class);
        var eval = as(project.child(), EvalExec.class);
        var topN = as(eval.child(), TopNExec.class);
        var exchange = asRemoteExchange(topN.child());

        project = as(exchange.child(), ProjectExec.class);
        assertThat(names(project.projections()), contains("languages", "salary"));
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("languages", "salary"));
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.sorts(), is(sorts(topN.order())));

        assertThat(source.limit(), is(l(1)));
        assertThat(source.sorts().size(), is(1));
        FieldSort order = source.sorts().get(0);
        assertThat(order.direction(), is(ASC));
        assertThat(name(order.field()), is("salary"));
    }

    /**
     * Expected
     * TopNExec[[Order[emp_no{f}#3,ASC,LAST]],1[INTEGER]]
     * \_FilterExec[emp_no{f}#3 > 10[INTEGER]]
     *   \_LimitExec[1[INTEGER]]
     *     \_ExchangeExec[]
     *       \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
     *         \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
     *           \_EsQueryExec[test], query[][_doc{f}#10], limit[1], sort[]
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
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.limit(), is(l(1)));
        assertNull(source.sorts());
    }

    /**
     * Expected
     * TopNExec[[Order[x{r}#3,ASC,LAST]],3[INTEGER]]
     * \_EvalExec[[emp_no{f}#5 AS x]]
     *   \_LimitExec[3[INTEGER]]
     *     \_ExchangeExec[]
     *       \_ProjectExec[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !gender, languages{f}#8, last_name{f}#9, salary{f}#10]]
     *         \_FieldExtractExec[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, !g..]
     *           \_EsQueryExec[test], query[][_doc{f}#12], limit[3], sort[]
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
        var exchange = asRemoteExchange(limit.child());
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
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
    }

    public void testPushDownDisjunction() {
        var plan = physicalPlan("""
            from test
            | where emp_no == 10010 or emp_no == 10011
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var tqb = as(sv(source.query(), "emp_no"), TermsQueryBuilder.class);
        assertThat(tqb.fieldName(), is("emp_no"));
        assertThat(tqb.values(), is(List.of(10010, 10011)));
    }

    public void testPushDownDisjunctionAndConjunction() {
        var plan = physicalPlan("""
            from test
            | where first_name == "Bezalel" or first_name == "Suzette"
            | where salary > 50000
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        BoolQueryBuilder query = as(source.query(), BoolQueryBuilder.class);
        assertThat(query.must(), hasSize(2));
        var tq = as(sv(query.must().get(0), "first_name"), TermsQueryBuilder.class);
        assertThat(tq.fieldName(), is("first_name"));
        assertThat(tq.values(), is(List.of("Bezalel", "Suzette")));
        var rqb = as(sv(query.must().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rqb.fieldName(), is("salary"));
        assertThat(rqb.from(), is(50_000));
        assertThat(rqb.includeLower(), is(false));
        assertThat(rqb.to(), nullValue());
    }

    public void testPushDownIn() {
        var plan = physicalPlan("""
            from test
            | where emp_no in (10020, 10030 + 10)
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var tqb = as(sv(source.query(), "emp_no"), TermsQueryBuilder.class);
        assertThat(tqb.fieldName(), is("emp_no"));
        assertThat(tqb.values(), is(List.of(10020, 10040)));
    }

    public void testPushDownInAndConjunction() {
        var plan = physicalPlan("""
            from test
            | where last_name in (concat("Sim", "mel"), "Pettey")
            | where salary > 60000
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        BoolQueryBuilder bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.must(), hasSize(2));
        var tqb = as(sv(bq.must().get(0), "last_name"), TermsQueryBuilder.class);
        assertThat(tqb.fieldName(), is("last_name"));
        assertThat(tqb.values(), is(List.of("Simmel", "Pettey")));
        var rqb = as(sv(bq.must().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rqb.fieldName(), is("salary"));
        assertThat(rqb.from(), is(60_000));
    }

    /* Expected:
       LimitExec[10000[INTEGER]]
       \_ExchangeExec[REMOTE_SOURCE]
         \_ExchangeExec[REMOTE_SINK]
           \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
             \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
               \_EsQueryExec[test], query[sv(not(emp_no IN (10010, 10011)))][_doc{f}#10],
                                        limit[10000], sort[]
     */
    public void testPushDownNegatedDisjunction() {
        var plan = physicalPlan("""
            from test
            | where not (emp_no == 10010 or emp_no == 10011)
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var boolQuery = as(sv(source.query(), "emp_no"), BoolQueryBuilder.class);
        assertThat(boolQuery.mustNot(), hasSize(1));
        var termsQuery = as(boolQuery.mustNot().get(0), TermsQueryBuilder.class);
        assertThat(termsQuery.fieldName(), is("emp_no"));
        assertThat(termsQuery.values(), is(List.of(10010, 10011)));
    }

    /* Expected:
       LimitExec[10000[INTEGER]]
       \_ExchangeExec[REMOTE_SOURCE]
         \_ExchangeExec[REMOTE_SINK]
           \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
             \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
               \_EsQueryExec[test], query[sv(emp_no, not(emp_no == 10010)) OR sv(not(first_name == "Parto"))], limit[10000], sort[]
     */
    public void testPushDownNegatedConjunction() {
        var plan = physicalPlan("""
            from test
            | where not (emp_no == 10010 and first_name == "Parto")
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.should(), hasSize(2));
        var empNo = as(sv(bq.should().get(0), "emp_no"), BoolQueryBuilder.class);
        assertThat(empNo.mustNot(), hasSize(1));
        var tq = as(empNo.mustNot().get(0), TermQueryBuilder.class);
        assertThat(tq.fieldName(), equalTo("emp_no"));
        assertThat(tq.value(), equalTo(10010));
        var firstName = as(sv(bq.should().get(1), "first_name"), BoolQueryBuilder.class);
        assertThat(firstName.mustNot(), hasSize(1));
        tq = as(firstName.mustNot().get(0), TermQueryBuilder.class);
        assertThat(tq.fieldName(), equalTo("first_name"));
        assertThat(tq.value(), equalTo("Parto"));
    }

    /* Expected:
       LimitExec[10000[INTEGER]]
       \_ExchangeExec[REMOTE_SOURCE]
         \_ExchangeExec[REMOTE_SINK]
           \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !gender, languages{f}#5, last_name{f}#6, salary{f}#7]]
             \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !ge..]
               \_EsQueryExec[test], query[{"bool":{"must_not":[{"term":{"emp_no":{"value":10010}}}],"boost":1.0}}][_doc{f}#9],
                                          limit[10000], sort[]

     */
    public void testPushDownNegatedEquality() {
        var plan = physicalPlan("""
            from test
            | where not emp_no == 10010
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var boolQuery = as(sv(source.query(), "emp_no"), BoolQueryBuilder.class);
        assertThat(boolQuery.mustNot(), hasSize(1));
        var termQuery = as(boolQuery.mustNot().get(0), TermQueryBuilder.class);
        assertThat(termQuery.fieldName(), is("emp_no"));
        assertThat(termQuery.value(), is(10010));  // TODO this will match multivalued fields and we don't want that
    }

    /* Expected:
       LimitExec[10000[INTEGER]]
       \_ExchangeExec[REMOTE_SOURCE]
         \_ExchangeExec[REMOTE_SINK]
           \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
             \_FieldExtractExec[_meta_field{f}#9, first_name{f}#4, !gender, last_na..]
               \_LimitExec[10000[INTEGER]]
                 \_FilterExec[NOT(emp_no{f}#3 == languages{f}#6)]
                   \_FieldExtractExec[emp_no{f}#3, languages{f}#6]
                     \_EsQueryExec[test], query[][_doc{f}#10], limit[], sort[]
     */
    public void testDontPushDownNegatedEqualityBetweenAttributes() {
        var plan = physicalPlan("""
            from test
            | where not emp_no == languages
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var localLimit = as(extractRest.child(), LimitExec.class);
        var filterExec = as(localLimit.child(), FilterExec.class);
        assertThat(filterExec.condition(), instanceOf(Not.class));
        var extractForFilter = as(filterExec.child(), FieldExtractExec.class);
        var source = source(extractForFilter.child());
        assertNull(source.query());
    }

    public void testEvalLike() {
        var plan = physicalPlan("""
            from test
            | eval x = concat(first_name, "--")
            | where x like "%foo%"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var eval = as(filter.child(), EvalExec.class);
        var fieldExtract = as(eval.child(), FieldExtractExec.class);
        assertEquals(EsQueryExec.class, fieldExtract.child().getClass());
    }

    public void testPushDownLike() {
        var plan = physicalPlan("""
            from test
            | where first_name like "*foo*"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertEquals(WildcardQueryBuilder.class, query.getClass());
        WildcardQueryBuilder wildcard = ((WildcardQueryBuilder) query);
        assertEquals("first_name", wildcard.fieldName());
        assertEquals("*foo*", wildcard.value());
    }

    public void testPushDownNotLike() {
        var plan = physicalPlan("""
            from test
            | where not first_name like "%foo%"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var boolQuery = as(sv(source.query(), "first_name"), BoolQueryBuilder.class);
        assertThat(boolQuery.mustNot(), hasSize(1));
        var tq = as(boolQuery.mustNot().get(0), TermQueryBuilder.class);
        assertThat(tq.fieldName(), is("first_name"));
        assertThat(tq.value(), is("%foo%"));
    }

    public void testEvalRLike() {
        var plan = physicalPlan("""
            from test
            | eval x = concat(first_name, "--")
            | where x rlike ".*foo.*"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var eval = as(filter.child(), EvalExec.class);
        var fieldExtract = as(eval.child(), FieldExtractExec.class);
        assertEquals(EsQueryExec.class, fieldExtract.child().getClass());
    }

    public void testPushDownRLike() {
        var plan = physicalPlan("""
            from test
            | where first_name rlike ".*foo.*"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertEquals(RegexpQueryBuilder.class, query.getClass());
        RegexpQueryBuilder wildcard = ((RegexpQueryBuilder) query);
        assertEquals("first_name", wildcard.fieldName());
        assertEquals(".*foo.*", wildcard.value());
    }

    public void testPushDownNotRLike() {
        var plan = physicalPlan("""
            from test
            | where not first_name rlike ".*foo.*"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        var boolQuery = (BoolQueryBuilder) query;
        List<QueryBuilder> mustNot = boolQuery.mustNot();
        assertThat(mustNot.size(), is(1));
        assertThat(mustNot.get(0), instanceOf(RegexpQueryBuilder.class));
        var regexpQuery = (RegexpQueryBuilder) mustNot.get(0);
        assertThat(regexpQuery.fieldName(), is("first_name"));
        assertThat(regexpQuery.value(), is(".*foo.*"));
    }

    public void testTopNNotPushedDownOnOverlimit() {
        int pageSize = config.pragmas().pageSize();
        var optimized = optimizedPlan(physicalPlan("from test | sort emp_no | limit " + (pageSize + 1) + " | keep emp_no"));

        var project = as(optimized, ProjectExec.class);
        var topN = as(project.child(), TopNExec.class);
        var exchange = asRemoteExchange(topN.child());
        project = as(exchange.child(), ProjectExec.class);
        List<String> projectionNames = project.projections().stream().map(NamedExpression::name).collect(Collectors.toList());
        assertTrue(projectionNames.containsAll(List.of("emp_no")));
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.sorts(), is(sorts(topN.order())));
        assertThat(source.limit(), equalTo(l(10000)));
    }

    private static EsQueryExec source(PhysicalPlan plan) {
        if (plan instanceof ExchangeExec exchange) {
            plan = exchange.child();
        }
        return as(plan, EsQueryExec.class);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan) {
        // System.out.println("* Physical Before\n" + plan);
        var p = physicalPlanOptimizer.optimize(plan);
        // System.out.println("* Physical After\n" + p);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is
        var l = p.transformUp(FragmentExec.class, fragment -> {
            var localPlan = PlannerUtils.localPlan(List.of(), config, fragment);
            return localPlan;
        });

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query) {
        var logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        assertSerialization(physical);
        return physical;
    }

    private List<FieldSort> sorts(List<Order> orders) {
        return orders.stream().map(o -> new FieldSort((FieldAttribute) o.child(), o.direction(), o.nullsPosition())).toList();
    }

    private ExchangeExec asRemoteExchange(PhysicalPlan plan) {
        return as(plan, ExchangeExec.class);
    }

    public void testFieldExtractWithoutSourceAttributes() {
        PhysicalPlan verifiedPlan = optimizedPlan(physicalPlan("""
            from test
            | where round(emp_no) > 10
            """));
        // Transform the verified plan so that it is invalid (i.e. no source attributes)
        List<Attribute> emptyAttrList = List.of();
        var badPlan = verifiedPlan.transformDown(
            EsQueryExec.class,
            node -> new EsSourceExec(node.source(), node.index(), emptyAttrList, node.query())
        );

        var e = expectThrows(PhysicalVerificationException.class, () -> physicalPlanOptimizer.verify(badPlan));
        assertThat(
            e.getMessage(),
            containsString(
                "Need to add field extractor for [[emp_no]] but cannot detect source attributes from node [EsSourceExec[test][]]"
            )
        );
    }

    /**
     * Asserts that a {@link QueryBuilder} is a {@link SingleValueQuery} that
     * acting on the provided field name and returns the {@link QueryBuilder}
     * that it wraps.
     */
    private QueryBuilder sv(QueryBuilder builder, String fieldName) {
        SingleValueQuery.Builder sv = as(builder, SingleValueQuery.Builder.class);
        assertThat(sv.field(), equalTo(fieldName));
        return sv.next();
    }
}
