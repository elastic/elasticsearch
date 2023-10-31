/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils.TestSearchStats;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.FINAL;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class LocalPhysicalPlanOptimizerTests extends ESTestCase {

    private static final String PARAM_FORMATTING = "%1$s";

    /**
     * Estimated size of a keyword field in bytes.
     */
    private static final int KEYWORD_EST = EstimatesRowSize.estimateSize(DataTypes.KEYWORD);

    private EsqlParser parser;
    private Analyzer analyzer;
    private LogicalPlanOptimizer logicalOptimizer;
    private PhysicalPlanOptimizer physicalPlanOptimizer;
    private Mapper mapper;
    private Map<String, EsField> mapping;
    private int allFieldRowSize;

    private final EsqlConfiguration config;
    private final SearchStats IS_SV_STATS = new TestSearchStats() {
        @Override
        public boolean isSingleValue(String field) {
            return true;
        }
    };

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() {
        return settings().stream().map(t -> {
            var settings = Settings.builder().loadFromMap(t.v2()).build();
            return new Object[] { t.v1(), configuration(new QueryPragmas(settings)) };
        }).toList();
    }

    private static List<Tuple<String, Map<String, Object>>> settings() {
        return asList(new Tuple<>("default", Map.of()));
    }

    public LocalPhysicalPlanOptimizerTests(String name, EsqlConfiguration config) {
        this.config = config;
    }

    @Before
    public void init() {
        parser = new EsqlParser();

        mapping = loadMapping("mapping-basic.json");
        allFieldRowSize = mapping.values()
            .stream()
            .mapToInt(
                f -> (EstimatesRowSize.estimateSize(EsqlDataTypes.widenSmallNumericTypes(f.getDataType())) + f.getProperties()
                    .values()
                    .stream()
                    // check one more level since the mapping contains TEXT fields with KEYWORD multi-fields
                    .mapToInt(x -> EstimatesRowSize.estimateSize(EsqlDataTypes.widenSmallNumericTypes(x.getDataType())))
                    .sum())
            )
            .sum();
        EsIndex test = new EsIndex("test", mapping);
        IndexResolution getIndexResult = IndexResolution.valid(test);
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        mapper = new Mapper(functionRegistry);
        var enrichResolution = new EnrichResolution(
            Set.of(
                new EnrichPolicyResolution(
                    "foo",
                    new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("idx"), "fld", List.of("a", "b")),
                    IndexResolution.valid(
                        new EsIndex(
                            "idx",
                            Map.ofEntries(
                                Map.entry("a", new EsField("a", DataTypes.INTEGER, Map.of(), true)),
                                Map.entry("b", new EsField("b", DataTypes.LONG, Map.of(), true))
                            )
                        )
                    )
                )
            ),
            Set.of("foo")
        );

        analyzer = new Analyzer(
            new AnalyzerContext(config, functionRegistry, getIndexResult, enrichResolution),
            new Verifier(new Metrics())
        );
    }

    /**
     * Expects
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#24, seen{r}#25],true]
     *     \_EsStatsQueryExec[test], stats[Stat[name=*, type=COUNT, query=null]]], query[{"esql_single_value":{"field":"emp_no","next":
     *       {"range":{"emp_no":{"lt":10050,"boost":1.0}}}}}][count{r}#40, seen{r}#41], limit[],
     */
    // TODO: this is suboptimal due to eval not being removed/folded
    public void testCountAllWithEval() {
        var plan = plan("""
              from test | eval s = salary | rename s as sr | eval hidden_s = sr | rename emp_no as e | where e < 10050
            | stats c = count(*)
            """);
        var stat = queryStatsFor(plan);
        assertThat(stat.type(), is(StatsType.COUNT));
        assertThat(stat.query(), is(nullValue()));
    }

    /**
     * Expects
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#14, seen{r}#15],true]
     *     \_EsStatsQueryExec[test], stats[Stat[name=*, type=COUNT, query=null]]],
     *     query[{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10040,"boost":1.0}}}}}][count{r}#30, seen{r}#31],
     *       limit[],
     */
    public void testCountAllWithFilter() {
        var plan = plan("from test | where emp_no > 10040 | stats c = count(*)");
        var stat = queryStatsFor(plan);
        assertThat(stat.type(), is(StatsType.COUNT));
        assertThat(stat.query(), is(nullValue()));
    }

    /**
     * Expects
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT(emp_no{f}#5) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#15, seen{r}#16],true]
     *     \_EsStatsQueryExec[test], stats[Stat[name=emp_no, type=COUNT, query={
     *   "exists" : {
     *     "field" : "emp_no",
     *     "boost" : 1.0
     *   }
     * }]]], query[{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10040,"boost":1.0}}}}}][count{r}#31, seen{r}#32],
     *   limit[],
     */
    public void testCountFieldWithFilter() {
        var plan = plan("from test | where emp_no > 10040 | stats c = count(emp_no)", IS_SV_STATS);
        var stat = queryStatsFor(plan);
        assertThat(stat.type(), is(StatsType.COUNT));
        assertThat(stat.query(), is(QueryBuilders.existsQuery("emp_no")));
    }

    /**
     * Expects
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT(salary{f}#20) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#25, seen{r}#26],true]
     *     \_EsStatsQueryExec[test], stats[Stat[name=salary, type=COUNT, query={
     *   "exists" : {
     *     "field" : "salary",
     *     "boost" : 1.0
     *   }
     */
    public void testCountFieldWithEval() {
        var plan = plan("""
              from test | eval s = salary | rename s as sr | eval hidden_s = sr | rename emp_no as e | where e < 10050
            | stats c = count(hidden_s)
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exg = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exg.child(), EsStatsQueryExec.class);

        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(QueryBuilders.existsQuery("salary")));
    }

    // optimized doesn't know yet how to push down count over field
    public void testCountOneFieldWithFilter() {
        var plan = plan("""
            from test
            | where salary > 1000
            | stats c = count(salary)
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(QueryBuilders.existsQuery("salary")));
        var expected = wrapWithSingleQuery(QueryBuilders.rangeQuery("salary").gt(1000), "salary");
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
    }

    // optimized doesn't know yet how to push down count over field
    public void testCountOneFieldWithFilterAndLimit() {
        var plan = plan("""
            from test
            | where salary > 1000
            | limit 10
            | stats c = count(salary)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    // optimized doesn't know yet how to break down different multi count
    public void testCountMultipleFieldsWithFilter() {
        var plan = plan("""
            from test
            | where salary > 1000 and emp_no > 10010
            | stats cs = count(salary), ce = count(emp_no)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    public void testAnotherCountAllWithFilter() {
        var plan = plan("""
            from test
            | where emp_no > 10010
            | stats c = count()
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var expected = wrapWithSingleQuery(QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no");
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
    }

    /**
     * Expected
     * ProjectExec[[c{r}#3, c{r}#3 AS call, c_literal{r}#7]]
     * \_LimitExec[500[INTEGER]]
     *   \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS c, COUNT(1[INTEGER]) AS c_literal],FINAL,null]
     *     \_ExchangeExec[[count{r}#18, seen{r}#19, count{r}#20, seen{r}#21],true]
     *       \_EsStatsQueryExec[test], stats[Stat[name=*, type=COUNT, query=null], Stat[name=*, type=COUNT, query=null]]],
     *         query[{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}}}}]
     *         [count{r}#23, seen{r}#24, count{r}#25, seen{r}#26], limit[],
     */
    public void testMultiCountAllWithFilter() {
        var plan = plan("""
            from test
            | where emp_no > 10010
            | stats c = count(), call = count(*), c_literal = count(1)
            """, IS_SV_STATS);

        var project = as(plan, ProjectExec.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("c", "call", "c_literal"));
        var alias = as(projections.get(1), Alias.class);
        assertThat(Expressions.name(alias.child()), is("c"));
        var limit = as(project.child(), LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c", "c_literal"));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen", "count", "seen"));
        var expected = wrapWithSingleQuery(QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no");
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
    }

    // optimized doesn't know yet how to break down different multi count
    public void testCountFieldsAndAllWithFilter() {
        var plan = plan("""
            from test
            | where emp_no > 10010
            | stats c = count(), cs = count(salary), ce = count(emp_no)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    /**
     * Expecting
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#14, seen{r}#15],true]
     *     \_LocalSourceExec[[c{r}#3],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     */
    public void testLocalAggOptimizedToLocalRelation() {
        var stats = new TestSearchStats() {
            @Override
            public boolean exists(String field) {
                return "emp_no".equals(field) == false;
            }
        };

        var plan = plan("""
            from test
            | where emp_no > 10010
            | stats c = count()
            """, stats);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        assertThat(exchange.isInBetweenAggs(), is(true));
        var localSource = as(exchange.child(), LocalSourceExec.class);
        assertThat(Expressions.names(localSource.output()), contains("count", "seen"));
    }

    private QueryBuilder wrapWithSingleQuery(QueryBuilder inner, String fieldName) {
        return FilterTests.singleValueQuery(inner, fieldName);
    }

    private Stat queryStatsFor(PhysicalPlan plan) {
        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exg = as(agg.child(), ExchangeExec.class);
        var statSource = as(exg.child(), EsStatsQueryExec.class);
        var stats = statSource.stats();
        assertThat(stats, hasSize(1));
        var stat = stats.get(0);
        return stat;
    }

    private PhysicalPlan plan(String query) {
        return plan(query, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    private PhysicalPlan plan(String query, SearchStats stats) {
        var physical = optimizedPlan(physicalPlan(query), stats);
        return physical;
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        // System.out.println("* Physical Before\n" + plan);
        var p = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(plan));
        // System.out.println("* Physical After\n" + p);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is

        var logicalTestOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(config, searchStats));
        var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(config, searchStats), true);
        var l = PlannerUtils.localPlan(plan, logicalTestOptimizer, physicalTestOptimizer);

        // handle local reduction alignment
        l = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query) {
        var logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        return physical;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
