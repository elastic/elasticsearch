/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.network.NetworkAddress;
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
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.FINAL;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
    private EsqlFunctionRegistry functionRegistry;
    private Mapper mapper;

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
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        functionRegistry = new EsqlFunctionRegistry();
        mapper = new Mapper(functionRegistry);
        EnrichResolution enrichResolution = new EnrichResolution();
        enrichResolution.addResolvedPolicy(
            "foo",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "fld",
                EnrichPolicy.MATCH_TYPE,
                List.of("a", "b"),
                Map.of("", "idx"),
                Map.ofEntries(
                    Map.entry("a", new EsField("a", DataTypes.INTEGER, Map.of(), true)),
                    Map.entry("b", new EsField("b", DataTypes.LONG, Map.of(), true))
                )
            )
        );
        analyzer = makeAnalyzer("mapping-basic.json", enrichResolution);
    }

    private Analyzer makeAnalyzer(String mappingFileName, EnrichResolution enrichResolution) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);

        return new Analyzer(new AnalyzerContext(config, functionRegistry, getIndexResult, enrichResolution), new Verifier(new Metrics()));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
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
     * LimitExec[1000[INTEGER]]
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
     * LimitExec[1000[INTEGER]]
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
     * LimitExec[1000[INTEGER]]
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
        Source source = new Source(2, 8, "salary > 1000");
        var exists = QueryBuilders.existsQuery("salary");
        assertThat(stat.query(), is(exists));
        var range = wrapWithSingleQuery(QueryBuilders.rangeQuery("salary").gt(1000), "salary", source);
        var expected = QueryBuilders.boolQuery().must(range).must(exists);
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
        var source = ((SingleValueQuery.Builder) esStatsQuery.query()).source();
        var expected = wrapWithSingleQuery(QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", source);
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
    }

    /**
     * Expected
     * ProjectExec[[c{r}#3, c{r}#3 AS call, c_literal{r}#7]]
     * \_LimitExec[1000[INTEGER]]
     *   \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS c, COUNT(1[INTEGER]) AS c_literal],FINAL,null]
     *     \_ExchangeExec[[count{r}#18, seen{r}#19, count{r}#20, seen{r}#21],true]
     *       \_EsStatsQueryExec[test], stats[Stat[name=*, type=COUNT, query=null], Stat[name=*, type=COUNT, query=null]]],
     *         query[{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},
     *         "source":"emp_no > 10010@2:9"}}][count{r}#23, seen{r}#24, count{r}#25, seen{r}#26], limit[],
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
        var source = ((SingleValueQuery.Builder) esStatsQuery.query()).source();
        var expected = wrapWithSingleQuery(QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", source);
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
     * LimitExec[1000[INTEGER]]
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

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, job{f}#10, job.raw{f}#11, languages{f}#6, last_n
     * ame{f}#7, long_noidx{f}#12, salary{f}#8]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"exists":{"field":"emp_no","boost":1.0}}][_doc{f}#13], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testIsNotNullPushdownFilter() {
        var plan = plan("from test | where emp_no is not null");

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));
        var expected = QueryBuilders.existsQuery("emp_no");
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expects
     *
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, job{f}#10, job.raw{f}#11, languages{f}#6, last_n
     * ame{f}#7, long_noidx{f}#12, salary{f}#8]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"bool":{"must_not":[{"exists":{"field":"emp_no","boost":1.0}}],"boost":1.0}}][_doc{f}#13],
     *         limit[1000], sort[] estimatedRowSize[324]
     */
    public void testIsNullPushdownFilter() {
        var plan = plan("from test | where emp_no is null");

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));
        var expected = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("emp_no"));
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expects
     *
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[COUNT(gender{f}#7) AS count(gender)],FINAL,null]
     *   \_ExchangeExec[[count{r}#15, seen{r}#16],true]
     *     \_AggregateExec[[],[COUNT(gender{f}#7) AS count(gender)],PARTIAL,8]
     *       \_FieldExtractExec[gender{f}#7]
     *         \_EsQueryExec[test], query[{"exists":{"field":"gender","boost":1.0}}][_doc{f}#17], limit[], sort[] estimatedRowSize[54]
     */
    public void testIsNotNull_TextField_Pushdown() {
        String textField = randomFrom("gender", "job");
        var plan = plan(String.format(Locale.ROOT, "from test | where %s is not null | stats count(%s)", textField, textField));

        var limit = as(plan, LimitExec.class);
        var finalAgg = as(limit.child(), AggregateExec.class);
        var exchange = as(finalAgg.child(), ExchangeExec.class);
        var partialAgg = as(exchange.child(), AggregateExec.class);
        var fieldExtract = as(partialAgg.child(), FieldExtractExec.class);
        var query = as(fieldExtract.child(), EsQueryExec.class);
        var expected = QueryBuilders.existsQuery(textField);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, job{f}#10, job.raw{f}#11, languages{f}#6, last_n
     *     ame{f}#7, long_noidx{f}#12, salary{f}#8]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"bool":{"must_not":[{"exists":{"field":"gender","boost":1.0}}],"boost":1.0}}]
     *         [_doc{f}#13], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testIsNull_TextField_Pushdown() {
        String textField = randomFrom("gender", "job");
        var plan = plan(String.format(Locale.ROOT, "from test | where %s is null", textField, textField));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var query = as(fieldExtract.child(), EsQueryExec.class);
        var expected = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(textField));
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * count(x) adds an implicit "exists(x)" filter in the pushed down query
     * This test checks this "exists" doesn't clash with the "is null" pushdown on the text field.
     * In this particular query, "exists(x)" and "x is null" cancel each other out.
     *
     * Expects
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[COUNT(job{f}#19) AS c],FINAL,8]
     *   \_ExchangeExec[[count{r}#22, seen{r}#23],true]
     *     \_LocalSourceExec[[count{r}#22, seen{r}#23],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]], BooleanVectorBlock
     * [vector=ConstantBooleanVector[positions=1, value=true]]]]
     */
    public void testIsNull_TextField_Pushdown_WithCount() {
        var plan = plan("""
              from test
              | eval filtered_job = job, count_job = job
              | where filtered_job IS NULL
              | stats c = COUNT(count_job)
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exg = as(agg.child(), ExchangeExec.class);
        as(exg.child(), LocalSourceExec.class);
    }

    /**
     * count(x) adds an implicit "exists(x)" filter in the pushed down query.
     * This test checks this "exists" doesn't clash with the "is null" pushdown on the text field.
     * In this particular query, "exists(x)" and "x is not null" go hand in hand and the query is pushed down to Lucene.
     *
     * Expects
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[COUNT(job{f}#19) AS c],FINAL,8]
     *   \_ExchangeExec[[count{r}#22, seen{r}#23],true]
     *     \_EsStatsQueryExec[test], stats[Stat[name=job, type=COUNT, query={
     *   "exists" : {
     *     "field" : "job",
     *     "boost" : 1.0
     *   }
     * }]]], query[{"exists":{"field":"job","boost":1.0}}][count{r}#25, seen{r}#26], limit[],
     */
    public void testIsNotNull_TextField_Pushdown_WithCount() {
        var plan = plan("""
              from test
              | eval filtered_job = job, count_job = job
              | where filtered_job IS NOT NULL
              | stats c = COUNT(count_job)
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exg = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exg.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(QueryBuilders.existsQuery("job")));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9,
     *     half_float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18,
     *     unsigned_long{f}#16, version{f}#19, wildcard{f}#20]]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#4, byte{f}#5, constant_k..][]
     *       \_EsQueryExec[test], query[{"esql_single_value":{"field":"ip","next":{"terms":{"ip":["127.0.0.0/24"],"boost":1.0}},"source":
     *         "cidr_match(ip, \"127.0.0.0/24\")@1:19"}}][_doc{f}#21], limit[1000], sort[] estimatedRowSize[389]
     */
    public void testCidrMatchPushdownFilter() {
        var allTypeMappingAnalyzer = makeAnalyzer("mapping-ip.json", new EnrichResolution());
        final String fieldName = "ip_addr";

        int cidrBlockCount = randomIntBetween(1, 10);
        ArrayList<String> cidrBlocks = new ArrayList<>();
        for (int i = 0; i < cidrBlockCount; i++) {
            cidrBlocks.add(randomCidrBlock());
        }
        String cidrBlocksString = cidrBlocks.stream().map((s) -> "\"" + s + "\"").collect(Collectors.joining(","));
        String cidrMatch = format(null, "cidr_match({}, {})", fieldName, cidrBlocksString);

        var query = "from test | where " + cidrMatch;
        var plan = plan(query, EsqlTestUtils.TEST_SEARCH_STATS, allTypeMappingAnalyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var queryExec = as(field.child(), EsQueryExec.class);
        assertThat(queryExec.limit().fold(), is(1000));

        var expectedInnerQuery = QueryBuilders.termsQuery(fieldName, cidrBlocks);
        var expectedQuery = wrapWithSingleQuery(expectedInnerQuery, fieldName, new Source(1, 18, cidrMatch));
        assertThat(queryExec.query().toString(), is(expectedQuery.toString()));
    }

    private record OutOfRangeTestCase(String fieldName, String tooLow, String tooHigh) {};

    public void testOutOfRangeFilterPushdown() {
        var allTypeMappingAnalyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());

        String largerThanInteger = String.valueOf(randomLongBetween(Integer.MAX_VALUE + 1L, Long.MAX_VALUE));
        String smallerThanInteger = String.valueOf(randomLongBetween(Long.MIN_VALUE, Integer.MIN_VALUE - 1L));

        // These values are already out of bounds for longs due to rounding errors.
        double longLowerBoundExclusive = (double) Long.MIN_VALUE;
        double longUpperBoundExclusive = (double) Long.MAX_VALUE;
        String largerThanLong = String.valueOf(randomDoubleBetween(longUpperBoundExclusive, Double.MAX_VALUE, true));
        String smallerThanLong = String.valueOf(randomDoubleBetween(-Double.MAX_VALUE, longLowerBoundExclusive, true));

        List<OutOfRangeTestCase> cases = List.of(
            new OutOfRangeTestCase("byte", smallerThanInteger, largerThanInteger),
            new OutOfRangeTestCase("short", smallerThanInteger, largerThanInteger),
            new OutOfRangeTestCase("integer", smallerThanInteger, largerThanInteger),
            new OutOfRangeTestCase("long", smallerThanLong, largerThanLong),
            // TODO: add unsigned_long https://github.com/elastic/elasticsearch/issues/102935
            // TODO: add half_float, float https://github.com/elastic/elasticsearch/issues/100130
            new OutOfRangeTestCase("double", "-1.0/0.0", "1.0/0.0"),
            new OutOfRangeTestCase("scaled_float", "-1.0/0.0", "1.0/0.0")
        );

        final String LT = "<";
        final String LTE = "<=";
        final String GT = ">";
        final String GTE = ">=";
        final String EQ = "==";
        final String NEQ = "!=";

        for (OutOfRangeTestCase testCase : cases) {
            List<String> trueForSingleValuesPredicates = List.of(
                LT + testCase.tooHigh,
                LTE + testCase.tooHigh,
                GT + testCase.tooLow,
                GTE + testCase.tooLow,
                NEQ + testCase.tooHigh,
                NEQ + testCase.tooLow,
                NEQ + "0.0/0.0"
            );
            List<String> alwaysFalsePredicates = List.of(
                LT + testCase.tooLow,
                LTE + testCase.tooLow,
                GT + testCase.tooHigh,
                GTE + testCase.tooHigh,
                EQ + testCase.tooHigh,
                EQ + testCase.tooLow,
                LT + "0.0/0.0",
                LTE + "0.0/0.0",
                GT + "0.0/0.0",
                GTE + "0.0/0.0",
                EQ + "0.0/0.0"
            );

            for (String truePredicate : trueForSingleValuesPredicates) {
                String comparison = testCase.fieldName + truePredicate;
                var query = "from test | where " + comparison;
                Source expectedSource = new Source(1, 18, comparison);

                EsQueryExec actualQueryExec = doTestOutOfRangeFilterPushdown(query, allTypeMappingAnalyzer);

                assertThat(actualQueryExec.query(), is(instanceOf(SingleValueQuery.Builder.class)));
                var actualLuceneQuery = (SingleValueQuery.Builder) actualQueryExec.query();
                assertThat(actualLuceneQuery.field(), equalTo(testCase.fieldName));
                assertThat(actualLuceneQuery.source(), equalTo(expectedSource));

                assertThat(actualLuceneQuery.next(), equalTo(QueryBuilders.matchAllQuery()));
            }

            for (String falsePredicate : alwaysFalsePredicates) {
                String comparison = testCase.fieldName + falsePredicate;
                var query = "from test | where " + comparison;
                Source expectedSource = new Source(1, 18, comparison);

                EsQueryExec actualQueryExec = doTestOutOfRangeFilterPushdown(query, allTypeMappingAnalyzer);

                assertThat(actualQueryExec.query(), is(instanceOf(SingleValueQuery.Builder.class)));
                var actualLuceneQuery = (SingleValueQuery.Builder) actualQueryExec.query();
                assertThat(actualLuceneQuery.field(), equalTo(testCase.fieldName));
                assertThat(actualLuceneQuery.source(), equalTo(expectedSource));

                var expectedInnerQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
                assertThat(actualLuceneQuery.next(), equalTo(expectedInnerQuery));
            }
        }
    }

    /**
     * Expects e.g.
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#190, byte{f}#191, constant_keyword-foo{f}#192, date{f}#193, double{f}#194, ...]]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#190, byte{f}#191, consta..][]
     *       \_EsQueryExec[test], query[{"esql_single_value":{"field":"byte","next":{"match_all":{"boost":1.0}},...}}]
     */
    private EsQueryExec doTestOutOfRangeFilterPushdown(String query, Analyzer analyzer) {
        var plan = plan(query, EsqlTestUtils.TEST_SEARCH_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var luceneQuery = as(fieldExtract.child(), EsQueryExec.class);

        return luceneQuery;
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{r}#2, first_name{r}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, first_n
     * ame{r}#3 AS last_name, long_noidx{f}#11, emp_no{r}#2 AS salary]]
     *     \_FieldExtractExec[_meta_field{f}#8, gender{f}#4, job{f}#9, job.raw{f}..]
     *       \_EvalExec[[null[INTEGER] AS emp_no, null[KEYWORD] AS first_name]]
     *         \_EsQueryExec[test], query[][_doc{f}#12], limit[1000], sort[] estimatedRowSize[270]
     */
    public void testMissingFieldsDoNotGetExtracted() {
        var stats = EsqlTestUtils.statsForMissingField("first_name", "last_name", "emp_no", "salary");

        var plan = plan("from test", stats);
        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var projections = project.projections();
        assertThat(
            Expressions.names(projections),
            contains("_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary")
        );
        // emp_no
        assertThat(projections.get(1), instanceOf(ReferenceAttribute.class));
        // first_name
        assertThat(projections.get(2), instanceOf(ReferenceAttribute.class));

        // last_name --> first_name
        var nullAlias = Alias.unwrap(projections.get(7));
        assertThat(Expressions.name(nullAlias), is("first_name"));
        // salary --> emp_no
        nullAlias = Alias.unwrap(projections.get(9));
        assertThat(Expressions.name(nullAlias), is("emp_no"));
        // check field extraction is skipped and that evaled fields are not extracted anymore
        var field = as(project.child(), FieldExtractExec.class);
        var fields = field.attributesToExtract();
        assertThat(Expressions.names(fields), contains("_meta_field", "gender", "job", "job.raw", "languages", "long_noidx"));
    }

    private QueryBuilder wrapWithSingleQuery(QueryBuilder inner, String fieldName, Source source) {
        return FilterTests.singleValueQuery(inner, fieldName, source);
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
        return plan(query, stats, analyzer);
    }

    private PhysicalPlan plan(String query, SearchStats stats, Analyzer analyzer) {
        var physical = optimizedPlan(physicalPlan(query, analyzer), stats);
        return physical;
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        // System.out.println("* Physical Before\n" + plan);
        var physicalPlan = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(plan));
        // System.out.println("* Physical After\n" + physicalPlan);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is

        var logicalTestOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(config, searchStats));
        var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(new LocalPhysicalOptimizerContext(config, searchStats), true);
        var l = PlannerUtils.localPlan(physicalPlan, logicalTestOptimizer, physicalTestOptimizer);

        // handle local reduction alignment
        l = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        var logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        return physical;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private String randomCidrBlock() {
        boolean ipv4 = randomBoolean();

        String address = NetworkAddress.format(randomIp(ipv4));
        int cidrPrefixLength = ipv4 ? randomIntBetween(0, 32) : randomIntBetween(0, 128);

        return format(null, "{}/{}", address, cidrPrefixLength);
    }
}
