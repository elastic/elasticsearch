/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils.TestSearchStats;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ExtractAggregateCommonFilter;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.core.querydsl.query.Query.unscore;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class LocalPhysicalPlanOptimizerTests extends MapperServiceTestCase {

    public static final List<DataType> UNNECESSARY_CASTING_DATA_TYPES = List.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT
    );
    private static final String PARAM_FORMATTING = "%1$s";

    /**
     * Estimated size of a keyword field in bytes.
     */
    private static final int KEYWORD_EST = EstimatesRowSize.estimateSize(DataType.KEYWORD);
    public static final String MATCH_OPERATOR_QUERY = "from test | where %s:%s";
    public static final String MATCH_FUNCTION_QUERY = "from test | where match(%s, %s)";

    private TestPlannerOptimizer plannerOptimizer;
    private final Configuration config;
    private final SearchStats IS_SV_STATS = new TestSearchStats() {
        @Override
        public boolean isSingleValue(String field) {
            return true;
        }
    };

    private final SearchStats CONSTANT_K_STATS = new TestSearchStats() {
        @Override
        public boolean isSingleValue(String field) {
            return true;
        }

        @Override
        public String constantValue(String name) {
            return name.startsWith("constant_keyword") ? "foo" : null;
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

    public LocalPhysicalPlanOptimizerTests(String name, Configuration config) {
        this.config = config;
    }

    @Before
    public void init() {
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
                    Map.entry("a", new EsField("a", DataType.INTEGER, Map.of(), true)),
                    Map.entry("b", new EsField("b", DataType.LONG, Map.of(), true))
                )
            )
        );
        plannerOptimizer = new TestPlannerOptimizer(config, makeAnalyzer("mapping-basic.json", enrichResolution));
    }

    private Analyzer makeAnalyzer(String mappingFileName, EnrichResolution enrichResolution) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);

        return new Analyzer(
            new AnalyzerContext(config, new EsqlFunctionRegistry(), getIndexResult, defaultLookupResolution(), enrichResolution),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L))
        );
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
        var plan = plannerOptimizer.plan("""
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
        var plan = plannerOptimizer.plan("from test | where emp_no > 10040 | stats c = count(*)");
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
        var plan = plannerOptimizer.plan("from test | where emp_no > 10040 | stats c = count(emp_no)", IS_SV_STATS);
        var stat = queryStatsFor(plan);
        assertThat(stat.type(), is(StatsType.COUNT));
        assertThat(stat.query(), is(existsQuery("emp_no")));
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
        var plan = plannerOptimizer.plan("""
              from test | eval s = salary | rename s as sr | eval hidden_s = sr | rename emp_no as e | where e < 10050
            | stats c = count(hidden_s)
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exg = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exg.child(), EsStatsQueryExec.class);

        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("$$c$count", "$$c$seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(existsQuery("salary")));
    }

    // optimized doesn't know yet how to push down count over field
    public void testCountOneFieldWithFilter() {
        String query = """
            from test
            | where salary > 1000
            | stats c = count(salary)
            """;
        var plan = plannerOptimizer.plan(query, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("$$c$count", "$$c$seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        Source source = new Source(2, 8, "salary > 1000");
        var exists = existsQuery("salary");
        assertThat(stat.query(), is(exists));
        var range = wrapWithSingleQuery(query, unscore(rangeQuery("salary").gt(1000)), "salary", source);
        var expected = boolQuery().must(range).must(unscore(exists));
        assertThat(esStatsQuery.query().toString(), is(expected.toString()));
    }

    // optimized doesn't know yet how to push down count over field
    public void testCountOneFieldWithFilterAndLimit() {
        var plan = plannerOptimizer.plan("""
            from test
            | where salary > 1000
            | limit 10
            | stats c = count(salary)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    public void testCountPushdownForSvAndMvFields() throws IOException {
        String properties = EsqlTestUtils.loadUtf8TextFile("/mapping-basic.json");
        String mapping = "{\"mappings\": " + properties + "}";

        String query = """
            from test
            | stats c = count(salary)
            """;

        PhysicalPlan plan;

        List<List<String>> docsCasesWithoutPushdown = List.of(
            // No pushdown yet in case of MVs
            List.of("{ \"salary\" : [1,2] }"),
            List.of("{ \"salary\" : [1,2] }", "{ \"salary\" : null}")
        );
        for (List<String> docs : docsCasesWithoutPushdown) {
            plan = planWithMappingAndDocs(query, mapping, docs);
            // No EsSatsQueryExec as leaf of the plan.
            assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
        }

        // Cases where we can push this down as a COUNT(*) since there are only SVs
        List<List<String>> docsCasesWithPushdown = List.of(List.of(), List.of("{ \"salary\" : 1 }"), List.of("{ \"salary\": null }"));
        for (List<String> docs : docsCasesWithPushdown) {
            plan = planWithMappingAndDocs(query, mapping, docs);

            Holder<EsStatsQueryExec> leaf = new Holder<>();
            plan.forEachDown(p -> {
                if (p instanceof EsStatsQueryExec s) {
                    leaf.set(s);
                }
            });

            String expectedStats = """
                [Stat[name=salary, type=COUNT, query={
                  "exists" : {
                    "field" : "salary",
                    "boost" : 1.0
                  }
                }]]""";
            assertNotNull(leaf.get());
            assertThat(leaf.get().stats().toString(), equalTo(expectedStats));
        }
    }

    private PhysicalPlan planWithMappingAndDocs(String query, String mapping, List<String> docs) throws IOException {
        MapperService mapperService = createMapperService(mapping);
        List<ParsedDocument> parsedDocs = docs.stream().map(d -> mapperService.documentMapper().parse(source(d))).toList();

        Holder<PhysicalPlan> plan = new Holder<>(null);
        withLuceneIndex(mapperService, indexWriter -> {
            for (ParsedDocument parsedDoc : parsedDocs) {
                indexWriter.addDocument(parsedDoc.rootDoc());
            }
        }, directoryReader -> {
            IndexSearcher searcher = newSearcher(directoryReader);
            SearchExecutionContext ctx = createSearchExecutionContext(mapperService, searcher);
            plan.set(plannerOptimizer.plan(query, SearchContextStats.from(List.of(ctx))));
        });

        return plan.get();
    }

    // optimizer doesn't know yet how to break down different multi count
    public void testCountMultipleFieldsWithFilter() {
        var plan = plannerOptimizer.plan("""
            from test
            | where salary > 1000 and emp_no > 10010
            | stats cs = count(salary), ce = count(emp_no)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    public void testAnotherCountAllWithFilter() {
        String query = """
            from test
            | where emp_no > 10010
            | stats c = count()
            """;
        var plan = plannerOptimizer.plan(query, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);
        assertThat(esStatsQuery.limit(), is(nullValue()));
        assertThat(Expressions.names(esStatsQuery.output()), contains("$$c$count", "$$c$seen"));
        var source = ((SingleValueQuery.Builder) esStatsQuery.query()).source();
        var expected = wrapWithSingleQuery(query, unscore(rangeQuery("emp_no").gt(10010)), "emp_no", source);
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
    }

    // optimizer doesn't know yet how to normalize and deduplicate cout(*), count(), count(1) etc.
    public void testMultiCountAllWithFilter() {
        var plan = plannerOptimizer.plan("""
            from test
            | where emp_no > 10010
            | stats c = count(), call = count(*), c_literal = count(1)
            """, IS_SV_STATS);
        assertThat(plan.anyMatch(EsQueryExec.class::isInstance), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testSingleCountWithStatsFilter() {
        // an optimizer that filters out the ExtractAggregateCommonFilter rule
        var logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext()) {
            @Override
            protected List<Batch<LogicalPlan>> batches() {
                var oldBatches = super.batches();
                List<Batch<LogicalPlan>> newBatches = new ArrayList<>(oldBatches.size());
                for (var batch : oldBatches) {
                    List<Rule<?, LogicalPlan>> rules = new ArrayList<>(List.of(batch.rules()));
                    rules.removeIf(r -> r instanceof ExtractAggregateCommonFilter);
                    newBatches.add(batch.with(rules.toArray(Rule[]::new)));
                }
                return newBatches;
            }
        };
        var analyzer = makeAnalyzer("mapping-default.json", new EnrichResolution());
        var plannerOptimizer = new TestPlannerOptimizer(config, analyzer, logicalOptimizer);
        var plan = plannerOptimizer.plan("""
            from test
            | stats c = count(hire_date) where emp_no < 10042
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        var exchange = as(agg.child(), ExchangeExec.class);
        var esStatsQuery = as(exchange.child(), EsStatsQueryExec.class);

        Function<String, String> compact = s -> s.replaceAll("\\s+", "");
        assertThat(compact.apply(esStatsQuery.query().toString()), is(compact.apply("""
            {
                "bool": {
                    "must": [
                        {
                            "exists": {
                                "field": "hire_date",
                                "boost": 0.0
                            }
                        },
                        {
                            "esql_single_value": {
                                "field": "emp_no",
                                "next": {
                                    "range": {
                                        "emp_no": {
                                            "lt": 10042,
                                            "boost": 0.0
                                        }
                                    }
                                },
                                "source": "emp_no < 10042@2:36"
                            }
                        }
                    ],
                    "boost": 1.0
                }
            }
            """)));
    }

    // optimizer doesn't know yet how to break down different multi count
    public void testCountFieldsAndAllWithFilter() {
        var plan = plannerOptimizer.plan("""
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

        var plan = plannerOptimizer.plan("""
            from test
            | where emp_no > 10010
            | stats c = count()
            """, stats);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(agg.getMode(), is(FINAL));
        assertThat(Expressions.names(agg.aggregates()), contains("c"));
        var exchange = as(agg.child(), ExchangeExec.class);
        assertThat(exchange.inBetweenAggs(), is(true));
        var localSource = as(exchange.child(), LocalSourceExec.class);
        assertThat(Expressions.names(localSource.output()), contains("$$c$count", "$$c$seen"));
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
        var plan = plannerOptimizer.plan("from test | where emp_no is not null");

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));
        var expected = unscore(existsQuery("emp_no"));
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
        var plan = plannerOptimizer.plan("from test | where emp_no is null");

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));
        var expected = boolQuery().mustNot(unscore(existsQuery("emp_no")));
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
        var plan = plannerOptimizer.plan(
            String.format(Locale.ROOT, "from test | where %s is not null | stats count(%s)", textField, textField)
        );

        var limit = as(plan, LimitExec.class);
        var finalAgg = as(limit.child(), AggregateExec.class);
        var exchange = as(finalAgg.child(), ExchangeExec.class);
        var partialAgg = as(exchange.child(), AggregateExec.class);
        var fieldExtract = as(partialAgg.child(), FieldExtractExec.class);
        var query = as(fieldExtract.child(), EsQueryExec.class);
        var expected = unscore(existsQuery(textField));
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
        var plan = plannerOptimizer.plan(String.format(Locale.ROOT, "from test | where %s is null", textField, textField));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var query = as(fieldExtract.child(), EsQueryExec.class);
        var expected = boolQuery().mustNot(unscore(existsQuery(textField)));
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
        var plan = plannerOptimizer.plan("""
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
        var plan = plannerOptimizer.plan("""
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
        assertThat(Expressions.names(esStatsQuery.output()), contains("$$c$count", "$$c$seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(existsQuery("job")));
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
            new OutOfRangeTestCase("long", smallerThanLong, largerThanLong)
            // TODO: add unsigned_long https://github.com/elastic/elasticsearch/issues/102935
            // TODO: add half_float, float https://github.com/elastic/elasticsearch/issues/100130
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
                NEQ + testCase.tooLow
            );
            List<String> alwaysFalsePredicates = List.of(
                LT + testCase.tooLow,
                LTE + testCase.tooLow,
                GT + testCase.tooHigh,
                GTE + testCase.tooHigh,
                EQ + testCase.tooHigh,
                EQ + testCase.tooLow
            );

            for (String truePredicate : trueForSingleValuesPredicates) {
                String comparison = testCase.fieldName + truePredicate;
                var query = "from test | where " + comparison;
                Source expectedSource = new Source(1, 18, comparison);

                logger.info("Query: " + query);
                EsQueryExec actualQueryExec = doTestOutOfRangeFilterPushdown(query, allTypeMappingAnalyzer);

                assertThat(actualQueryExec.query(), is(instanceOf(SingleValueQuery.Builder.class)));
                var actualLuceneQuery = (SingleValueQuery.Builder) actualQueryExec.query();
                assertThat(actualLuceneQuery.field(), equalTo(testCase.fieldName));
                assertThat(actualLuceneQuery.source(), equalTo(expectedSource));

                assertThat(actualLuceneQuery.next(), equalTo(unscore(matchAllQuery())));
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

                var expectedInnerQuery = unscore(boolQuery().mustNot(unscore(matchAllQuery())));
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
        var plan = plannerOptimizer.plan(query, EsqlTestUtils.TEST_SEARCH_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var luceneQuery = as(fieldExtract.child(), EsQueryExec.class);

        return luceneQuery;
    }

    /*
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, hire_date{f}#9, job{f}#10, job.raw{f}#11, langua
     * ges{f}#5, last_name{f}#6, long_noidx{f}#12, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{r}#2, first_name{r}#3, gender{f}#4, hire_date{f}#9, job{f}#10, job.raw{f}#11, langua
     * ges{f}#5, first_name{r}#3 AS last_name, long_noidx{f}#12, emp_no{r}#2 AS salary]]
     *     \_FieldExtractExec[_meta_field{f}#8, gender{f}#4, hire_date{f}#9, job{..]<[],[]>
     *       \_EvalExec[[null[INTEGER] AS emp_no, null[KEYWORD] AS first_name]]
     *         \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#13], limit[1000], sort[] estimatedRowSize[278]
     */
    public void testMissingFieldsDoNotGetExtracted() {
        var stats = EsqlTestUtils.statsForMissingField("first_name", "last_name", "emp_no", "salary");

        var plan = plannerOptimizer.plan("from test", stats);
        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var projections = project.projections();
        assertThat(
            Expressions.names(projections),
            contains(
                "_meta_field",
                "emp_no",
                "first_name",
                "gender",
                "hire_date",
                "job",
                "job.raw",
                "languages",
                "last_name",
                "long_noidx",
                "salary"
            )
        );
        // emp_no
        assertThat(projections.get(1), instanceOf(ReferenceAttribute.class));
        // first_name
        assertThat(projections.get(2), instanceOf(ReferenceAttribute.class));

        // last_name --> first_name
        var nullAlias = Alias.unwrap(projections.get(8));
        assertThat(Expressions.name(nullAlias), is("first_name"));
        // salary --> emp_no
        nullAlias = Alias.unwrap(projections.get(10));
        assertThat(Expressions.name(nullAlias), is("emp_no"));
        // check field extraction is skipped and that evaled fields are not extracted anymore
        var field = as(project.child(), FieldExtractExec.class);
        var fields = field.attributesToExtract();
        assertThat(Expressions.names(fields), contains("_meta_field", "gender", "hire_date", "job", "job.raw", "languages", "long_noidx"));
    }

    /*
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[language_code{r}#6],[COUNT(emp_no{f}#12,true[BOOLEAN]) AS c#11, language_code{r}#6],FINAL,[language_code{r}#6, $
     *      $c$count{r}#25, $$c$seen{r}#26],12]
     *   \_ExchangeExec[[language_code{r}#6, $$c$count{r}#25, $$c$seen{r}#26],true]
     *     \_AggregateExec[[languages{r}#15],[COUNT(emp_no{f}#12,true[BOOLEAN]) AS c#11, languages{r}#15 AS language_code#6],INITIAL,[langua
     *          ges{r}#15, $$c$count{r}#27, $$c$seen{r}#28],12]
     *       \_FieldExtractExec[emp_no{f}#12]<[],[]>
     *         \_EvalExec[[null[INTEGER] AS languages#15]]
     *           \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#29], limit[], sort[] estimatedRowSize[12]
     */
    public void testMissingFieldsPurgesTheJoinLocally() {
        var stats = EsqlTestUtils.statsForMissingField("languages");

        var plan = plannerOptimizer.plan("""
            from test
            | keep emp_no, languages
            | rename languages AS language_code
            | lookup join languages_lookup ON language_code
            | stats c = count(emp_no) by language_code
            """, stats);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(Expressions.names(agg.output()), contains("c", "language_code"));

        var exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        var extract = as(agg.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
        var source = as(eval.child(), EsQueryExec.class);
    }

    /*
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[language_code{r}#7],[COUNT(emp_no{r}#31,true[BOOLEAN]) AS c#17, language_code{r}#7],FINAL,[language_code{r}#7, $
     *      $c$count{r}#32, $$c$seen{r}#33],12]
     *   \_ExchangeExec[[language_code{r}#7, $$c$count{r}#32, $$c$seen{r}#33],true]
     *     \_AggregateExec[[language_code{r}#7],[COUNT(emp_no{r}#31,true[BOOLEAN]) AS c#17, language_code{r}#7],INITIAL,[language_code{r}#7,
     *          $$c$count{r}#34, $$c$seen{r}#35],12]
     *       \_GrokExec[first_name{f}#19,Parser[pattern=%{WORD:foo}, grok=org.elasticsearch.grok.Grok@75389ac1],[foo{r}#12]]
     *         \_MvExpandExec[emp_no{f}#18,emp_no{r}#31]
     *           \_ProjectExec[[emp_no{f}#18, languages{r}#21 AS language_code#7, first_name{f}#19]]
     *             \_FieldExtractExec[emp_no{f}#18, first_name{f}#19]<[],[]>
     *               \_EvalExec[[null[INTEGER] AS languages#21]]
     *                 \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#36], limit[], sort[] estimatedRowSize[112]
     */
    public void testMissingFieldsPurgesTheJoinLocallyThroughCommands() {
        var stats = EsqlTestUtils.statsForMissingField("languages");

        var plan = plannerOptimizer.plan("""
            from test
            | keep emp_no, languages, first_name
            | rename languages AS language_code
            | mv_expand emp_no
            | grok first_name "%{WORD:foo}"
            | lookup join languages_lookup ON language_code
            | stats c = count(emp_no) by language_code
            """, stats);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(Expressions.names(agg.output()), contains("c", "language_code"));

        var exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        var grok = as(agg.child(), GrokExec.class);
        var mvexpand = as(grok.child(), MvExpandExec.class);
        var project = as(mvexpand.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
        var source = as(eval.child(), EsQueryExec.class);
    }

    /*
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[language_code{r}#12],[COUNT(emp_no{r}#31,true[BOOLEAN]) AS c#17, language_code{r}#12],FINAL,[language_code{r}#12
     * , $$c$count{r}#32, $$c$seen{r}#33],12]
     *   \_ExchangeExec[[language_code{r}#12, $$c$count{r}#32, $$c$seen{r}#33],true]
     *     \_AggregateExec[[language_code{r}#12],[COUNT(emp_no{r}#31,true[BOOLEAN]) AS c#17, language_code{r}#12],INITIAL,[language_code{r}#
     *          12, $$c$count{r}#34, $$c$seen{r}#35],12]
     *       \_LookupJoinExec[[language_code{r}#12],[language_code{f}#29],[]]
     *         |_GrokExec[first_name{f}#19,Parser[pattern=%{NUMBER:language_code:int}, grok=org.elasticsearch.grok.Grok@764e5109],[languag
     *              e_code{r}#12]]
     *         | \_MvExpandExec[emp_no{f}#18,emp_no{r}#31]
     *         |   \_ProjectExec[[emp_no{f}#18, languages{r}#21 AS language_code#7, first_name{f}#19]]
     *         |     \_FieldExtractExec[emp_no{f}#18, first_name{f}#19]<[],[]>
     *         |       \_EvalExec[[null[INTEGER] AS languages#21]]
     *         |         \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#36], limit[], sort[] estimatedRowSize[66]
     *         \_EsQueryExec[languages_lookup], indexMode[lookup], query[][_doc{f}#37], limit[], sort[] estimatedRowSize[4]
     */
    public void testMissingFieldsNotPurgingTheJoinLocally() {
        var stats = EsqlTestUtils.statsForMissingField("languages");

        var plan = plannerOptimizer.plan("""
            from test
            | keep emp_no, languages, first_name
            | rename languages AS language_code
            | mv_expand emp_no
            | grok first_name "%{NUMBER:language_code:int}" // this reassigns language_code
            | lookup join languages_lookup ON language_code
            | stats c = count(emp_no) by language_code
            """, stats);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat(Expressions.names(agg.output()), contains("c", "language_code"));

        var exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        var join = as(agg.child(), LookupJoinExec.class);
        var grok = as(join.left(), GrokExec.class);
        var mvexpand = as(grok.child(), MvExpandExec.class);
        var project = as(mvexpand.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
        var source = as(eval.child(), EsQueryExec.class);
        var right = as(join.right(), EsQueryExec.class);
    }

    /*
     * LimitExec[1000[INTEGER]]
     * \_LookupJoinExec[[language_code{r}#6],[language_code{f}#23],[language_name{f}#24]]
     *   |_LimitExec[1000[INTEGER]]
     *   | \_AggregateExec[[languages{f}#15],[COUNT(emp_no{f}#12,true[BOOLEAN]) AS c#10, languages{f}#15 AS language_code#6],FINAL,[language
     *          s{f}#15, $$c$count{r}#25, $$c$seen{r}#26],62]
     *   |   \_ExchangeExec[[languages{f}#15, $$c$count{r}#25, $$c$seen{r}#26],true]
     *   |     \_AggregateExec[[languages{r}#15],[COUNT(emp_no{f}#12,true[BOOLEAN]) AS c#10, languages{r}#15 AS language_code#6],INITIAL,
     *              [languages{r}#15, $$c$count{r}#27, $$c$seen{r}#28],12]
     *   |       \_FieldExtractExec[emp_no{f}#12]<[],[]>
     *   |         \_EvalExec[[null[INTEGER] AS languages#15]]
     *   |           \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#29], limit[], sort[] estimatedRowSize[12]
     *   \_EsQueryExec[languages_lookup], indexMode[lookup], query[][_doc{f}#30], limit[], sort[] estimatedRowSize[4]
     */
    public void testMissingFieldsDoesNotPurgeTheJoinOnCoordinator() {
        var stats = EsqlTestUtils.statsForMissingField("languages");

        // same as the query above, but with the last two lines swapped, so that the join is no longer pushed to the data nodes
        var plan = plannerOptimizer.plan("""
            from test
            | keep emp_no, languages
            | rename languages AS language_code
            | stats c = count(emp_no) by language_code
            | lookup join languages_lookup ON language_code
            """, stats);

        var limit = as(plan, LimitExec.class);
        var join = as(limit.child(), LookupJoinExec.class);
        limit = as(join.left(), LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        var extract = as(agg.child(), FieldExtractExec.class);
        var eval = as(extract.child(), EvalExec.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().get(0), Alias.class);
        assertThat(alias.name(), is("languages"));
        var literal = as(alias.child(), Literal.class);
        assertNull(literal.value());
        var source = as(eval.child(), EsQueryExec.class);
        assertThat(source.indexPattern(), is("test"));
        assertThat(source.indexMode(), is(IndexMode.STANDARD));

        source = as(join.right(), EsQueryExec.class);
        assertThat(source.indexPattern(), is("languages_lookup"));
        assertThat(source.indexMode(), is(IndexMode.LOOKUP));
    }

    /*
     Checks that match filters are pushed down to Lucene when using no casting, for example:
     WHERE first_name:"Anna")
     WHERE age:17
     WHERE salary:24.5
     */
    public void testSingleMatchOperatorFilterPushdownWithoutCasting() {
        checkMatchFunctionPushDown(
            (value, dataType) -> DataType.isString(dataType) ? "\"" + value + "\"" : value.toString(),
            value -> value,
            UNNECESSARY_CASTING_DATA_TYPES,
            MATCH_OPERATOR_QUERY
        );
    }

    /*
    Checks that match filters are pushed down to Lucene when using strings, for example:
    WHERE ip:"127.0.0.1"
    WHERE date:"2024-07-01"
    WHERE date:"8.17.1"
    */
    public void testSingleMatchOperatorFilterPushdownWithStringValues() {
        checkMatchFunctionPushDown(
            (value, dataType) -> "\"" + value + "\"",
            Object::toString,
            Match.FIELD_DATA_TYPES,
            MATCH_OPERATOR_QUERY
        );
    }

    /*
     Checks that match filters are pushed down to Lucene when using no casting, for example:
     WHERE match(first_name, "Anna")
     WHERE match(age, 17)
     WHERE match(salary, 24.5)
     */
    public void testSingleMatchFunctionFilterPushdownWithoutCasting() {
        checkMatchFunctionPushDown(
            (value, dataType) -> DataType.isString(dataType) ? "\"" + value + "\"" : value.toString(),
            value -> value,
            UNNECESSARY_CASTING_DATA_TYPES,
            MATCH_FUNCTION_QUERY
        );
    }

    /*
    Checks that match filters are pushed down to Lucene when using casting, for example:
    WHERE match(ip, "127.0.0.1"::IP)
    WHERE match(date, "2024-07-01"::DATETIME)
    WHERE match(date, "8.17.1"::VERSION)
    */
    public void testSingleMatchFunctionPushdownWithCasting() {
        checkMatchFunctionPushDown(
            LocalPhysicalPlanOptimizerTests::queryValueAsCasting,
            value -> value,
            Match.FIELD_DATA_TYPES,
            MATCH_FUNCTION_QUERY
        );
    }

    /*
    Checks that match filters are pushed down to Lucene when using strings, for example:
    WHERE match(ip, "127.0.0.1")
    WHERE match(date, "2024-07-01")
    WHERE match(date, "8.17.1")
    */
    public void testSingleMatchFunctionFilterPushdownWithStringValues() {
        checkMatchFunctionPushDown(
            (value, dataType) -> "\"" + value + "\"",
            Object::toString,
            Match.FIELD_DATA_TYPES,
            MATCH_FUNCTION_QUERY
        );
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, job{f}#10, job.raw{f}#11, languages{f}#6, last_n
     * ame{f}#7, long_noidx{f}#12, salary{f}#8]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], indexMode[standard], query[{"match":{"first_name":{"query":"Anna"}}}][_doc{f}#13], limit[1000], sort[]
     *       estimatedRowSize[324]
     */
    private void checkMatchFunctionPushDown(
        BiFunction<Object, DataType, String> queryValueProvider,
        Function<Object, Object> expectedValueProvider,
        Collection<DataType> fieldDataTypes,
        String queryFormat
    ) {
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        // Check for every possible query data type
        for (DataType fieldDataType : fieldDataTypes) {
            if (DataType.UNDER_CONSTRUCTION.containsKey(fieldDataType)) {
                continue;
            }

            var queryValue = randomQueryValue(fieldDataType);

            String fieldName = fieldDataType == DataType.DATETIME ? "date" : fieldDataType.name().toLowerCase(Locale.ROOT);
            var esqlQuery = String.format(Locale.ROOT, queryFormat, fieldName, queryValueProvider.apply(queryValue, fieldDataType));

            try {
                var plan = plannerOptimizer.plan(esqlQuery, IS_SV_STATS, analyzer);
                var limit = as(plan, LimitExec.class);
                var exchange = as(limit.child(), ExchangeExec.class);
                var project = as(exchange.child(), ProjectExec.class);
                var fieldExtract = as(project.child(), FieldExtractExec.class);
                var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();

                var expectedLuceneQuery = new MatchQueryBuilder(fieldName, expectedValueProvider.apply(queryValue)).lenient(true);
                assertThat("Unexpected match query for data type " + fieldDataType, actualLuceneQuery, equalTo(expectedLuceneQuery));
            } catch (ParsingException e) {
                fail("Error parsing ESQL query: " + esqlQuery + "\n" + e.getMessage());
            }
        }
    }

    private static Object randomQueryValue(DataType dataType) {
        return switch (dataType) {
            case BOOLEAN -> randomBoolean();
            case INTEGER -> randomInt();
            case LONG -> randomLong();
            case UNSIGNED_LONG -> randomBigInteger();
            case DATE_NANOS -> EsqlDataTypeConverter.nanoTimeToString(randomMillisUpToYear9999());
            case DATETIME -> EsqlDataTypeConverter.dateTimeToString(randomMillisUpToYear9999());
            case DOUBLE -> randomDouble();
            case KEYWORD -> randomAlphaOfLength(5);
            case IP -> NetworkAddress.format(randomIp(randomBoolean()));
            case TEXT -> randomAlphaOfLength(50);
            case VERSION -> VersionUtils.randomVersion(random()).toString();
            default -> throw new IllegalArgumentException("Unexpected type: " + dataType);
        };
    }

    private static String queryValueAsCasting(Object value, DataType dataType) {
        if (value instanceof String) {
            value = "\"" + value + "\"";
        }
        return switch (dataType) {
            case VERSION -> value + "::VERSION";
            case IP -> value + "::IP";
            case DATETIME -> value + "::DATETIME";
            case DATE_NANOS -> value + "::DATE_NANOS";
            case INTEGER -> value + "::INTEGER";
            case LONG -> value + "::LONG";
            case BOOLEAN -> String.valueOf(value).toLowerCase(Locale.ROOT);
            case UNSIGNED_LONG -> "\"" + value + "\"::UNSIGNED_LONG";
            default -> value.toString();
        };
    }

    public void testMatchOptionsPushDown() {
        String query = """
            from test
            | where match(first_name, "Anna", {"fuzziness": "AUTO", "prefix_length": 3, "max_expansions": 10,
            "fuzzy_transpositions": false, "auto_generate_synonyms_phrase_query": true, "analyzer": "my_analyzer",
            "boost": 2.1, "minimum_should_match": 2, "operator": "AND"})
            """;
        var plan = plannerOptimizer.plan(query);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();

        Source filterSource = new Source(4, 8, "emp_no > 10000");
        var expectedLuceneQuery = new MatchQueryBuilder("first_name", "Anna").fuzziness(Fuzziness.AUTO)
            .prefixLength(3)
            .maxExpansions(10)
            .fuzzyTranspositions(false)
            .autoGenerateSynonymsPhraseQuery(true)
            .analyzer("my_analyzer")
            .boost(2.1f)
            .minimumShouldMatch("2")
            .operator(Operator.AND)
            .prefixLength(3)
            .lenient(true);
        assertThat(actualLuceneQuery.toString(), is(expectedLuceneQuery.toString()));
    }

    public void testQStrOptionsPushDown() {
        String query = """
            from test
            | where QSTR("first_name: Anna", {"allow_leading_wildcard": "true", "analyze_wildcard": "true",
            "analyzer": "auto", "auto_generate_synonyms_phrase_query": "false", "default_field": "test", "default_operator": "AND",
            "enable_position_increments": "true", "fuzziness": "auto", "fuzzy_max_expansions": 4, "fuzzy_prefix_length": 3,
            "fuzzy_transpositions": "true", "lenient": "false", "max_determinized_states": 10, "minimum_should_match": 3,
            "quote_analyzer": "q_analyzer", "quote_field_suffix": "q_field_suffix", "phrase_slop": 20, "rewrite": "fuzzy",
            "time_zone": "America/Los_Angeles"})
            """;
        var plan = plannerOptimizer.plan(query);

        AtomicReference<String> planStr = new AtomicReference<>();
        plan.forEachDown(EsQueryExec.class, result -> planStr.set(result.query().toString()));

        var expectedQStrQuery = new QueryStringQueryBuilder("first_name: Anna").allowLeadingWildcard(true)
            .analyzeWildcard(true)
            .analyzer("auto")
            .autoGenerateSynonymsPhraseQuery(false)
            .defaultField("test")
            .defaultOperator(Operator.fromString("AND"))
            .enablePositionIncrements(true)
            .fuzziness(Fuzziness.fromString("auto"))
            .fuzzyPrefixLength(3)
            .fuzzyMaxExpansions(4)
            .fuzzyTranspositions(true)
            .lenient(false)
            .maxDeterminizedStates(10)
            .minimumShouldMatch("3")
            .quoteAnalyzer("q_analyzer")
            .quoteFieldSuffix("q_field_suffix")
            .phraseSlop(20)
            .rewrite("fuzzy")
            .timeZone("America/Los_Angeles");
        assertThat(expectedQStrQuery.toString(), is(planStr.get()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"term":{"last_name":{"query":"Smith"}}}]
     */
    public void testTermFunction() {
        // Skip test if the term function is not enabled.
        assumeTrue("term function capability not available", EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled());

        var plan = plannerOptimizer.plan("""
            from test
            | where term(last_name, "Smith")
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));
        var expected = termQuery("last_name", "Smith");
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, hire_date{f}#10, job{f}#11, job.raw{f}#12
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"match":{"emp_no":{"query":123456}}}][_doc{f}#14],
     *       limit[1000], sort[] estimatedRowSize[332]
     */
    public void testMatchWithFieldCasting() {
        String query = """
            from test
            | where emp_no::long : 123456
            """;
        var plan = plannerOptimizer.plan(query);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var queryExec = as(fieldExtract.child(), EsQueryExec.class);
        var queryBuilder = as(queryExec.query(), MatchQueryBuilder.class);
        assertThat(queryBuilder.fieldName(), is("emp_no"));
        assertThat(queryBuilder.value(), is(123456));
    }

    public void testMatchFunction() {
        testFullTextFunction(new MatchFunctionTestCase());
    }

    public void testMatchOperator() {
        testFullTextFunction(new MatchOperatorTestCase());
    }

    public void testQstrFunction() {
        testFullTextFunction(new QueryStringFunctionTestCase());
    }

    public void testKqlFunction() {
        testFullTextFunction(new KqlFunctionTestCase());
    }

    /**
     * Executes all tests for full text functions
     */
    private void testFullTextFunction(FullTextFunctionTestCase testCase) {
        // TODO create a new class for testing full text functions that uses parameterized tests
        testBasicFullTextFunction(testCase);
        testFullTextFunctionWithFunctionsPushedToLucene(testCase);
        testFullTextFunctionConjunctionWhereOperands(testCase);
        testFullTextFunctionMultipleWhereClauses(testCase);
        testFullTextFunctionMultipleFullTextFunctions(testCase);
        testFullTextFunctionWithNonPushableConjunction(testCase);
        testFullTextFunctionWithPushableConjunction(testCase);
        testFullTextFunctionWithNonPushableDisjunction(testCase);
        testFullTextFunctionWithPushableDisjunction(testCase);
        testFullTextFunctionWithPushableDisjunction(testCase);
        testMultipleFullTextFunctionFilterPushdown(testCase);
        testFullTextFunctionsDisjunctionPushdown(testCase);
        testFullTextFunctionsDisjunctionWithFiltersPushdown(testCase);
        testFullTextFunctionWithStatsWherePushable(testCase);
        testFullTextFunctionWithStatsPushableAndNonPushableCondition(testCase);
        testFullTextFunctionStatsWithNonPushableCondition(testCase);
        testFullTextFunctionWithStatsBy(testCase);
    }

    private void testBasicFullTextFunction(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | where %s
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var esQuery = as(field.child(), EsQueryExec.class);
        assertThat(as(esQuery.limit(), Literal.class).value(), is(1000));
        var expected = testCase.queryBuilder();
        assertEquals(expected.toString(), esQuery.query().toString());
    }

    private void testFullTextFunctionWithFunctionsPushedToLucene(FullTextFunctionTestCase testCase) {
        String queryText = String.format(Locale.ROOT, """
            from test
            | where %s and cidr_match(ip, "127.0.0.1/32")
            """, testCase.esqlQuery());
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));

        Source filterSource = new Source(2, testCase.esqlQuery().length() + 13, "cidr_match(ip, \"127.0.0.1/32\")");
        var terms = wrapWithSingleQuery(queryText, unscore(termsQuery("ip", "127.0.0.1/32")), "ip", filterSource);
        var queryBuilder = testCase.queryBuilder();
        var expected = boolQuery().must(queryBuilder).must(terms);
        assertEquals(expected.toString(), query.query().toString());
    }

    private void testFullTextFunctionConjunctionWhereOperands(FullTextFunctionTestCase testCase) {
        String queryText = String.format(Locale.ROOT, """
            from test
            | where %s and integer > 10010
            """, testCase.esqlQuery());
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));

        Source filterSource = new Source(2, testCase.esqlQuery().length() + 13, "integer > 10000");
        var range = wrapWithSingleQuery(queryText, unscore(rangeQuery("integer").gt(10010)), "integer", filterSource);
        var queryBuilder = testCase.queryBuilder();
        var expected = boolQuery().must(queryBuilder).must(range);
        assertEquals(expected.toString(), query.query().toString());
    }

    private void testFullTextFunctionMultipleFullTextFunctions(FullTextFunctionTestCase testCase) {
        FullTextFunctionTestCase second = randomFullTextFunctionTestCase();

        String queryText = String.format(Locale.ROOT, """
            from test
            | where %s and %s
            """, testCase.esqlQuery(), second.esqlQuery());
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));

        var queryBuiilderLeft = testCase.queryBuilder();
        var queryBuilderRight = second.queryBuilder();
        var expected = boolQuery().must(queryBuiilderLeft).must(queryBuilderRight);
        assertEquals(expected.toString(), query.query().toString());
    }

    private void testFullTextFunctionMultipleWhereClauses(FullTextFunctionTestCase testCase) {
        String queryText = String.format(Locale.ROOT, """
            from test
            | where %s
            | where integer > 10010
            """, testCase.esqlQuery());
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));

        Source filterSource = new Source(3, 8, "integer > 10000");
        var range = wrapWithSingleQuery(queryText, unscore(rangeQuery("integer").gt(10010)), "integer", filterSource);
        var queryBuilder = testCase.queryBuilder();
        var expected = boolQuery().must(queryBuilder).must(range);
        assertEquals(expected.toString(), query.query().toString());
    }

    private void testFullTextFunctionWithNonPushableConjunction(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | where %s and length(text) > 10
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var filterLimit = as(fieldExtract.child(), LimitExec.class);
        var filter = as(filterLimit.child(), FilterExec.class);
        assertThat(filter.condition(), instanceOf(GreaterThan.class));
        var fieldFilterExtract = as(filter.child(), FieldExtractExec.class);
        var esQuery = as(fieldFilterExtract.child(), EsQueryExec.class);
        assertEquals(testCase.queryBuilder().toString(), esQuery.query().toString());
    }

    private void testFullTextFunctionWithPushableConjunction(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test metadata _score
            | where %s and integer > 10000
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var esQuery = as(fieldExtract.child(), EsQueryExec.class);
        Source source = new Source(2, testCase.esqlQuery().length() + 13, "integer > 10000");
        BoolQueryBuilder expected = new BoolQueryBuilder().must(testCase.queryBuilder())
            .must(wrapWithSingleQuery(query, unscore(rangeQuery("integer").gt(10000)), "integer", source));
        assertEquals(expected.toString(), esQuery.query().toString());
    }

    private void testFullTextFunctionWithNonPushableDisjunction(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | where %s or length(text) > 10
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var filterLimit = as(field.child(), LimitExec.class);
        var filter = as(filterLimit.child(), FilterExec.class);
        Or or = as(filter.condition(), Or.class);
        assertThat(or.left(), instanceOf(testCase.fullTextFunction()));
        assertThat(or.right(), instanceOf(GreaterThan.class));
        var fieldExtract = as(filter.child(), FieldExtractExec.class);
        assertThat(fieldExtract.child(), instanceOf(EsQueryExec.class));
    }

    private void testFullTextFunctionWithPushableDisjunction(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | where %s or integer > 10000
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var esQuery = as(fieldExtract.child(), EsQueryExec.class);
        Source source = new Source(2, testCase.esqlQuery().length() + 12, "integer > 10000");
        BoolQueryBuilder expected = new BoolQueryBuilder().should(testCase.queryBuilder())
            .should(wrapWithSingleQuery(query, unscore(rangeQuery("integer").gt(10000)), "integer", source));
        assertEquals(expected.toString(), esQuery.query().toString());
    }

    private FullTextFunctionTestCase randomFullTextFunctionTestCase() {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new MatchFunctionTestCase();
            case 1 -> new MatchOperatorTestCase();
            case 2 -> new KqlFunctionTestCase();
            case 3 -> new QueryStringFunctionTestCase();
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    private void testMultipleFullTextFunctionFilterPushdown(FullTextFunctionTestCase testCase) {
        FullTextFunctionTestCase second = randomFullTextFunctionTestCase();
        FullTextFunctionTestCase third = new MatchFunctionTestCase();

        String query = String.format(Locale.ROOT, """
            from test
            | where %s and %s
            | sort integer
            | where integer > 10000
            | eval description = concat("integer: ", to_str(integer), ", text: ", text, " ", keyword)
            | where %s
            """, testCase.esqlQuery(), second.esqlQuery(), third.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var eval = as(plan, EvalExec.class);
        var topNExec = as(eval.child(), TopNExec.class);
        var exchange = as(topNExec.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();

        Source filterSource = new Source(4, 8, "integer > 10000");
        var expectedLuceneQuery = new BoolQueryBuilder().must(testCase.queryBuilder())
            .must(second.queryBuilder())
            .must(wrapWithSingleQuery(query, unscore(rangeQuery("integer").gt(10000)), "integer", filterSource))
            .must(third.queryBuilder());
        assertEquals(expectedLuceneQuery.toString(), actualLuceneQuery.toString());
    }

    public void testFullTextFunctionsDisjunctionPushdown(FullTextFunctionTestCase testCase) {
        FullTextFunctionTestCase second = randomFullTextFunctionTestCase();
        FullTextFunctionTestCase third = randomFullTextFunctionTestCase();

        String query = String.format(Locale.ROOT, """
            from test
            | where (%s or %s) and %s
            | sort integer
            """, testCase.esqlQuery(), second.esqlQuery(), third.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));
        var topNExec = as(plan, TopNExec.class);
        var exchange = as(topNExec.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();
        var expectedLuceneQuery = new BoolQueryBuilder().must(
            new BoolQueryBuilder().should(testCase.queryBuilder()).should(second.queryBuilder())
        ).must(third.queryBuilder());
        assertEquals(expectedLuceneQuery.toString(), actualLuceneQuery.toString());
    }

    public void testFullTextFunctionsDisjunctionWithFiltersPushdown(FullTextFunctionTestCase testCase) {
        FullTextFunctionTestCase second = randomFullTextFunctionTestCase();

        String query = String.format(Locale.ROOT, """
            from test
            | where (%s or %s) and length(keyword) > 5
            | sort integer
            """, testCase.esqlQuery(), second.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));
        var topNExec = as(plan, TopNExec.class);
        var exchange = as(topNExec.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var secondTopNExec = as(fieldExtract.child(), TopNExec.class);
        var secondFieldExtract = as(secondTopNExec.child(), FieldExtractExec.class);
        var filterExec = as(secondFieldExtract.child(), FilterExec.class);
        var thirdFilterExtract = as(filterExec.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(thirdFilterExtract.child(), EsQueryExec.class).query();
        var expectedLuceneQuery = new BoolQueryBuilder().should(testCase.queryBuilder()).should(second.queryBuilder());
        assertEquals(expectedLuceneQuery.toString(), actualLuceneQuery.toString());
    }

    public void testFullTextFunctionWithStatsWherePushable(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | stats c = count(*) where %s
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exchange = as(agg.child(), ExchangeExec.class);
        var stats = as(exchange.child(), EsStatsQueryExec.class);
        QueryBuilder expected = testCase.queryBuilder();
        assertThat(stats.query().toString(), equalTo(expected.toString()));
    }

    public void testFullTextFunctionWithStatsPushableAndNonPushableCondition(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | where length(keyword) > 10
            | stats c = count(*) where %s
            """, testCase.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exchange = as(agg.child(), ExchangeExec.class);
        var aggExec = as(exchange.child(), AggregateExec.class);
        var filter = as(aggExec.child(), FilterExec.class);
        assertTrue(filter.condition() instanceof GreaterThan);
        var fieldExtract = as(filter.child(), FieldExtractExec.class);
        var esQuery = as(fieldExtract.child(), EsQueryExec.class);
        QueryBuilder expected = testCase.queryBuilder();
        assertThat(esQuery.query().toString(), equalTo(expected.toString()));
    }

    public void testFullTextFunctionStatsWithNonPushableCondition(FullTextFunctionTestCase testCase) {
        FullTextFunctionTestCase second = randomFullTextFunctionTestCase();

        String query = String.format(Locale.ROOT, """
            from test
            | stats c = count(*) where %s, d = count(*) where %s
            """, testCase.esqlQuery(), second.esqlQuery());
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, makeAnalyzer("mapping-all-types.json", new EnrichResolution()));

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var aggregates = agg.aggregates();
        assertThat(aggregates.size(), is(2));
        for (NamedExpression aggregate : aggregates) {
            var alias = as(aggregate, Alias.class);
            var count = as(alias.child(), Count.class);
            var fullTextFunction = as(count.filter(), FullTextFunction.class);
        }
        var exchange = as(agg.child(), ExchangeExec.class);
        var aggExec = as(exchange.child(), AggregateExec.class);
        aggExec.forEachDown(EsQueryExec.class, esQueryExec -> { assertNull(esQueryExec.query()); });
    }

    public void testFullTextFunctionWithStatsBy(FullTextFunctionTestCase testCase) {
        String query = String.format(Locale.ROOT, """
            from test
            | stats count(*) where %s by keyword
            """, testCase.esqlQuery());
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plannerOptimizer = new TestPlannerOptimizer(config, analyzer);
        var plan = plannerOptimizer.plan(query, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var grouping = as(agg.groupings().get(0), FieldAttribute.class);
        assertEquals("keyword", grouping.name());
        var aggregateAlias = as(agg.aggregates().get(0), Alias.class);
        assertEquals("count(*) where " + testCase.esqlQuery(), aggregateAlias.name());
        var count = as(aggregateAlias.child(), Count.class);
        var countFilter = as(count.filter(), testCase.fullTextFunction());
        var aggregateFieldAttr = as(agg.aggregates().get(1), FieldAttribute.class);
        assertEquals("keyword", aggregateFieldAttr.name());
        var exchange = as(agg.child(), ExchangeExec.class);
        var aggExec = as(exchange.child(), AggregateExec.class);
        aggExec.forEachDown(EsQueryExec.class, esQueryExec -> { assertNull(esQueryExec.query()); });
    }

    /**
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#415, byte{f}#416, constant_keyword-foo{f}#417, date{f}#418, date_nanos{f}#419,
     *   double{f}#420, float{f}#421, half_float{f}#422, integer{f}#424, ip{f}#425, keyword{f}#426, long{f}#427, scaled_float{f}#423,
     *   !semantic_text, short{f}#429, text{f}#430, unsigned_long{f}#428, version{f}#431, wildcard{f}#432], false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#415, byte{f}#416, constant_keyword-foo{f}#417, date{f}#418, date_nanos{f}#419,
     *     double{f}#420, float{f}#421, half_float{f}#422, integer{f}#424, ip{f}#425, keyword{f}#426, long{f}#427, scaled_float{f}#423,
     *     !semantic_text, short{f}#429, text{f}#430, unsigned_long{f}#428, version{f}#431, wildcard{f}#432]]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#415, byte{f}#416, consta..]
     *       \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#434], limit[1000], sort[] estimatedRowSize[412]
     */
    public void testConstantKeywordWithMatchingFilter() {
        String queryText = """
            from test
            | where `constant_keyword-foo` == "foo"
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, CONSTANT_K_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(as(query.limit(), Literal.class).value(), is(1000));
        assertNull(query.query());
    }

    /**
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, date_nanos{f}#8, double{f}#9,
     *    float{f}#10, half_float{f}#11, integer{f}#13, ip{f}#14, keyword{f}#15, long{f}#16, scaled_float{f}#12, !semantic_text,
     *    short{f}#18, text{f}#19, unsigned_long{f}#17, version{f}#20, wildcard{f}#21], false]
     *   \_LocalSourceExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, date_nanos{f}#8, double{f}#9,
     *     float{f}#10, half_float{f}#11, integer{f}#13, ip{f}#14, keyword{f}#15, long{f}#16, scaled_float{f}#12, !semantic_text,
     *     short{f}#18, text{f}#19, unsigned_long{f}#17, version{f}#20, wildcard{f}#21], EMPTY]
     */
    public void testConstantKeywordWithNonMatchingFilter() {
        String queryText = """
            from test
            | where `constant_keyword-foo` == "non-matching"
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, CONSTANT_K_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var source = as(exchange.child(), LocalSourceExec.class);
    }

    /**
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#6, byte{f}#7, constant_keyword-foo{r}#25, date{f}#9, date_nanos{f}#10, double{f}#1...
     *   \_ProjectExec[[!alias_integer, boolean{f}#6, byte{f}#7, constant_keyword-foo{r}#25, date{f}#9, date_nanos{f}#10, double{f}#1...
     *     \_FieldExtractExec[!alias_integer, boolean{f}#6, byte{f}#7, date{f}#9,
     *       \_LimitExec[1000[INTEGER]]
     *         \_FilterExec[constant_keyword-foo{r}#25 == [66 6f 6f][KEYWORD]]
     *           \_MvExpandExec[constant_keyword-foo{f}#8,constant_keyword-foo{r}#25]
     *             \_FieldExtractExec[constant_keyword-foo{f}#8]
     *               \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#26], limit[], sort[] estimatedRowSize[412]
     */
    public void testConstantKeywordExpandFilter() {
        String queryText = """
            from test
            | mv_expand `constant_keyword-foo`
            | where `constant_keyword-foo` == "foo"
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, CONSTANT_K_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var limit2 = as(fieldExtract.child(), LimitExec.class);
        var filter = as(limit2.child(), FilterExec.class);
        var expand = as(filter.child(), MvExpandExec.class);
        var field = as(expand.child(), FieldExtractExec.class); // MV_EXPAND is not optimized yet (it doesn't accept literals)
        as(field.child(), EsQueryExec.class);
    }

    /**
     * DissectExec[constant_keyword-foo{f}#8,Parser[pattern=%{bar}, appendSeparator=, ...
     * \_LimitExec[1000[INTEGER]]
     *   \_ExchangeExec[[!alias_integer, boolean{f}#6, byte{f}#7, constant_keyword-foo{f}#8, date{f}#9, date_nanos{f}#10, double{f}#11...
     *     \_ProjectExec[[!alias_integer, boolean{f}#6, byte{f}#7, constant_keyword-foo{f}#8, date{f}#9, date_nanos{f}#10, double{f}#11...
     *       \_FieldExtractExec[!alias_integer, boolean{f}#6, byte{f}#7, constant_k..]
     *         \_EsQueryExec[test], indexMode[standard], query[][_doc{f}#25], limit[1000], sort[] estimatedRowSize[462]
     */
    public void testConstantKeywordDissectFilter() {
        String queryText = """
            from test
            | dissect `constant_keyword-foo` "%{bar}"
            | where `constant_keyword-foo` == "foo"
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json", new EnrichResolution());
        var plan = plannerOptimizer.plan(queryText, CONSTANT_K_STATS, analyzer);

        var dissect = as(plan, DissectExec.class);
        var limit = as(dissect.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertNull(query.query());
    }

    private QueryBuilder wrapWithSingleQuery(String query, QueryBuilder inner, String fieldName, Source source) {
        return FilterTests.singleValueQuery(query, inner, fieldName, source);
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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static KqlQueryBuilder kqlQueryBuilder(String query) {
        return new KqlQueryBuilder(query);
    }

    /**
     * Base class for random full text function test cases.
     * Each test case should implement the queryBuilder and query methods to return the expected QueryBuilder and query string.
     */
    private abstract class FullTextFunctionTestCase {
        private final Class<? extends FullTextFunction> fullTextFunction;
        private final Object queryString;
        private final String fieldName;

        protected FullTextFunctionTestCase(Class<? extends FullTextFunction> fullTextFunction, String fieldName, Object queryString) {
            this.fullTextFunction = fullTextFunction;
            this.fieldName = fieldName;
            this.queryString = queryString;
        }

        protected FullTextFunctionTestCase(Class<? extends FullTextFunction> fullTextFunction) {
            this(fullTextFunction, randomFrom("text", "keyword"), randomAlphaOfLengthBetween(1, 10));
        }

        public Class<? extends FullTextFunction> fullTextFunction() {
            return fullTextFunction;
        }

        public Object queryString() {
            return queryString;
        }

        public String fieldName() {
            return fieldName;
        }

        /**
         * Returns the expected QueryBuilder for the full text function.
         */
        public abstract QueryBuilder queryBuilder();

        /**
         * Returns the query as a string representation that can be used in the ESQL query.
         * @return
         */
        public abstract String esqlQuery();
    }

    private class MatchFunctionTestCase extends FullTextFunctionTestCase {
        MatchFunctionTestCase() {
            super(Match.class);
        }

        @Override
        public QueryBuilder queryBuilder() {
            return new MatchQueryBuilder(fieldName(), queryString()).lenient(true);
        }

        @Override
        public String esqlQuery() {
            return "match(" + fieldName() + ", \"" + queryString() + "\")";
        }
    }

    private class MatchOperatorTestCase extends FullTextFunctionTestCase {
        MatchOperatorTestCase() {
            super(MatchOperator.class);
        }

        @Override
        public QueryBuilder queryBuilder() {
            return new MatchQueryBuilder(fieldName(), queryString()).lenient(true);
        }

        @Override
        public String esqlQuery() {
            return fieldName() + ": \"" + queryString() + "\"";
        }
    }

    private class KqlFunctionTestCase extends FullTextFunctionTestCase {
        KqlFunctionTestCase() {
            super(Kql.class);
        }

        @Override
        public QueryBuilder queryBuilder() {
            return new KqlQueryBuilder(fieldName() + ": " + queryString());
        }

        @Override
        public String esqlQuery() {
            return "kql(\"" + fieldName() + ": " + queryString() + "\")";
        }
    }

    private class QueryStringFunctionTestCase extends FullTextFunctionTestCase {
        QueryStringFunctionTestCase() {
            super(QueryString.class);
        }

        @Override
        public QueryBuilder queryBuilder() {
            return new QueryStringQueryBuilder(fieldName() + ": " + queryString());
        }

        @Override
        public String esqlQuery() {
            return "qstr(\"" + fieldName() + ": " + queryString() + "\")";
        }
    }
}
