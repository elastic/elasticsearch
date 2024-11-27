/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.XPackLicenseState;
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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ExtractAggregateCommonFilter;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class LocalPhysicalPlanOptimizerTests extends MapperServiceTestCase {

    private static final String PARAM_FORMATTING = "%1$s";

    /**
     * Estimated size of a keyword field in bytes.
     */
    private static final int KEYWORD_EST = EstimatesRowSize.estimateSize(DataType.KEYWORD);

    private TestPlannerOptimizer plannerOptimizer;
    private final Configuration config;
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
            new AnalyzerContext(config, new EsqlFunctionRegistry(), getIndexResult, enrichResolution),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L))
        );
    }

    private Analyzer makeAnalyzer(String mappingFileName) {
        return makeAnalyzer(mappingFileName, new EnrichResolution());
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
        var plan = plannerOptimizer.plan("""
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
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        Source source = new Source(2, 8, "salary > 1000");
        var exists = QueryBuilders.existsQuery("salary");
        assertThat(stat.query(), is(exists));
        var range = wrapWithSingleQuery(query, QueryBuilders.rangeQuery("salary").gt(1000), "salary", source);
        var expected = QueryBuilders.boolQuery().must(range).must(exists);
        assertThat(expected.toString(), is(esStatsQuery.query().toString()));
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
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var source = ((SingleValueQuery.Builder) esStatsQuery.query()).source();
        var expected = wrapWithSingleQuery(query, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", source);
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
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(config)) {
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
        var analyzer = makeAnalyzer("mapping-default.json");
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
                                "boost": 1.0
                            }
                        },
                        {
                            "esql_single_value": {
                                "field": "emp_no",
                                "next": {
                                    "range": {
                                        "emp_no": {
                                            "lt": 10042,
                                            "boost": 1.0
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

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"query_string":{"query":"last_name: Smith","fields":[]}}]
     */
    public void testQueryStringFunction() {
        var plan = plannerOptimizer.plan("""
            from test
            | where qstr("last_name: Smith")
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));
        var expected = QueryBuilders.queryStringQuery("last_name: Smith");
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418],false]
     *   \_ProjectExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418]]
     *     \_FieldExtractExec[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"query_string":{"query":"last_name: Smith","fields":[]}}
     *        ,{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},"source":"emp_no > 10010"}}],
     *        "boost":1.0}}][_doc{f}#1423], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testQueryStringFunctionConjunctionWhereOperands() {
        String queryText = """
            from test
            | where qstr("last_name: Smith") and emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 37, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var queryString = QueryBuilders.queryStringQuery("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(queryString).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16],
     * false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#4, byte{f}#5, constant_k..]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"query_string":{"query":"last_name: Smith","fields":[]}},{
     *       "esql_single_value":{"field":"ip","next":{"terms":{"ip":["127.0.0.1/32"],"boost":1.0}},
     *       "source":"cidr_match(ip, \"127.0.0.1/32\")@2:38"}}],"boost":1.0}}][_doc{f}#21], limit[1000], sort[] estimatedRowSize[354]
     */
    public void testQueryStringFunctionWithFunctionsPushedToLucene() {
        String queryText = """
            from test
            | where qstr("last_name: Smith") and cidr_match(ip, "127.0.0.1/32")
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json");
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 37, "cidr_match(ip, \"127.0.0.1/32\")");
        var terms = wrapWithSingleQuery(queryText, QueryBuilders.termsQuery("ip", "127.0.0.1/32"), "ip", filterSource);
        var queryString = QueryBuilders.queryStringQuery("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(queryString).must(terms);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162],false]
     *   \_ProjectExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162]]
     *     \_FieldExtractExec[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#]
     *       \_EsQueryExec[test], indexMode[standard],
     *       query[{"bool":{"must":[{"query_string":{"query":"last_name: Smith","fields":[]}},
     *       {"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},"source":"emp_no > 10010@3:9"}}],
     *       "boost":1.0}}][_doc{f}#1167], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testQueryStringFunctionMultipleWhereClauses() {
        String queryText = """
            from test
            | where qstr("last_name: Smith")
            | where emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(3, 8, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var queryString = QueryBuilders.queryStringQuery("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(queryString).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":
     *       {"must":[{"query_string":{"query":"last_name: Smith","fields":[]}},
     *       {"query_string":{"query":"emp_no: [10010 TO *]","fields":[]}}],"boost":1.0}}]
     */
    public void testQueryStringFunctionMultipleQstrClauses() {
        String queryText = """
            from test
            | where qstr("last_name: Smith") and qstr("emp_no: [10010 TO *]")
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        var queryStringLeft = QueryBuilders.queryStringQuery("last_name: Smith");
        var queryStringRight = QueryBuilders.queryStringQuery("emp_no: [10010 TO *]");
        var expected = QueryBuilders.boolQuery().must(queryStringLeft).must(queryStringRight);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"match":{"last_name":{"query":"Smith"}}}]
     */
    public void testMatchFunction() {
        var plan = plannerOptimizer.plan("""
            from test
            | where match(last_name, "Smith")
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));
        var expected = QueryBuilders.matchQuery("last_name", "Smith");
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418],false]
     *   \_ProjectExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418]]
     *     \_FieldExtractExec[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#]
     *       \EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"match":{"last_name":{"query":"Smith"}}},
     *       {"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},
     *       "source":"emp_no > 10010@2:39"}}],"boost":1.0}}][_doc{f}#14], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testMatchFunctionConjunctionWhereOperands() {
        String queryText = """
            from test
            | where match(last_name, "Smith") and emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 38, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var queryString = QueryBuilders.matchQuery("last_name", "Smith");
        var expected = QueryBuilders.boolQuery().must(queryString).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16],
     * false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#4, byte{f}#5, constant_k..]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"match":{"text":{"query":"beta"}}},
     *       {"esql_single_value":{"field":"ip","next":{"terms":{"ip":["127.0.0.1/32"],"boost":1.0}},
     *       "source":"cidr_match(ip, \"127.0.0.1/32\")@2:33"}}],"boost":1.0}}][_doc{f}#22], limit[1000], sort[] estimatedRowSize[354]
     */
    public void testMatchFunctionWithFunctionsPushedToLucene() {
        String queryText = """
            from test
            | where match(text, "beta") and cidr_match(ip, "127.0.0.1/32")
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json");
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 32, "cidr_match(ip, \"127.0.0.1/32\")");
        var terms = wrapWithSingleQuery(queryText, QueryBuilders.termsQuery("ip", "127.0.0.1/32"), "ip", filterSource);
        var queryString = QueryBuilders.matchQuery("text", "beta");
        var expected = QueryBuilders.boolQuery().must(queryString).must(terms);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162],false]
     *   \_ProjectExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162]]
     *     \_FieldExtractExec[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"match":{"last_name":{"query":"Smith"}}},
     *       {"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},
     *       "source":"emp_no > 10010@3:9"}}],"boost":1.0}}][_doc{f}#14], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testMatchFunctionMultipleWhereClauses() {
        String queryText = """
            from test
            | where match(last_name, "Smith")
            | where emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(3, 8, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var queryString = QueryBuilders.matchQuery("last_name", "Smith");
        var expected = QueryBuilders.boolQuery().must(queryString).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"match":{"last_name":{"query":"Smith"}}},
     *       {"match":{"first_name":{"query":"John"}}}],"boost":1.0}}][_doc{f}#14], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testMatchFunctionMultipleMatchClauses() {
        String queryText = """
            from test
            | where match(last_name, "Smith") and match(first_name, "John")
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        var queryStringLeft = QueryBuilders.matchQuery("last_name", "Smith");
        var queryStringRight = QueryBuilders.matchQuery("first_name", "John");
        var expected = QueryBuilders.boolQuery().must(queryStringLeft).must(queryStringRight);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"kql":{"query":"last_name: Smith"}}]
     */
    public void testKqlFunction() {
        // Skip test if the kql function is not enabled.
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());

        var plan = plannerOptimizer.plan("""
            from test
            | where kql("last_name: Smith")
            """, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));
        var expected = kqlQueryBuilder("last_name: Smith");
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418],false]
     *   \_ProjectExec[[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#1414, gender{f}#1415, job{f}#1420, job.raw{f}#1421, langua
     * ges{f}#1416, last_name{f}#1417, long_noidx{f}#1422, salary{f}#1418]]
     *     \_FieldExtractExec[_meta_field{f}#1419, emp_no{f}#1413, first_name{f}#]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"kql":{"query":"last_name: Smith"}}
     *        ,{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},"source":"emp_no > 10010"}}],
     *        "boost":1.0}}][_doc{f}#1423], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testKqlFunctionConjunctionWhereOperands() {
        // Skip test if the kql function is not enabled.
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());

        String queryText = """
            from test
            | where kql("last_name: Smith") and emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 36, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var kqlQuery = kqlQueryBuilder("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(kqlQuery).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16],
     * false]
     *   \_ProjectExec[[!alias_integer, boolean{f}#4, byte{f}#5, constant_keyword-foo{f}#6, date{f}#7, double{f}#8, float{f}#9, half_
     * float{f}#10, integer{f}#12, ip{f}#13, keyword{f}#14, long{f}#15, scaled_float{f}#11, short{f}#17, text{f}#18, unsigned_long{f}#16]
     *     \_FieldExtractExec[!alias_integer, boolean{f}#4, byte{f}#5, constant_k..]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"kql":{"query":"last_name: Smith"}},{
     *       "esql_single_value":{"field":"ip","next":{"terms":{"ip":["127.0.0.1/32"],"boost":1.0}},
     *       "source":"cidr_match(ip, \"127.0.0.1/32\")@2:38"}}],"boost":1.0}}][_doc{f}#21], limit[1000], sort[] estimatedRowSize[354]
     */
    public void testKqlFunctionWithFunctionsPushedToLucene() {
        // Skip test if the kql function is not enabled.
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());

        String queryText = """
            from test
            | where kql("last_name: Smith") and cidr_match(ip, "127.0.0.1/32")
            """;
        var analyzer = makeAnalyzer("mapping-all-types.json");
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS, analyzer);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(2, 36, "cidr_match(ip, \"127.0.0.1/32\")");
        var terms = wrapWithSingleQuery(queryText, QueryBuilders.termsQuery("ip", "127.0.0.1/32"), "ip", filterSource);
        var kqlQuery = kqlQueryBuilder("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(kqlQuery).must(terms);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162],false]
     *   \_ProjectExec[[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#1158, gender{f}#1159, job{f}#1164, job.raw{f}#1165, langua
     * ges{f}#1160, last_name{f}#1161, long_noidx{f}#1166, salary{f}#1162]]
     *     \_FieldExtractExec[_meta_field{f}#1163, emp_no{f}#1157, first_name{f}#]
     *       \_EsQueryExec[test], indexMode[standard],
     *       query[{"bool":{"must":[{"kql":{"query":"last_name: Smith"}},
     *       {"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10010,"boost":1.0}}},"source":"emp_no > 10010@3:9"}}],
     *       "boost":1.0}}][_doc{f}#1167], limit[1000], sort[] estimatedRowSize[324]
     */
    public void testKqlFunctionMultipleWhereClauses() {
        // Skip test if the kql function is not enabled.
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());

        String queryText = """
            from test
            | where kql("last_name: Smith")
            | where emp_no > 10010
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        Source filterSource = new Source(3, 8, "emp_no > 10000");
        var range = wrapWithSingleQuery(queryText, QueryBuilders.rangeQuery("emp_no").gt(10010), "emp_no", filterSource);
        var kqlQuery = kqlQueryBuilder("last_name: Smith");
        var expected = QueryBuilders.boolQuery().must(kqlQuery).must(range);
        assertThat(query.query().toString(), is(expected.toString()));
    }

    /**
     * Expecting
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7],false]
     *   \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, job{f}#9, job.raw{f}#10, languages{f}#5, last_na
     * me{f}#6, long_noidx{f}#11, salary{f}#7]]
     *     \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen]
     *       \_EsQueryExec[test], indexMode[standard], query[{"bool": {"must":[
     *       {"kql":{"query":"last_name: Smith"}},
     *       {"kql":{"query":"emp_no > 10010"}}],"boost":1.0}}]
     */
    public void testKqlFunctionMultipleKqlClauses() {
        // Skip test if the kql function is not enabled.
        assumeTrue("kql function capability not available", EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled());

        String queryText = """
            from test
            | where kql("last_name: Smith") and kql("emp_no > 10010")
            """;
        var plan = plannerOptimizer.plan(queryText, IS_SV_STATS);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var field = as(project.child(), FieldExtractExec.class);
        var query = as(field.child(), EsQueryExec.class);
        assertThat(query.limit().fold(), is(1000));

        var kqlQueryLeft = kqlQueryBuilder("last_name: Smith");
        var kqlQueryRight = kqlQueryBuilder("emp_no > 10010");
        var expected = QueryBuilders.boolQuery().must(kqlQueryLeft).must(kqlQueryRight);
        assertThat(query.query().toString(), is(expected.toString()));
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
        var plan = plannerOptimizer.plan("from test | where emp_no is not null");

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
        var plan = plannerOptimizer.plan("from test | where emp_no is null");

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
        var plan = plannerOptimizer.plan(
            String.format(Locale.ROOT, "from test | where %s is not null | stats count(%s)", textField, textField)
        );

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
        var plan = plannerOptimizer.plan(String.format(Locale.ROOT, "from test | where %s is null", textField, textField));

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
        assertThat(Expressions.names(esStatsQuery.output()), contains("count", "seen"));
        var stat = as(esStatsQuery.stats().get(0), Stat.class);
        assertThat(stat.query(), is(QueryBuilders.existsQuery("job")));
    }

    private record OutOfRangeTestCase(String fieldName, String tooLow, String tooHigh) {};

    private static final String LT = "<";
    private static final String LTE = "<=";
    private static final String GT = ">";
    private static final String GTE = ">=";
    private static final String EQ = "==";
    private static final String NEQ = "!=";

    public void testOutOfRangeFilterPushdown() {
        var allTypeMappingAnalyzer = makeAnalyzer("mapping-all-types.json");

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
        );

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

    public void testOutOfRangeFilterPushdownWithFloatAndHalfFloat() {
        var allTypeMappingAnalyzer = makeAnalyzer("mapping-all-types.json");

        String smallerThanFloat = String.valueOf(randomDoubleBetween(-Double.MAX_VALUE, -Float.MAX_VALUE - 1d, true));
        String largerThanFloat = String.valueOf(randomDoubleBetween(Float.MAX_VALUE + 1d, Double.MAX_VALUE, true));

        List<OutOfRangeTestCase> cases = List.of(
            new OutOfRangeTestCase("float", smallerThanFloat, largerThanFloat),
            new OutOfRangeTestCase("half_float", smallerThanFloat, largerThanFloat)
        );

        for (OutOfRangeTestCase testCase : cases) {
            for (var value : List.of(testCase.tooHigh, testCase.tooLow)) {
                for (String predicate : List.of(LT, LTE, GT, GTE, EQ, NEQ)) {
                    String comparison = testCase.fieldName + predicate + value;
                    var query = "from test | where " + comparison;

                    Source expectedSource = new Source(1, 18, comparison);

                    logger.info("Query: " + query);
                    EsQueryExec actualQueryExec = doTestOutOfRangeFilterPushdown(query, allTypeMappingAnalyzer);

                    assertThat(actualQueryExec.query(), is(instanceOf(SingleValueQuery.Builder.class)));
                    var actualLuceneQuery = (SingleValueQuery.Builder) actualQueryExec.query();
                    assertThat(actualLuceneQuery.field(), equalTo(testCase.fieldName));
                    assertThat(actualLuceneQuery.source(), equalTo(expectedSource));

                    QueryBuilder actualInnerLuceneQuery = actualLuceneQuery.next();

                    if (predicate.equals(EQ)) {
                        QueryBuilder expectedInnerQuery = QueryBuilders.termQuery(testCase.fieldName, Double.parseDouble(value));
                        assertThat(actualInnerLuceneQuery, equalTo(expectedInnerQuery));
                    } else if (predicate.equals(NEQ)) {
                        QueryBuilder expectedInnerQuery = QueryBuilders.boolQuery()
                            .mustNot(QueryBuilders.termQuery(testCase.fieldName, Double.parseDouble(value)));
                        assertThat(actualInnerLuceneQuery, equalTo(expectedInnerQuery));
                    } else { // one of LT, LTE, GT, GTE
                        assertTrue(actualInnerLuceneQuery instanceof RangeQueryBuilder);
                        assertThat(((RangeQueryBuilder) actualInnerLuceneQuery).fieldName(), equalTo(testCase.fieldName));
                    }
                }
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

        var plan = plannerOptimizer.plan("from test", stats);
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
    public void testSingleMatchFilterPushdown() {
        var plan = plannerOptimizer.plan("""
            from test
            | where first_name:"Anna"
            """);

        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();

        var expectedLuceneQuery = new MatchQueryBuilder("first_name", "Anna");
        assertThat(actualLuceneQuery, equalTo(expectedLuceneQuery));
    }

    /**
     * Expects
     * EvalExec[[CONCAT([65 6d 70 5f 6e 6f 3a 20][KEYWORD],TOSTRING(emp_no{f}#12),[2c 20 6e 61 6d 65 3a 20][KEYWORD],first_nam
     * e{f}#13,[20][KEYWORD],last_name{f}#16) AS description]]
     * \_TopNExec[[Order[emp_no{f}#12,ASC,LAST]],1000[INTEGER],50]
     *   \_ExchangeExec[[],false]
     *     \_ProjectExec[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, job{f}#19, job.raw{f}#20, languages{f}#15, l
     * ast_name{f}#16, long_noidx{f}#21, salary{f}#17]]
     *       \_FieldExtractExec[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *         \_EsQueryExec[test], indexMode[standard], query[{"bool":{"must":[{"bool":{"should":[{"match":{"first_name":{"query":"Anna"}}}
     *         ,{"match":{"first_name":{"query":"Anneke"}}}],"boost":1.0}},{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":
     *         {"gt":10000,"boost":1.0}}},"source":"emp_no > 10000@4:9"}},{"match":{"last_name":{"query":"Xinglin"}}}],"boost":1.0}}]
     *         [_doc{f}#22], limit[1000], sort[[FieldSort[field=emp_no{f}#12, direction=ASC, nulls=LAST]]] estimatedRowSize[336]
     */
    public void testMultipleMatchFilterPushdown() {
        String query = """
            from test
            | where first_name:"Anna" and first_name:"Anneke"
            | sort emp_no
            | where emp_no > 10000
            | eval description = concat("emp_no: ", to_str(emp_no), ", name: ", first_name, " ", last_name)
            | where last_name:"Xinglin"
            """;
        var plan = plannerOptimizer.plan(query);

        var eval = as(plan, EvalExec.class);
        var topNExec = as(eval.child(), TopNExec.class);
        var exchange = as(topNExec.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var actualLuceneQuery = as(fieldExtract.child(), EsQueryExec.class).query();

        Source filterSource = new Source(4, 8, "emp_no > 10000");
        var expectedLuceneQuery = new BoolQueryBuilder().must(new MatchQueryBuilder("first_name", "Anna"))
            .must(new MatchQueryBuilder("first_name", "Anneke"))
            .must(wrapWithSingleQuery(query, QueryBuilders.rangeQuery("emp_no").gt(10000), "emp_no", filterSource))
            .must(new MatchQueryBuilder("last_name", "Xinglin"));
        assertThat(actualLuceneQuery.toString(), is(expectedLuceneQuery.toString()));
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
}
