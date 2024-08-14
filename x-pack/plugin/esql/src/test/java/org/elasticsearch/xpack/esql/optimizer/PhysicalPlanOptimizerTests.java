/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.FieldSort;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querydsl.query.SpatialRelatesQuery;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.statsForMissingField;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.assertSerialization;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.name;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.names;
import static org.elasticsearch.xpack.esql.core.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.parser.ExpressionBuilder.MAX_EXPRESSION_DEPTH;
import static org.elasticsearch.xpack.esql.parser.LogicalPlanBuilder.MAX_QUERY_DEPTH;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.FINAL;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.PARTIAL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PhysicalPlanOptimizerTests extends ESTestCase {

    private static final String PARAM_FORMATTING = "%1$s";

    /**
     * Estimated size of a keyword field in bytes.
     */
    private static final int KEYWORD_EST = EstimatesRowSize.estimateSize(DataType.KEYWORD);

    private EsqlParser parser;
    private LogicalPlanOptimizer logicalOptimizer;
    private PhysicalPlanOptimizer physicalPlanOptimizer;
    private Mapper mapper;
    private TestDataSource testData;
    private int allFieldRowSize;    // TODO: Move this into testDataSource so tests that load other indexes can also assert on this
    private TestDataSource airports;
    private TestDataSource airportsWeb;
    private TestDataSource countriesBbox;
    private TestDataSource countriesBboxWeb;

    private final Configuration config;

    private record TestDataSource(Map<String, EsField> mapping, EsIndex index, Analyzer analyzer) {}

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

    public PhysicalPlanOptimizerTests(String name, Configuration config) {
        this.config = config;
    }

    @Before
    public void init() {
        parser = new EsqlParser();
        logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG));
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        mapper = new Mapper(functionRegistry);
        var enrichResolution = setupEnrichResolution();
        // Most tests used data from the test index, so we load it here, and use it in the plan() function.
        this.testData = makeTestDataSource("test", "mapping-basic.json", functionRegistry, enrichResolution);
        allFieldRowSize = testData.mapping.values()
            .stream()
            .mapToInt(
                f -> (EstimatesRowSize.estimateSize(f.getDataType().widenSmallNumeric()) + f.getProperties()
                    .values()
                    .stream()
                    // check one more level since the mapping contains TEXT fields with KEYWORD multi-fields
                    .mapToInt(x -> EstimatesRowSize.estimateSize(x.getDataType().widenSmallNumeric()))
                    .sum())
            )
            .sum();

        // Some tests use data from the airports and countries indexes, so we load that here, and use it in the plan(q, airports) function.
        this.airports = makeTestDataSource("airports", "mapping-airports.json", functionRegistry, enrichResolution);
        this.airportsWeb = makeTestDataSource("airports_web", "mapping-airports_web.json", functionRegistry, enrichResolution);
        this.countriesBbox = makeTestDataSource("countriesBbox", "mapping-countries_bbox.json", functionRegistry, enrichResolution);
        this.countriesBboxWeb = makeTestDataSource(
            "countriesBboxWeb",
            "mapping-countries_bbox_web.json",
            functionRegistry,
            enrichResolution
        );
    }

    TestDataSource makeTestDataSource(
        String indexName,
        String mappingFileName,
        EsqlFunctionRegistry functionRegistry,
        EnrichResolution enrichResolution
    ) {
        Map<String, EsField> mapping = loadMapping(mappingFileName);
        EsIndex index = new EsIndex(indexName, mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(index);
        Analyzer analyzer = new Analyzer(new AnalyzerContext(config, functionRegistry, getIndexResult, enrichResolution), TEST_VERIFIER);
        return new TestDataSource(mapping, index, analyzer);
    }

    private static EnrichResolution setupEnrichResolution() {
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
        enrichResolution.addResolvedPolicy(
            "city_boundaries",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "city_boundary",
                EnrichPolicy.GEO_MATCH_TYPE,
                List.of("city", "airport", "region", "city_boundary"),
                Map.of("", "airport_city_boundaries"),
                Map.ofEntries(
                    Map.entry("city", new EsField("city", DataType.KEYWORD, Map.of(), true)),
                    Map.entry("airport", new EsField("airport", DataType.TEXT, Map.of(), false)),
                    Map.entry("region", new EsField("region", DataType.TEXT, Map.of(), false)),
                    Map.entry("city_boundary", new EsField("city_boundary", DataType.GEO_SHAPE, Map.of(), false))
                )
            )
        );
        enrichResolution.addResolvedPolicy(
            "departments",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "employee_id",
                EnrichPolicy.MATCH_TYPE,
                List.of("department"),
                Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2"),
                Map.of("department", new EsField("department", DataType.KEYWORD, Map.of(), true))
            )
        );
        enrichResolution.addResolvedPolicy(
            "departments",
            Enrich.Mode.COORDINATOR,
            new ResolvedEnrichPolicy(
                "employee_id",
                EnrichPolicy.MATCH_TYPE,
                List.of("department"),
                Map.of("", ".enrich-departments-3"),
                Map.of("department", new EsField("department", DataType.KEYWORD, Map.of(), true))
            )
        );
        enrichResolution.addResolvedPolicy(
            "departments",
            Enrich.Mode.REMOTE,
            new ResolvedEnrichPolicy(
                "employee_id",
                EnrichPolicy.MATCH_TYPE,
                List.of("department"),
                Map.of("cluster_1", ".enrich-departments-2"),
                Map.of("department", new EsField("department", DataType.KEYWORD, Map.of(), true))
            )
        );
        enrichResolution.addResolvedPolicy(
            "supervisors",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "department",
                EnrichPolicy.MATCH_TYPE,
                List.of("supervisor"),
                Map.of("", ".enrich-supervisors-a", "cluster_1", ".enrich-supervisors-b"),
                Map.of("supervisor", new EsField("supervisor", DataType.KEYWORD, Map.of(), true))
            )
        );
        enrichResolution.addResolvedPolicy(
            "supervisors",
            Enrich.Mode.COORDINATOR,
            new ResolvedEnrichPolicy(
                "department",
                EnrichPolicy.MATCH_TYPE,
                List.of("supervisor"),
                Map.of("", ".enrich-supervisors-c"),
                Map.of("supervisor", new EsField("supervisor", DataType.KEYWORD, Map.of(), true))
            )
        );
        enrichResolution.addResolvedPolicy(
            "supervisors",
            Enrich.Mode.REMOTE,
            new ResolvedEnrichPolicy(
                "department",
                EnrichPolicy.MATCH_TYPE,
                List.of("supervisor"),
                Map.of("cluster_1", ".enrich-supervisors-b"),
                Map.of("supervisor", new EsField("supervisor", DataType.KEYWORD, Map.of(), true))
            )
        );
        return enrichResolution;
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

        assertEquals(
            Sets.difference(allFields(testData.mapping), Set.of("emp_no")),
            Sets.newHashSet(names(restExtract.attributesToExtract()))
        );
        assertEquals(Set.of("emp_no"), Sets.newHashSet(names(extract.attributesToExtract())));

        var query = as(extract.child(), EsQueryExec.class);
        assertThat(query.estimatedRowSize(), equalTo(Integer.BYTES + allFieldRowSize));
    }

    private Set<String> allFields(Map<String, EsField> mapping) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String key = entry.getKey();
            result.add(key);
            for (Map.Entry<String, EsField> sub : entry.getValue().getProperties().entrySet()) {
                result.add(key + "." + sub.getKey());
            }
        }
        return result;
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

        assertEquals(
            Sets.difference(allFields(testData.mapping), Set.of("emp_no")),
            Sets.newHashSet(names(restExtract.attributesToExtract()))
        );
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var query = source(extract.child());
        // An int for doc id and one for c
        assertThat(query.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SUM(salary{f}#882) AS x],FINAL,null]
     *   \_ExchangeExec[[sum{r}#887, seen{r}#888],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SUM(salary{f}#882) AS x]]
     * \_Filter[ROUND(emp_no{f}#877) > 10[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#883, emp_no{f}#877, first_name{f}#87..]]]
     */
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
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));

        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("salary"));

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var query = source(extract.child());
        assertThat(query.estimatedRowSize(), equalTo(Integer.BYTES * 3 /* for doc id, emp_no and salary*/));
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

        var filter = as(extract.child(), FilterExec.class);
        extract = as(filter.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var query = source(extract.child());
        // for doc ids, emp_no, salary
        int estimatedSize = Integer.BYTES * 3;
        assertThat(query.estimatedRowSize(), equalTo(estimatedSize));
    }

    /**
     * Expected
     * LimitExec[10000[INTEGER]]
     * \_AggregateExec[[],[AVG(salary{f}#14) AS x],FINAL]
     *   \_AggregateExec[[],[AVG(salary{f}#14) AS x],PARTIAL]
     *     \_FilterExec[ROUND(emp_no{f}#9) > 10[INTEGER]]
     *       \_TopNExec[[Order[last_name{f}#13,ASC,LAST]],10[INTEGER]]
     *         \_ExchangeExec[]
     *           \_ProjectExec[[salary{f}#14, first_name{f}#10, emp_no{f}#9, last_name{f}#13]]     -- project away _doc
     *             \_FieldExtractExec[salary{f}#14, first_name{f}#10, emp_no{f}#9, last_n..]       -- local field extraction
     *               \_EsQueryExec[test], query[][_doc{f}#16], limit[10], sort[[last_name]]
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
        assertThat(aggregateFinal.estimatedRowSize(), equalTo(Long.BYTES));

        var aggregatePartial = as(aggregateFinal.child(), AggregateExec.class);
        var filter = as(aggregatePartial.child(), FilterExec.class);
        var topN = as(filter.child(), TopNExec.class);

        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("salary", "emp_no", "last_name"));
        var source = source(extract.child());
        assertThat(source.limit(), is(topN.limit()));
        assertThat(source.sorts(), is(sorts(topN.order())));

        assertThat(source.limit(), is(l(10)));
        assertThat(source.sorts().size(), is(1));
        FieldSort order = source.sorts().get(0);
        assertThat(order.direction(), is(Order.OrderDirection.ASC));
        assertThat(name(order.field()), is("last_name"));
        // last name is keyword, salary, emp_no, doc id, segment, forwards and backwards doc id maps are all ints
        int estimatedSize = KEYWORD_EST + Integer.BYTES * 6;
        assertThat(source.estimatedRowSize(), equalTo(estimatedSize));
    }

    /**
     * Expected
     * EvalExec[[emp_no{f}#7 + 1[INTEGER] AS e, emp_no{f}#7 + 1[INTEGER] AS emp_no]]
     * \_LimitExec[10000[INTEGER]]
     *   \_ExchangeExec[[],false]
     *     \_ProjectExec[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, job{f}#14, job.raw{f}#15, languages{f}#10, last
     * _name{f}#11, salary{f}#12]]
     *       \_FieldExtractExec[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *         \_EsQueryExec[test], query[][_doc{f}#16], limit[10000], sort[] estimatedRowSize[324]
     */
    public void testExtractorMultiEvalWithDifferentNames() {
        var plan = physicalPlan("""
            from test
            | eval e = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);

        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary")
        );
    }

    /**
     * Expected
     * EvalExec[[emp_no{f}#7 + 1[INTEGER] AS emp_no, emp_no{r}#3 + 1[INTEGER] AS emp_no]]
     * \_LimitExec[10000[INTEGER]]
     *   \_ExchangeExec[[],false]
     *     \_ProjectExec[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, job{f}#14, job.raw{f}#15, languages{f}#10, last
     * _name{f}#11, salary{f}#12]]
     *       \_FieldExtractExec[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *         \_EsQueryExec[test], query[][_doc{f}#16], limit[10000], sort[] estimatedRowSize[324]
     */
    public void testExtractorMultiEvalWithSameName() {
        var plan = physicalPlan("""
            from test
            | eval emp_no = emp_no + 1
            | eval emp_no = emp_no + 1
            """);

        var optimized = optimizedPlan(plan);

        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        assertThat(
            names(extract.attributesToExtract()),
            contains("_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary")
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
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));
        assertThat(aggregate.groupings(), hasSize(1));

        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));
        assertThat(aggregate.groupings(), hasSize(1));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("salary")));

        var source = source(extract.child());
        // doc id and salary are ints. salary isn't extracted.
        // TODO salary kind of is extracted. At least sometimes it is. should it count?
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES * 2));
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
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));

        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("first_name")));

        var source = source(extract.child());
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES + KEYWORD_EST));
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
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));

        var exchange = asRemoteExchange(aggregate.child());
        aggregate = as(exchange.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES + KEYWORD_EST));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), equalTo(List.of("first_name")));

        var source = source(extract.child());
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES + KEYWORD_EST));
    }

    /**
     * Expects
     * EvalExec[[agg_emp{r}#4 + 7[INTEGER] AS x]]
     * \_LimitExec[1000[INTEGER]]
     *   \_AggregateExec[[],[SUM(emp_no{f}#8) AS agg_emp],FINAL,16]
     *     \_ExchangeExec[[sum{r}#18, seen{r}#19],true]
     *       \_AggregateExec[[],[SUM(emp_no{f}#8) AS agg_emp],PARTIAL,8]
     *         \_FieldExtractExec[emp_no{f}#8]
     *           \_EsQueryExec[test], query[{"exists":{"field":"emp_no","boost":1.0}}][_doc{f}#34], limit[], sort[] estimatedRowSize[8]
     */
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
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));

        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));

        var query = source(extract.child());
        assertThat(query.estimatedRowSize(), equalTo(Integer.BYTES * 2 /* for doc id, emp_no*/));
        assertThat(query.query(), is(existsQuery("emp_no")));
    }

    /**
     * Expects
     * EvalExec[[agg_emp{r}#4 + 7[INTEGER] AS x]]
     * \_LimitExec[1000[INTEGER]]
     *   \_AggregateExec[[],[SUM(emp_no{f}#8) AS agg_emp],FINAL,16]
     *     \_ExchangeExec[[sum{r}#18, seen{r}#19],true]
     *       \_AggregateExec[[],[SUM(emp_no{f}#8) AS agg_emp],PARTIAL,8]
     *         \_FieldExtractExec[emp_no{f}#8]
     *           \_EsQueryExec[test], query[{"exists":{"field":"emp_no","boost":1.0}}][_doc{f}#34], limit[], sort[] estimatedRowSize[8]
     */
    public void testQueryWithAggAfterEval() {
        var plan = physicalPlan("""
            from test
            | stats agg_emp = sum(emp_no)
            | eval x = agg_emp + 7
            """);

        var optimized = optimizedPlan(plan);
        var eval = as(optimized, EvalExec.class);
        var topLimit = as(eval.child(), LimitExec.class);
        var agg = as(topLimit.child(), AggregateExec.class);
        // sum and x are longs
        assertThat(agg.estimatedRowSize(), equalTo(Long.BYTES * 2));
        var exchange = asRemoteExchange(agg.child());
        var aggregate = as(exchange.child(), AggregateExec.class);
        // sum is long, x isn't calculated until the agg above
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));
        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no"));

        var query = source(extract.child());
        assertThat(query.estimatedRowSize(), equalTo(Integer.BYTES * 2 /* for doc id, emp_no*/));
        assertThat(query.query(), is(existsQuery("emp_no")));
    }

    public void testQueryForStatWithMultiAgg() {
        var plan = physicalPlan("""
            from test
            | stats agg_1 = sum(emp_no), agg_2 = min(salary)
            """);

        var stats = statsWithIndexedFields("emp_no", "salary");
        var optimized = optimizedPlan(plan, stats);
        var topLimit = as(optimized, LimitExec.class);
        var agg = as(topLimit.child(), AggregateExec.class);
        var exchange = asRemoteExchange(agg.child());
        var aggregate = as(exchange.child(), AggregateExec.class);
        // sum is long, x isn't calculated until the agg above
        var extract = as(aggregate.child(), FieldExtractExec.class);
        assertThat(names(extract.attributesToExtract()), contains("emp_no", "salary"));

        var query = source(extract.child());
        assertThat(query.estimatedRowSize(), equalTo(Integer.BYTES * 3 /* for doc id, emp_no, salary*/));
        assertThat(query.query(), is(boolQuery().should(existsQuery("emp_no")).should(existsQuery("salary"))));
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
        // no fields are added after the top n - so 0 here
        assertThat(topN.estimatedRowSize(), equalTo(0));

        var exchange = asRemoteExchange(topN.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topNLocal = as(extract.child(), TopNExec.class);
        // All fields except emp_no are loaded after this topn. We load an extra int for the doc and segment mapping.
        assertThat(topNLocal.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        var extractForEval = as(topNLocal.child(), FieldExtractExec.class);
        var eval = as(extractForEval.child(), EvalExec.class);
        var source = source(eval.child());
        // emp_no and nullsum are longs, doc id is an int
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES * 2 + Integer.BYTES));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));
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
        // two extra ints for forwards and backwards map
        assertThat(topNLocal.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2));

        var eval = as(topNLocal.child(), EvalExec.class);
        var source = source(eval.child());
        // nullsum and doc id are ints. we don't actually load emp_no here because we know we don't need it.
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES * 2));
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
        // an int for doc id, an int for segment id, two ints for doc id map, and int for emp_no.
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES * 5 + KEYWORD_EST));
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
        // extra ints for doc id and emp_no_10
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2));
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
            contains("_meta_field", "emp_no", "first_name", "gender", "job", "job.raw", "languages", "last_name", "long_noidx", "salary")
        );

        var source = source(extract.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));
    }

    /**
     * Expected
     * ProjectExec[[emp_no{f}#7, first_name{f}#8 AS x]]
     * \_TopNExec[[Order[emp_no{f}#7,ASC,LAST]],5[INTEGER],0]
     *   \_ExchangeExec[[],false]
     *     \_ProjectExec[[emp_no{f}#7, first_name{f}#8]]
     *       \_FieldExtractExec[emp_no{f}#7, first_name{f}#8]
     *         \_EsQueryExec[test], query[][_doc{f}#28], limit[5], sort[[FieldSort[field=emp_no{f}#7, direction=ASC, nulls=LAST]]]...
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
        assertThat(names(project.projections()), contains("emp_no", "first_name"));
        var extract = as(project.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);
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
        assertThat(order.direction(), is(Order.OrderDirection.ASC));
        assertThat(name(order.field()), is("salary"));
        // ints for doc id, segment id, forwards and backwards mapping, languages, and salary
        assertThat(source.estimatedRowSize(), equalTo(Integer.BYTES * 6));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));
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
        // an int for doc id and one for x
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        BoolQueryBuilder bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.must(), hasSize(2));
        var tqb = as(sv(bq.must().get(0), "last_name"), TermsQueryBuilder.class);
        assertThat(tqb.fieldName(), is("last_name"));
        assertThat(tqb.values(), is(List.of("Simmel", "Pettey")));
        var rqb = as(sv(bq.must().get(1), "salary"), RangeQueryBuilder.class);
        assertThat(rqb.fieldName(), is("salary"));
        assertThat(rqb.from(), is(60_000));
    }

    // `where "Pettey" in (last_name, "Simmel") or last_name == "Parto"` --> `where last_name in ("Pettey", "Parto")`
    // LimitExec[10000[INTEGER]]
    // \_ExchangeExec[]
    // \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, languages{f}#6, last_name{f}#7, salary{f}#8]]
    // \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
    // \_EsQueryExec[test],
    // query[{"esql_single_value":{"field":"last_name","next":{"terms":{"last_name":["Pettey","Parto"],"boost":1.0}}}}][_doc{f}#10],
    // limit[10000], sort[]
    public void testPushDownRecombinedIn() {
        var plan = physicalPlan("""
            from test
            | where "Pettey" in (last_name, "Simmel") or last_name == "Parto"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());

        var tqb = as(sv(source.query(), "last_name"), TermsQueryBuilder.class);
        assertThat(tqb.fieldName(), is("last_name"));
        assertThat(tqb.values(), is(List.of("Pettey", "Parto")));
    }

    /**
     * Expected:
     *  LimitExec[10000[INTEGER]]
     *  \_ExchangeExec[REMOTE_SOURCE]
     *    \_ExchangeExec[REMOTE_SINK]
     *      \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
     *        \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
     *          \_EsQueryExec[test], query[sv(not(emp_no IN (10010, 10011)))][_doc{f}#10],
     *                                   limit[10000], sort[]
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        var boolQuery = as(sv(source.query(), "emp_no"), BoolQueryBuilder.class);
        assertThat(boolQuery.mustNot(), hasSize(1));
        var termsQuery = as(boolQuery.mustNot().get(0), TermsQueryBuilder.class);
        assertThat(termsQuery.fieldName(), is("emp_no"));
        assertThat(termsQuery.values(), is(List.of(10010, 10011)));
    }

    /**
     * Expected:
     *  LimitExec[10000[INTEGER]]
     *  \_ExchangeExec[REMOTE_SOURCE]
     *    \_ExchangeExec[REMOTE_SINK]
     *      \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
     *        \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !ge..]
     *          \_EsQueryExec[test], query[sv(emp_no, not(emp_no == 10010)) OR sv(not(first_name == "Parto"))], limit[10000], sort[]
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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

    /**
     * Expected:
     *  LimitExec[10000[INTEGER]]
     *  \_ExchangeExec[REMOTE_SOURCE]
     *    \_ExchangeExec[REMOTE_SINK]
     *      \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !gender, languages{f}#5, last_name{f}#6, salary{f}#7]]
     *        \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, !ge..]
     *          \_EsQueryExec[test], query[{"bool":{"must_not":[{"term":{"emp_no":{"value":10010}}}],"boost":1.0}}][_doc{f}#9],
     *                                     limit[10000], sort[]
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        var boolQuery = as(sv(source.query(), "emp_no"), BoolQueryBuilder.class);
        assertThat(boolQuery.mustNot(), hasSize(1));
        var termQuery = as(boolQuery.mustNot().get(0), TermQueryBuilder.class);
        assertThat(termQuery.fieldName(), is("emp_no"));
        assertThat(termQuery.value(), is(10010));  // TODO this will match multivalued fields and we don't want that
    }

    /**
     * Expected:
     *  LimitExec[10000[INTEGER]]
     *  \_ExchangeExec[REMOTE_SOURCE]
     *    \_ExchangeExec[REMOTE_SINK]
     *      \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, !gender, languages{f}#6, last_name{f}#7, salary{f}#8]]
     *        \_FieldExtractExec[_meta_field{f}#9, first_name{f}#4, !gender, last_na..]
     *          \_LimitExec[10000[INTEGER]]
     *            \_FilterExec[NOT(emp_no{f}#3 == languages{f}#6)]
     *              \_FieldExtractExec[emp_no{f}#3, languages{f}#6]
     *                \_EsQueryExec[test], query[][_doc{f}#10], limit[], sort[]
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));
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
        var source = source(fieldExtract.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES + KEYWORD_EST));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertEquals(SingleValueQuery.Builder.class, query.getClass());
        assertThat(((SingleValueQuery.Builder) query).next(), instanceOf(WildcardQueryBuilder.class));
        WildcardQueryBuilder wildcard = ((WildcardQueryBuilder) ((SingleValueQuery.Builder) query).next());
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

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

        var source = source(fieldExtract.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES + KEYWORD_EST));
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertEquals(SingleValueQuery.Builder.class, query.getClass());
        assertThat(((SingleValueQuery.Builder) query).next(), instanceOf(RegexpQueryBuilder.class));
        RegexpQueryBuilder wildcard = ((RegexpQueryBuilder) ((SingleValueQuery.Builder) query).next());
        assertEquals("first_name", wildcard.fieldName());
        assertEquals(".*foo.*", wildcard.value());
    }

    /**
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, job{f}#10, job.raw{f}#11, languages{f}#6, last_n
     * ame{f}#7, long_noidx{f}#12, salary{f}#8]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"esql_single_value":{"field":"first_name","next":
     *       {"term":{"first_name":{"value":"foo","case_insensitive":true}}},"source":"first_name =~ \"foo\"@2:9"}}]
     *       [_doc{f}#23], limit[1000], sort[] estimatedRowSize[324]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103599")
    public void testPushDownEqualsIgnoreCase() {
        var plan = physicalPlan("""
            from test
            | where first_name =~ "foo"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var source = source(extractRest.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        QueryBuilder query = source.query();
        assertNotNull(query);
    }

    /**
     * LimitExec[1000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, job{f}#13, job.raw{f}#14, languages{f}#9, last_
     * name{f}#10, long_noidx{f}#15, salary{f}#11, x{r}#4]]
     *     \_FieldExtractExec[_meta_field{f}#12, emp_no{f}#6, gender{f}#8, job{f}..]
     *       \_LimitExec[1000[INTEGER]]
     *         \_FilterExec[x{r}#4 =~ [66 6f 6f][KEYWORD]]
     *           \_EvalExec[[CONCAT(first_name{f}#7,[66 6f 6f][KEYWORD]) AS x]]
     *             \_FieldExtractExec[first_name{f}#7]
     *               \_EsQueryExec[test], query[][_doc{f}#27], limit[], sort[] estimatedRowSize[374]
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103599")
    public void testNoPushDownEvalEqualsIgnoreCase() {
        var plan = physicalPlan("""
            from test
            | eval x = concat(first_name, "foo")
            | where x =~ "foo"
            """);

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(topLimit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extractRest = as(project.child(), FieldExtractExec.class);
        var limit = as(extractRest.child(), LimitExec.class);
        var filter = as(limit.child(), FilterExec.class);
        var eval = as(filter.child(), EvalExec.class);
        var extract = as(eval.child(), FieldExtractExec.class);
        var source = source(extract.child());

        QueryBuilder query = source.query();
        assertNull(query);
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
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES));

        QueryBuilder query = source.query();
        assertNotNull(query);
        assertThat(query, instanceOf(SingleValueQuery.Builder.class));
        assertThat(((SingleValueQuery.Builder) query).next(), instanceOf(BoolQueryBuilder.class));
        var boolQuery = (BoolQueryBuilder) ((SingleValueQuery.Builder) query).next();
        List<QueryBuilder> mustNot = boolQuery.mustNot();
        assertThat(mustNot.size(), is(1));
        assertThat(mustNot.get(0), instanceOf(RegexpQueryBuilder.class));
        var regexpQuery = (RegexpQueryBuilder) mustNot.get(0);
        assertThat(regexpQuery.fieldName(), is("first_name"));
        assertThat(regexpQuery.value(), is(".*foo.*"));
    }

    /**
     * EnrichExec[first_name{f}#3,foo,fld,idx,[a{r}#11, b{r}#12]]
     *  \_LimitExec[10000[INTEGER]]
     *    \_ExchangeExec[]
     *      \_ProjectExec[[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gender{f}#4, languages{f}#5, last_name{f}#6, salary{f}#7]]
     *        \_FieldExtractExec[_meta_field{f}#8, emp_no{f}#2, first_name{f}#3, gen..]
     *          \_EsQueryExec[test], query[][_doc{f}#13], limit[10000], sort[] estimatedRowSize[216]
     */
    public void testEnrich() {
        var plan = physicalPlan("""
            from test
            | enrich foo on first_name
            """);

        var optimized = optimizedPlan(plan);
        var enrich = as(optimized, EnrichExec.class);
        var limit = as(enrich.child(), LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        // an int for doc id, and int for the "a" enriched field, and a long for the "b" enriched field
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES * 2 + Long.BYTES));
    }

    /**
     * Expects the filter to transform the source into a local relationship
     * LimitExec[10000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_LocalSourceExec[[_meta_field{f}#8, emp_no{r}#2, first_name{f}#3, gender{f}#4, languages{f}#5, last_name{f}#6, salary{f}#7],EMPT
     * Y]
     */
    public void testLocallyMissingField() {
        var testStats = statsForMissingField("emp_no");

        var optimized = optimizedPlan(physicalPlan("""
              from test
            | where emp_no > 10
            """), testStats);

        var limit = as(optimized, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var source = as(exchange.child(), LocalSourceExec.class);
        assertEquals(LocalSupplier.EMPTY, source.supplier());
    }

    /**
     * GrokExec[first_name{f}#4,Parser[pattern=%{WORD:b}.*, grok=org.elasticsearch.grok.Grok@60a20ab6],[b{r}#2]]
     * \_LimitExec[10000[INTEGER]]
     *   \_ExchangeExec[]
     *     \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, languages{f}#6, last_name{f}#7, salary{f}#8]]
     *       \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *         \_EsQueryExec[test], query[][_doc{f}#10], limit[10000], sort[] estimatedRowSize[216]
     */
    public void testGrok() {
        var plan = physicalPlan("""
            from test
            | grok first_name "%{WORD:b}.*"
            """);

        var optimized = optimizedPlan(plan);
        var grok = as(optimized, GrokExec.class);
        var limit = as(grok.child(), LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES + KEYWORD_EST));
    }

    public void testDissect() {
        var plan = physicalPlan("""
            from test
            | dissect first_name "%{b} "
            """);

        var optimized = optimizedPlan(plan);
        var dissect = as(optimized, DissectExec.class);
        var limit = as(dissect.child(), LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());
        assertThat(source.estimatedRowSize(), equalTo(allFieldRowSize + Integer.BYTES + KEYWORD_EST));
    }

    public void testPushDownMetadataIndexInWildcard() {
        var plan = physicalPlan("""
            from test metadata _index
            | where _index like "test*"
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());

        var tq = as(source.query(), WildcardQueryBuilder.class);
        assertThat(tq.fieldName(), is("_index"));
        assertThat(tq.value(), is("test*"));
    }

    /*
     * LimitExec[10000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, languages{f}#6, last_name{f}#7, salary{f}#8,
     *     _index{m}#1]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"esql_single_value":{"field":"_index","next":{"term":{"_index":{"value":"test"}}}}}]
     *         [_doc{f}#10], limit[10000], sort[] estimatedRowSize[266]
     */
    public void testPushDownMetadataIndexInEquality() {
        var plan = physicalPlan("""
            from test metadata _index
            | where _index == "test"
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());

        var tq = as(source.query(), TermQueryBuilder.class);
        assertThat(tq.fieldName(), is("_index"));
        assertThat(tq.value(), is("test"));
    }

    /*
     * LimitExec[10000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, languages{f}#6, last_name{f}#7, salary{f}#8,
     *     _index{m}#1]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_EsQueryExec[test], query[{"bool":{"must_not":[{"term":{"_index":{"value":"test"}}}],"boost":1.0}}]
     *         [_doc{f}#10], limit[10000], sort[] estimatedRowSize[266]
     */
    public void testPushDownMetadataIndexInNotEquality() {
        var plan = physicalPlan("""
            from test metadata _index
            | where _index != "test"
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = source(extract.child());

        var bq = as(source.query(), BoolQueryBuilder.class);
        assertThat(bq.mustNot().size(), is(1));
        var tq = as(bq.mustNot().get(0), TermQueryBuilder.class);
        assertThat(tq.fieldName(), is("_index"));
        assertThat(tq.value(), is("test"));
    }

    /*
     * LimitExec[10000[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gender{f}#5, languages{f}#6, last_name{f}#7, salary{f}#8, _in
     *     dex{m}#1]]
     *     \_FieldExtractExec[_meta_field{f}#9, emp_no{f}#3, first_name{f}#4, gen..]
     *       \_LimitExec[10000[INTEGER]]
     *         \_FilterExec[_index{m}#1 > [74 65 73 74][KEYWORD]]
     *           \_FieldExtractExec[_index{m}#1]
     *             \_EsQueryExec[test], query[][_doc{f}#10], limit[], sort[] estimatedRowSize[266]
     */
    public void testDontPushDownMetadataIndexInInequality() {
        for (var t : List.of(
            tuple(">", GreaterThan.class),
            tuple(">=", GreaterThanOrEqual.class),
            tuple("<", LessThan.class),
            tuple("<=", LessThanOrEqual.class)
            // no NullEquals use
        )) {
            var plan = physicalPlan("from test metadata _index | where _index " + t.v1() + " \"test\"");

            var optimized = optimizedPlan(plan);
            var limit = as(optimized, LimitExec.class);
            var exchange = asRemoteExchange(limit.child());
            var project = as(exchange.child(), ProjectExec.class);
            var extract = as(project.child(), FieldExtractExec.class);
            limit = as(extract.child(), LimitExec.class);
            var filter = as(limit.child(), FilterExec.class);

            var comp = as(filter.condition(), t.v2());
            var metadataAttribute = as(comp.left(), MetadataAttribute.class);
            assertThat(metadataAttribute.name(), is("_index"));

            extract = as(filter.child(), FieldExtractExec.class);
            var source = source(extract.child());
        }
    }

    public void testDontPushDownMetadataVersionAndId() {
        for (var t : List.of(tuple("_version", "2"), tuple("_id", "\"2\""))) {
            var plan = physicalPlan("from test metadata " + t.v1() + " | where " + t.v1() + " == " + t.v2());

            var optimized = optimizedPlan(plan);
            var limit = as(optimized, LimitExec.class);
            var exchange = asRemoteExchange(limit.child());
            var project = as(exchange.child(), ProjectExec.class);
            var extract = as(project.child(), FieldExtractExec.class);
            limit = as(extract.child(), LimitExec.class);
            var filter = as(limit.child(), FilterExec.class);

            assertThat(filter.condition(), instanceOf(Equals.class));
            assertThat(((Equals) filter.condition()).left(), instanceOf(MetadataAttribute.class));
            var metadataAttribute = (MetadataAttribute) ((Equals) filter.condition()).left();
            assertThat(metadataAttribute.name(), is(t.v1()));

            extract = as(filter.child(), FieldExtractExec.class);
            var source = source(extract.child());
        }
    }

    public void testNoTextFilterPushDown() {
        var plan = physicalPlan("""
            from test
            | where gender == "M"
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var limit2 = as(extract.child(), LimitExec.class);
        var filter = as(limit2.child(), FilterExec.class);
        var extract2 = as(filter.child(), FieldExtractExec.class);
        var source = source(extract2.child());
        assertNull(source.query());
    }

    public void testNoNonIndexedFilterPushDown() {
        var plan = physicalPlan("""
            from test
            | where long_noidx == 1
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var limit2 = as(extract.child(), LimitExec.class);
        var filter = as(limit2.child(), FilterExec.class);
        var extract2 = as(filter.child(), FieldExtractExec.class);
        var source = source(extract2.child());
        assertNull(source.query());
    }

    public void testTextWithRawFilterPushDown() {
        var plan = physicalPlan("""
            from test
            | where job == "foo"
            """);

        var optimized = optimizedPlan(plan);
        var limit = as(optimized, LimitExec.class);
        var exchange = asRemoteExchange(limit.child());
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);
        var qb = as(source.query(), SingleValueQuery.Builder.class);
        assertThat(qb.field(), equalTo("job.raw"));
    }

    public void testNoTextSortPushDown() {
        var plan = physicalPlan("""
            from test
            | sort gender
            """);

        var optimized = optimizedPlan(plan);
        var topN = as(optimized, TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topN2 = as(extract.child(), TopNExec.class);
        var extract2 = as(topN2.child(), FieldExtractExec.class);
        var source = source(extract2.child());
        assertNull(source.sorts());
    }

    public void testNoNonIndexedSortPushDown() {
        var plan = physicalPlan("""
            from test
            | sort long_noidx
            """);

        var optimized = optimizedPlan(plan);
        var topN = as(optimized, TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var topN2 = as(extract.child(), TopNExec.class);
        var extract2 = as(topN2.child(), FieldExtractExec.class);
        var source = source(extract2.child());
        assertNull(source.sorts());
    }

    public void testTextWithRawSortPushDown() {
        var plan = physicalPlan("""
            from test
            | sort job
            """);

        var optimized = optimizedPlan(plan);
        var topN = as(optimized, TopNExec.class);
        var exchange = as(topN.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var extract = as(project.child(), FieldExtractExec.class);
        var source = as(extract.child(), EsQueryExec.class);
        assertThat(source.sorts().size(), equalTo(1));
        assertThat(source.sorts().get(0).field().name(), equalTo("job.raw"));
    }

    public void testFieldExtractForTextAndSubfield() {
        var plan = physicalPlan("""
            from test
            | keep job*
            """);

        var project = as(plan, ProjectExec.class);
        assertThat(Expressions.names(project.projections()), contains("job", "job.raw"));
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
            node -> new EsSourceExec(node.source(), node.index(), emptyAttrList, node.query(), IndexMode.STANDARD)
        );

        var e = expectThrows(VerificationException.class, () -> physicalPlanOptimizer.verify(badPlan));
        assertThat(
            e.getMessage(),
            containsString(
                "Need to add field extractor for [[emp_no]] but cannot detect source attributes from node [EsSourceExec[test][]]"
            )
        );
    }

    /**
     * Expects
     * ProjectExec[[x{r}#3]]
     * \_EvalExec[[1[INTEGER] AS x]]
     *   \_LimitExec[10000[INTEGER]]
     *     \_ExchangeExec[[],false]
     *       \_ProjectExec[[&lt;all-fields-projected&gt;{r}#12]]
     *         \_EvalExec[[null[NULL] AS &lt;all-fields-projected&gt;]]
     *           \_EsQueryExec[test], query[{"esql_single_value":{"field":"emp_no","next":{"range":{"emp_no":{"gt":10,"boost":1.0}}}}}]
     *            [_doc{f}#13], limit[10000], sort[] estimatedRowSize[8]
     */
    public void testProjectAllFieldsWhenOnlyTheCountMatters() {
        var plan = optimizedPlan(physicalPlan("""
            from test
            | where emp_no > 10
            | eval x = 1
            | keep x
            """));

        var project = as(plan, ProjectExec.class);
        var eval = as(project.child(), EvalExec.class);
        var limit = as(eval.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var nullField = "<all-fields-projected>";
        project = as(exchange.child(), ProjectExec.class);
        assertThat(Expressions.names(project.projections()), contains(nullField));
        eval = as(project.child(), EvalExec.class);
        assertThat(Expressions.names(eval.fields()), contains(nullField));
        var source = source(eval.child());
    }

    /**
     * ProjectExec[[a{r}#5]]
     * \_EvalExec[[__a_SUM@81823521{r}#15 / __a_COUNT@31645621{r}#16 AS a]]
     *   \_LimitExec[10000[INTEGER]]
     *     \_AggregateExec[[],[SUM(salary{f}#11) AS __a_SUM@81823521, COUNT(salary{f}#11) AS __a_COUNT@31645621],FINAL,24]
     *       \_AggregateExec[[],[SUM(salary{f}#11) AS __a_SUM@81823521, COUNT(salary{f}#11) AS __a_COUNT@31645621],PARTIAL,16]
     *         \_LimitExec[10[INTEGER]]
     *           \_ExchangeExec[[],false]
     *             \_ProjectExec[[salary{f}#11]]
     *               \_FieldExtractExec[salary{f}#11]
     *                 \_EsQueryExec[test], query[][_doc{f}#17], limit[10], sort[] estimatedRowSize[8]
     */
    public void testAvgSurrogateFunctionAfterRenameAndLimit() {
        var plan = optimizedPlan(physicalPlan("""
            from test
            | limit 10
            | rename first_name as FN
            | stats a = avg(salary)
            """));

        var project = as(plan, ProjectExec.class);
        var eval = as(project.child(), EvalExec.class);
        var limit = as(eval.child(), LimitExec.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(limit.limit().fold(), equalTo(10000));
        var aggFinal = as(limit.child(), AggregateExec.class);
        assertThat(aggFinal.getMode(), equalTo(FINAL));
        var aggPartial = as(aggFinal.child(), AggregateExec.class);
        assertThat(aggPartial.getMode(), equalTo(PARTIAL));
        limit = as(aggPartial.child(), LimitExec.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(limit.limit().fold(), equalTo(10));

        var exchange = as(limit.child(), ExchangeExec.class);
        project = as(exchange.child(), ProjectExec.class);
        var expectedFields = List.of("salary");
        assertThat(Expressions.names(project.projections()), is(expectedFields));
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        assertThat(Expressions.names(fieldExtract.attributesToExtract()), is(expectedFields));
        var source = source(fieldExtract.child());
        assertThat(source.limit().fold(), equalTo(10));
    }

    /**
     * Expects
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[languages{f}#9],[MIN(salary{f}#11) AS m, languages{f}#9],FINAL,8]
     *   \_ExchangeExec[[languages{f}#9, min{r}#16, seen{r}#17],true]
     *     \_LocalSourceExec[[languages{f}#9, min{r}#16, seen{r}#17],EMPTY]
     */
    public void testAggToLocalRelationOnDataNode() {
        var plan = physicalPlan("""
            from test
            | where first_name is not null
            | stats m = min(salary) by languages
            """);

        var stats = new EsqlTestUtils.TestSearchStats() {
            public boolean exists(String field) {
                return "salary".equals(field);
            }
        };
        var optimized = optimizedPlan(plan, stats);

        var limit = as(optimized, LimitExec.class);
        var aggregate = as(limit.child(), AggregateExec.class);
        assertThat(aggregate.groupings(), hasSize(1));
        assertThat(aggregate.estimatedRowSize(), equalTo(Long.BYTES));

        var exchange = asRemoteExchange(aggregate.child());
        var localSourceExec = as(exchange.child(), LocalSourceExec.class);
        assertThat(Expressions.names(localSourceExec.output()), contains("languages", "min", "seen"));
    }

    /**
     * Expects
     * intermediate plan
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[COUNT(emp_no{f}#6) AS c],FINAL,null]
     *   \_ExchangeExec[[count{r}#16, seen{r}#17],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[COUNT(emp_no{f}#6) AS c]]
     * \_Filter[emp_no{f}#6 > 10[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]]]
     *
     * and final plan is
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[COUNT(emp_no{f}#6) AS c],FINAL,8]
     *   \_ExchangeExec[[count{r}#16, seen{r}#17],true]
     *     \_LocalSourceExec[[count{r}#16, seen{r}#17],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     */
    public void testPartialAggFoldingOutput() {
        var plan = physicalPlan("""
              from test
            | where emp_no > 10
            | stats c = count(emp_no)
            """);

        var stats = statsForMissingField("emp_no");
        var optimized = optimizedPlan(plan, stats);

        var limit = as(optimized, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        var exchange = as(agg.child(), ExchangeExec.class);
        assertThat(Expressions.names(exchange.output()), contains("count", "seen"));
        var source = as(exchange.child(), LocalSourceExec.class);
        assertThat(Expressions.names(source.output()), contains("count", "seen"));
    }

    /**
     * Checks that when the folding happens on the coordinator, the intermediate agg state
     * are not used anymore.
     *
     * Expects
     * LimitExec[10000[INTEGER]]
     * \_AggregateExec[[],[COUNT(emp_no{f}#5) AS c],FINAL,8]
     *   \_AggregateExec[[],[COUNT(emp_no{f}#5) AS c],PARTIAL,8]
     *     \_LimitExec[10[INTEGER]]
     *       \_ExchangeExec[[],false]
     *         \_ProjectExec[[emp_no{r}#5]]
     *           \_EvalExec[[null[INTEGER] AS emp_no]]
     *             \_EsQueryExec[test], query[][_doc{f}#26], limit[10], sort[] estimatedRowSize[8]
     */
    public void testGlobalAggFoldingOutput() {
        var plan = physicalPlan("""
              from test
            | limit 10
            | stats c = count(emp_no)
            """);

        var stats = statsForMissingField("emp_no");
        var optimized = optimizedPlan(plan, stats);

        var limit = as(optimized, LimitExec.class);
        var aggFinal = as(limit.child(), AggregateExec.class);
        var aggPartial = as(aggFinal.child(), AggregateExec.class);
        assertThat(Expressions.names(aggPartial.output()), contains("c"));
        limit = as(aggPartial.child(), LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
    }

    /**
     * Checks the folded aggregation preserves the intermediate output.
     *
     * Expects
     * ProjectExec[[a{r}#5]]
     * \_EvalExec[[__a_SUM@734e2841{r}#16 / __a_COUNT@12536eab{r}#17 AS a]]
     *   \_LimitExec[1000[INTEGER]]
     *     \_AggregateExec[[],[SUM(emp_no{f}#6) AS __a_SUM@734e2841, COUNT(emp_no{f}#6) AS __a_COUNT@12536eab],FINAL,24]
     *       \_ExchangeExec[[sum{r}#18, seen{r}#19, count{r}#20, seen{r}#21],true]
     *         \_LocalSourceExec[[sum{r}#18, seen{r}#19, count{r}#20, seen{r}#21],[LongArrayBlock[positions=1, mvOrdering=UNORDERED,
     *         values=[0,
     * 0]], BooleanVectorBlock[vector=ConstantBooleanVector[positions=1, value=true]],
     *      LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]],
     *      BooleanVectorBlock[vector=ConstantBooleanVector[positions=1, value=true]]]]
     */
    public void testPartialAggFoldingOutputForSyntheticAgg() {
        var plan = physicalPlan("""
              from test
            | where emp_no > 10
            | stats a = avg(emp_no)
            """);

        var stats = statsForMissingField("emp_no");
        var optimized = optimizedPlan(plan, stats);

        var project = as(optimized, ProjectExec.class);
        var eval = as(project.child(), EvalExec.class);
        var limit = as(eval.child(), LimitExec.class);
        var aggFinal = as(limit.child(), AggregateExec.class);
        assertThat(aggFinal.output(), hasSize(2));
        var exchange = as(aggFinal.child(), ExchangeExec.class);
        assertThat(Expressions.names(exchange.output()), contains("sum", "seen", "count", "seen"));
        var source = as(exchange.child(), LocalSourceExec.class);
        assertThat(Expressions.names(source.output()), contains("sum", "seen", "count", "seen"));
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#9) AS centroid],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#10, xDel{r}#11, yVal{r}#12, yDel{r}#13, count{r}#14],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#9) AS centroid]]
     * \_EsRelation[airports][abbrev{f}#5, location{f}#9, name{f}#6, scalerank{f}..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#9) AS centroid],FINAL,50]
     *   \_ExchangeExec[[xVal{r}#10, xDel{r}#11, yVal{r}#12, yDel{r}#13, count{r}#14],true]
     *     \_AggregateExec[[],[SPATIALCENTROID(location{f}#9) AS centroid],PARTIAL,50]
     *       \_FilterExec[ISNOTNULL(location{f}#9)]
     *         \_FieldExtractExec[location{f}#9][location{f}#9]
     *           \_EsQueryExec[airports], query[][_doc{f}#26], limit[], sort[] estimatedRowSize[54]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     *
     * Also note that the type converting function is removed when it does not actually convert the type,
     * ensuring that ReferenceAttributes are not created for the same field, and the optimization can still work.
     */
    public void testSpatialTypesAndStatsUseDocValues() {
        for (String query : new String[] {
            "from airports | stats centroid = st_centroid_agg(location)",
            "from airports | stats centroid = st_centroid_agg(to_geopoint(location))",
            "from airports | eval location = to_geopoint(location) | stats centroid = st_centroid_agg(location)" }) {
            var plan = this.physicalPlan(query, airports);

            var limit = as(plan, LimitExec.class);
            var agg = as(limit.child(), AggregateExec.class);
            // Before optimization the aggregation does not use doc-values
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

            var exchange = as(agg.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var fAgg = as(fragment.fragment(), Aggregate.class);
            as(fAgg.child(), EsRelation.class);

            // Now optimize the plan and assert the aggregation uses doc-values
            var optimized = optimizedPlan(plan);
            limit = as(optimized, LimitExec.class);
            agg = as(limit.child(), AggregateExec.class);
            // Above the exchange (in coordinator) the aggregation is not using doc-values
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
            exchange = as(agg.child(), ExchangeExec.class);
            agg = as(exchange.child(), AggregateExec.class);
            // below the exchange (in data node) the aggregation is using doc-values
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
            var extract = as(agg.child(), FieldExtractExec.class);
            source(extract.child());
            assertTrue(
                "Expect field attribute to be extracted as doc-values",
                extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
            );
        }
    }

    /**
     * This test does not have real index fields, and therefor asserts that doc-values field extraction does NOT occur.
     *
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(__centroid_SPATIALCENTROID@ec8dd77e{r}#7) AS centroid],FINAL,null]
     *   \_AggregateExec[[],[SPATIALCENTROID(__centroid_SPATIALCENTROID@ec8dd77e{r}#7) AS centroid],PARTIAL,null]
     *     \_EvalExec[[[1 1 0 0 0 0 0 30 e2 4c 7c 45 40 0 0 e0 92 b0 82 2d 40][GEO_POINT] AS __centroid_SPATIALCENTROID@ec8dd77e]]
     *       \_RowExec[[[50 4f 49 4e 54 28 34 32 2e 39 37 31 30 39 36 32 39 39 35 38 38 36 38 20 31 34 2e 37 35 35 32 35 33 34 30 30
     *       36 35 33 36 29][KEYWORD] AS wkt]]
     *
     * After local optimizations we expect no changes because field is extracted:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(__centroid_SPATIALCENTROID@7ff910a{r}#7) AS centroid],FINAL,50]
     *   \_AggregateExec[[],[SPATIALCENTROID(__centroid_SPATIALCENTROID@7ff910a{r}#7) AS centroid],PARTIAL,50]
     *     \_EvalExec[[[1 1 0 0 0 0 0 30 e2 4c 7c 45 40 0 0 e0 92 b0 82 2d 40][GEO_POINT] AS __centroid_SPATIALCENTROID@7ff910a]]
     *       \_RowExec[[[50 4f 49 4e 54 28 34 32 2e 39 37 31 30 39 36 32 39 39 35 38 38 36 38 20 31 34 2e 37 35 35 32 35 33 34 30 30
     *       36 35 33 36 29][KEYWORD] AS wkt]]
     */
    public void testSpatialTypesAndStatsUseDocValuesNestedLiteral() {
        var plan = this.physicalPlan("""
            row wkt = "POINT(42.97109629958868 14.7552534006536)"
            | stats centroid = st_centroid_agg(to_geopoint(wkt))
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        var eval = as(agg.child(), EvalExec.class);
        as(eval.child(), RowExec.class);

        // Now optimize the plan and assert the same plan again, since no FieldExtractExec is added
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        eval = as(agg.child(), EvalExec.class);
        as(eval.child(), RowExec.class);
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#11) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#12, xDel{r}#13, yVal{r}#14, yDel{r}#15, count{r}#16, count{r}#17, seen{r}#18],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#11) AS centroid, COUNT([2a][KEYWORD]) AS count]]
     * \_EsRelation[airports][abbrev{f}#7, location{f}#11, name{f}#8, scalerank{f..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#11) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,58]
     *   \_ExchangeExec[[xVal{r}#12, xDel{r}#13, yVal{r}#14, yDel{r}#15, count{r}#16, count{r}#17, seen{r}#18],true]
     *     \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS count, SPATIALCENTROID(location{f}#11) AS centroid],PARTIAL,58]
     *       \_FieldExtractExec[location{f}#11][location{f}#11]
     *         \_EsQueryExec[airports], query[][_doc{f}#33], limit[], sort[] estimatedRowSize[54]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     */
    public void testSpatialTypesAndStatsUseDocValuesMultiAggregations() {
        var plan = this.physicalPlan("""
            from airports
            | stats centroid = st_centroid_agg(location), count = COUNT()
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        as(fAgg.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        source(extract.child());
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#14) AS airports, SPATIALCENTROID(city_location{f}#17) AS cities, COUNT([2a][KEY
     * WORD]) AS count],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#18, xDel{r}#19, yVal{r}#20, yDel{r}#21, count{r}#22, xVal{r}#23, xDel{r}#24, yVal{r}#25, yDel{r}#26,
     * count{r}#27, count{r}#28, seen{r}#29],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#14) AS airports, SPATIALCENTROID(city_location{f}#17) AS cities, COUNT([2a][KEY
     * WORD]) AS count]]
     * \_EsRelation[airports][abbrev{f}#10, city{f}#16, city_location{f}#17, coun..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#14) AS airports, SPATIALCENTROID(city_location{f}#17) AS cities, COUNT([2a][KEY
     * WORD]) AS count],FINAL,108]
     *   \_ExchangeExec[[xVal{r}#18, xDel{r}#19, yVal{r}#20, yDel{r}#21, count{r}#22, xVal{r}#23, xDel{r}#24, yVal{r}#25, yDel{r}#26,
     * count{r}#27, count{r}#28, seen{r}#29],true]
     *     \_AggregateExec[[],[SPATIALCENTROID(location{f}#14) AS airports, SPATIALCENTROID(city_location{f}#17) AS cities, COUNT([2a][KEY
     * WORD]) AS count],PARTIAL,108]
     *       \_FieldExtractExec[location{f}#14, city_location{f}#17][location{f}#14, city_location{f}#17]
     *         \_EsQueryExec[airports], query[][_doc{f}#53], limit[], sort[] estimatedRowSize[104]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     */
    public void testSpatialTypesAndStatsUseDocValuesMultiSpatialAggregations() {
        var plan = this.physicalPlan("""
            FROM airports
            | STATS airports=ST_CENTROID_AGG(location), cities=ST_CENTROID_AGG(city_location), count=COUNT()
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "airports", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "cities", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        as(fAgg.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "airports", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "cities", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "airports", SpatialCentroid.class, GEO_POINT, true);
        assertAggregation(agg, "cities", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        source(extract.child());
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#13, xDel{r}#14, yVal{r}#15, yDel{r}#16, count{r}#17, count{r}#18, seen{r}#19],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count]]
     * \_Filter[scalerank{f}#10 == 9[INTEGER]]
     *   \_EsRelation[airports][abbrev{f}#8, location{f}#12, name{f}#9, scalerank{f..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#11) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,58]
     *   \_ExchangeExec[[xVal{r}#12, xDel{r}#13, yVal{r}#14, yDel{r}#15, count{r}#16, count{r}#17, seen{r}#18],true]
     *     \_AggregateExec[[],[COUNT([2a][KEYWORD]) AS count, SPATIALCENTROID(location{f}#11) AS centroid],PARTIAL,58]
     *       \_FieldExtractExec[location{f}#11][location{f}#11]
     *         \_EsQueryExec[airports], query[{"esql_single_value":{"field":"scalerank","next":{"term":{"scalerank":{"value":9}}},
     *                                         "source":"scalerank == 9@2:9"}}][_doc{f}#34], limit[], sort[] estimatedRowSize[54]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     */
    public void testSpatialTypesAndStatsUseDocValuesMultiAggregationsFiltered() {
        var plan = this.physicalPlan("""
            FROM airports
            | WHERE scalerank == 9
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT()
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        var filter = as(fAgg.child(), Filter.class);
        assertFilterCondition(filter, Equals.class, "scalerank", 9);
        as(filter.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
        var source = source(extract.child());
        var qb = as(source.query(), SingleValueQuery.Builder.class);
        assertThat("Expected predicate to be passed to Lucene query", qb.source().text(), equalTo("scalerank == 9"));
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[scalerank{f}#10],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count, scalerank{f}#10],
     * FINAL,null]
     *   \_ExchangeExec[[scalerank{f}#10, xVal{r}#13, xDel{r}#14, yVal{r}#15, yDel{r}#16, count{r}#17, count{r}#18, seen{r}#19],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[scalerank{f}#10],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count, scalerank{f}#10]]
     * \_EsRelation[airports][abbrev{f}#8, location{f}#12, name{f}#9, scalerank{f..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[scalerank{f}#10],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count, scalerank{f}#10],
     * FINAL,62]
     *   \_ExchangeExec[[scalerank{f}#10, xVal{r}#13, xDel{r}#14, yVal{r}#15, yDel{r}#16, count{r}#17, count{r}#18, seen{r}#19],true]
     *     \_AggregateExec[[scalerank{f}#10],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count, scalerank{f}#10],
     * PARTIAL,62]
     *       \_FieldExtractExec[location{f}#12][location{f}#12]
     *         \_EsQueryExec[airports], query[][_doc{f}#34], limit[], sort[] estimatedRowSize[54]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     */
    public void testSpatialTypesAndStatsUseDocValuesMultiAggregationsGrouped() {
        var plan = this.physicalPlan("""
            FROM airports
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT() BY scalerank
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("One grouping in aggregation", agg.groupings().size(), equalTo(1));
        var att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        as(fAgg.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
        source(extract.child());
    }

    /**
     * Before local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(centroid{r}#4) AS centroid, SUM(count{r}#6) AS count],FINAL,null]
     *   \_AggregateExec[[],[SPATIALCENTROID(centroid{r}#4) AS centroid, SUM(count{r}#6) AS count],PARTIAL,null]
     *     \_AggregateExec[[scalerank{f}#16],[SPATIALCENTROID(location{f}#18) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,null]
     *       \_ExchangeExec[[scalerank{f}#16, xVal{r}#19, xDel{r}#20, yVal{r}#21, yDel{r}#22, count{r}#23, count{r}#24, seen{r}#25],true]
     *         \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[scalerank{f}#16],[SPATIALCENTROID(location{f}#18) AS centroid, COUNT([2a][KEYWORD]) AS count]]
     * \_EsRelation[airports][abbrev{f}#14, location{f}#18, name{f}#15, scalerank..]]]
     *
     * After local optimizations:
     *
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(centroid{r}#4) AS centroid, SUM(count{r}#6) AS count],FINAL,58]
     *   \_AggregateExec[[],[SPATIALCENTROID(centroid{r}#4) AS centroid, SUM(count{r}#6) AS count],PARTIAL,58]
     *     \_AggregateExec[[scalerank{f}#16],[SPATIALCENTROID(location{f}#18) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,58]
     *       \_ExchangeExec[[scalerank{f}#16, xVal{r}#19, xDel{r}#20, yVal{r}#21, yDel{r}#22, count{r}#23, count{r}#24, seen{r}#25],true]
     *         \_AggregateExec[[scalerank{f}#16],[SPATIALCENTROID(location{f}#18) AS centroid, COUNT([2a][KEYWORD]) AS count],PARTIAL,58]
     *           \_FieldExtractExec[location{f}#18][location{f}#18]
     *             \_EsQueryExec[airports], query[][_doc{f}#42], limit[], sort[] estimatedRowSize[54]
     *
     * Note the FieldExtractExec has 'location' set for stats: FieldExtractExec[location{f}#9][location{f}#9]
     */
    public void testSpatialTypesAndStatsUseDocValuesMultiAggregationsGroupedAggregated() {
        var plan = this.physicalPlan("""
            FROM airports
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT() BY scalerank
            | STATS centroid=ST_CENTROID_AGG(centroid), count=SUM(count)
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "count", Sum.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "count", Sum.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("One grouping in aggregation", agg.groupings().size(), equalTo(1));
        var att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        as(fAgg.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "count", Sum.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        assertAggregation(agg, "count", Sum.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        agg = as(agg.child(), AggregateExec.class);
        assertThat("Aggregation is FINAL", agg.getMode(), equalTo(FINAL));
        assertThat("One grouping in aggregation", agg.groupings().size(), equalTo(1));
        att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("One grouping in aggregation", agg.groupings().size(), equalTo(1));
        att = as(agg.groupings().get(0), Attribute.class);
        assertThat(att.name(), equalTo("scalerank"));
        // below the exchange (in data node) the aggregation is using doc-values
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
        source(extract.child());
    }

    /**
     * Plan:
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(city_location{f}#16) AS centroid],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#24, xDel{r}#25, yVal{r}#26, yDel{r}#27, count{r}#28],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(city_location{f}#16) AS centroid]]
     * \_Enrich[ANY,[63 69 74 79 5f 62 6f 75 6e 64 61 72 69 65 73][KEYWORD],city_location{f}#16,{"geo_match":{"indices":[],"match
     * _field":"city_boundary","enrich_fields":["city","airport","region","city_boundary"]}},{=airport_city_boundaries
     * },[airport{r}#21, region{r}#22, city_boundary{r}#23]]
     *   \_EsRelation[airports][abbrev{f}#9, city{f}#15, city_location{f}#16, count..]]]
     *
     * Optimized:
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(city_location{f}#16) AS centroid],FINAL,50]
     *   \_ExchangeExec[[xVal{r}#24, xDel{r}#25, yVal{r}#26, yDel{r}#27, count{r}#28],true]
     *     \_AggregateExec[[],[SPATIALCENTROID(city_location{f}#16) AS centroid],PARTIAL,50]
     *       \_EnrichExec[ANY,geo_match,city_location{f}#16,city_boundaries,city_boundary,{=airport_city_boundaries},[airport{r}#21,
     *                    region{r}#22, city_boundary{r}#23]]
     *         \_FieldExtractExec[city_location{f}#16][city_location{f}#16]
     *           \_EsQueryExec[airports], query[{"exists":{"field":"city_location","boost":1.0}}][_doc{f}#46], limit[], sort[]
     *                         estimatedRowSize[204]
     *
     * Note the FieldExtractExec has 'city_location' set for doc-values: FieldExtractExec[city_location{f}#16][city_location{f}#16]
     */
    public void testEnrichBeforeSpatialAggregationSupportsDocValues() {
        var plan = physicalPlan("""
            from airports
            | enrich city_boundaries ON city_location WITH airport, region, city_boundary
            | stats centroid = st_centroid_agg(city_location)
            """, airports);

        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        var enrich = as(fAgg.child(), Enrich.class);
        assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
        assertThat(enrich.concreteIndices(), equalTo(Map.of("", "airport_city_boundaries")));
        assertThat(enrich.enrichFields().size(), equalTo(3));
        as(enrich.child(), EsRelation.class);

        // Now optimize the plan and assert the aggregation uses doc-values
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
        var enrichExec = as(agg.child(), EnrichExec.class);
        assertThat(enrichExec.mode(), equalTo(Enrich.Mode.ANY));
        assertThat(enrichExec.concreteIndices(), equalTo(Map.of("", "airport_city_boundaries")));
        assertThat(enrichExec.enrichFields().size(), equalTo(3));
        var extract = as(enrichExec.child(), FieldExtractExec.class);
        source(extract.child());
        assertTrue(
            "Expect field attribute to be extracted as doc-values",
            extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
        );
    }

    /**
     * Plan:
     * LimitExec[500[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Limit[500[INTEGER]]
     * \_Filter[SPATIALINTERSECTS(location{f}#7,[50 4f 4c 59 47 4f 4e 28 29][KEYWORD])]
     *   \_EsRelation[airports][abbrev{f}#3, city{f}#9, city_location{f}#10, countr..]]]
     *
     * Optimized:
     * LimitExec[500[INTEGER]]
     * \_ExchangeExec[[],false]
     *   \_ProjectExec[[abbrev{f}#3, city{f}#9, city_location{f}#10, country{f}#8, location{f}#7, name{f}#4, scalerank{f}#5, type{f}#
     * 6]]
     *     \_FieldExtractExec[abbrev{f}#3, city{f}#9, city_location{f}#10, countr..][]
     *       \_EsQueryExec[airports], query[{
     *         "esql_single_value":{
     *           "field":"location",
     *           "next":{
     *             "geo_shape":{
     *               "location":{
     *                 "shape":{
     *                   "type":"Polygon",
     *                   "coordinates":[[[42.0,14.0],[43.0,14.0],[43.0,15.0],[42.0,15.0],[42.0,14.0]]]
     *                 },
     *                 "relation":"intersects"
     *               },
     *               "ignore_unmapped":false,
     *               "boost":1.0
     *             }
     *           },
     *           "source":"ST_INTERSECTS(location, \"POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))\")@2:9"
     *         }
     *       }][_doc{f}#19], limit[500], sort[] estimatedRowSize[358]
     */
    public void testPushSpatialIntersectsStringToSource() {
        for (String query : new String[] { """
            FROM airports
            | WHERE ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
            """, """
            FROM airports
            | WHERE ST_INTERSECTS(TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"), location)
            """ }) {

            var plan = this.physicalPlan(query, airports);
            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            var source = source(fieldExtract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    private record TestSpatialRelation(ShapeRelation relation, TestDataSource index, boolean literalRight, boolean canPushToSource) {
        String function() {
            return switch (relation) {
                case INTERSECTS -> "ST_INTERSECTS";
                case DISJOINT -> "ST_DISJOINT";
                case WITHIN -> "ST_WITHIN";
                case CONTAINS -> "ST_CONTAINS";
                default -> throw new IllegalArgumentException("Unsupported relation: " + relation);
            };
        }

        Class<? extends SpatialRelatesFunction> functionClass() {
            return switch (relation) {
                case INTERSECTS -> SpatialIntersects.class;
                case DISJOINT -> SpatialDisjoint.class;
                case WITHIN -> literalRight ? SpatialWithin.class : SpatialContains.class;
                case CONTAINS -> literalRight ? SpatialContains.class : SpatialWithin.class;
                default -> throw new IllegalArgumentException("Unsupported relation: " + relation);
            };
        }

        ShapeRelation relationship() {
            return switch (relation) {
                case WITHIN -> literalRight ? ShapeRelation.WITHIN : ShapeRelation.CONTAINS;
                case CONTAINS -> literalRight ? ShapeRelation.CONTAINS : ShapeRelation.WITHIN;
                default -> relation;
            };
        }

        DataType locationType() {
            return index.index.name().endsWith("_web") ? CARTESIAN_POINT : GEO_POINT;
        }

        String castFunction() {
            return index.index.name().endsWith("_web") ? "TO_CARTESIANSHAPE" : "TO_GEOSHAPE";
        }

        String predicate() {
            String field = "location";
            String literal = castFunction() + "(\"POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))\")";
            return literalRight ? function() + "(" + field + ", " + literal + ")" : function() + "(" + literal + ", " + field + ")";
        }
    }

    public void testPushDownSpatialRelatesStringToSource() {
        TestSpatialRelation[] tests = new TestSpatialRelation[] {
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airports, true, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airports, false, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airports, true, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airports, false, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airports, true, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airports, false, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airports, true, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airports, false, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airportsWeb, false, true) };
        for (TestSpatialRelation test : tests) {
            var plan = this.physicalPlan("FROM " + test.index.index.name() + " | WHERE " + test.predicate(), test.index);
            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            assertThat(test.predicate(), filter.condition(), instanceOf(test.functionClass()));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            if (test.canPushToSource) {
                var source = source(fieldExtract.child());
                // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
                // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
                var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
                assertThat("Geometry field name: " + test.predicate(), condition.fieldName(), equalTo("location"));
                assertThat("Spatial relationship: " + test.predicate(), condition.relation(), equalTo(test.relationship()));
                assertThat("Geometry is Polygon: " + test.predicate(), condition.shape().type(), equalTo(ShapeType.POLYGON));
                var polygon = as(condition.shape(), Polygon.class);
                assertThat("Polygon shell length: " + test.predicate(), polygon.getPolygon().length(), equalTo(5));
                assertThat("Polygon holes: " + test.predicate(), polygon.getNumberOfHoles(), equalTo(0));
            } else {
                // Currently CARTESIAN fields do not support lucene push-down for CONTAINS/WITHIN
                var limitExec = as(fieldExtract.child(), LimitExec.class);
                var filterExec = as(limitExec.child(), FilterExec.class);
                var fieldExtractLocation = as(filterExec.child(), FieldExtractExec.class);
                assertThat(test.predicate(), fieldExtractLocation.attributesToExtract().size(), equalTo(1));
                assertThat(test.predicate(), fieldExtractLocation.attributesToExtract().get(0).name(), equalTo("location"));
                var source = source(fieldExtractLocation.child());
                assertThat(test.predicate(), source.query(), equalTo(null));
            }
        }
    }

    public void testPushDownSpatialRelatesStringToSourceAndUseDocValuesForCentroid() {
        TestSpatialRelation[] tests = new TestSpatialRelation[] {
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airports, true, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airports, false, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airports, true, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airports, false, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airports, true, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airports, false, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airports, true, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airports, false, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.INTERSECTS, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.DISJOINT, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.WITHIN, airportsWeb, false, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airportsWeb, true, true),
            new TestSpatialRelation(ShapeRelation.CONTAINS, airportsWeb, false, true) };
        for (TestSpatialRelation test : tests) {
            var centroidExpr = "centroid=ST_CENTROID_AGG(location), count=COUNT()";
            var plan = this.physicalPlan(
                "FROM " + test.index.index.name() + " | WHERE " + test.predicate() + " | STATS " + centroidExpr,
                test.index
            );
            var limit = as(plan, LimitExec.class);
            var agg = as(limit.child(), AggregateExec.class);
            assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
            // Before optimization the aggregation does not use doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, test.locationType(), false);
            var exchange = as(agg.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var fAgg = as(fragment.fragment(), Aggregate.class);
            var filter = as(fAgg.child(), Filter.class);
            assertThat(test.predicate(), filter.condition(), instanceOf(test.functionClass()));

            // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
            var optimized = optimizedPlan(plan);
            limit = as(optimized, LimitExec.class);
            agg = as(limit.child(), AggregateExec.class);
            // Above the exchange (in coordinator) the aggregation is not using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, test.locationType(), false);
            exchange = as(agg.child(), ExchangeExec.class);
            agg = as(exchange.child(), AggregateExec.class);
            assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
            // below the exchange (in data node) the aggregation is using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, test.locationType(), true);
            if (test.canPushToSource) {
                var extract = as(agg.child(), FieldExtractExec.class);
                assertTrue(
                    "Expect field attribute to be extracted as doc-values",
                    extract.attributesToExtract()
                        .stream()
                        .allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == test.locationType())
                );
                var source = source(extract.child());
                // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
                // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
                var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
                assertThat("Geometry field name: " + test.predicate(), condition.fieldName(), equalTo("location"));
                assertThat("Spatial relationship: " + test.predicate(), condition.relation(), equalTo(test.relationship()));
                assertThat("Geometry is Polygon: " + test.predicate(), condition.shape().type(), equalTo(ShapeType.POLYGON));
                var polygon = as(condition.shape(), Polygon.class);
                assertThat("Polygon shell length: " + test.predicate(), polygon.getPolygon().length(), equalTo(5));
                assertThat("Polygon holes: " + test.predicate(), polygon.getNumberOfHoles(), equalTo(0));
            } else {
                // Currently CARTESIAN fields do not support lucene push-down for CONTAINS/WITHIN
                var filterExec = as(agg.child(), FilterExec.class);
                var fieldExtractLocation = as(filterExec.child(), FieldExtractExec.class);
                assertThat(test.predicate(), fieldExtractLocation.attributesToExtract().size(), equalTo(1));
                assertThat(test.predicate(), fieldExtractLocation.attributesToExtract().get(0).name(), equalTo("location"));
                var source = source(fieldExtractLocation.child());
                assertThat(test.predicate(), source.query(), equalTo(null));

            }
        }
    }

    /**
     * Plan:
     * Plan:
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#16, xDel{r}#17, yVal{r}#18, yDel{r}#19, count{r}#20, count{r}#21, seen{r}#22],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count]]
     * \_Filter[SPATIALINTERSECTS(location{f}#12,[50 4f 4c 59 47 4f 4e 28 28 34 32 20 31 34 2c 20 34 33 20 31 34 2c 20 34 33 2
     * 0 31 35 2c 20 34 32 20 31 35 2c 20 34 32 20 31 34 29 29][KEYWORD])]
     *   \_EsRelation[airports][abbrev{f}#8, city{f}#14, city_location{f}#15, count..]]]
     *
     * Optimized:
     * LimitExec[500[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count],FINAL,58]
     *   \_ExchangeExec[[xVal{r}#16, xDel{r}#17, yVal{r}#18, yDel{r}#19, count{r}#20, count{r}#21, seen{r}#22],true]
     *     \_AggregateExec[[],[SPATIALCENTROID(location{f}#12) AS centroid, COUNT([2a][KEYWORD]) AS count],PARTIAL,58]
     *       \_FieldExtractExec[location{f}#12][location{f}#12]
     *         \_EsQueryExec[airports], query[{
     *           "esql_single_value":{
     *             "field":"location",
     *             "next":{
     *               "geo_shape":{
     *                 "location":{
     *                   "shape":{
     *                     "type":"Polygon",
     *                     "coordinates":[[[42.0,14.0],[43.0,14.0],[43.0,15.0],[42.0,15.0],[42.0,14.0]]]
     *                   },
     *                   "relation":"intersects"
     *                 },
     *                 "ignore_unmapped":false,
     *                 "boost":1.0
     *               }
     *             },
     *             "source":"ST_INTERSECTS(location, \"POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))\")@2:9"
     *           }
     *         }][_doc{f}#140, limit[], sort[] estimatedRowSize[54]
     */
    public void testPushSpatialIntersectsStringToSourceAndUseDocValuesForCentroid() {
        for (String query : new String[] { """
            FROM airports
            | WHERE ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT()
            """, """
            FROM airports
            | WHERE ST_INTERSECTS(TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"), location)
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT()
            """ }) {

            var plan = this.physicalPlan(query, airports);
            var limit = as(plan, LimitExec.class);
            var agg = as(limit.child(), AggregateExec.class);
            assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
            // Before optimization the aggregation does not use doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

            var exchange = as(agg.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var fAgg = as(fragment.fragment(), Aggregate.class);
            var filter = as(fAgg.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
            var optimized = optimizedPlan(plan);
            limit = as(optimized, LimitExec.class);
            agg = as(limit.child(), AggregateExec.class);
            // Above the exchange (in coordinator) the aggregation is not using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
            exchange = as(agg.child(), ExchangeExec.class);
            agg = as(exchange.child(), AggregateExec.class);
            assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
            // below the exchange (in data node) the aggregation is using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
            var extract = as(agg.child(), FieldExtractExec.class);
            assertTrue(
                "Expect field attribute to be extracted as doc-values",
                extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
            );
            var source = source(extract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testPushSpatialIntersectsStringToSourceCompoundPredicate() {
        for (String query : new String[] { """
            FROM airports
            | WHERE scalerank == 9
              AND ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
              AND type == "mid"
            """, """
            FROM airports
            | WHERE scalerank == 9
              AND ST_INTERSECTS(TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"), location)
              AND type == "mid"
            """ }) {

            var plan = this.physicalPlan(query, airports);
            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            var and = as(filter.condition(), And.class);
            var left = as(and.left(), And.class);
            assertThat("filter contains ST_INTERSECTS", left.right(), instanceOf(SpatialIntersects.class));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            var source = source(fieldExtract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var booleanQuery = as(source.query(), BoolQueryBuilder.class);
            assertThat("Expected boolean query of three predicates", booleanQuery.must().size(), equalTo(3));
            var condition = as(booleanQuery.must().get(1), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testPushSpatialIntersectsStringToSourceCompoundPredicateAndUseDocValuesForCentroid() {
        for (String query : new String[] { """
            FROM airports
            | WHERE scalerank == 9
              AND ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
              AND type == "mid"
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT()
            """, """
            FROM airports
            | WHERE scalerank == 9
              AND ST_INTERSECTS(TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"), location)
              AND type == "mid"
            | STATS centroid=ST_CENTROID_AGG(location), count=COUNT()
            """ }) {

            var plan = this.physicalPlan(query, airports);
            var limit = as(plan, LimitExec.class);
            var agg = as(limit.child(), AggregateExec.class);
            assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
            // Before optimization the aggregation does not use doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);

            var exchange = as(agg.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var fAgg = as(fragment.fragment(), Aggregate.class);
            var filter = as(fAgg.child(), Filter.class);
            var and = as(filter.condition(), And.class);
            var left = as(and.left(), And.class);
            assertThat("filter contains ST_INTERSECTS", left.right(), instanceOf(SpatialIntersects.class));

            // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
            var optimized = optimizedPlan(plan);
            limit = as(optimized, LimitExec.class);
            agg = as(limit.child(), AggregateExec.class);
            // Above the exchange (in coordinator) the aggregation is not using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, false);
            exchange = as(agg.child(), ExchangeExec.class);
            agg = as(exchange.child(), AggregateExec.class);
            assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
            // below the exchange (in data node) the aggregation is using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, "centroid", SpatialCentroid.class, GEO_POINT, true);
            var extract = as(agg.child(), FieldExtractExec.class);
            assertTrue(
                "Expect field attribute to be extracted as doc-values",
                extract.attributesToExtract().stream().allMatch(attr -> extract.hasDocValuesAttribute(attr) && attr.dataType() == GEO_POINT)
            );
            var source = source(extract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var booleanQuery = as(source.query(), BoolQueryBuilder.class);
            assertThat("Expected boolean query of three predicates", booleanQuery.must().size(), equalTo(3));
            var condition = as(booleanQuery.must().get(1), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    /**
     * Plan:
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#16) AS location, SPATIALCENTROID(city_location{f}#19) AS city_location, COUNT([
     * 2a][KEYWORD]) AS count],FINAL,null]
     *   \_ExchangeExec[[xVal{r}#20, xDel{r}#21, yVal{r}#22, yDel{r}#23, count{r}#24, xVal{r}#25, xDel{r}#26, yVal{r}#27, yDel{r}#28,
     * count{r}#29, count{r}#30, seen{r}#31],true]
     *     \_FragmentExec[filter=null, estimatedRowSize=0, fragment=[
     * Aggregate[[],[SPATIALCENTROID(location{f}#16) AS location, SPATIALCENTROID(city_location{f}#19) AS city_location, COUNT([
     * 2a][KEYWORD]) AS count]]
     * \_Filter[SPATIALINTERSECTS(location{f}#16,city_location{f}#19)]
     *   \_EsRelation[airports][abbrev{f}#12, city{f}#18, city_location{f}#19, coun..]]]
     *
     * Optimized:
     * LimitExec[1000[INTEGER]]
     * \_AggregateExec[[],[SPATIALCENTROID(location{f}#16) AS location, SPATIALCENTROID(city_location{f}#19) AS city_location, COUNT([
     * 2a][KEYWORD]) AS count],FINAL,108]
     *   \_ExchangeExec[[xVal{r}#20, xDel{r}#21, yVal{r}#22, yDel{r}#23, count{r}#24, xVal{r}#25, xDel{r}#26, yVal{r}#27, yDel{r}#28,
     * count{r}#29, count{r}#30, seen{r}#31],true]
     *     \_AggregateExec[[],[SPATIALCENTROID(location{f}#16) AS location, SPATIALCENTROID(city_location{f}#19) AS city_location, COUNT([
     * 2a][KEYWORD]) AS count],PARTIAL,108]
     *       \_FilterExec[SPATIALINTERSECTS(location{f}#16,city_location{f}#19)]
     *         \_FieldExtractExec[location{f}#16, city_location{f}#19][city_location{f}#19, location{f}#16]
     *           \_EsQueryExec[airports], query[][_doc{f}#55], limit[], sort[] estimatedRowSize[104]
     */
    public void testIntersectsOnTwoPointFieldAndBothCentroidUsesDocValues() {
        String query = """
            FROM airports
            | WHERE ST_INTERSECTS(location, city_location)
            | STATS location=ST_CENTROID_AGG(location), city_location=ST_CENTROID_AGG(city_location), count=COUNT()
            """;

        var plan = this.physicalPlan(query, airports);
        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        var filter = as(fAgg.child(), Filter.class);
        assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

        // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, true);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, false);
        var filterExec = as(agg.child(), FilterExec.class);
        var extract = as(filterExec.child(), FieldExtractExec.class);
        assertFieldExtractionWithDocValues(extract, GEO_POINT, "location");
        source(extract.child());
    }

    public void testIntersectsOnTwoPointFieldAndOneCentroidUsesDocValues() {
        for (String query : new String[] { """
            FROM airports
            | WHERE ST_INTERSECTS(location, city_location)
            | STATS location=ST_CENTROID_AGG(location), count=COUNT()
            """, """
            FROM airports
            | WHERE ST_INTERSECTS(location, city_location)
            | STATS city_location=ST_CENTROID_AGG(city_location), count=COUNT()
            """ }) {

            var plan = this.physicalPlan(query, airports);
            var limit = as(plan, LimitExec.class);
            var agg = as(limit.child(), AggregateExec.class);
            assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
            // Before optimization the aggregation does not use doc-values
            assertAggregation(agg, "count", Count.class);
            var aggFieldName = findSingleAggregation(agg, "location", "city_location");
            assertAggregation(agg, aggFieldName, SpatialCentroid.class, GEO_POINT, false);

            var exchange = as(agg.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var fAgg = as(fragment.fragment(), Aggregate.class);
            var filter = as(fAgg.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
            var optimized = optimizedPlan(plan);
            limit = as(optimized, LimitExec.class);
            agg = as(limit.child(), AggregateExec.class);
            // Above the exchange (in coordinator) the aggregation is not using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, aggFieldName, SpatialCentroid.class, GEO_POINT, false);
            exchange = as(agg.child(), ExchangeExec.class);
            agg = as(exchange.child(), AggregateExec.class);
            assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
            // below the exchange (in data node) the aggregation is using doc-values
            assertAggregation(agg, "count", Count.class);
            assertAggregation(agg, aggFieldName, SpatialCentroid.class, GEO_POINT, true);
            var filterExec = as(agg.child(), FilterExec.class);
            var extract = as(filterExec.child(), FieldExtractExec.class);
            assertFieldExtractionWithDocValues(extract, GEO_POINT, aggFieldName);
            source(extract.child());
        }
    }

    public void testTwoIntersectsWithTwoCentroidsUsesDocValues() {
        String query = """
            FROM airports
            | WHERE ST_INTERSECTS(location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
                AND ST_INTERSECTS(city_location, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
            | STATS location=ST_CENTROID_AGG(location), city_location=ST_CENTROID_AGG(city_location), count=COUNT()
            """;

        var plan = this.physicalPlan(query, airports);
        var limit = as(plan, LimitExec.class);
        var agg = as(limit.child(), AggregateExec.class);
        assertThat("No groupings in aggregation", agg.groupings().size(), equalTo(0));
        // Before optimization the aggregation does not use doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, false);

        var exchange = as(agg.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var fAgg = as(fragment.fragment(), Aggregate.class);
        var filter = as(fAgg.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        assertThat("filter contains ST_INTERSECTS", and.left(), instanceOf(SpatialIntersects.class));
        assertThat("filter contains ST_INTERSECTS", and.right(), instanceOf(SpatialIntersects.class));

        // Now verify that optimization re-writes the ExchangeExec and pushed down the filter into the Lucene query
        var optimized = optimizedPlan(plan);
        limit = as(optimized, LimitExec.class);
        agg = as(limit.child(), AggregateExec.class);
        // Above the exchange (in coordinator) the aggregation is not using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, false);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, false);
        exchange = as(agg.child(), ExchangeExec.class);
        agg = as(exchange.child(), AggregateExec.class);
        assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(PARTIAL));
        // below the exchange (in data node) the aggregation is using doc-values
        assertAggregation(agg, "count", Count.class);
        assertAggregation(agg, "location", SpatialCentroid.class, GEO_POINT, true);
        assertAggregation(agg, "city_location", SpatialCentroid.class, GEO_POINT, true);
        var extract = as(agg.child(), FieldExtractExec.class);
        assertFieldExtractionWithDocValues(extract, GEO_POINT, "location", "city_location");
        var source = source(extract.child());
        // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
        // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
        var booleanQuery = as(source.query(), BoolQueryBuilder.class);
        assertThat("Expected boolean query of two predicates", booleanQuery.must().size(), equalTo(2));
        String[] fieldNames = new String[] { "location", "city_location" };
        for (String fieldName : fieldNames) {
            var condition = as(findQueryBuilder(booleanQuery, fieldName), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo(fieldName));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testPushSpatialIntersectsShapeToSource() {
        for (String query : new String[] { """
            FROM countriesBbox
            | WHERE ST_INTERSECTS(shape, TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"))
            """, """
            FROM countriesBbox
            | WHERE ST_INTERSECTS(TO_GEOSHAPE("POLYGON((42 14, 43 14, 43 15, 42 15, 42 14))"), shape)
            """ }) {

            var plan = this.physicalPlan(query, countriesBbox);
            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            var source = source(fieldExtract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("shape"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testPushSpatialDistanceToSource() {
        for (String distanceFunction : new String[] {
            "ST_DISTANCE(location, TO_GEOPOINT(\"POINT(12.565 55.673)\"))",
            "ST_DISTANCE(TO_GEOPOINT(\"POINT(12.565 55.673)\"), location)" }) {

            for (boolean reverse : new Boolean[] { false, true }) {
                for (String op : new String[] { "<", "<=", ">", ">=", "==" }) {
                    var expected = ExpectedComparison.from(op, reverse, 600000.0);
                    var predicate = reverse ? "600000 " + op + " " + distanceFunction : distanceFunction + " " + op + " 600000";
                    var query = "FROM airports | WHERE " + predicate + " AND scalerank > 1";
                    var plan = this.physicalPlan(query, airports);
                    var limit = as(plan, LimitExec.class);
                    var exchange = as(limit.child(), ExchangeExec.class);
                    var fragment = as(exchange.child(), FragmentExec.class);
                    var limit2 = as(fragment.fragment(), Limit.class);
                    var filter = as(limit2.child(), Filter.class);
                    var and = as(filter.condition(), And.class);
                    var comp = as(and.left(), EsqlBinaryComparison.class);
                    assertThat("filter contains expected binary comparison for " + predicate, comp, instanceOf(expected.comp));
                    assertThat("filter contains ST_DISTANCE", comp.left(), instanceOf(StDistance.class));

                    var optimized = optimizedPlan(plan);
                    var topLimit = as(optimized, LimitExec.class);
                    exchange = as(topLimit.child(), ExchangeExec.class);
                    var project = as(exchange.child(), ProjectExec.class);
                    var fieldExtract = as(project.child(), FieldExtractExec.class);
                    var source = source(fieldExtract.child());
                    var bool = as(source.query(), BoolQueryBuilder.class);
                    var rangeQueryBuilders = bool.filter().stream().filter(p -> p instanceof SingleValueQuery.Builder).toList();
                    assertThat("Expected one range query builder", rangeQueryBuilders.size(), equalTo(1));
                    assertThat(((SingleValueQuery.Builder) rangeQueryBuilders.get(0)).field(), equalTo("scalerank"));
                    if (op.equals("==")) {
                        var boolQueryBuilders = bool.filter().stream().filter(p -> p instanceof BoolQueryBuilder).toList();
                        assertThat("Expected one sub-bool query builder", boolQueryBuilders.size(), equalTo(1));
                        var bool2 = as(boolQueryBuilders.get(0), BoolQueryBuilder.class);
                        var shapeQueryBuilders = bool2.must()
                            .stream()
                            .filter(p -> p instanceof SpatialRelatesQuery.ShapeQueryBuilder)
                            .toList();
                        assertShapeQueryRange(shapeQueryBuilders, Math.nextDown(expected.value), expected.value);
                    } else {
                        var shapeQueryBuilders = bool.filter()
                            .stream()
                            .filter(p -> p instanceof SpatialRelatesQuery.ShapeQueryBuilder)
                            .toList();
                        assertThat("Expected one shape query builder", shapeQueryBuilders.size(), equalTo(1));
                        var condition = as(shapeQueryBuilders.get(0), SpatialRelatesQuery.ShapeQueryBuilder.class);
                        assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
                        assertThat("Spatial relationship", condition.relation(), equalTo(expected.shapeRelation()));
                        assertThat("Geometry is Circle", condition.shape().type(), equalTo(ShapeType.CIRCLE));
                        var circle = as(condition.shape(), Circle.class);
                        assertThat("Circle center-x", circle.getX(), equalTo(12.565));
                        assertThat("Circle center-y", circle.getY(), equalTo(55.673));
                        assertThat("Circle radius for predicate " + predicate, circle.getRadiusMeters(), equalTo(expected.value));
                    }
                }
            }
        }
    }

    public void testPushSpatialDistanceBandToSource() {
        var query = """
            FROM airports
            | WHERE ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 600000
                AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 400000
            """;
        var plan = this.physicalPlan(query, airports);
        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var limit2 = as(fragment.fragment(), Limit.class);
        var filter = as(limit2.child(), Filter.class);
        var and = as(filter.condition(), And.class);
        for (Expression expression : and.arguments()) {
            var comp = as(expression, EsqlBinaryComparison.class);
            var expectedComp = comp.equals(and.left()) ? LessThanOrEqual.class : GreaterThanOrEqual.class;
            assertThat("filter contains expected binary comparison", comp, instanceOf(expectedComp));
            assertThat("filter contains ST_DISTANCE", comp.left(), instanceOf(StDistance.class));
        }

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());
        var bool = as(source.query(), BoolQueryBuilder.class);
        var rangeQueryBuilders = bool.filter().stream().filter(p -> p instanceof SingleValueQuery.Builder).toList();
        assertThat("Expected zero range query builder", rangeQueryBuilders.size(), equalTo(0));
        var shapeQueryBuilders = bool.must().stream().filter(p -> p instanceof SpatialRelatesQuery.ShapeQueryBuilder).toList();
        assertShapeQueryRange(shapeQueryBuilders, 400000.0, 600000.0);
    }

    public void testPushSpatialDistanceDisjointBandsToSource() {
        var query = """
            FROM airports
            | WHERE (ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 600000
                 AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 400000)
               OR
                    (ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 300000
                 AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 200000)
            """;
        var plan = this.physicalPlan(query, airports);
        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var limit2 = as(fragment.fragment(), Limit.class);
        var filter = as(limit2.child(), Filter.class);
        var or = as(filter.condition(), Or.class);
        assertThat("OR has two predicates", or.arguments().size(), equalTo(2));
        for (Expression expression : or.arguments()) {
            var and = as(expression, And.class);
            for (Expression exp : and.arguments()) {
                var comp = as(exp, EsqlBinaryComparison.class);
                var expectedComp = comp.equals(and.left()) ? LessThanOrEqual.class : GreaterThanOrEqual.class;
                assertThat("filter contains expected binary comparison", comp, instanceOf(expectedComp));
                assertThat("filter contains ST_DISTANCE", comp.left(), instanceOf(StDistance.class));
            }
        }

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());
        var bool = as(source.query(), BoolQueryBuilder.class);
        var disjuntiveQueryBuilders = bool.should().stream().filter(p -> p instanceof BoolQueryBuilder).toList();
        assertThat("Expected two disjunctive query builders", disjuntiveQueryBuilders.size(), equalTo(2));
        for (int i = 0; i < disjuntiveQueryBuilders.size(); i++) {
            var subRangeBool = as(disjuntiveQueryBuilders.get(i), BoolQueryBuilder.class);
            var shapeQueryBuilders = subRangeBool.must().stream().filter(p -> p instanceof SpatialRelatesQuery.ShapeQueryBuilder).toList();
            assertShapeQueryRange(shapeQueryBuilders, i == 0 ? 400000.0 : 200000.0, i == 0 ? 600000.0 : 300000.0);
        }
    }

    public void testPushSpatialDistanceComplexPredicateToSource() {
        var query = """
            FROM airports
            | WHERE ((ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 600000
                  AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 400000
                  AND NOT (ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 500000
                       AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 430000))
                  OR (ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) <= 300000
                           AND ST_DISTANCE(location, TO_GEOPOINT("POINT(12.565 55.673)")) >= 200000))
                AND NOT abbrev == "PLQ"
                AND scalerank < 6
            """;
        var plan = this.physicalPlan(query, airports);
        var limit = as(plan, LimitExec.class);
        var exchange = as(limit.child(), ExchangeExec.class);
        var fragment = as(exchange.child(), FragmentExec.class);
        var limit2 = as(fragment.fragment(), Limit.class);
        var filter = as(limit2.child(), Filter.class);
        var outerAnd = as(filter.condition(), And.class);
        var outerLeft = as(outerAnd.left(), And.class);
        as(outerLeft.right(), Not.class);
        as(outerAnd.right(), LessThan.class);
        var or = as(outerLeft.left(), Or.class);
        var innerAnd1 = as(or.left(), And.class);
        var innerAnd2 = as(or.right(), And.class);
        for (Expression exp : innerAnd2.arguments()) {
            var comp = as(exp, EsqlBinaryComparison.class);
            var expectedComp = comp.equals(innerAnd2.left()) ? LessThanOrEqual.class : GreaterThanOrEqual.class;
            assertThat("filter contains expected binary comparison", comp, instanceOf(expectedComp));
            assertThat("filter contains ST_DISTANCE", comp.left(), instanceOf(StDistance.class));
        }

        var optimized = optimizedPlan(plan);
        var topLimit = as(optimized, LimitExec.class);
        exchange = as(topLimit.child(), ExchangeExec.class);
        var project = as(exchange.child(), ProjectExec.class);
        var fieldExtract = as(project.child(), FieldExtractExec.class);
        var source = source(fieldExtract.child());
        var bool = as(source.query(), BoolQueryBuilder.class);
        assertThat("Expected boolean query of three MUST clauses", bool.must().size(), equalTo(2));
        assertThat("Expected boolean query of one FILTER clause", bool.filter().size(), equalTo(1));
        var boolDisjuntive = as(bool.filter().get(0), BoolQueryBuilder.class);
        var disjuntiveQueryBuilders = boolDisjuntive.should().stream().filter(p -> p instanceof BoolQueryBuilder).toList();
        assertThat("Expected two disjunctive query builders", disjuntiveQueryBuilders.size(), equalTo(2));
        for (int i = 0; i < disjuntiveQueryBuilders.size(); i++) {
            var subRangeBool = as(disjuntiveQueryBuilders.get(i), BoolQueryBuilder.class);
            var shapeQueryBuilders = subRangeBool.must().stream().filter(p -> p instanceof SpatialRelatesQuery.ShapeQueryBuilder).toList();
            assertShapeQueryRange(shapeQueryBuilders, i == 0 ? 400000.0 : 200000.0, i == 0 ? 600000.0 : 300000.0);
        }
    }

    private void assertShapeQueryRange(List<QueryBuilder> shapeQueryBuilders, double min, double max) {
        assertThat("Expected two shape query builders", shapeQueryBuilders.size(), equalTo(2));
        var relationStats = new HashMap<ShapeRelation, Integer>();
        for (var builder : shapeQueryBuilders) {
            var condition = as(builder, SpatialRelatesQuery.ShapeQueryBuilder.class);
            var expected = condition.relation() == ShapeRelation.INTERSECTS ? max : min;
            relationStats.compute(condition.relation(), (r, c) -> c == null ? 1 : c + 1);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Geometry is Circle", condition.shape().type(), equalTo(ShapeType.CIRCLE));
            var circle = as(condition.shape(), Circle.class);
            assertThat("Circle center-x", circle.getX(), equalTo(12.565));
            assertThat("Circle center-y", circle.getY(), equalTo(55.673));
            assertThat("Circle radius for shape relation " + condition.relation(), circle.getRadiusMeters(), equalTo(expected));
        }
        assertThat("Expected one INTERSECTS and one DISJOINT", relationStats.size(), equalTo(2));
        assertThat("Expected one INTERSECTS", relationStats.get(ShapeRelation.INTERSECTS), equalTo(1));
        assertThat("Expected one DISJOINT", relationStats.get(ShapeRelation.DISJOINT), equalTo(1));
    }

    private record ExpectedComparison(Class<? extends EsqlBinaryComparison> comp, double value) {
        ShapeRelation shapeRelation() {
            return comp.getSimpleName().startsWith("GreaterThan") ? ShapeRelation.DISJOINT : ShapeRelation.INTERSECTS;
        }

        static ExpectedComparison from(String op, boolean reverse, double value) {
            double up = Math.nextUp(value);
            double down = Math.nextDown(value);
            return switch (op) {
                case "<" -> reverse ? from(GreaterThan.class, up) : from(LessThan.class, down);
                case "<=" -> reverse ? from(GreaterThanOrEqual.class, value) : from(LessThanOrEqual.class, value);
                case ">" -> reverse ? from(LessThan.class, down) : from(GreaterThan.class, up);
                case ">=" -> reverse ? from(LessThanOrEqual.class, value) : from(GreaterThanOrEqual.class, value);
                default -> from(Equals.class, value);
            };
        }

        static ExpectedComparison from(Class<? extends EsqlBinaryComparison> comp, double value) {
            return new ExpectedComparison(comp, value);
        }
    }

    public void testPushCartesianSpatialIntersectsToSource() {
        for (String query : new String[] { """
            FROM airports_web
            | WHERE ST_INTERSECTS(
                location,
                TO_CARTESIANSHAPE("POLYGON((4700000 1600000, 4800000 1600000, 4800000 1700000, 4700000 1700000, 4700000 1600000))")
              )
            """, """
            FROM airports_web
            | WHERE ST_INTERSECTS(
                TO_CARTESIANSHAPE("POLYGON((4700000 1600000, 4800000 1600000, 4800000 1700000, 4700000 1700000, 4700000 1600000))"),
                location
              )
            """ }) {

            var plan = this.physicalPlan(query, airportsWeb);
            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            var source = source(fieldExtract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("location"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testPushCartesianSpatialIntersectsShapeToSource() {
        for (String query : new String[] { """
            FROM countriesBboxWeb
            | WHERE ST_INTERSECTS(
                shape,
                TO_CARTESIANSHAPE(
                  "POLYGON((4700000 1600000, 4800000 1600000, 4800000 1700000, 4700000 1700000, 4700000 1600000))"
                )
              )
            """, """
            FROM countriesBboxWeb
            | WHERE ST_INTERSECTS(
                TO_CARTESIANSHAPE(
                  "POLYGON((4700000 1600000, 4800000 1600000, 4800000 1700000, 4700000 1700000, 4700000 1600000))"
                ),
                shape
              )
            """ }) {

            var plan = this.physicalPlan(query, countriesBboxWeb);

            var limit = as(plan, LimitExec.class);
            var exchange = as(limit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var limit2 = as(fragment.fragment(), Limit.class);
            var filter = as(limit2.child(), Filter.class);
            assertThat("filter contains ST_INTERSECTS", filter.condition(), instanceOf(SpatialIntersects.class));

            var optimized = optimizedPlan(plan);
            var topLimit = as(optimized, LimitExec.class);
            exchange = as(topLimit.child(), ExchangeExec.class);
            var project = as(exchange.child(), ProjectExec.class);
            var fieldExtract = as(project.child(), FieldExtractExec.class);
            var source = source(fieldExtract.child());
            // TODO: bring back SingleValueQuery once it can handle LeafShapeFieldData
            // var condition = as(sv(source.query(), "location"), AbstractGeometryQueryBuilder.class);
            var condition = as(source.query(), SpatialRelatesQuery.ShapeQueryBuilder.class);
            assertThat("Geometry field name", condition.fieldName(), equalTo("shape"));
            assertThat("Spatial relationship", condition.relation(), equalTo(ShapeRelation.INTERSECTS));
            assertThat("Geometry is Polygon", condition.shape().type(), equalTo(ShapeType.POLYGON));
            var polygon = as(condition.shape(), Polygon.class);
            assertThat("Polygon shell length", polygon.getPolygon().length(), equalTo(5));
            assertThat("Polygon holes", polygon.getNumberOfHoles(), equalTo(0));
        }
    }

    public void testEnrichBeforeAggregation() {
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _any:departments
                | STATS size=count(*) BY department""");
            var limit = as(plan, LimitExec.class);
            var finalAggs = as(limit.child(), AggregateExec.class);
            assertThat(finalAggs.getMode(), equalTo(FINAL));
            var exchange = as(finalAggs.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialAggs = as(fragment.fragment(), Aggregate.class);
            var enrich = as(partialAggs.child(), Enrich.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                | STATS size=count(*) BY department""");
            var limit = as(plan, LimitExec.class);
            var finalAggs = as(limit.child(), AggregateExec.class);
            assertThat(finalAggs.getMode(), equalTo(FINAL));
            var partialAggs = as(finalAggs.child(), AggregateExec.class);
            assertThat(partialAggs.getMode(), equalTo(PARTIAL));
            var enrich = as(partialAggs.child(), EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var exchange = as(enrich.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var eval = as(fragment.fragment(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _remote:departments
                | STATS size=count(*) BY department""");
            var limit = as(plan, LimitExec.class);
            var finalAggs = as(limit.child(), AggregateExec.class);
            assertThat(finalAggs.getMode(), equalTo(FINAL));
            var exchange = as(finalAggs.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialAggs = as(fragment.fragment(), Aggregate.class);
            var enrich = as(partialAggs.child(), Enrich.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.REMOTE));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
    }

    public void testEnrichAfterAggregation() {
        {
            var plan = physicalPlan("""
                from test
                | STATS size=count(*) BY emp_no
                | eval employee_id = to_str(emp_no)
                | ENRICH _any:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), EvalExec.class);
            var limit = as(eval.child(), LimitExec.class);
            var finalAggs = as(limit.child(), AggregateExec.class);
            assertThat(finalAggs.getMode(), equalTo(FINAL));
            var exchange = as(finalAggs.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialAggs = as(fragment.fragment(), Aggregate.class);
            as(partialAggs.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | STATS size=count(*) BY emp_no
                | eval employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var eval = as(enrich.child(), EvalExec.class);
            var limit = as(eval.child(), LimitExec.class);
            var finalAggs = as(limit.child(), AggregateExec.class);
            assertThat(finalAggs.getMode(), equalTo(FINAL));
            var exchange = as(finalAggs.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialAggs = as(fragment.fragment(), Aggregate.class);
            as(partialAggs.child(), EsRelation.class);
        }
    }

    public void testAggThenEnrichRemote() {
        var error = expectThrows(VerificationException.class, () -> physicalPlan("""
            from test
            | STATS size=count(*) BY emp_no
            | eval employee_id = to_str(emp_no)
            | ENRICH _remote:departments
            """));
        assertThat(error.getMessage(), containsString("line 4:3: ENRICH with remote policy can't be executed after STATS"));
    }

    public void testEnrichBeforeLimit() {
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _any:departments
                | LIMIT 10""");
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), EvalExec.class);
            var finalLimit = as(eval.child(), LimitExec.class);
            var exchange = as(finalLimit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialLimit = as(fragment.fragment(), Limit.class);
            as(partialLimit.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                | LIMIT 10""");
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var eval = as(enrich.child(), EvalExec.class);
            var finalLimit = as(eval.child(), LimitExec.class);
            var exchange = as(finalLimit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialLimit = as(fragment.fragment(), Limit.class);
            as(partialLimit.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _remote:departments
                | LIMIT 10""");
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.REMOTE));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), EvalExec.class);
            var finalLimit = as(eval.child(), LimitExec.class);
            var exchange = as(finalLimit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialLimit = as(fragment.fragment(), Limit.class);
            as(partialLimit.child(), EsRelation.class);
        }
    }

    public void testLimitThenEnrich() {
        {
            var plan = physicalPlan("""
                FROM test
                | LIMIT 10
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _any:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), EvalExec.class);
            var finalLimit = as(eval.child(), LimitExec.class);
            var exchange = as(finalLimit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialLimit = as(fragment.fragment(), Limit.class);
            as(partialLimit.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | LIMIT 10
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var eval = as(enrich.child(), EvalExec.class);
            var finalLimit = as(eval.child(), LimitExec.class);
            var exchange = as(finalLimit.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialLimit = as(fragment.fragment(), Limit.class);
            as(partialLimit.child(), EsRelation.class);
        }
    }

    public void testLimitThenEnrichRemote() {
        var error = expectThrows(VerificationException.class, () -> physicalPlan("""
            FROM test
            | LIMIT 10
            | EVAL employee_id = to_str(emp_no)
            | ENRICH _remote:departments
            """));
        assertThat(error.getMessage(), containsString("line 4:3: ENRICH with remote policy can't be executed after LIMIT"));
    }

    public void testEnrichBeforeTopN() {
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _any:departments
                | SORT department
                | LIMIT 10""");
            var topN = as(plan, TopNExec.class);
            var exchange = as(topN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            var enrich = as(partialTopN.child(), Enrich.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                | SORT department
                | LIMIT 10""");
            var topN = as(plan, TopNExec.class);
            var enrich = as(topN.child(), EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var exchange = as(enrich.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var eval = as(fragment.fragment(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _remote:departments
                | SORT department
                | LIMIT 10""");
            var topN = as(plan, TopNExec.class);
            var exchange = as(topN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            var enrich = as(partialTopN.child(), Enrich.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.REMOTE));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
    }

    public void testEnrichAfterTopN() {
        {
            var plan = physicalPlan("""
                FROM test
                | SORT emp_no
                | LIMIT 10
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _any:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.ANY));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-1", "cluster_1", ".enrich-departments-2")));
            var eval = as(enrich.child(), EvalExec.class);
            var topN = as(eval.child(), TopNExec.class);
            var exchange = as(topN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            as(partialTopN.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                FROM test
                | SORT emp_no
                | LIMIT 10
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                """);
            var enrich = as(plan, EnrichExec.class);
            assertThat(enrich.mode(), equalTo(Enrich.Mode.COORDINATOR));
            assertThat(enrich.concreteIndices(), equalTo(Map.of("", ".enrich-departments-3")));
            var eval = as(enrich.child(), EvalExec.class);
            var topN = as(eval.child(), TopNExec.class);
            var exchange = as(topN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            as(partialTopN.child(), EsRelation.class);
        }
    }

    public void testManyEnrich() {
        {
            var plan = physicalPlan("""
                FROM test
                | EVAL employee_id = to_str(emp_no)
                | ENRICH _any:departments
                | SORT emp_no
                | LIMIT 100
                | ENRICH _any:supervisors
                | STATS teams=count(*) BY supervisor
                """);
            var limit = as(plan, LimitExec.class);
            var finalAgg = as(limit.child(), AggregateExec.class);
            var partialAgg = as(finalAgg.child(), AggregateExec.class);
            var enrich1 = as(partialAgg.child(), EnrichExec.class);
            assertThat(enrich1.policyName(), equalTo("supervisors"));
            assertThat(enrich1.mode(), equalTo(Enrich.Mode.ANY));
            var finalTopN = as(enrich1.child(), TopNExec.class);
            var exchange = as(finalTopN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            var enrich2 = as(partialTopN.child(), Enrich.class);
            assertThat(BytesRefs.toString(enrich2.policyName().fold()), equalTo("departments"));
            assertThat(enrich2.mode(), equalTo(Enrich.Mode.ANY));
            var eval = as(enrich2.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _any:departments
                | SORT emp_no
                | LIMIT 100
                | ENRICH _coordinator:supervisors
                | STATS teams=count(*) BY supervisor
                """);
            var limit = as(plan, LimitExec.class);
            var finalAgg = as(limit.child(), AggregateExec.class);
            var partialAgg = as(finalAgg.child(), AggregateExec.class);
            var enrich1 = as(partialAgg.child(), EnrichExec.class);
            assertThat(enrich1.policyName(), equalTo("supervisors"));
            assertThat(enrich1.mode(), equalTo(Enrich.Mode.COORDINATOR));
            var finalTopN = as(enrich1.child(), TopNExec.class);
            var exchange = as(finalTopN.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var partialTopN = as(fragment.fragment(), TopN.class);
            var enrich2 = as(partialTopN.child(), Enrich.class);
            assertThat(BytesRefs.toString(enrich2.policyName().fold()), equalTo("departments"));
            assertThat(enrich2.mode(), equalTo(Enrich.Mode.ANY));
            var eval = as(enrich2.child(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                | SORT emp_no
                | LIMIT 100
                | ENRICH _any:supervisors
                | STATS teams=count(*) BY supervisor
                """);
            var limit = as(plan, LimitExec.class);
            var finalAgg = as(limit.child(), AggregateExec.class);
            var partialAgg = as(finalAgg.child(), AggregateExec.class);
            var enrich1 = as(partialAgg.child(), EnrichExec.class);
            assertThat(enrich1.policyName(), equalTo("supervisors"));
            assertThat(enrich1.mode(), equalTo(Enrich.Mode.ANY));
            var topN = as(enrich1.child(), TopNExec.class);
            var enrich2 = as(topN.child(), EnrichExec.class);
            assertThat(enrich2.policyName(), equalTo("departments"));
            assertThat(enrich2.mode(), equalTo(Enrich.Mode.COORDINATOR));
            var exchange = as(enrich2.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var eval = as(fragment.fragment(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
        {
            var plan = physicalPlan("""
                from test
                | eval employee_id = to_str(emp_no)
                | ENRICH _coordinator:departments
                | SORT emp_no
                | LIMIT 100
                | ENRICH _any:supervisors
                | STATS teams=count(*) BY supervisor
                """);
            var limit = as(plan, LimitExec.class);
            var finalAgg = as(limit.child(), AggregateExec.class);
            var partialAgg = as(finalAgg.child(), AggregateExec.class);
            var enrich1 = as(partialAgg.child(), EnrichExec.class);
            assertThat(enrich1.policyName(), equalTo("supervisors"));
            assertThat(enrich1.mode(), equalTo(Enrich.Mode.ANY));
            var topN = as(enrich1.child(), TopNExec.class);
            var enrich2 = as(topN.child(), EnrichExec.class);
            assertThat(enrich2.policyName(), equalTo("departments"));
            assertThat(enrich2.mode(), equalTo(Enrich.Mode.COORDINATOR));
            var exchange = as(enrich2.child(), ExchangeExec.class);
            var fragment = as(exchange.child(), FragmentExec.class);
            var eval = as(fragment.fragment(), Eval.class);
            as(eval.child(), EsRelation.class);
        }
    }

    public void testRejectRemoteEnrichAfterCoordinatorEnrich() {
        var error = expectThrows(VerificationException.class, () -> physicalPlan("""
            from test
            | eval employee_id = to_str(emp_no)
            | ENRICH _coordinator:departments
            | ENRICH _remote:supervisors
            """));
        assertThat(
            error.getMessage(),
            containsString("ENRICH with remote policy can't be executed after another ENRICH with coordinator policy")
        );
    }

    public void testMaxExpressionDepth_cast() {
        StringBuilder queryBuilder = new StringBuilder(randomBoolean() ? "row a = 1" : "row a = 1 | eval b = a");
        queryBuilder.append("::long::int".repeat(MAX_EXPRESSION_DEPTH / 2 - 1));
        var query = queryBuilder.toString();

        physicalPlan(query);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(query + "::long"));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );
    }

    public void testMaxExpressionDepth_math() {
        StringBuilder queryBuilder = new StringBuilder(randomBoolean() ? "row a = 1" : "row a = 1 | eval b = a");
        String expression = " " + randomFrom("+", "-", "*", "/") + " 1";
        queryBuilder.append(expression.repeat(MAX_EXPRESSION_DEPTH - 2));
        var query = queryBuilder.toString();

        physicalPlan(query);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(query + expression));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );
    }

    public void testMaxExpressionDepth_boolean() {
        StringBuilder queryBuilder = new StringBuilder(randomBoolean() ? "row a = true " : "row a = true | eval b = a");
        String expression = " " + randomFrom("and", "or") + " true";
        queryBuilder.append(expression.repeat(MAX_EXPRESSION_DEPTH - 2));
        var query = queryBuilder.toString();

        physicalPlan(query);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(query + expression));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );
    }

    public void testMaxExpressionDepth_parentheses() {
        String query = "row a = true | eval b = ";
        StringBuilder expression = new StringBuilder("(".repeat(MAX_EXPRESSION_DEPTH / 2 - 1));
        expression.append("a");
        expression.append(")".repeat(MAX_EXPRESSION_DEPTH / 2 - 1));

        physicalPlan(query + expression);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(query + "(" + expression + ")"));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );
    }

    public void testMaxExpressionDepth_mixed() {
        String prefix = "abs(";
        String suffix = " + 12)";

        String from = "row a = 1 | eval b = ";

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(prefix.repeat(MAX_EXPRESSION_DEPTH / 2 - 1));
        queryBuilder.append("a");
        queryBuilder.append(suffix.repeat(MAX_EXPRESSION_DEPTH / 2 - 1));
        var expression = queryBuilder.toString();

        physicalPlan(from + expression);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(from + prefix + expression + suffix));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );
    }

    public void testMaxQueryDepth() {
        StringBuilder from = new StringBuilder("row a = 1 ");
        for (int i = 0; i < MAX_QUERY_DEPTH; i++) {
            from.append(randomBoolean() ? "| where a > 0 " : " | eval b" + i + " = a + " + i);
        }
        physicalPlan(from.toString());
        var e = expectThrows(ParsingException.class, () -> physicalPlan(from + (randomBoolean() ? "| sort a" : " | eval c = 10")));
        assertThat(e.getMessage(), containsString("ESQL statement exceeded the maximum query depth allowed (" + MAX_QUERY_DEPTH + ")"));
    }

    public void testMaxQueryDepthPlusExpressionDepth() {
        StringBuilder mainQuery = new StringBuilder("row a = 1 ");
        for (int i = 0; i < MAX_QUERY_DEPTH; i++) {
            mainQuery.append(" | eval b" + i + " = a + " + i);
        }

        physicalPlan(mainQuery.toString());

        var cast = "::long::int".repeat(MAX_EXPRESSION_DEPTH / 2 - 2) + "::long";

        physicalPlan(mainQuery + cast);

        var e = expectThrows(ParsingException.class, () -> physicalPlan(mainQuery + cast + "::int"));
        assertThat(
            e.getMessage(),
            containsString("ESQL statement exceeded the maximum expression depth allowed (" + MAX_EXPRESSION_DEPTH + ")")
        );

        e = expectThrows(ParsingException.class, () -> physicalPlan(mainQuery + cast + " | eval x = 10"));
        assertThat(e.getMessage(), containsString("ESQL statement exceeded the maximum query depth allowed (" + MAX_QUERY_DEPTH + ")"));
    }

    public void testLookupSimple() {
        String query = """
            FROM test
            | RENAME languages AS int
            | LOOKUP int_number_names ON int""";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        PhysicalPlan plan = physicalPlan(query);
        var join = as(plan, HashJoinExec.class);
        assertMap(join.matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertMap(
            join.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                .item(startsWith("int{r}"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
                .item(startsWith("name{r}"))
        );
    }

    /**
     * Expected
     * {@code
     * ProjectExec[[emp_no{f}#17, int{r}#5 AS languages, name{f}#28 AS lang_name]]
     * \_HashJoinExec[
     *      LocalSourceExec[[int{f}#27, name{f}#28],[...]],
     *      [int{r}#5],
     *      [name{r}#28, _meta_field{f}#23, emp_no{f}#17, ...]]
     *   \_ProjectExec[[_meta_field{f}#23, emp_no{f}#17, ...]]
     *     \_TopNExec[[Order[emp_no{f}#17,ASC,LAST]],4[INTEGER],370]
     *       \_ExchangeExec[[],false]
     *         \_ProjectExec[[emp_no{f}#17, ..., languages{f}#20]]
     *           \_FieldExtractExec[emp_no{f}#17, _meta_field{f}#23, first_name{f}#18, ..]<[]>
     *             \_EsQueryExec[...]
     * }
     */
    public void testLookupThenProject() {
        String query = """
            FROM employees
            | SORT emp_no
            | LIMIT 4
            | RENAME languages AS int
            | LOOKUP int_number_names ON int
            | RENAME int AS languages, name AS lang_name
            | KEEP emp_no, languages, lang_name""";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 5:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        PhysicalPlan plan = optimizedPlan(physicalPlan(query));

        var outerProject = as(plan, ProjectExec.class);
        assertThat(outerProject.projections().toString(), containsString("AS lang_name"));
        var join = as(outerProject.child(), HashJoinExec.class);
        assertMap(join.matchFields().stream().map(Object::toString).toList(), matchesList().item(startsWith("int{r}")));
        assertMap(
            join.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                .item(startsWith("int{r}"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
                .item(startsWith("name{r}"))
        );

        var middleProject = as(join.child(), ProjectExec.class);
        assertThat(middleProject.projections().stream().map(Objects::toString).toList(), not(hasItem(startsWith("name{f}"))));
        /*
         * At the moment we don't push projections past the HashJoin so we still include first_name here
         */
        assertThat(middleProject.projections().stream().map(Objects::toString).toList(), hasItem(startsWith("first_name{f}")));

        var outerTopn = as(middleProject.child(), TopNExec.class);
        var exchange = as(outerTopn.child(), ExchangeExec.class);
        var innerProject = as(exchange.child(), ProjectExec.class);
        assertThat(innerProject.projections().stream().map(Objects::toString).toList(), not(hasItem(startsWith("name{f}"))));
    }

    /**
     * Expects optimized data node plan of
     * <pre>{@code
     * TopN[[Order[name{r}#25,ASC,LAST], Order[emp_no{f}#14,ASC,LAST]],1000[INTEGER]]
     * \_Join[JoinConfig[type=LEFT OUTER, unionFields=[int{r}#4]]]
     *   |_EsqlProject[[..., long_noidx{f}#23, salary{f}#19]]
     *   | \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     *   \_LocalRelation[[int{f}#24, name{f}#25],[...]]
     * }</pre>
     */
    public void testLookupThenTopN() {
        String query = """
            FROM employees
            | RENAME languages AS int
            | LOOKUP int_number_names ON int
            | RENAME name AS languages
            | KEEP languages, emp_no
            | SORT languages ASC, emp_no ASC""";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> analyze(query));
            assertThat(e.getMessage(), containsString("line 3:4: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var plan = physicalPlan(query);

        ProjectExec outerProject = as(plan, ProjectExec.class);
        TopNExec outerTopN = as(outerProject.child(), TopNExec.class);
        ExchangeExec exchange = as(outerTopN.child(), ExchangeExec.class);
        FragmentExec frag = as(exchange.child(), FragmentExec.class);

        LogicalPlan opt = logicalOptimizer.optimize(frag.fragment());
        TopN innerTopN = as(opt, TopN.class);
        assertMap(
            innerTopN.order().stream().map(o -> o.child().toString()).toList(),
            matchesList().item(startsWith("name{r}")).item(startsWith("emp_no{f}"))
        );
        Join join = as(innerTopN.child(), Join.class);
        assertThat(join.config().type(), equalTo(JoinType.LEFT));
        assertMap(join.config().matchFields().stream().map(Objects::toString).toList(), matchesList().item(startsWith("int{r}")));

        Project innerProject = as(join.left(), Project.class);
        assertThat(innerProject.projections(), hasSize(10));
        assertMap(
            innerProject.projections().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field{f}"))
                .item(startsWith("emp_no{f}"))
                .item(startsWith("first_name{f}"))
                .item(startsWith("gender{f}"))
                .item(startsWith("job{f}"))
                .item(startsWith("job.raw{f}"))
                .item(matchesRegex("languages\\{f}#\\d+ AS int#\\d+"))
                .item(startsWith("last_name{f}"))
                .item(startsWith("long_noidx{f}"))
                .item(startsWith("salary{f}"))
        );

        LocalRelation lookup = as(join.right(), LocalRelation.class);
        assertMap(
            lookup.output().stream().map(Object::toString).toList(),
            matchesList().item(startsWith("int{f}")).item(startsWith("name{f}"))
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertFilterCondition(
        Filter filter,
        Class<? extends BinaryComparison> conditionClass,
        String fieldName,
        Object expected
    ) {
        var condition = as(filter.condition(), conditionClass);
        var field = as(condition.left(), FieldAttribute.class);
        assertThat("Expected filter field", field.name(), equalTo(fieldName));
        var value = as(condition.right(), Literal.class);
        assertThat("Expected filter value", value.value(), equalTo(expected));
    }

    private static void assertAggregation(
        PhysicalPlan plan,
        String aliasName,
        Class<? extends AggregateFunction> aggClass,
        DataType fieldType,
        boolean useDocValues
    ) {
        var aggFunc = assertAggregation(plan, aliasName, aggClass);
        var aggField = as(aggFunc.field(), Attribute.class);
        var spatialAgg = as(aggFunc, SpatialAggregateFunction.class);
        assertThat("Expected spatial aggregation to use doc-values", spatialAgg.useDocValues(), equalTo(useDocValues));
        assertThat("", aggField.dataType(), equalTo(fieldType));
    }

    private static AggregateFunction assertAggregation(PhysicalPlan plan, String aliasName, Class<? extends AggregateFunction> aggClass) {
        var agg = as(plan, AggregateExec.class);
        var aggExp = agg.aggregates().stream().filter(a -> {
            var alias = as(a, Alias.class);
            return alias.name().equals(aliasName);
        }).findFirst().orElseThrow(() -> new AssertionError("Expected aggregation " + aliasName + " not found"));
        var alias = as(aggExp, Alias.class);
        assertThat(alias.name(), is(aliasName));
        var aggFunc = as(alias.child(), AggregateFunction.class);
        assertThat(aggFunc, instanceOf(aggClass));
        return aggFunc;
    }

    private static String findSingleAggregation(PhysicalPlan plan, String... aliasNames) {
        var agg = as(plan, AggregateExec.class);
        var aggExps = agg.aggregates().stream().filter(a -> {
            var alias = as(a, Alias.class);
            return Arrays.stream(aliasNames).anyMatch(name -> name.equals(alias.name()));
        }).toList();
        if (aggExps.size() != 1) {
            throw new AssertionError(
                "Expected single aggregation from " + Arrays.toString(aliasNames) + " but found " + aggExps.size() + " aggregations"
            );
        }
        var aggExp = aggExps.get(0);
        var alias = as(aggExp, Alias.class);
        return alias.name();
    }

    private static QueryBuilder findQueryBuilder(BoolQueryBuilder booleanQuery, String fieldName) {
        return booleanQuery.must()
            .stream()
            .filter(b -> ((SpatialRelatesQuery.ShapeQueryBuilder) b).fieldName().equals(fieldName))
            .findFirst()
            .get();
    }

    private void assertFieldExtractionWithDocValues(FieldExtractExec extract, DataType dataType, String... fieldNames) {
        extract.attributesToExtract().forEach(attr -> {
            String name = attr.name();
            if (asList(fieldNames).contains(name)) {
                assertThat("Expected field '" + name + "' to use doc-values", extract.hasDocValuesAttribute(attr), equalTo(true));
                assertThat("Expected field '" + name + "' to have data type " + dataType, attr.dataType(), equalTo(dataType));
            } else {
                assertThat("Expected field '" + name + "' to NOT use doc-values", extract.hasDocValuesAttribute(attr), equalTo(false));
            }
        });
    }

    private static EsQueryExec source(PhysicalPlan plan) {
        if (plan instanceof ExchangeExec exchange) {
            plan = exchange.child();
        }
        return as(plan, EsQueryExec.class);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan) {
        return optimizedPlan(plan, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        // System.out.println("* Physical Before\n" + plan);
        var p = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(plan));
        // System.out.println("* Physical After\n" + p);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is

        var l = p.transformUp(FragmentExec.class, fragment -> {
            var localPlan = PlannerUtils.localPlan(config, fragment, searchStats);
            return EstimatesRowSize.estimateRowSize(fragment.estimatedRowSize(), localPlan);
        });

        // handle local reduction alignment
        l = localRelationshipAlignment(l);
        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    static SearchStats statsWithIndexedFields(String... names) {
        return new EsqlTestUtils.TestSearchStats() {
            private final Set<String> indexedFields = Set.of(names);

            @Override
            public boolean isIndexed(String field) {
                return indexedFields.contains(field);
            }
        };
    }

    static PhysicalPlan localRelationshipAlignment(PhysicalPlan l) {
        // handle local reduction alignment
        return l.transformUp(ExchangeExec.class, exg -> {
            PhysicalPlan pl = exg;
            if (exg.isInBetweenAggs() && exg.child() instanceof LocalSourceExec lse) {
                var output = exg.output();
                if (lse.output().equals(output) == false) {
                    pl = exg.replaceChild(new LocalSourceExec(lse.source(), output, lse.supplier()));
                }
            }
            return pl;
        });

    }

    private PhysicalPlan physicalPlan(String query) {
        return physicalPlan(query, testData);
    }

    private PhysicalPlan physicalPlan(String query, TestDataSource dataSource) {
        var logical = logicalOptimizer.optimize(dataSource.analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        // System.out.println(physical);
        assertSerialization(physical);
        return physical;
    }

    private List<FieldSort> sorts(List<Order> orders) {
        return orders.stream().map(o -> new FieldSort((FieldAttribute) o.child(), o.direction(), o.nullsPosition())).toList();
    }

    private ExchangeExec asRemoteExchange(PhysicalPlan plan) {
        return as(plan, ExchangeExec.class);
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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
