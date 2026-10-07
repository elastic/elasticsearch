/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that a metrics query run with {@code SET exemplars=true} is rewritten into a query over the exemplar data stream that keeps
 * the dimension filters of the metrics query and selects the exemplars of the aggregated metrics by {@code metric_name}. The metrics
 * query is analyzed and the rewritten plan is taken through analysis again like {@code EsqlSession} does, so the tests observe the
 * analyzed exemplar query.
 * <p>
 * The metrics and exemplar indices are built the way {@code IndexResolver} sees real OTel indices: {@code attributes} and
 * {@code metrics} are passthrough objects, so next to {@code attributes.cpu} and {@code metrics.cpu_time} field caps report the
 * root-level aliases {@code cpu} and {@code cpu_time} as regular fields, indistinguishable from the fields they alias.
 */
public class ExemplarsRewriterTests extends ESTestCase {

    public void testRewritesMetricsQueryToExemplarQuery() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | WHERE attributes.state == "idle"
            | STATS AVG(cpu_time)
            """);

        assertFalse(plan.anyMatch(TimeSeriesAggregate.class::isInstance));

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.state", "metric_name"), referenceNames(filter));
        assertEquals(Set.of("cpu_time"), selectedMetricNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("exemplars-generic.otel-default", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    /** The exemplars come most recent first, under the default limit of a regular query when the setting gives none. */
    public void testExemplarsAreSortedByTimestampDescending() {
        LogicalPlan plan = analyze("TS metrics-generic.otel-default | STATS AVG(cpu_time)");

        Limit limit = as(plan, Limit.class);
        assertEquals(1000, as(limit.limit(), Literal.class).value());
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Order order = as(orderBy.order().getFirst(), Order.class);
        assertEquals("@timestamp", as(order.child(), Attribute.class).name());
        assertEquals(Order.OrderDirection.DESC, order.direction());
        as(orderBy.child(), Filter.class);
    }

    /** A limit in the setting caps the exemplars explicitly; the analysis then only adds the maximum cap on top, like for a user LIMIT. */
    public void testExemplarsLimit() {
        LogicalPlan plan = analyze(
            "TS metrics-generic.otel-default | STATS AVG(cpu_time)",
            testAnalyzer(),
            exemplarsResolution(EXEMPLARS_INDEX),
            5
        );

        Limit maxCap = as(plan, Limit.class);
        Limit limit = as(maxCap.child(), Limit.class);
        assertEquals(5, as(limit.limit(), Literal.class).value());
        as(limit.child(), OrderBy.class);
    }

    public void testMultipleMetricsSelectExemplarsOfAny() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | STATS AVG(cpu_time), MAX(memory_usage)
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("metric_name"), referenceNames(filter));
        assertTrue(filter.condition().anyMatch(In.class::isInstance));
        assertEquals(Set.of("cpu_time", "memory_usage"), selectedMetricNames(filter));
    }

    /**
     * The filter of an aggregate function applies to its metric only, on top of the {@code WHERE}s before the aggregation which apply
     * to all of them, and also when the function is nested in another one. Metrics under the same filters share one condition; a value
     * filter is dropped like in a {@code WHERE}, so {@code MIN(memory_usage) WHERE memory_usage > 100} is under no filter at all.
     */
    public void testAggregateFiltersApplyToTheirMetricOnly() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | WHERE attributes.cpu == "cpu1"
            | STATS AVG(AVG_OVER_TIME(cpu_time)) WHERE attributes.state == "idle",
                    MAX(memory_usage) WHERE attributes.state == "idle",
                    MIN(memory_usage) WHERE memory_usage > 100,
                    MAX(cpu_time)
            """);

        assertThat(
            selections(findFilter(plan)),
            containsInAnyOrder(
                new Selection(Set.of("cpu_time", "memory_usage"), Set.of("attributes.cpu", "attributes.state", "metric_name")),
                new Selection(Set.of("cpu_time", "memory_usage"), Set.of("attributes.cpu", "metric_name"))
            )
        );
    }

    /**
     * A PromQL binary operation over selectors with the same labels is translated to a single time series aggregation of both metrics,
     * each under the filter of its own label matchers, which the exemplar query keeps apart.
     */
    public void testPromqlBinaryOperationOverSharedAggregation() {
        LogicalPlan plan = analyze("""
            PROMQL index=metrics-generic.otel-default step=5m start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z"
              (sum by (attributes.cpu) (avg_over_time(cpu_time{attributes.state="idle"}[5m]))
               / sum by (attributes.cpu) (avg_over_time(memory_usage{attributes.cpu="cpu1"}[5m])))
            """);

        assertThat(
            selections(findFilter(plan)),
            containsInAnyOrder(
                new Selection(Set.of("cpu_time"), Set.of("@timestamp", "attributes.state", "metric_name")),
                new Selection(Set.of("memory_usage"), Set.of("@timestamp", "attributes.cpu", "metric_name"))
            )
        );
    }

    /**
     * With explicit vector matching the operands are translated to separate time series aggregations, each with its own filters below
     * it, and the exemplar query is the same as for the shared aggregation.
     */
    public void testPromqlBinaryOperationWithVectorMatching() {
        LogicalPlan plan = analyze("""
            PROMQL index=metrics-generic.otel-default step=5m start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z"
              (sum by (attributes.cpu) (avg_over_time(cpu_time{attributes.state="idle"}[5m]))
               / on (attributes.cpu) sum by (attributes.cpu) (avg_over_time(memory_usage{attributes.cpu="cpu1"}[5m])))
            """);

        assertThat(
            selections(findFilter(plan)),
            containsInAnyOrder(
                new Selection(Set.of("cpu_time"), Set.of("@timestamp", "attributes.state", "metric_name")),
                new Selection(Set.of("memory_usage"), Set.of("@timestamp", "attributes.cpu", "metric_name"))
            )
        );
    }

    /**
     * The metric name is the name of the metric field. A metric referred to by the full name of its field in the passthrough object
     * {@code metrics} thus selects the exemplars of {@code metrics.cpu_time}, which no exemplar carries as metric name: see the TODO on
     * {@code ExemplarsRewriter#metricName}. Dimensions may be referred to by the full field name or by the root-level alias alike, as
     * the exemplar index has both.
     */
    public void testMetricsAndDimensionsReferencedByFullFieldName() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | WHERE attributes.state == "idle" AND metrics.memory_usage IS NULL
            | STATS AVG(metrics.cpu_time), MAX(memory_usage)
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.state", "metric_name"), referenceNames(filter));
        assertEquals(Set.of("metrics.cpu_time", "memory_usage"), selectedMetricNames(filter));
        // metrics.memory_usage IS NULL became metric_name != "metrics.memory_usage"
        NotEquals notEquals = as(filter.condition().collectFirstChildren(NotEquals.class::isInstance).getFirst(), NotEquals.class);
        assertEquals("metrics.memory_usage", literalValue(notEquals.right()));
    }

    /**
     * Only filters on dimensions carry over to the exemplars; a filter on the metric value would select exemplars by their own
     * value rather than by the series they belong to, so it is dropped.
     */
    public void testMetricValueFiltersAreNotAppliedToExemplars() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | WHERE attributes.state == "idle" AND cpu_time > 5
            | STATS AVG(cpu_time)
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.state", "metric_name"), referenceNames(filter));
        assertFalse(filter.condition().anyMatch(GreaterThan.class::isInstance));
    }

    /**
     * Null checks on metrics, also through (chained) conversion functions, describe which series are read and carry over as conditions
     * on {@code metric_name}, while the metrics they mention are not what the exemplars are fetched for: only the aggregated metric is.
     */
    public void testMetricNullChecksAreAppliedToExemplars() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | WHERE memory_usage IS NULL AND TO_STRING(TO_LONG(cpu_time)) IS NOT NULL
            | STATS AVG(cpu_time)
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("metric_name"), referenceNames(filter));
        assertEquals(Set.of("cpu_time"), selectedMetricNames(filter));
        // memory_usage IS NULL became metric_name != "memory_usage"
        NotEquals notEquals = as(filter.condition().collectFirstChildren(NotEquals.class::isInstance).getFirst(), NotEquals.class);
        assertEquals("memory_usage", literalValue(notEquals.right()));
        assertFalse(filter.condition().anyMatch(e -> e instanceof IsNull || e instanceof IsNotNull || e instanceof ConvertFunction));
    }

    /**
     * A metric referenced anywhere but in a time series aggregation function, here in a filter on its value and in an {@code EVAL},
     * does not select exemplars; and a filter on a computed value is not a filter on the series, so it is dropped.
     */
    public void testOnlyAggregatedMetricsAndSeriesFiltersAreUsed() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | EVAL memory = memory_usage * 2
            | WHERE memory_usage > 0 AND memory > 0 AND attributes.cpu == "cpu0"
            | STATS AVG(cpu_time) BY attributes.state
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.cpu", "metric_name"), referenceNames(filter));
        assertEquals(Set.of("cpu_time"), selectedMetricNames(filter));
        assertFalse(filter.condition().anyMatch(GreaterThan.class::isInstance));
    }

    /**
     * Only the filters between the time series aggregation and the source relation describe the series read. Everything after the
     * aggregation, a filter on a grouping dimension as much as the projection and limit of the aggregated result, is ignored: the
     * exemplars are the result of the query.
     */
    public void testEverythingAfterAggregationIsIgnored() {
        LogicalPlan plan = analyze("""
            TS metrics-generic.otel-default
            | STATS avg = AVG(cpu_time) BY attributes.state
            | WHERE attributes.state == "idle"
            | KEEP avg
            | LIMIT 5
            """);

        assertFalse(plan.anyMatch(Project.class::isInstance));
        Filter filter = findFilter(plan);
        assertEquals(Set.of("metric_name"), referenceNames(filter));
        assertEquals(
            Set.of("@timestamp", "attributes.cpu", "attributes.state", "cpu", "metric_name", "span_id", "state", "trace_id", "value"),
            plan.output().stream().map(Attribute::name).collect(Collectors.toSet())
        );
    }

    public void testPromqlMetricsQuery() {
        LogicalPlan plan = analyze("""
            PROMQL index=metrics-generic.otel-default step=5m start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z"
              (avg(avg_over_time(cpu_time{attributes.state="idle"}[5m])))
            """);

        assertFalse(plan.anyMatch(PromqlCommand.class::isInstance));
        assertFalse(plan.anyMatch(TimeSeriesAggregate.class::isInstance));

        Filter filter = findFilter(plan);
        // label matchers become dimension filters and the PromQL evaluation range becomes a filter on @timestamp
        assertEquals(Set.of("@timestamp", "attributes.state", "metric_name"), referenceNames(filter));
        assertEquals(Set.of("cpu_time"), selectedMetricNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("exemplars-generic.otel-default", relation.indexPattern());
    }

    /**
     * The exemplar data streams are derived from the metrics data streams the metrics query matched, recovered from the backing indices
     * of the resolutions of all its time series relations: aliases and wildcards are followed, cluster prefixes kept, generations
     * collapsed, and data streams not named {@code metrics-*} skipped.
     */
    public void testExemplarIndexPatternIsDerivedFromMatchedBackingIndices() {
        EsIndex metricsIndex = new EsIndex(
            "otel-metrics,remote:metrics-*",
            metricsMapping(),
            Map.of(),
            Map.of("", List.of("otel-metrics"), "remote", List.of("metrics-*")),
            Map.of(
                "",
                List.of(
                    ".ds-metrics-generic.otel-default-2026.09.01-000001",
                    ".ds-metrics-generic.otel-default-2026.09.08-000002",
                    ".ds-metrics-k8s-2026.09.08-000001",
                    "custom-tsdb"
                ),
                "remote",
                List.of(".ds-metrics-cpu-2026.09.08-000001")
            )
        );
        EsIndex otherMetricsIndex = new EsIndex(
            "metrics-other",
            metricsMapping(),
            Map.of(),
            Map.of("", List.of("metrics-other")),
            Map.of("", List.of(".ds-metrics-other-2026.09.08-000001"))
        );
        assertEquals(
            "exemplars-generic.otel-default,exemplars-k8s,remote:exemplars-cpu",
            ExemplarsRewriter.exemplarIndexPattern(List.of(IndexResolution.valid(metricsIndex)))
        );
        assertEquals(
            "exemplars-generic.otel-default,exemplars-k8s,exemplars-other,remote:exemplars-cpu",
            ExemplarsRewriter.exemplarIndexPattern(List.of(IndexResolution.valid(metricsIndex), IndexResolution.valid(otherMetricsIndex)))
        );

        // nothing to derive from: no metrics-* data stream matched, or the metrics pattern is not resolved at all
        EsIndex customIndex = new EsIndex(
            "otel-metrics,remote:metrics-*",
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of("", List.of("custom-tsdb"))
        );
        assertNull(ExemplarsRewriter.exemplarIndexPattern(List.of(IndexResolution.valid(customIndex))));
        assertNull(ExemplarsRewriter.exemplarIndexPattern(List.of()));
        assertNull(ExemplarsRewriter.exemplarIndexPattern(List.of(IndexResolution.notFound("otel-metrics,remote:metrics-*"))));
    }

    /**
     * Only {@code metrics-generic.otel-default} has exemplars: the dimensions of {@code metrics-k8s} exist in none of the exemplar
     * indices and resolve as {@code null} there, so the filter on the k8s dimension does not fail the analysis. Its metric, which lives
     * outside a {@code metrics} object, is selected by its full field name, which no exemplar carries as metric name.
     */
    public void testFieldsOfDataStreamWithoutExemplarsAreNull() {
        String metricsPattern = "metrics-generic.otel-default,metrics-k8s";
        Map<String, EsField> metricsMapping = new LinkedHashMap<>(metricsMapping());
        metricsMapping.putAll(EsqlTestUtils.loadMapping("k8s-mappings.json"));
        List<String> metricsIndices = List.of("metrics-generic.otel-default", "metrics-k8s");
        EsIndex metricsIndex = new EsIndex(
            metricsPattern,
            metricsMapping,
            metricsIndices.stream().collect(Collectors.toMap(index -> index, index -> new IndexProperties(IndexMode.TIME_SERIES, 0))),
            Map.of("", metricsIndices),
            Map.of("", metricsIndices)
        );
        // the exemplar pattern derived from both matched metrics indices; field caps found only the generic exemplars
        LogicalPlan plan = analyze(
            """
                TS metrics-generic.otel-default,metrics-k8s
                | WHERE cluster == "prod" AND attributes.state == "idle"
                | STATS AVG(cpu_time), MAX(RATE(network.total_bytes_in))
                """,
            EsqlTestUtils.analyzer().addIndex(metricsPattern, IndexResolution.valid(metricsIndex)),
            exemplarsResolution(EXEMPLARS_INDEX)
        );

        Filter filter = findFilter(plan);
        assertEquals(Set.of("cluster", "attributes.state", "metric_name"), referenceNames(filter));
        assertEquals(Set.of("cpu_time", "network.total_bytes_in"), selectedMetricNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        Map<String, DataType> outputTypes = relation.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));
        assertEquals(DataType.NULL, outputTypes.get("cluster"));
        assertEquals(DataType.KEYWORD, outputTypes.get("attributes.state"));
        assertEquals(DataType.KEYWORD, outputTypes.get("metric_name"));
        assertEquals(DataType.DOUBLE, outputTypes.get("value"));
    }

    public void testNoMetricsIndexPatternYieldsNoExemplars() {
        LogicalPlan plan = analyze(
            "TS custom-tsdb | WHERE attributes.state == \"idle\" | STATS AVG(cpu_time)",
            EsqlTestUtils.analyzer().addIndex("custom-tsdb", metricsResolution("custom-tsdb")),
            null
        );

        // no exemplar data streams to derive: the source is an empty local relation with the referenced fields nullified on top
        assertTrue(as(assertEmptyExemplars(plan), LocalRelation.class).hasEmptySupplier());
    }

    public void testMissingExemplarIndicesYieldNoExemplars() {
        LogicalPlan plan = analyze(
            "TS metrics-generic.otel-default | WHERE attributes.state == \"idle\" | STATS AVG(cpu_time)",
            testAnalyzer(),
            IndexResolution.empty(EXEMPLARS_INDEX)
        );

        // the derived data stream does not exist: the source is a relation without indices, whose referenced fields are all unmapped
        EsRelation relation = as(assertEmptyExemplars(plan), EsRelation.class);
        assertEquals(EXEMPLARS_INDEX, relation.indexPattern());
        assertTrue(relation.concreteIndices().isEmpty());
    }

    /**
     * Asserts that the query has no exemplar columns but the {@code null} typed fields its filter refers to, and returns the source of
     * the plan.
     */
    private static LogicalPlan assertEmptyExemplars(LogicalPlan plan) {
        assertEquals(
            Set.of("@timestamp", "attributes.state", "metric_name"),
            plan.output().stream().map(Attribute::name).collect(Collectors.toSet())
        );
        assertEquals(List.of(DataType.NULL, DataType.NULL, DataType.NULL), plan.output().stream().map(Attribute::dataType).toList());
        LogicalPlan source = plan;
        while (source.children().isEmpty() == false) {
            source = source.children().getFirst();
        }
        return source;
    }

    public void testRequiresMetricReference() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("TS metrics-generic.otel-default | STATS COUNT(attributes.state)")
        );
        assertThat(e.getMessage(), containsString("[exemplars] requires a TS or PROMQL query that aggregates at least one metric field"));
    }

    public void testRequiresTimeSeriesQuery() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("FROM metrics-generic.otel-default | STATS AVG(cpu_time)")
        );
        assertThat(e.getMessage(), containsString("[exemplars] requires a TS or PROMQL query that aggregates at least one metric field"));
        // emitted while analyzing the metrics query, which is not a time series aggregation either
        assertWarnings(DEFAULT_LIMIT_WARNING);
    }

    /**
     * Unmapped fields are {@code null} in the metrics query too, so a misspelled metric is not an unknown column but leaves the metrics
     * query without a metric to read.
     */
    public void testUnknownMetricIsNull() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("TS metrics-generic.otel-default | STATS AVG(unknown)")
        );
        assertThat(e.getMessage(), containsString("[exemplars] requires a TS or PROMQL query that aggregates at least one metric field"));
    }

    public void testMetricsQueryIsVerified() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("TS metrics-generic.otel-default | STATS AVG(attributes.state)")
        );
        assertThat(e.getMessage(), containsString("argument of [AVG(attributes.state)] must be"));
    }

    private static Set<String> referenceNames(LogicalPlan plan) {
        return plan.references().stream().map(attribute -> attribute.name()).collect(Collectors.toSet());
    }

    /** One disjunct of the exemplar filter: the metrics whose exemplars it selects and the names of all the fields it refers to. */
    private record Selection(Set<String> metricNames, Set<String> referenceNames) {}

    private static List<Selection> selections(Filter filter) {
        List<Selection> selections = new ArrayList<>();
        for (Expression disjunct : Predicates.splitOr(filter.condition())) {
            selections.add(
                new Selection(
                    selectedMetricNames(disjunct),
                    disjunct.references().stream().map(Attribute::name).collect(Collectors.toSet())
                )
            );
        }
        return selections;
    }

    /** The metric names the filter compares {@code metric_name} with for equality, i.e. the metrics whose exemplars it selects. */
    private static Set<String> selectedMetricNames(Filter filter) {
        return selectedMetricNames(filter.condition());
    }

    private static Set<String> selectedMetricNames(Expression condition) {
        Set<String> names = new HashSet<>();
        condition.forEachDown(e -> {
            if (e instanceof Equals equals && isMetricName(equals.left())) {
                names.add(literalValue(equals.right()));
            } else if (e instanceof In in && isMetricName(in.value())) {
                in.list().forEach(name -> names.add(literalValue(name)));
            }
        });
        return names;
    }

    private static boolean isMetricName(Expression expression) {
        return expression instanceof Attribute attribute && attribute.name().equals(ExemplarsRewriter.METRIC_NAME_FIELD);
    }

    private static String literalValue(Expression expression) {
        return BytesRefs.toString(as(expression, Literal.class).value());
    }

    /** The filter selecting the exemplars, which sits directly on the exemplar relation below the limit and the sort. */
    private static Filter findFilter(LogicalPlan plan) {
        while (plan instanceof Limit || plan instanceof OrderBy) {
            plan = ((UnaryPlan) plan).child();
        }
        return as(plan, Filter.class);
    }

    private LogicalPlan analyze(String metricsQuery) {
        return analyze(metricsQuery, testAnalyzer(), exemplarsResolution(EXEMPLARS_INDEX));
    }

    private LogicalPlan analyze(String metricsQuery, TestAnalyzer testAnalyzer, @Nullable IndexResolution exemplarsResolution) {
        return analyze(metricsQuery, testAnalyzer, exemplarsResolution, null);
    }

    /**
     * Mirrors {@code EsqlSession} for {@code SET exemplars=true}: analyze the metrics query with {@code unmapped_fields=nullify}, build
     * the exemplar query from it and analyze that against the exemplar relation.
     */
    private LogicalPlan analyze(
        String metricsQuery,
        TestAnalyzer testAnalyzer,
        @Nullable IndexResolution exemplarsResolution,
        @Nullable Integer limit
    ) {
        LogicalPlan parsed = EsqlTestUtils.TEST_PARSER.parseQuery(metricsQuery);
        Analyzer analyzer = testAnalyzer.unmappedResolution(UnmappedResolution.NULLIFY).buildAnalyzer();
        LogicalPlan metricsPlan = analyzer.analyze(parsed);
        ExemplarsSettings settings = limit == null ? ExemplarsSettings.ENABLED : new ExemplarsSettings(true, limit);
        LogicalPlan plan = analyzer.analyze(ExemplarsRewriter.exemplarsQuery(metricsPlan, exemplarsResolution, settings));
        if (limit == null) {
            // unlike the metrics aggregation, the exemplar query gets the default limit of a regular query
            assertWarnings(DEFAULT_LIMIT_WARNING);
        }
        return plan;
    }

    private static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    private static final String METRICS_PATTERN = "metrics-generic.otel-default";
    private static final String EXEMPLARS_INDEX = "exemplars-generic.otel-default";

    private static TestAnalyzer testAnalyzer() {
        return EsqlTestUtils.analyzer().addIndex(METRICS_PATTERN, metricsResolution(METRICS_PATTERN));
    }

    private static IndexResolution metricsResolution(String indexName) {
        return resolution(indexName, metricsMapping());
    }

    /** The pre-analysis result the session produces for the exemplar data stream of the metrics query, resolved. */
    private static IndexResolution exemplarsResolution(String indexName) {
        return resolution(indexName, exemplarsMapping());
    }

    private static IndexResolution resolution(String indexName, Map<String, EsField> mapping) {
        return IndexResolution.valid(
            new EsIndex(
                indexName,
                mapping,
                Map.of(indexName, new IndexProperties(IndexMode.TIME_SERIES, 0)),
                Map.of("", List.of(indexName)),
                Map.of("", List.of(indexName))
            )
        );
    }

    /**
     * An OTel metrics index as {@code IndexResolver} sees it: the dimensions {@code cpu} and {@code state} in the passthrough object
     * {@code attributes}, the gauges {@code cpu_time} and {@code memory_usage} and the exponential histogram {@code request_duration} in
     * the passthrough object {@code metrics}, each of them also reported under its root-level alias. The metrics queries of the tests
     * refer to the metrics by that alias, which is the metric name their exemplars carry.
     */
    private static Map<String, EsField> metricsMapping() {
        Map<String, EsField> mapping = new LinkedHashMap<>();
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        addPassthroughObject(mapping, "attributes", Map.of("cpu", dimension("cpu"), "state", dimension("state")));
        addPassthroughObject(
            mapping,
            "metrics",
            Map.of(
                "cpu_time",
                metric("cpu_time", DataType.DOUBLE),
                "memory_usage",
                metric("memory_usage", DataType.DOUBLE),
                "request_duration",
                metric("request_duration", DataType.EXPONENTIAL_HISTOGRAM)
            )
        );
        return mapping;
    }

    /**
     * The exemplar index of {@link #metricsMapping()}: the same dimensions, and the metric name and value of each exemplar instead of
     * the metric fields, next to the trace and span it points to.
     */
    private static Map<String, EsField> exemplarsMapping() {
        Map<String, EsField> mapping = new LinkedHashMap<>();
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        addPassthroughObject(mapping, "attributes", Map.of("cpu", dimension("cpu"), "state", dimension("state")));
        mapping.put(ExemplarsRewriter.METRIC_NAME_FIELD, dimension(ExemplarsRewriter.METRIC_NAME_FIELD));
        mapping.put("value", new EsField("value", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        for (String field : List.of("trace_id", "span_id")) {
            mapping.put(field, new KeywordEsField(field, Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.NONE));
        }
        return mapping;
    }

    private static void addPassthroughObject(Map<String, EsField> mapping, String name, Map<String, EsField> fields) {
        mapping.put(name, new EsField(name, DataType.OBJECT, new LinkedHashMap<>(fields), false, EsField.TimeSeriesFieldType.NONE));
        mapping.putAll(fields);
    }

    private static EsField dimension(String name) {
        return new KeywordEsField(name, Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.DIMENSION);
    }

    private static EsField metric(String name, DataType type) {
        return new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.METRIC);
    }
}
