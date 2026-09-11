/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesExemplars;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that {@code TS_EXEMPLARS (...)} is rewritten into a query over the exemplar data stream that keeps the dimension filters
 * of the metrics query, selects only documents carrying one of the fetched metrics and tags each row with the metric name. The
 * rewritten plan is taken through analysis like {@code EsqlSession} does, so the tests observe the analyzed exemplar query.
 */
public class TimeSeriesExemplarsRewriterTests extends ESTestCase {

    public void testRewritesMetricsQueryToExemplarQuery() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | WHERE attributes.state == "idle"
              | STATS AVG(metrics.cpu_time)
            )
            | LIMIT 10
            """);

        assertFalse(plan.anyMatch(TimeSeriesExemplars.class::isInstance));
        assertFalse(plan.anyMatch(TimeSeriesAggregate.class::isInstance));

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.state", "metrics.cpu_time"), referenceNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("exemplars-generic.otel-default", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    public void testMultipleMetricsSelectExemplarsOfEither() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | STATS AVG(metrics.cpu_time), MAX(metrics.memory_usage)
            )
            | LIMIT 10
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("metrics.cpu_time", "metrics.memory_usage"), referenceNames(filter));
        assertTrue(filter.condition().anyMatch(Or.class::isInstance));
    }

    /**
     * Only filters on dimensions carry over to the exemplars; a filter on the metric value would select exemplars by their own
     * value rather than by the series they belong to, so it is dropped.
     */
    public void testMetricValueFiltersAreNotAppliedToExemplars() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | WHERE attributes.state == "idle" AND metrics.cpu_time > 5
              | STATS AVG(metrics.cpu_time)
            )
            | LIMIT 10
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.state", "metrics.cpu_time"), referenceNames(filter));
        assertTrue(filter.condition().anyMatch(Equals.class::isInstance));
        assertFalse(filter.condition().anyMatch(GreaterThan.class::isInstance));
    }

    /**
     * Null checks on metrics, also through (chained) conversion functions, describe which series are read and carry over, while the
     * metrics they mention are not what the exemplars are fetched for: only the aggregated metric is.
     */
    public void testMetricNullChecksAreAppliedToExemplars() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | WHERE metrics.memory_usage IS NULL AND TO_STRING(TO_LONG(metrics.cpu_time)) IS NOT NULL
              | STATS AVG(metrics.cpu_time)
            )
            | LIMIT 10
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("metrics.memory_usage", "metrics.cpu_time"), referenceNames(filter));
        assertTrue(filter.condition().anyMatch(IsNull.class::isInstance));
        assertTrue(filter.condition().anyMatch(ToLong.class::isInstance));
    }

    /**
     * A metric referenced anywhere but as the aggregated field, here in a filter on its value and in an {@code EVAL}, does not select
     * exemplars; and a filter on a computed value or after the aggregation is not a filter on the series, so it is dropped.
     */
    public void testOnlyAggregatedMetricsAndSeriesFiltersAreUsed() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | EVAL memory = metrics.memory_usage * 2
              | WHERE metrics.memory_usage > 0 AND memory > 0 AND attributes.cpu == "cpu0"
              | STATS AVG(metrics.cpu_time) BY attributes.state
              | WHERE `AVG(metrics.cpu_time)` > 1
            )
            | LIMIT 10
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("attributes.cpu", "metrics.cpu_time"), referenceNames(filter));
        assertFalse(filter.condition().anyMatch(GreaterThan.class::isInstance));
    }

    /**
     * Only the filters between the time series aggregation and the source relation describe the series read. A filter written after
     * STATS, even on a grouping dimension, is not pushed through the aggregation by the optimizer and does not carry over.
     */
    public void testFiltersAfterAggregationAreNotApplied() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | STATS AVG(metrics.cpu_time) BY attributes.state
              | WHERE attributes.state == "idle"
            )
            | LIMIT 10
            """);

        Filter filter = findFilter(plan);
        assertEquals(Set.of("metrics.cpu_time"), referenceNames(filter));
        assertFalse(filter.condition().anyMatch(Equals.class::isInstance));
    }

    public void testPromqlMetricsQuery() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (
              PROMQL index=metrics-generic.otel-default step=5m start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z"
                (avg(avg_over_time(metrics.cpu_time{attributes.state="idle"}[5m])))
            )
            | LIMIT 10
            """);

        assertFalse(plan.anyMatch(PromqlCommand.class::isInstance));
        assertFalse(plan.anyMatch(TimeSeriesAggregate.class::isInstance));

        Filter filter = findFilter(plan);
        // label matchers become dimension filters and the PromQL evaluation range becomes a filter on @timestamp
        assertEquals(Set.of("@timestamp", "attributes.state", "metrics.cpu_time"), referenceNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("exemplars-generic.otel-default", relation.indexPattern());
    }

    public void testPlansWithoutExemplarsAreLeftUntouched() {
        LogicalPlan parsed = EsqlTestUtils.TEST_PARSER.parseQuery("TS metrics-generic.otel-default | STATS AVG(metrics.cpu_time)");
        assertSame(parsed, rewrite(parsed));
        assertTrue(TimeSeriesExemplarsRewriter.preAnalysisMetrics(parsed).isEmpty());
    }

    public void testUsageTelemetryIsRecordedFromTheParsedPlan() {
        LogicalPlan parsed = EsqlTestUtils.TEST_PARSER.parseQuery(
            "TS_EXEMPLARS (TS metrics-generic.otel-default | STATS AVG(metrics.cpu_time))"
        );
        BitSet metrics = TimeSeriesExemplarsRewriter.preAnalysisMetrics(parsed);
        assertTrue(metrics.get(FeatureMetric.TS_EXEMPLARS.ordinal()));
        assertEquals(1, metrics.cardinality());
    }

    /**
     * A plan with {@code TS_EXEMPLARS} is analyzed with {@code unmapped_fields=nullify} unless the setting is given explicitly; other
     * plans keep the default.
     */
    public void testDefaultsToNullifyingUnmappedFields() {
        LogicalPlan exemplars = EsqlTestUtils.TEST_PARSER.parseQuery(
            "TS_EXEMPLARS (TS metrics-generic.otel-default | STATS AVG(metrics.cpu_time)) | LIMIT 10"
        );
        LogicalPlan metrics = EsqlTestUtils.TEST_PARSER.parseQuery("TS metrics-generic.otel-default | STATS AVG(metrics.cpu_time)");

        assertEquals(UnmappedResolution.NULLIFY, TimeSeriesExemplarsRewriter.unmappedResolution(exemplars, UnmappedResolution.DEFAULT));
        assertEquals(UnmappedResolution.LOAD, TimeSeriesExemplarsRewriter.unmappedResolution(exemplars, UnmappedResolution.LOAD));
        assertEquals(UnmappedResolution.DEFAULT, TimeSeriesExemplarsRewriter.unmappedResolution(metrics, UnmappedResolution.DEFAULT));
    }

    /**
     * The exemplar data streams are derived from the metrics data streams the metrics query matched, recovered from the backing indices
     * of its resolution: aliases and wildcards are followed, cluster prefixes kept, generations collapsed, and data streams not named
     * {@code metrics-*} skipped.
     */
    public void testExemplarIndexPatternIsDerivedFromMatchedBackingIndices() {
        Map<String, List<String>> concreteIndices = Map.of(
            "",
            List.of(
                ".ds-metrics-generic.otel-default-2026.09.01-000001",
                ".ds-metrics-generic.otel-default-2026.09.08-000002",
                ".ds-metrics-k8s-2026.09.08-000001",
                "custom-tsdb"
            ),
            "remote",
            List.of(".ds-metrics-cpu-2026.09.08-000001")
        );
        EsIndex metricsIndex = new EsIndex(
            "otel-metrics,remote:metrics-*",
            EsqlTestUtils.loadMapping("otel-exemplar-source-metrics-mappings.json"),
            Map.of(),
            Map.of("", List.of("otel-metrics"), "remote", List.of("metrics-*")),
            concreteIndices
        );
        assertEquals(
            "exemplars-generic.otel-default,exemplars-k8s,remote:exemplars-cpu",
            TimeSeriesExemplarsRewriter.exemplarIndexPattern(IndexResolution.valid(metricsIndex))
        );

        // nothing to derive from: no metrics-* data stream matched, or the metrics pattern is not resolved at all
        EsIndex customIndex = new EsIndex(
            "otel-metrics,remote:metrics-*",
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of("", List.of("custom-tsdb"))
        );
        assertNull(TimeSeriesExemplarsRewriter.exemplarIndexPattern(IndexResolution.valid(customIndex)));
        assertNull(TimeSeriesExemplarsRewriter.exemplarIndexPattern(null));
        assertNull(TimeSeriesExemplarsRewriter.exemplarIndexPattern(IndexResolution.notFound("otel-metrics,remote:metrics-*")));
    }

    /**
     * Only {@code metrics-generic.otel-default} has exemplars: the fields of {@code metrics-k8s} exist in none of the exemplar indices and
     * resolve as {@code null} there, so the filter on the k8s dimension and the {@code IS NOT NULL} condition on the k8s metric do not
     * fail the analysis.
     */
    public void testFieldsOfDataStreamWithoutExemplarsAreNull() {
        String metricsPattern = "metrics-generic.otel-default,metrics-k8s";
        Map<String, EsField> metricsMapping = new HashMap<>(EsqlTestUtils.loadMapping("otel-exemplar-source-metrics-mappings.json"));
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
                TS_EXEMPLARS (
                  TS metrics-generic.otel-default,metrics-k8s
                  | WHERE cluster == "prod" AND attributes.state == "idle"
                  | STATS AVG(metrics.cpu_time), MAX(RATE(network.total_bytes_in))
                )
                | LIMIT 10
                """,
            EsqlTestUtils.analyzer().addIndex(metricsPattern, IndexResolution.valid(metricsIndex)),
            exemplarsResolution(
                metricsPattern,
                TestAnalyzer.loadMapping("otel-exemplars-mappings.json", "exemplars-generic.otel-default", IndexMode.TIME_SERIES)
            )
        );

        Filter filter = findFilter(plan);
        assertEquals(Set.of("cluster", "attributes.state", "metrics.cpu_time", "network.total_bytes_in"), referenceNames(filter));
        EsRelation relation = as(filter.child(), EsRelation.class);
        Map<String, DataType> outputTypes = relation.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));
        assertEquals(DataType.NULL, outputTypes.get("cluster"));
        assertEquals(DataType.NULL, outputTypes.get("network.total_bytes_in"));
        assertEquals(DataType.KEYWORD, outputTypes.get("attributes.state"));
        assertEquals(DataType.DOUBLE, outputTypes.get("metrics.cpu_time"));
    }

    public void testNoMetricsIndexPatternYieldsNoExemplars() {
        LogicalPlan plan = analyze(
            """
                TS_EXEMPLARS (TS custom-tsdb | WHERE attributes.state == "idle" | STATS AVG(metrics.cpu_time))
                | KEEP @timestamp, attributes.cpu, attributes.state
                | LIMIT 10
                """,
            testAnalyzer().addIndex("custom-tsdb", "otel-exemplar-source-metrics-mappings.json", IndexMode.TIME_SERIES),
            new ExemplarsResolution()
        );

        // no exemplar data streams to derive: the source is an empty local relation with the referenced fields nullified on top
        assertTrue(as(assertEmptyExemplars(plan), LocalRelation.class).hasEmptySupplier());
    }

    public void testMissingExemplarIndicesYieldNoExemplars() {
        LogicalPlan plan = analyze("""
            TS_EXEMPLARS (TS metrics-generic.otel-default | STATS AVG(metrics.cpu_time))
            | KEEP @timestamp, attributes.cpu, attributes.state
            | LIMIT 10
            """, testAnalyzer(), exemplarsResolution(METRICS_PATTERN, IndexResolution.empty("exemplars-generic.otel-default")));

        // the derived data stream does not exist: the source is a relation without indices, whose referenced fields are all unmapped
        EsRelation relation = as(assertEmptyExemplars(plan), EsRelation.class);
        assertEquals("exemplars-generic.otel-default", relation.indexPattern());
        assertTrue(relation.concreteIndices().isEmpty());
    }

    /** Asserts that the columns of the exemplars are {@code null} typed and returns the source of the plan. */
    private static LogicalPlan assertEmptyExemplars(LogicalPlan plan) {
        assertEquals(List.of("@timestamp", "attributes.cpu", "attributes.state"), plan.output().stream().map(Attribute::name).toList());
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
            () -> analyze("TS_EXEMPLARS (TS metrics-generic.otel-default | STATS COUNT(attributes.state))")
        );
        assertThat(e.getMessage(), containsString("TS_EXEMPLARS requires the metrics query to aggregate at least one metric field"));
    }

    /**
     * Unmapped fields are {@code null} in the metrics query too, so a misspelled metric is not an unknown column but leaves the metrics
     * query without a metric to read.
     */
    public void testUnknownMetricIsNull() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("TS_EXEMPLARS (TS metrics-generic.otel-default | STATS AVG(metrics.unknown))")
        );
        assertThat(e.getMessage(), containsString("TS_EXEMPLARS requires the metrics query to aggregate at least one metric field"));
    }

    public void testMetricsQueryIsVerified() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyze("TS_EXEMPLARS (TS metrics-generic.otel-default | STATS AVG(attributes.state))")
        );
        assertThat(e.getMessage(), containsString("argument of [AVG(attributes.state)] must be"));
    }

    private static Set<String> referenceNames(LogicalPlan plan) {
        return plan.references().stream().map(attribute -> attribute.name()).collect(Collectors.toSet());
    }

    /** The filter selecting the exemplars, which sits directly on the exemplar relation once the implicit limit is skipped. */
    private static Filter findFilter(LogicalPlan plan) {
        while (plan instanceof Limit limit) {
            plan = limit.child();
        }
        return as(plan, Filter.class);
    }

    /**
     * Mirrors {@code EsqlSession}: pick the unmapped field resolution for the parsed plan, rewrite it, then analyze it with the telemetry
     * of the removed commands.
     */
    private static LogicalPlan analyze(String query) {
        return analyze(query, testAnalyzer(), exemplarsResolution());
    }

    private static LogicalPlan analyze(String query, TestAnalyzer testAnalyzer, ExemplarsResolution exemplarsResolution) {
        LogicalPlan parsed = EsqlTestUtils.TEST_PARSER.parseQuery(query);
        Analyzer analyzer = testAnalyzer.unmappedResolution(
            TimeSeriesExemplarsRewriter.unmappedResolution(parsed, UnmappedResolution.DEFAULT)
        ).buildAnalyzer();
        return analyzer.analyze(rewrite(parsed, analyzer, exemplarsResolution), TimeSeriesExemplarsRewriter.preAnalysisMetrics(parsed));
    }

    private static LogicalPlan rewrite(LogicalPlan parsed) {
        return rewrite(parsed, testAnalyzer().buildAnalyzer(), exemplarsResolution());
    }

    private static LogicalPlan rewrite(LogicalPlan parsed, Analyzer analyzer, ExemplarsResolution exemplarsResolution) {
        TimeSeriesExemplarsRewriter rewriter = new TimeSeriesExemplarsRewriter(
            analyzer,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), TransportVersion.current())),
            exemplarsResolution
        );
        return rewriter.rewrite(parsed);
    }

    private static final String METRICS_PATTERN = "metrics-generic.otel-default";

    private static TestAnalyzer testAnalyzer() {
        return EsqlTestUtils.analyzer().addIndex(METRICS_PATTERN, "otel-exemplar-source-metrics-mappings.json", IndexMode.TIME_SERIES);
    }

    /** The pre-analysis result the session produces for the default metrics pattern: its exemplar data stream, resolved. */
    private static ExemplarsResolution exemplarsResolution() {
        return exemplarsResolution(
            METRICS_PATTERN,
            TestAnalyzer.loadMapping("otel-exemplars-mappings.json", "exemplars-generic.otel-default", IndexMode.TIME_SERIES)
        );
    }

    private static ExemplarsResolution exemplarsResolution(String metricsPattern, IndexResolution resolution) {
        ExemplarsResolution exemplarsResolution = new ExemplarsResolution();
        exemplarsResolution.addResolution(new IndexPattern(Source.EMPTY, metricsPattern), resolution);
        return exemplarsResolution;
    }
}
