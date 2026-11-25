/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug tests")
public abstract class AbstractPromqlPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    protected AbstractPromqlPlanOptimizerTests(VersionMode versionMode) {
        super(versionMode);
    }

    protected TestAnalyzer tsAnalyzer() {
        return analyzerWithEnrichPolicies().addK8s()
            .addK8sDateNanos()
            .addOtelMetrics()
            .addEmptyIndex()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .minimumTransportVersion(minimumVersionAtLeast(TimeSeriesCollapse.TS_COLLAPSE));
    }

    protected LogicalPlan planPromql(String query) {
        return planPromql(query, false, true);
    }

    protected LogicalPlan planPromql(String query, boolean optimize) {
        return planPromql(query, false, optimize);
    }

    /**
     * PromQL translation writes non-finite-preserving expressions gated on several transport versions. The shared
     * optimizer draws a random version, which can predate those gates and silently strip the semantics, so optimized
     * PromQL plans use {@link org.elasticsearch.TransportVersion#current()}. Downgrade behavior itself is covered by
     * {@code SubstituteTransportVersionAwareExpressionsTests}.
     */
    protected LogicalPlan planPromql(String query, boolean allowEmptyReferences, boolean optimize) {
        var now = Instant.now();
        query = query.replace("$now-1h", "\"" + now.minus(1, ChronoUnit.HOURS) + "\"");
        query = query.replace("$now", "\"" + now + "\"");
        var analyzed = tsAnalyzer().query(query);
        AttributeSet.Builder references = AttributeSet.builder();
        analyzed.forEachDown(lp -> references.addAll(lp.references()));
        if (allowEmptyReferences) {
            assertThat(references.build(), empty());
        } else {
            assertThat(references.build(), not(empty()));
        }
        logger.trace("analyzed plan:\n{}", analyzed);
        if (optimize == false) {
            return analyzed;
        }
        var optimized = logicalOptimizerWithLatestVersion.optimize(analyzed);
        logger.trace("optimized plan:\n{}", optimized);
        return optimized;
    }

    protected void assertConstantResult(String query, Matcher<Double> matcher) {
        var plan = planPromql("PROMQL index=k8s step=1m " + query, true);
        Eval eval = plan.collect(Eval.class).getFirst();
        Literal literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(as(literal.value(), Double.class), matcher);

        Aggregate aggregate = eval.collect(Aggregate.class).getFirst();
        ReferenceAttribute step = as(aggregate.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(step.name(), equalTo("step"));

        TimeSeriesAggregate tsAgg = aggregate.collect(TimeSeriesAggregate.class).getFirst();
        ReferenceAttribute stepInTsAgg = as(Alias.unwrap(tsAgg.aggregates().getFirst()), ReferenceAttribute.class);
        assertThat(stepInTsAgg.name(), equalTo("step"));

        Eval stepEval = tsAgg.collect(Eval.class).getFirst();
        Alias bucketAlias = as(stepEval.fields().getFirst(), Alias.class);
        assertThat(bucketAlias.id(), equalTo(stepInTsAgg.id()));
        assertThat(bucketAlias.id(), equalTo(step.id()));
    }

    /**
     * Plans against the layout of {@code metrics-prometheus@template}: {@code labels} and {@code metrics} are passthrough
     * objects, so each sub-field is also exposed under its bare name, and {@code labels.__name__} is a dimension.
     */
    protected LogicalPlan planPrometheusLayout(String promql) {
        EsField name = dimension("__name__");
        EsField instance = dimension("instance");
        EsField job = dimension("job");
        EsField available = metric("node_memory_MemAvailable_bytes", DataType.DOUBLE);
        EsField total = metric("node_memory_MemTotal_bytes", DataType.DOUBLE);
        EsField cpu = metric("node_cpu_seconds_total", DataType.COUNTER_DOUBLE);
        Map<String, EsField> labels = Map.of(name.getName(), name, instance.getName(), instance, job.getName(), job);
        Map<String, EsField> metrics = Map.of(available.getName(), available, total.getName(), total, cpu.getName(), cpu);
        Map<String, EsField> mapping = new HashMap<>(labels);
        mapping.putAll(metrics);
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("labels", new EsField("labels", DataType.OBJECT, labels, false, EsField.TimeSeriesFieldType.NONE));
        mapping.put("metrics", new EsField("metrics", DataType.OBJECT, metrics, false, EsField.TimeSeriesFieldType.NONE));
        var index = new EsIndex(
            "metrics-node.prometheus-default",
            mapping,
            Map.of("metrics-node.prometheus-default", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        var analyzed = tsAnalyzer().addIndex(index).query("PROMQL index=metrics-node.prometheus-default step=1m result=(" + promql + ")");
        return logicalOptimizerWithLatestVersion.optimize(analyzed);
    }

    protected static EsField dimension(String name) {
        return new KeywordEsField(name, Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.DIMENSION);
    }

    protected static EsField metric(String name, DataType type) {
        return new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.METRIC);
    }
}
