/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesExemplars;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Replaces every {@code TS_EXEMPLARS (<metrics query>)} in a parsed plan with a query over the exemplar data streams, before the
 * plan itself is analyzed.
 * <p>
 * The metrics query is analyzed and logically optimized like a regular query but is never executed. Working on its optimized plan
 * means the PromQL translation has happened and filters have been pushed down as far as possible, so the metrics aggregated by each
 * time series aggregation and the filters between it and the source relation describe exactly which series the metrics query reads
 * (see {@link #exemplarSelection}). The asynchronous pre-optimization (folding of inference functions) is skipped: the plan is not
 * executed, and a filter on an inference function does not carry over to the exemplars anyway. Exemplars are then selected with the
 * same filters, restricted to documents that carry one of those metric values.
 * <p>
 * The exemplar data streams are those of the metrics data streams the metrics query actually matched (see
 * {@link #exemplarIndexPattern}), which the pre-analysis resolves into the {@link ExemplarsResolution} once the metrics index pattern
 * is resolved. Fields are carried over to the
 * exemplar query by name and resolved against the exemplar relation by the analysis of the rewritten plan, which also verifies it like
 * any other query. Metrics and dimensions of the metrics query that none of the exemplar indices has (because only some of the metrics
 * data streams have exemplars, or none at all) are unmapped there. Queries with this command therefore default to
 * {@link UnmappedResolution#NULLIFY} (see {@link #unmappedResolution}), which makes the analysis resolve them as {@code null} columns,
 * like a field mapped in only some indices of a pattern is {@code null} for the others. The dimension filters then keep excluding what
 * they exclude in the metrics query, the metrics without exemplars contribute none, and the rest of the query can still refer to these
 * columns. Exemplar data streams that do not exist resolve to an empty relation for the same reason, so the command yields no rows for
 * them, as it does when the metrics query matches no {@code metrics-*} data stream at all.
 */
public final class TimeSeriesExemplarsRewriter {

    private static final String METRICS_DATA_STREAM_PREFIX = "metrics-";
    private static final String EXEMPLARS_DATA_STREAM_PREFIX = "exemplars-";

    private final Analyzer analyzer;
    private final LogicalPlanOptimizer optimizer;
    private final ExemplarsResolution exemplarsResolution;

    public TimeSeriesExemplarsRewriter(Analyzer analyzer, LogicalPlanOptimizer optimizer, ExemplarsResolution exemplarsResolution) {
        this.analyzer = analyzer;
        this.optimizer = optimizer;
        this.exemplarsResolution = exemplarsResolution;
    }

    /**
     * The exemplar data streams of a {@code TS_EXEMPLARS} command, as an index pattern: for every {@code metrics-<name>} data stream its
     * metrics query matched, the {@code exemplars-<name>} data stream on the same cluster. The matched data streams are recovered from the
     * backing indices in the resolution of the metrics index pattern, like {@code METRICS_INFO} does, so this follows aliases, remote
     * wildcards and the narrowing of a request filter rather than the text of the pattern. Returns {@code null} when the metrics query
     * matched no {@code metrics-*} data stream (or its pattern is not resolved), in which case there are no exemplars to fetch.
     */
    @Nullable
    public static String exemplarIndexPattern(@Nullable IndexResolution metricsResolution) {
        if (metricsResolution == null || metricsResolution.isValid() == false) {
            return null;
        }
        Set<String> exemplarDataStreams = new TreeSet<>();
        metricsResolution.get().concreteIndices().forEach((cluster, indices) -> {
            for (String index : indices) {
                String dataStream = RemoteClusterAware.splitIndexName(MetricsInfoOperator.resolveDataStreamName(index)).indexExpression();
                if (dataStream.startsWith(METRICS_DATA_STREAM_PREFIX)) {
                    String exemplarDataStream = EXEMPLARS_DATA_STREAM_PREFIX + dataStream.substring(METRICS_DATA_STREAM_PREFIX.length());
                    exemplarDataStreams.add(RemoteClusterAware.buildRemoteIndexName(cluster, exemplarDataStream));
                }
            }
        });
        return exemplarDataStreams.isEmpty() ? null : String.join(",", exemplarDataStreams);
    }

    /**
     * Usage telemetry for the commands this rewriter removes from the plan. They are gone by the time the analyzed plan is verified,
     * so they have to be recorded from the parsed plan.
     */
    public static BitSet preAnalysisMetrics(LogicalPlan parsed) {
        BitSet metrics = new BitSet(FeatureMetric.values().length);
        parsed.forEachDown(TimeSeriesExemplars.class, exemplars -> FeatureMetric.set(exemplars, metrics));
        return metrics;
    }

    /**
     * The unmapped field resolution to analyze a parsed plan with: {@code TS_EXEMPLARS} refers to fields of the metrics query that the
     * exemplar indices may not have, so a plan containing it treats them as {@code null} unless the {@code unmapped_fields} query setting
     * asks for something else explicitly.
     */
    public static UnmappedResolution unmappedResolution(LogicalPlan parsed, UnmappedResolution requested) {
        if (requested == UnmappedResolution.DEFAULT
            && parsed.collectFirstChildren(p -> p instanceof TimeSeriesExemplars).isEmpty() == false) {
            return UnmappedResolution.NULLIFY;
        }
        return requested;
    }

    /**
     * Rewrites all {@link TimeSeriesExemplars} commands in {@code parsed}. Returns the plan itself when there is none.
     */
    public LogicalPlan rewrite(LogicalPlan parsed) {
        return parsed.transformUp(TimeSeriesExemplars.class, exemplars -> {
            LogicalPlan optimizedMetricsQuery = planMetricsQuery(exemplars.metricsQuery());
            return exemplarsQuery(exemplars, optimizedMetricsQuery, exemplarsRelation(exemplars));
        });
    }

    private LogicalPlan planMetricsQuery(LogicalPlan metricsQuery) {
        LogicalPlan analyzed = analyzer.analyze(metricsQuery);
        analyzed.setAnalyzed();
        return optimizer.optimize(analyzed);
    }

    /**
     * The relation to fetch the exemplars from: the exemplar data streams the pre-analysis resolved for the metrics query of this
     * command, or an empty relation when there are none. Exemplars live in time series data streams but are plain documents to this
     * query, so the relation reads them like {@code FROM} does.
     */
    private LogicalPlan exemplarsRelation(TimeSeriesExemplars exemplars) {
        Source source = exemplars.source();
        IndexResolution resolution = exemplarsResolution.resolution(exemplars.metricsIndexPattern());
        if (resolution == null) {
            return new LocalRelation(source, List.of(), EmptyLocalSupplier.EMPTY);
        }
        if (resolution.isValid() == false) {
            throw new VerificationException(List.of(Failure.fail(exemplars, resolution.toString())));
        }
        EsIndex esIndex = resolution.get();
        List<Attribute> attributes = Analyzer.mappingAsAttributes(source, esIndex.mapping());
        return new EsRelation(
            source,
            esIndex.name(),
            IndexMode.STANDARD,
            esIndex.originalIndices(),
            esIndex.concreteIndices(),
            esIndex.indexProperties(),
            attributes.isEmpty() ? Analyzer.NO_FIELDS : attributes
        );
    }

    /**
     * Builds the exemplar query replacing one {@code TS_EXEMPLARS} command from the optimized plan of its metrics query.
     */
    static LogicalPlan exemplarsQuery(TimeSeriesExemplars exemplars, LogicalPlan optimizedMetricsQuery, LogicalPlan exemplarsRelation) {
        Source source = exemplars.source();
        List<ExemplarSelection> selections = new ArrayList<>();
        optimizedMetricsQuery.forEachDown(TimeSeriesAggregate.class, aggregate -> selections.add(exemplarSelection(aggregate)));

        List<Expression> selectionConditions = new ArrayList<>(selections.size());
        for (ExemplarSelection selection : selections) {
            if (selection.metricFields().isEmpty()) {
                continue;
            }
            List<Expression> metricPresent = new ArrayList<>(selection.metricFields().size());
            for (FieldAttribute metricField : selection.metricFields()) {
                metricPresent.add(new IsNotNull(source, exemplarField(metricField)));
            }
            List<Expression> conditions = new ArrayList<>(selection.filters().size() + 1);
            for (Expression filter : selection.filters()) {
                conditions.add(filter.transformUp(FieldAttribute.class, TimeSeriesExemplarsRewriter::exemplarField));
            }
            conditions.add(Predicates.combineOr(metricPresent));
            selectionConditions.add(Predicates.combineAnd(conditions));
        }
        if (selectionConditions.isEmpty()) {
            throw new VerificationException(
                List.of(Failure.fail(exemplars, "TS_EXEMPLARS requires the metrics query to aggregate at least one metric field"))
            );
        }
        return new Filter(source, exemplarsRelation, Predicates.combineOr(selectionConditions));
    }

    /**
     * What one time series aggregation of the optimized metrics query contributes to the exemplar query: the metric fields it
     * aggregates and the filter conjuncts applied to the series before the aggregation that hold for exemplars as well.
     */
    private record ExemplarSelection(List<FieldAttribute> metricFields, List<Expression> filters) {}

    /**
     * Reads the exemplar selection off one {@link TimeSeriesAggregate}. A PromQL binary operation over two selectors yields two of them,
     * each keeping its own label matchers.
     * <p>
     * The metrics are the fields aggregated per time series in the node, looking through conversion functions. By the time the plan is
     * optimized, the analysis has wrapped a bare metric in an implicit time series aggregation function, and the surrogate rules have
     * replaced some time series aggregation functions with windowed regular ones (e.g. {@code AVG_OVER_TIME} with {@code SUM} and
     * {@code COUNT}), so every {@link AggregateFunction} in the node counts, not only the {@link TimeSeriesAggregateFunction}s. The filters
     * are the top-level conjuncts of the {@code WHERE}s between the aggregation and the source relation; those after the aggregation
     * apply to aggregated values and are ignored. A conjunct is kept unless it does something that has no meaning for exemplars: refer to
     * anything other than a dimension, {@code @timestamp}, or a metric tested for {@code IS NULL} / {@code IS NOT NULL} (again looking
     * through conversion functions). In particular a comparison of a metric value would select exemplars by their own value rather than
     * by the series they belong to, so it is dropped.
     */
    private static ExemplarSelection exemplarSelection(TimeSeriesAggregate aggregate) {
        LinkedHashSet<FieldAttribute> metricFields = new LinkedHashSet<>();
        for (Expression aggregateExpression : aggregate.aggregates()) {
            aggregateExpression.forEachDown(AggregateFunction.class, function -> {
                FieldAttribute metricField = metricField(function.field());
                if (metricField != null) {
                    metricFields.add(metricField);
                }
            });
        }
        List<Expression> filters = new ArrayList<>();
        LogicalPlan plan = aggregate.child();
        while (plan instanceof UnaryPlan unary) {
            if (unary instanceof Filter filter) {
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (appliesToExemplars(conjunct)) {
                        filters.add(conjunct);
                    }
                }
            }
            plan = unary.child();
        }
        return new ExemplarSelection(List.copyOf(metricFields), filters);
    }

    private static boolean appliesToExemplars(Expression conjunct) {
        Expression withoutMetricNullChecks = conjunct.transformDown(e -> isMetricNullCheck(e) ? Literal.TRUE : e);
        for (Attribute reference : withoutMetricNullChecks.references()) {
            boolean allowed = reference instanceof FieldAttribute field
                && (field.isDimension() || field.fieldName().string().equals(MetadataAttribute.TIMESTAMP_FIELD));
            if (allowed == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMetricNullCheck(Expression expression) {
        return (expression instanceof IsNull || expression instanceof IsNotNull)
            && metricField(((UnaryScalarFunction) expression).field()) != null;
    }

    /**
     * The metric field an expression refers to, looking through conversion functions, or {@code null} if it is not one.
     */
    @Nullable
    private static FieldAttribute metricField(Expression expression) {
        return unwrapConversionFunctions(expression) instanceof FieldAttribute field && field.isMetric() ? field : null;
    }

    /**
     * The expression under a (possibly chained) stack of conversion functions, e.g. the field in {@code TO_LONG(TO_DOUBLE(field))};
     * the expression itself if it is not a conversion.
     */
    private static Expression unwrapConversionFunctions(Expression expression) {
        return expression instanceof ConvertFunction convert ? unwrapConversionFunctions(convert.field()) : expression;
    }

    /**
     * Refers to a field of the metrics relation by its name in the index so that it resolves against the exemplar relation (or as
     * {@code null} when the exemplar indices do not have it) instead.
     */
    private static UnresolvedAttribute exemplarField(FieldAttribute field) {
        return new UnresolvedAttribute(field.source(), field.fieldName().string());
    }
}
