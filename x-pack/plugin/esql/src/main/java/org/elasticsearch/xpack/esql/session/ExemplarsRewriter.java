/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a metrics query into the query for the exemplars of the time series it reads, for queries run with
 * {@link QuerySettings#EXEMPLARS SET exemplars=true}.
 * <p>
 * The metrics query is analyzed like a regular query but is never executed. In its analyzed plan the PromQL translation has happened
 * and every metric read per time series is aggregated by a {@link TimeSeriesAggregate}, so the metrics aggregated by each of these
 * nodes, the filters of the aggregate functions and the filters between the node and the source relation describe which series the
 * metrics query reads (see {@link #exemplarSelections}). Exemplars are then selected with the same filters, restricted to those of
 * these metrics. Everything the metrics query does after its time series aggregations is ignored: the exemplars are the result of the
 * query.
 * <p>
 * An exemplar document does not carry the metric field: it records the name of its metric in {@code metric_name} and the value in
 * {@code value}, next to the dimensions of its series and the trace and span it points to. So where the metrics query refers to a metric
 * field, the exemplar query refers to {@code metric_name} and the metric name (see {@link #metricName}), and it selects the exemplars of
 * the aggregated metrics with {@code metric_name IN (...)}.
 * <p>
 * The exemplar data streams are those of the metrics data streams the metrics query actually matched (see
 * {@link #exemplarIndexPattern}), which the pre-analysis resolves once the metrics index patterns are resolved. Fields are carried over
 * to the exemplar query by name and resolved against the exemplar relation by the analysis of the rewritten plan, which also verifies
 * it like any other query. Dimensions of the metrics query that none of the exemplar indices has (because only some of the metrics
 * data streams have exemplars, or none at all) are unmapped there. Queries with the setting therefore default to
 * {@link UnmappedResolution#NULLIFY}, which makes the analysis resolve them as {@code null} columns, like a field mapped in only some
 * indices of a pattern is {@code null} for the others. The dimension filters then keep excluding what they exclude in the metrics
 * query. Exemplar data streams that do not exist resolve to an empty relation for the same reason, so the query yields no rows for
 * them, as it does when the metrics query matches no {@code metrics-*} data stream at all.
 */
public final class ExemplarsRewriter {

    /**
     * The exemplar field naming the metric an exemplar belongs to, see {@link #metricName}.
     */
    public static final String METRIC_NAME_FIELD = "metric_name";

    private static final String METRICS_DATA_STREAM_PREFIX = "metrics-";
    private static final String EXEMPLARS_DATA_STREAM_PREFIX = "exemplars-";
    private static final Pattern BACKING_INDEX_PATTERN = Pattern.compile("^(?:.*-)?\\.(?:ds|fs)-(.+)-\\d{4}\\.\\d{2}\\.\\d{2}-\\d{6}$");

    private ExemplarsRewriter() {}

    /**
     * The exemplar data streams of a metrics query, as an index pattern: for every {@code metrics-<name>} data stream its time series
     * relations matched, the {@code exemplars-<name>} data stream on the same cluster. The matched data streams are recovered from the
     * backing indices in the resolutions of the metrics index patterns, like {@code METRICS_INFO} does, so this follows aliases, remote
     * wildcards and the narrowing of a request filter rather than the text of the patterns. Returns {@code null} when the metrics query
     * matched no {@code metrics-*} data stream (or its patterns are not resolved), in which case there are no exemplars to fetch.
     */
    @Nullable
    public static String exemplarIndexPattern(Collection<IndexResolution> metricsResolutions) {
        Set<String> exemplarDataStreams = new TreeSet<>();
        for (IndexResolution metricsResolution : metricsResolutions) {
            if (metricsResolution == null || metricsResolution.isValid() == false) {
                continue;
            }
            metricsResolution.get().concreteIndices().forEach((cluster, indices) -> {
                for (String index : indices) {
                    String dataStream = RemoteClusterAware.splitIndexName(resolveDataStreamName(index)).indexExpression();
                    if (dataStream.startsWith(METRICS_DATA_STREAM_PREFIX)) {
                        String exemplarDataStream = EXEMPLARS_DATA_STREAM_PREFIX + dataStream.substring(
                            METRICS_DATA_STREAM_PREFIX.length()
                        );
                        exemplarDataStreams.add(RemoteClusterAware.buildRemoteIndexName(cluster, exemplarDataStream));
                    }
                }
            });
        }
        return exemplarDataStreams.isEmpty() ? null : String.join(",", exemplarDataStreams);
    }

    private static String resolveDataStreamName(String indexName) {
        var split = RemoteClusterAware.splitIndexName(indexName);
        Matcher matcher = BACKING_INDEX_PATTERN.matcher(split.indexExpression());
        String resolved = matcher.matches() ? matcher.group(1) : split.indexExpression();
        return RemoteClusterAware.buildRemoteIndexName(split.clusterAlias(), resolved);
    }

    /**
     * Builds the exemplar query from the analyzed plan of the metrics query: a filter over the exemplar data streams the pre-analysis
     * resolved for it (see {@link #exemplarsRelation}), sorted most recent first and capped at the limit configured in
     * {@code settings}, if one is given (otherwise the analysis adds the default limit of a regular query). The result is unresolved
     * and has to be analyzed to resolve its fields against that relation.
     */
    public static LogicalPlan exemplarsQuery(
        LogicalPlan analyzedMetricsQuery,
        @Nullable IndexResolution exemplarsResolution,
        ExemplarsSettings settings
    ) {
        Source source = analyzedMetricsQuery.source();
        LogicalPlan exemplarsRelation = exemplarsRelation(analyzedMetricsQuery, exemplarsResolution);
        List<Expression> selectionConditions = new ArrayList<>();
        analyzedMetricsQuery.forEachDown(TimeSeriesAggregate.class, aggregate -> selectionConditions.addAll(exemplarSelections(aggregate)));
        if (selectionConditions.isEmpty()) {
            throw new VerificationException(
                List.of(
                    Failure.fail(
                        analyzedMetricsQuery,
                        "[{}] requires a TS or PROMQL query that aggregates at least one metric field",
                        QuerySettings.EXEMPLARS.name()
                    )
                )
            );
        }
        LogicalPlan exemplars = new Filter(source, exemplarsRelation, Predicates.combineOr(selectionConditions));
        Order mostRecentFirst = new Order(
            source,
            new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD),
            Order.OrderDirection.DESC,
            Order.NullsPosition.LAST
        );
        exemplars = new OrderBy(source, exemplars, List.of(mostRecentFirst));
        return settings.limit() == null ? exemplars : new Limit(source, new Literal(source, settings.limit(), DataType.INTEGER), exemplars);
    }

    /**
     * The relation to fetch the exemplars from: the exemplar data streams the pre-analysis resolved for the metrics query, or an empty
     * relation when there are none. Exemplars live in time series data streams but are plain documents to this query, so the relation
     * reads them like {@code FROM} does.
     */
    private static LogicalPlan exemplarsRelation(LogicalPlan metricsQuery, @Nullable IndexResolution resolution) {
        Source source = metricsQuery.source();
        if (resolution == null) {
            return new LocalRelation(source, List.of(), EmptyLocalSupplier.EMPTY);
        }
        if (resolution.isValid() == false) {
            throw new VerificationException(List.of(Failure.fail(metricsQuery, resolution.toString())));
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
     * The conditions selecting the exemplars of the series one {@link TimeSeriesAggregate} of the analyzed metrics query reads, one per
     * distinct filter under which it aggregates metrics; the exemplar query ORs them together. A PromQL binary operation over two
     * selectors with the same labels, for instance, yields a single node aggregating both metrics, each under the filter of its own
     * label matchers.
     * <p>
     * The metrics are the metric fields the aggregate functions in the node are applied to, looking through conversion functions: the
     * time series functions of a {@code TS} query, a bare metric having been wrapped in an implicit {@code LAST_OVER_TIME} by the
     * analysis, or the windowed aggregates a PromQL range function translates to. Each metric is selected under the filters of the
     * aggregate functions applied to it (a {@code STATS ... WHERE}, or the label matchers of a PromQL selector) on top of the
     * {@code WHERE}s between the aggregation and the source relation, which hold for all of them; filters after the aggregation apply
     * to aggregated values and are ignored. Metrics under the same filters share one condition. Of every filter only the top-level
     * conjuncts that have a meaning for exemplars are kept, see {@link #exemplarFilters}.
     */
    private static List<Expression> exemplarSelections(TimeSeriesAggregate aggregate) {
        List<Expression> sharedFilters = new ArrayList<>();
        LogicalPlan plan = aggregate.child();
        while (plan instanceof UnaryPlan unary) {
            if (unary instanceof Filter filter) {
                sharedFilters.addAll(exemplarFilters(filter.condition()));
            }
            plan = unary.child();
        }
        Map<List<Expression>, Set<FieldAttribute>> metricFieldsByFilters = new LinkedHashMap<>();
        for (Expression aggregateExpression : aggregate.aggregates()) {
            collectMetricFields(aggregateExpression, List.of(), metricFieldsByFilters);
        }
        List<Expression> selections = new ArrayList<>(metricFieldsByFilters.size());
        metricFieldsByFilters.forEach((filters, metricFields) -> {
            List<Expression> conditions = new ArrayList<>(sharedFilters.size() + filters.size() + 1);
            for (Expression filter : sharedFilters) {
                conditions.add(exemplarFilter(filter));
            }
            for (Expression filter : filters) {
                conditions.add(exemplarFilter(filter));
            }
            conditions.add(metricNameIn(aggregate.source(), metricFields));
            selections.add(Predicates.combineAnd(conditions));
        });
        return selections;
    }

    /**
     * Collects the metric fields aggregated by the aggregate functions in an expression, keyed by the exemplar filters of these
     * functions. A function nested in another one, like the {@code RATE} in {@code AVG(RATE(metric)) WHERE ...} of a {@code TS} query,
     * aggregates its metric under the filters of both. The filters are kept as they are in the metrics query, where equal conditions
     * are equal expressions, and only translated for the exemplar relation afterwards.
     */
    private static void collectMetricFields(
        Expression expression,
        List<Expression> enclosingFilters,
        Map<List<Expression>, Set<FieldAttribute>> metricFieldsByFilters
    ) {
        if (expression instanceof AggregateFunction function) {
            List<Expression> filters = enclosingFilters;
            if (function.hasFilter()) {
                filters = new ArrayList<>(enclosingFilters);
                filters.addAll(exemplarFilters(function.filter()));
            }
            for (Expression field : function.fields()) {
                FieldAttribute metricField = metricField(field);
                if (metricField != null) {
                    metricFieldsByFilters.computeIfAbsent(List.copyOf(filters), k -> new LinkedHashSet<>()).add(metricField);
                } else {
                    collectMetricFields(field, filters, metricFieldsByFilters);
                }
            }
        } else {
            for (Expression child : expression.children()) {
                collectMetricFields(child, enclosingFilters, metricFieldsByFilters);
            }
        }
    }

    /**
     * The top-level conjuncts of a filter condition of the metrics query that hold for exemplars as well, to be translated for the
     * exemplar relation by {@link #exemplarFilter}. A conjunct is kept unless it does something that has no meaning for exemplars: refer
     * to anything other than a dimension, {@code @timestamp}, or a metric tested for {@code IS NULL} / {@code IS NOT NULL} (again
     * looking through conversion functions). In particular a comparison of a metric value would select exemplars by their own value
     * rather than by the series they belong to, so it is dropped, and so is a condition on a value computed by an {@code EVAL}.
     */
    private static List<Expression> exemplarFilters(Expression condition) {
        List<Expression> filters = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(condition)) {
            if (appliesToExemplars(conjunct)) {
                filters.add(conjunct);
            }
        }
        return filters;
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
     * Restricts exemplars to those of the given metrics: {@code metric_name IN (<metric names>)}.
     */
    private static Expression metricNameIn(Source source, Collection<FieldAttribute> metricFields) {
        LinkedHashSet<String> metricNames = new LinkedHashSet<>();
        for (FieldAttribute metricField : metricFields) {
            metricNames.add(metricName(metricField));
        }
        List<Expression> names = new ArrayList<>(metricNames.size());
        for (String metricName : metricNames) {
            names.add(Literal.keyword(source, metricName));
        }
        return names.size() == 1
            ? new Equals(source, metricNameField(source), names.get(0))
            : new In(source, metricNameField(source), names);
    }

    /**
     * Translates a filter conjunct of the metrics query for the exemplar relation. Exemplars do not carry the metric fields, so a
     * {@code <metric> IS NOT NULL} (or {@code IS NULL}) becomes {@code metric_name == "<metric name>"} (or {@code !=}); every other field
     * is referred to by its name in the index so that it resolves against the exemplar relation (or as {@code null} when the exemplar
     * indices do not have it) instead.
     */
    private static Expression exemplarFilter(Expression conjunct) {
        Expression withoutMetricNullChecks = conjunct.transformDown(e -> {
            FieldAttribute metricField = isMetricNullCheck(e) ? metricField(((UnaryScalarFunction) e).field()) : null;
            if (metricField == null) {
                return e;
            }
            Literal metricName = Literal.keyword(e.source(), metricName(metricField));
            return e instanceof IsNull
                ? new NotEquals(e.source(), metricNameField(e.source()), metricName)
                : new Equals(e.source(), metricNameField(e.source()), metricName);
        });
        return withoutMetricNullChecks.transformUp(
            FieldAttribute.class,
            field -> new UnresolvedAttribute(field.source(), field.fieldName().string())
        );
    }

    private static UnresolvedAttribute metricNameField(Source source) {
        return new UnresolvedAttribute(source, METRIC_NAME_FIELD);
    }

    /**
     * The name an exemplar records in {@code metric_name} for a metric field of the metrics query: the name of the field.
     * <p>
     * TODO: handle metrics in passthrough objects. OTel ingestion maps the metric {@code cpu_time} to the field {@code metrics.cpu_time}
     * of the passthrough object {@code metrics}, which also registers the root-level alias {@code cpu_time} for it. Field caps reports
     * both as regular fields without a link between them, so only a metric referred to through the alias currently selects its
     * exemplars, while {@code metrics.cpu_time} selects none. Once field caps marks passthrough objects, the object prefix of a field in
     * such an object is to be stripped here.
     */
    private static String metricName(FieldAttribute metricField) {
        return metricField.fieldName().string();
    }
}
