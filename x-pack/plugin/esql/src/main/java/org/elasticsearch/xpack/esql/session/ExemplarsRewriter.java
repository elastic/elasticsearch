/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the exemplar query for queries run with {@link QuerySettings#EXEMPLARS SET exemplars=true}.
 */
public final class ExemplarsRewriter {

    private static final String METRICS_DATA_STREAM_PREFIX = "metrics-";
    private static final String EXEMPLARS_DATA_STREAM_PREFIX = "exemplars-";
    private static final Pattern BACKING_INDEX_PATTERN = Pattern.compile("^(?:.*-)?\\.(?:ds|fs)-(.+)-\\d{4}\\.\\d{2}\\.\\d{2}-\\d{6}$");
    /** The exemplar field naming the metric an exemplar belongs to, see {@link #metricFilters}. */
    private static final String METRIC_NAME_FIELD = "metric_name";
    /** The exemplar field holding the observed value. */
    private static final String VALUE_FIELD = "value";

    private ExemplarsRewriter() {}

    /**
     * The exemplar data streams of a metrics query, as an index pattern. The concrete indices in the metrics resolutions reflect alias
     * expansion, remote wildcards, and request-filter narrowing. For each of them, a candidate parent data-stream name is inferred from
     * the standard backing-index name; an unrecognized name is ignored. A {@code metrics-<name>} result maps to
     * {@code exemplars-<name>} on the same cluster. Returns {@code null} when no resolved backing index maps to a
     * {@code metrics-*} data stream.
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
                    String resolvedDataStream = resolveDataStreamName(index);
                    if (resolvedDataStream == null) {
                        continue;
                    }
                    String dataStream = RemoteClusterAware.splitIndexName(resolvedDataStream).indexExpression();
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

    @Nullable
    private static String resolveDataStreamName(String indexName) {
        // TODO: Replace this name heuristic (originally introduced for METRICS_INFO) with a proper resolution.
        // This would probably involve extending field-caps to include data stream name or a similar solution.
        // This is however a low priority task, because for 99% of the cases the heuristic works fine
        // If it doesn't some exemplars might be missing or we are returning too many, which is not a big deal for
        // this anyway a little fuzzy exemplars feature
        var split = RemoteClusterAware.splitIndexName(indexName);
        Matcher matcher = BACKING_INDEX_PATTERN.matcher(split.indexExpression());
        return matcher.matches() ? RemoteClusterAware.buildRemoteIndexName(split.clusterAlias(), matcher.group(1)) : null;
    }

    /**
     * Replaces the analyzed metrics query with the most recent exemplars of the metrics it aggregates under its filters, read from all
     * resolved exemplar data streams. The metrics query has been verified with {@link #verifyMetricsQuery} before analysis. Without a
     * configured {@link ExemplarsSettings#limit() limit}, the analyzer applies the default limit of a regular query.
     * <p>
     * Analysis has translated the {@code TS ... | STATS} command to a time series aggregation of per-time-series aggregate functions,
     * which carry the filter of the {@code STATS} aggregate they belong to. A {@code PROMQL} command has been translated to one time
     * series aggregation per vector matching operand, with its label matchers and evaluated time range as filters. Each time series
     * aggregation selects the exemplars of its {@link #metricFilters metrics} under its {@link #sharedFilters shared filters}.
     *
     * @param metricsResolutions the resolutions of the metrics query's index patterns, used to recognize passthrough objects
     */
    public static LogicalPlan exemplarsQuery(
        LogicalPlan analyzedMetricsQuery,
        Collection<IndexResolution> metricsResolutions,
        @Nullable IndexResolution exemplarsResolution,
        ExemplarsSettings settings
    ) {
        Source source = analyzedMetricsQuery.source();
        TimeSeriesMetadataAttribute timeseries = settings.includeTimeseries() ? FieldAttribute.timeSeriesAttribute(source) : null;
        LogicalPlan exemplars = exemplarsRelation(analyzedMetricsQuery, exemplarsResolution, timeseries);
        if (exemplars instanceof LocalRelation) {
            return exemplars;
        }
        Set<String> passthroughPaths = new HashSet<>();
        for (IndexResolution resolution : metricsResolutions) {
            if (resolution != null && resolution.isValid()) {
                collectPassthroughPaths(null, resolution.get().mapping(), passthroughPaths);
            }
        }
        List<Expression> aggregationFilters = new ArrayList<>();
        // Note that we currently only support a single PROMQL / TS .. STATS Command,
        // still due to vector matching there may be multiple TimeSeriesAggregates
        // But we know they load their metrics from the same index patterns
        analyzedMetricsQuery.forEachDown(TimeSeriesAggregate.class, aggregate -> {
            List<Expression> metricFilters = metricFilters(aggregate, passthroughPaths);
            if (metricFilters.isEmpty() == false) {
                List<Expression> conditions = new ArrayList<>(sharedFilters(aggregate, passthroughPaths));
                conditions.add(Predicates.combineOr(metricFilters));
                aggregationFilters.add(Predicates.combineAnd(conditions));
            }
        });
        // Without any aggregated metric, there are no exemplars to select.
        Expression filter = aggregationFilters.isEmpty() ? Literal.FALSE : Predicates.combineOr(aggregationFilters);
        exemplars = new Filter(source, exemplars, filter);
        exemplars = new Keep(source, exemplars, exemplarProjections(source, timeseries));
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
     * The columns an exemplars query returns, in output order: the exemplar's timestamp, the metric it belongs to and its observed
     * value, then the dimensions identifying its series when {@code timeseries} is requested, and finally its full document. The named
     * columns are left unresolved so that analysis binds them against the exemplar data streams, or nullifies the ones none of them
     * maps.
     */
    private static List<NamedExpression> exemplarProjections(Source source, @Nullable TimeSeriesMetadataAttribute timeseries) {
        List<NamedExpression> projections = new ArrayList<>(5);
        projections.add(new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD));
        projections.add(new UnresolvedAttribute(source, METRIC_NAME_FIELD));
        projections.add(new UnresolvedAttribute(source, VALUE_FIELD));
        if (timeseries != null) {
            projections.add(timeseries);
        }
        projections.add(new UnresolvedAttribute(source, SourceFieldMapper.NAME));
        return projections;
    }

    /**
     * Verifies that a metrics query, before analysis, contains at most one {@code TS ... | STATS} or one {@code PROMQL} command.
     * This has to be checked before analysis, which translates a {@code PROMQL} command into one time series aggregation per vector
     * matching operand. A query without either aggregates no metrics and therefore selects no exemplars.
     */
    public static void verifyMetricsQuery(LogicalPlan metricsQuery) {
        int metricsCommands = metricsQuery.collect(TimeSeriesAggregate.class).size() + metricsQuery.collect(PromqlCommand.class).size();
        if (metricsCommands > 1) {
            throw new VerificationException(
                List.of(
                    Failure.fail(
                        metricsQuery,
                        "[{}] supports at most one TS ... | STATS or PROMQL command, found [{}]",
                        QuerySettings.EXEMPLARS.name(),
                        metricsCommands
                    )
                )
            );
        }
    }

    /**
     * Collects the metric fields read by the aggregate functions in an expression and the conjuncts of their filters that apply to
     * exemplars (see {@link #exemplarConjuncts}).
     */
    private static void collectAggregatedMetricFields(Expression expression, Set<FieldAttribute> metricFields, List<Expression> filters) {
        if (expression instanceof AggregateFunction function) {
            if (function.hasFilter()) {
                filters.addAll(exemplarConjuncts(function.filter()));
            }
            for (Expression field : function.fields()) {
                field.forEachDown(FieldAttribute.class, attribute -> {
                    if (attribute.isMetric()) {
                        metricFields.add(attribute);
                    }
                });
            }
        } else {
            for (Expression child : expression.children()) {
                collectAggregatedMetricFields(child, metricFields, filters);
            }
        }
    }

    /**
     * One filter per aggregate expression of a time series aggregation, selecting the exemplars of the metrics it aggregates under its
     * filter, so that they can be combined with OR.
     * <p>
     * Exemplars record the metric in {@code metric_name} by its field name without passthrough parents: OTel stores the metric
     * {@code cpu_time} in the passthrough object {@code metrics}, so both the field {@code metrics.cpu_time} and its root-level alias
     * {@code cpu_time} select the exemplars of {@code cpu_time}.
     */
    private static List<Expression> metricFilters(TimeSeriesAggregate aggregate, Set<String> passthroughPaths) {
        List<Expression> filters = new ArrayList<>();
        for (Expression aggregateExpression : aggregate.aggregates()) {
            // Linked hashset to get a stable order matching the order in the original query
            // Not required for correctness, but nicer for testing
            Set<FieldAttribute> metricFields = new LinkedHashSet<>();
            List<Expression> aggregateFilters = new ArrayList<>();
            collectAggregatedMetricFields(aggregateExpression, metricFields, aggregateFilters);
            if (metricFields.isEmpty()) {
                continue;
            }
            Source source = aggregateExpression.source();
            List<Expression> conditions = new ArrayList<>();
            for (Expression conjunct : aggregateFilters) {
                conditions.add(exemplarFilter(conjunct, passthroughPaths));
            }
            List<Expression> nameFilters = metricFields.stream()
                .map(field -> withoutPassthroughParents(field.fieldName().string(), passthroughPaths))
                .distinct()
                .map(name -> {
                    return (Expression) new Equals(
                        source,
                        new UnresolvedAttribute(source, METRIC_NAME_FIELD),
                        Literal.keyword(source, name)
                    );
                })
                .toList();
            conditions.add(Predicates.combineOr(nameFilters));
            filters.add(Predicates.combineAnd(conditions));
        }
        return filters;
    }

    /**
     * The filters of the {@code WHERE}s between a time series aggregation and its source relation, which apply to all metrics it
     * aggregates. Filters after the aggregation apply to aggregated values and are ignored.
     */
    private static List<Expression> sharedFilters(TimeSeriesAggregate aggregate, Set<String> passthroughPaths) {
        List<Expression> filters = new ArrayList<>();
        for (LogicalPlan plan = aggregate.child(); plan instanceof UnaryPlan unary; plan = unary.child()) {
            if (unary instanceof Filter filter) {
                for (Expression conjunct : exemplarConjuncts(filter.condition())) {
                    filters.add(exemplarFilter(conjunct, passthroughPaths));
                }
            }
        }
        return filters;
    }

    /**
     * The top-level conjuncts of a filter of the metrics query that hold for exemplars as well, to be translated for the exemplar
     * relation by {@link #exemplarFilter}. A conjunct is kept if it only refers to dimensions, {@code @timestamp}, and metrics tested
     * for {@code IS NULL} or {@code IS NOT NULL} (looking through conversion functions). Any other reference to a metric would select
     * exemplars by their own value rather than by the series they belong to, and a condition on a value computed by an {@code EVAL}
     * cannot be evaluated on exemplars, so such conjuncts are dropped.
     */
    private static List<Expression> exemplarConjuncts(Expression condition) {
        List<Expression> conjuncts = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(condition)) {
            if (appliesToExemplars(conjunct)) {
                conjuncts.add(conjunct);
            }
        }
        return conjuncts;
    }

    private static boolean appliesToExemplars(Expression conjunct) {
        Expression withoutMetricNullChecks = conjunct.transformDown(e -> isMetricNullCheck(e) ? Literal.TRUE : e);
        for (Attribute reference : withoutMetricNullChecks.references()) {
            if (reference instanceof FieldAttribute == false) {
                return false;
            }
            FieldAttribute field = (FieldAttribute) reference;
            if (field.isDimension() == false && field.fieldName().string().equals(MetadataAttribute.TIMESTAMP_FIELD) == false) {
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
        while (expression instanceof ConvertFunction convert) {
            expression = convert.field();
        }
        return expression instanceof FieldAttribute field && field.isMetric() ? field : null;
    }

    /**
     * Translates a conjunct kept by {@link #exemplarConjuncts} for the exemplar relation. Exemplars do not have the metric fields, so
     * {@code <metric> IS NOT NULL} becomes {@code metric_name == "<metric name>"} and {@code <metric> IS NULL} becomes
     * {@code metric_name != "<metric name>"}. Every other field is referred to by its name, so that it resolves against the exemplar
     * relation, or is nullified when the exemplar data streams do not have it.
     */
    private static Expression exemplarFilter(Expression conjunct, Set<String> passthroughPaths) {
        Expression withMetricNullChecksTranslated = conjunct.transformDown(e -> {
            if (isMetricNullCheck(e) == false) {
                return e;
            }
            FieldAttribute metric = metricField(((UnaryScalarFunction) e).field());
            Literal metricName = Literal.keyword(e.source(), withoutPassthroughParents(metric.fieldName().string(), passthroughPaths));
            if (e instanceof IsNull) {
                return new NotEquals(e.source(), new UnresolvedAttribute(e.source(), METRIC_NAME_FIELD), metricName);
            } else {
                assert e instanceof IsNotNull;
                return new Equals(e.source(), new UnresolvedAttribute(e.source(), METRIC_NAME_FIELD), metricName);
            }
        });
        return withMetricNullChecksTranslated.transformUp(
            FieldAttribute.class,
            field -> new UnresolvedAttribute(field.source(), field.fieldName().string())
        );
    }

    private static void collectPassthroughPaths(@Nullable String parentPath, Map<String, EsField> mapping, Set<String> passthroughPaths) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String path = parentPath == null ? entry.getKey() : parentPath + "." + entry.getKey();
            if (entry.getValue().isPassthrough()) {
                passthroughPaths.add(path);
            }
            collectPassthroughPaths(path, entry.getValue().getProperties(), passthroughPaths);
        }
    }

    /**
     * The field name relative to its innermost passthrough parent, which is the name of its root-level alias.
     */
    private static String withoutPassthroughParents(String fieldName, Set<String> passthroughPaths) {
        for (int dot = fieldName.lastIndexOf('.'); dot > 0; dot = fieldName.lastIndexOf('.', dot - 1)) {
            if (passthroughPaths.contains(fieldName.substring(0, dot))) {
                return fieldName.substring(dot + 1);
            }
        }
        return fieldName;
    }

    /**
     * The relation over the exemplar data streams that pre-analysis resolved. A {@code null} resolution means pre-analysis found no
     * {@code metrics-*} target from which to derive an exemplar data stream, so the query is empty. Derived data streams that do not
     * exist have already been omitted from the resolution. Exemplars live in time series data streams but are plain documents to this
     * query, so the relation reads them like {@code FROM ... METADATA _source} does.
     */
    private static LogicalPlan exemplarsRelation(
        LogicalPlan metricsQuery,
        @Nullable IndexResolution resolution,
        @Nullable TimeSeriesMetadataAttribute timeseries
    ) {
        Source source = metricsQuery.source();
        if (resolution == null) {
            return new LocalRelation(source, List.of(), EmptyLocalSupplier.EMPTY);
        }
        if (resolution.isValid() == false) {
            throw new VerificationException(List.of(Failure.fail(metricsQuery, resolution.toString())));
        }
        EsIndex esIndex = timeSeriesIndicesOnly(resolution.get());
        if (esIndex == null) {
            return new LocalRelation(source, List.of(), EmptyLocalSupplier.EMPTY);
        }
        List<Attribute> attributes = Analyzer.mappingAsAttributes(source, esIndex.mapping());
        attributes.add(MetadataAttribute.create(source, SourceFieldMapper.NAME).toAttribute());
        if (timeseries != null) {
            attributes.add(timeseries);
        }
        return new EsRelation(
            source,
            esIndex.name(),
            IndexMode.STANDARD,
            esIndex.originalIndices(),
            esIndex.concreteIndices(),
            esIndex.indexProperties(),
            attributes
        );
    }

    /**
     * The resolved exemplar indices that are time series ones.
     * We require exemplar indices to be time series indices too, even though they don't store metrics:
     * They contain (and mark) all dimensions of the original metrics and mark them as dimensions, which
     * we need to load the _timeseries column if requested.
     */
    @Nullable
    static EsIndex timeSeriesIndicesOnly(EsIndex esIndex) {
        Map<String, IndexProperties> timeSeriesIndices = new LinkedHashMap<>(esIndex.indexProperties().size());
        // Sorted, so that the warning names them in the same order however field caps happened to report them.
        Set<String> others = new TreeSet<>();
        esIndex.indexProperties().forEach((index, properties) -> {
            if (properties.indexMode().isTsdb()) {
                timeSeriesIndices.put(index, properties);
            } else {
                others.add(index);
            }
        });
        if (others.isEmpty()) {
            return esIndex;
        }
        HeaderWarning.addWarning(
            "[{}] skipped [{}]: exemplars indices must be time series indices",
            QuerySettings.EXEMPLARS.name(),
            String.join(", ", others)
        );
        if (timeSeriesIndices.isEmpty()) {
            return null;
        }
        // Keyed by cluster alias and holding unqualified names, unlike the index properties, see IndexResolver.
        Map<String, List<String>> concreteIndices = new LinkedHashMap<>();
        for (String index : timeSeriesIndices.keySet()) {
            var split = RemoteClusterAware.splitIndexName(index);
            concreteIndices.computeIfAbsent(split.getClusterGroupingKey(), cluster -> new ArrayList<>()).add(split.indexExpression());
        }
        // The original indices stay as they are: they name the patterns to resolve shards from, which the concrete indices then select
        // from, and a cluster left without any index is skipped by the compute service.
        return new EsIndex(esIndex.name(), esIndex.mapping(), timeSeriesIndices, esIndex.originalIndices(), concreteIndices);
    }
}
