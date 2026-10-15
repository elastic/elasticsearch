/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    /** The columns of an exemplars query, in output order: the exemplar's timestamp, metric and value plus its full document. */
    static final List<String> EXEMPLAR_COLUMNS = List.of(MetadataAttribute.TIMESTAMP_FIELD, "metric_name", "value", SourceFieldMapper.NAME);

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
     * Replaces the analyzed metrics query with the most recent exemplars of all resolved exemplar data streams. Without a configured
     * {@link ExemplarsSettings#limit() limit}, the analyzer applies the default limit of a regular query.
     */
    public static LogicalPlan exemplarsQuery(
        LogicalPlan analyzedMetricsQuery,
        @Nullable IndexResolution exemplarsResolution,
        ExemplarsSettings settings
    ) {
        // TODO add filters based on the original query, right now this queries just every exemplar from the related data streams
        Source source = analyzedMetricsQuery.source();
        LogicalPlan exemplars = exemplarsRelation(analyzedMetricsQuery, exemplarsResolution);
        if (exemplars instanceof LocalRelation) {
            return exemplars;
        }
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
     * The relation over the exemplar data streams that pre-analysis resolved, kept to {@link #EXEMPLAR_COLUMNS}. A {@code null}
     * resolution means pre-analysis found no {@code metrics-*} target from which to derive an exemplar data stream, so the query is
     * empty. Derived data streams that do not exist have already been omitted from the resolution. Exemplars live in time series data
     * streams but are plain documents to this query, so the relation reads them like {@code FROM ... METADATA _source} does. The
     * {@code KEEP} is left for the analyzer to resolve, which nullifies the exemplar columns missing from the mapping.
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
        attributes.add(MetadataAttribute.create(source, SourceFieldMapper.NAME).toAttribute());
        EsRelation relation = new EsRelation(
            source,
            esIndex.name(),
            IndexMode.STANDARD,
            esIndex.originalIndices(),
            esIndex.concreteIndices(),
            esIndex.indexProperties(),
            attributes
        );
        List<NamedExpression> projections = new ArrayList<>(EXEMPLAR_COLUMNS.size());
        for (String column : EXEMPLAR_COLUMNS) {
            projections.add(new UnresolvedAttribute(source, column));
        }
        return new Keep(source, relation, projections);
    }

}
