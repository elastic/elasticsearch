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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a metrics query into the query for the exemplars of the time series it reads, for queries run with
 * {@link QuerySettings#EXEMPLARS SET exemplars=true}.
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

    /** Builds a query over all exemplar data streams resolved for the analyzed metrics query. */
    public static LogicalPlan exemplarsQuery(
        LogicalPlan analyzedMetricsQuery,
        @Nullable IndexResolution exemplarsResolution,
        ExemplarsSettings settings
    ) {
        return exemplarsRelation(analyzedMetricsQuery, exemplarsResolution);
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

}
