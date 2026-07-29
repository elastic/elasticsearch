/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves aliases that point to multiple key/value indices or their OTel data stream counterpart.
 *
 * For each K/V index pattern such as {@code profiling-executables} the resolver first checks
 * whether a corresponding OTel data stream ({@code profiling-otel-executables}) exists. If it does,
 * its backing indices are returned directly. If both the OTel data stream and the legacy K/V alias
 * exist simultaneously a {@link RestStatus#CONFLICT} exception is raised because mixed schemas are
 * not supported. When no OTel data stream is present the resolver falls back to the existing
 * time-range K/V logic.
 */
public class KvIndexResolver {
    private static final Logger log = LogManager.getLogger(KvIndexResolver.class);

    private static final String KV_PREFIX = "profiling-";
    private static final String OTEL_PREFIX = "profiling-otel-";

    private final IndexNameExpressionResolver resolver;
    /**
     * Specifies the time period for which K/V indices should be considered to overlap. See
     * also @{@link TransportGetStackTracesAction#PROFILING_KV_INDEX_OVERLAP}.
     */
    private final TimeValue kvIndexOverlapPeriod;

    public KvIndexResolver(IndexNameExpressionResolver resolver, TimeValue kvIndexOverlapPeriod) {
        this.resolver = resolver;
        this.kvIndexOverlapPeriod = kvIndexOverlapPeriod;
    }

    /**
     * Resolves the backing indices for a K/V index pattern.
     *
     * The OTel data stream is checked first. If found, its backing indices are returned. If both the
     * OTel data stream and the legacy K/V alias exist a {@link RestStatus#CONFLICT} exception is
     * thrown. Otherwise the resolver falls back to the time-range K/V logic.
     *
     * @param clusterState the current cluster state
     * @param indexPattern a legacy K/V index pattern such as {@code profiling-executables}
     * @param eventStart the earliest point in time to consider, used only on the K/V path
     * @param eventEnd the latest point in time to consider, used only on the K/V path
     * @return a list of indices that satisfy the query
     */
    public List<Index> resolve(ClusterState clusterState, String indexPattern, Instant eventStart, Instant eventEnd) {
        String otelDsName = toOtelName(indexPattern);
        DataStream otelDataStream = clusterState.metadata().getProject().dataStreams().get(otelDsName);

        if (otelDataStream != null) {
            if (clusterState.metadata().getProject().hasAlias(indexPattern)) {
                throw new ElasticsearchStatusException(
                    "Both K/V indices and an OTel data stream exist for ["
                        + indexPattern
                        + "]. Mixed schemas are not supported. Delete the legacy K/V indices to continue using the OTel data stream.",
                    RestStatus.CONFLICT
                );
            }
            List<Index> dsIndices = otelDataStream.getIndices();
            log.debug("Resolved [{}] to OTel data stream backing indices {}.", indexPattern, dsIndices.stream().map(Index::getName).toList());
            return Collections.unmodifiableList(dsIndices);
        }

        // K/V index path: filter by time range when multiple indices exist.
        Index[] indices = resolver.concreteIndices(clusterState, IndicesOptions.STRICT_EXPAND_OPEN, indexPattern);
        List<Index> matchingIndices = new ArrayList<>();
        // find matching index for the current time range (indices are non-overlapping)
        if (indices.length > 1) {
            List<Tuple<Index, Instant>> indicesWithTime = new ArrayList<>();
            Map<String, IndexMetadata> indicesMetadata = clusterState.getMetadata().getProject().indices();
            for (Index i : indices) {
                IndexMetadata indexMetadata = indicesMetadata.get(i.getName());
                // Prefer ILM creation date over the actual creation date. This is mainly intended for testing as
                // during regular operation the actual creation date should suffice. Using LIFECYCLE_ORIGINATION_DATE
                // allows for consistency between index resolution and how ILM operates on these indices.
                long creationDate;
                if (indexMetadata.getSettings().hasValue(IndexSettings.LIFECYCLE_ORIGINATION_DATE)) {
                    creationDate = IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.get(indexMetadata.getSettings());
                    log.trace("Using lifecycle origination date [{}] for index [{}]", creationDate, i.getName());
                } else {
                    creationDate = indexMetadata.getCreationDate();
                    log.trace("Using index creation date [{}] for index [{}]", creationDate, i.getName());
                }
                indicesWithTime.add(Tuple.tuple(i, Instant.ofEpochMilli(creationDate)));
            }
            // sort newest index first, then work backwards to find overlaps
            indicesWithTime.sort((i1, i2) -> i2.v2().compareTo(i1.v2()));
            Instant intervalEnd = Instant.MAX;
            for (Tuple<Index, Instant> indexAndTime : indicesWithTime) {
                Instant intervalStart = indexAndTime.v2();
                if ((intervalStart.isBefore(eventEnd)) && intervalEnd.isAfter(eventStart)) {
                    matchingIndices.add(indexAndTime.v1());
                }
                // prior interval ends when this interval starts (+ overlap to account for client-side caching)
                intervalEnd = intervalStart.plusMillis(kvIndexOverlapPeriod.millis());
            }
        }
        // either we have only one index or there was no overlap in time ranges
        if (matchingIndices.isEmpty()) {
            log.debug("Querying all indices for [" + indexPattern + "].");
            matchingIndices.addAll(Arrays.asList(indices));
        }

        if (log.isDebugEnabled()) {
            log.debug(
                "Resolved index pattern ["
                    + indexPattern
                    + "] in time range ["
                    + eventStart
                    + ", "
                    + eventEnd
                    + "] to indices ["
                    + matchingIndices.stream().map(Index::getName).collect(Collectors.joining(", "))
                    + "]."
            );
        }
        return Collections.unmodifiableList(matchingIndices);
    }

    /**
     * Derives the OTel data stream name for a given legacy K/V index name.
     * For example {@code profiling-executables} becomes {@code profiling-otel-executables}.
     */
    static String toOtelName(String kvIndexPattern) {
        if (kvIndexPattern.startsWith(KV_PREFIX) == false) {
            return OTEL_PREFIX + kvIndexPattern;
        }
        return OTEL_PREFIX + kvIndexPattern.substring(KV_PREFIX.length());
    }
}
