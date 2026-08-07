/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves index patterns and data stream patterns to concrete backing indices, optionally filtered
 * to the subset whose active period overlaps a given query time range.
 */
public class KvIndexResolver {
    private static final Logger log = LogManager.getLogger(KvIndexResolver.class);

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
     *
     * Resolves aliases that point to multiple K/V indices. When resolving indices it sorts indices by their creation timestamp (or
     * lifecycle origination date, if specified) and assumes that an index contains data from its creation until the creation of the next
     * index plus an overlap that is controlled by <code>kvIndexOverlapPeriod</code>. It will only return matching indices within the
     * specified time range.
     *
     * @param clusterState The current cluster state.
     * @param indexPattern An index pattern to match.
     * @param eventStart The earliest point in time to consider
     * @param eventEnd The latest point in time to consider
     * @return A list of indices that match both the provided index pattern and the time range between event start and end.
     */
    public List<Index> resolve(ClusterState clusterState, String indexPattern, Instant eventStart, Instant eventEnd) {
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
            // sort - newest index first, then work backwards to find overlaps
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
     * Resolves a data stream pattern to its backing indices relevant for the given time range. Applies the same
     * creation-date filtering as {@link #resolve}: backing indices whose active period does not overlap
     * {@code [eventStart, eventEnd]} are excluded. Falls back to all backing indices when no match is found.
     *
     * @param clusterState The current cluster state.
     * @param dataStreamPattern A data stream name or pattern to match.
     * @param eventStart The earliest point in time to consider.
     * @param eventEnd The latest point in time to consider.
     * @return A list of backing indices whose active period overlaps the given time range.
     */
    public List<Index> resolveDataStream(ClusterState clusterState, String dataStreamPattern, Instant eventStart, Instant eventEnd) {
        // LENIENT_EXPAND_OPEN_HIDDEN: data stream backing indices are hidden, so HIDDEN is required to resolve them.
        // LENIENT is used rather than STRICT because the data stream may not exist yet on a fresh cluster; in that
        // case concreteIndices returns an empty array and resolveByCreationDate falls back to returning nothing.
        Index[] indices = resolver.concreteIndices(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN, true, dataStreamPattern);
        return resolveByCreationDate(Arrays.asList(indices), clusterState, dataStreamPattern, eventStart, eventEnd);
    }

    private List<Index> resolveByCreationDate(
        List<Index> indices,
        ClusterState clusterState,
        String pattern,
        Instant eventStart,
        Instant eventEnd
    ) {
        List<Index> matchingIndices = new ArrayList<>();
        if (indices.size() > 1) {
            Map<String, IndexMetadata> indicesMetadata = clusterState.getMetadata().getProject().indices();
            List<Tuple<Index, Instant>> indicesWithTime = new ArrayList<>();
            for (Index i : indices) {
                IndexMetadata indexMetadata = indicesMetadata.get(i.getName());
                indicesWithTime.add(Tuple.tuple(i, Instant.ofEpochMilli(indexMetadata.getCreationDate())));
            }
            indicesWithTime.sort((i1, i2) -> i2.v2().compareTo(i1.v2()));
            Instant intervalEnd = Instant.MAX;
            for (Tuple<Index, Instant> indexAndTime : indicesWithTime) {
                Instant intervalStart = indexAndTime.v2();
                if (intervalStart.isBefore(eventEnd) && intervalEnd.isAfter(eventStart)) {
                    matchingIndices.add(indexAndTime.v1());
                }
                intervalEnd = intervalStart.plusMillis(kvIndexOverlapPeriod.millis());
            }
        }
        if (matchingIndices.isEmpty()) {
            log.debug("Querying all indices for [{}].", pattern);
            matchingIndices.addAll(indices);
        }
        return Collections.unmodifiableList(matchingIndices);
    }
}
