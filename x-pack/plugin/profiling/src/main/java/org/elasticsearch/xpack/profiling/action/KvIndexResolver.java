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
import org.elasticsearch.cluster.metadata.DataStream;
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

/**
 * Resolves aliases that point to multiple key/value indices.
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
     * Resolves indices for a given index pattern. Supports both K/V indices (legacy) and data streams. If both a K/V
     * index and a data stream exist for the same name, an exception is thrown — mixed schemas are not supported.
     * Users must delete legacy K/V indices before data stream queries can proceed.
     *
     * @param clusterState The current cluster state.
     * @param indexPattern An index pattern to match.
     * @param eventStart The earliest point in time to consider (used for K/V index time-range filtering).
     * @param eventEnd The latest point in time to consider (used for K/V index time-range filtering).
     * @return A list of indices matching the provided index pattern and time range.
     * @throws IllegalStateException if both K/V indices and a data stream exist for the same name.
     */
    public List<Index> resolve(ClusterState clusterState, String indexPattern, Instant eventStart, Instant eventEnd) {
        Index[] kvIndices = resolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), indexPattern);
        DataStream dataStream = clusterState.metadata().getProject().dataStreams().get(indexPattern);

        if (kvIndices.length > 0 && dataStream != null) {
            throw new IllegalStateException(
                "Both K/V indices and a data stream exist for ["
                    + indexPattern
                    + "]. Mixed schemas are not supported. Delete the K/V indices to continue using the data stream."
            );
        }

        if (dataStream != null) {
            List<Index> dsIndices = dataStream.getIndices();
            log.debug("Resolved [{}] to data stream backing indices {}.", indexPattern, dsIndices.stream().map(Index::getName).toList());
            return Collections.unmodifiableList(dsIndices);
        }

        // K/V index path: filter by time range when multiple indices exist.
        List<Index> matchingIndices = new ArrayList<>();
        if (kvIndices.length > 1) {
            List<Tuple<Index, Instant>> indicesWithTime = new ArrayList<>();
            Map<String, IndexMetadata> indicesMetadata = clusterState.getMetadata().getProject().indices();
            for (Index i : kvIndices) {
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
        if (matchingIndices.isEmpty()) {
            matchingIndices.addAll(Arrays.asList(kvIndices));
        }
        log.debug("Resolved [{}] to K/V indices {}.", indexPattern, matchingIndices.stream().map(Index::getName).toList());
        return Collections.unmodifiableList(matchingIndices);
    }
}
