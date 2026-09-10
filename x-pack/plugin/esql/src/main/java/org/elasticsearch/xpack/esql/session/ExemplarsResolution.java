/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-analysis result for the {@code TS_EXEMPLARS} commands of a query: the resolution of the exemplar data streams derived for the
 * metrics index pattern each command reads (see {@link TimeSeriesExemplarsRewriter#exemplarIndexPattern}). Unlike the patterns the
 * user wrote, these are not part of {@link EsqlSession.PreAnalysisResult#indexResolution()}: they are only reached through the
 * command, and matching nothing is not an error for them. A metrics pattern that matched no {@code metrics-*} data stream has no entry,
 * since there are no exemplars to fetch for it.
 */
public final class ExemplarsResolution {

    private final Map<IndexPattern, IndexResolution> resolutions = new HashMap<>();

    /**
     * The resolution of the exemplar data streams derived for a metrics index pattern, or {@code null} if it has none.
     */
    @Nullable
    public IndexResolution resolution(IndexPattern metricsIndexPattern) {
        return resolutions.get(metricsIndexPattern);
    }

    public List<IndexResolution> resolutions() {
        return List.copyOf(resolutions.values());
    }

    public void addResolution(IndexPattern metricsIndexPattern, IndexResolution resolution) {
        resolutions.put(metricsIndexPattern, resolution);
    }
}
