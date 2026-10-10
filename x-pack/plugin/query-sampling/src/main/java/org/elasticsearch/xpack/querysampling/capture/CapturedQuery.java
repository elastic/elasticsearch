/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * A kNN search picked by the capture gate, copied out of the request so the rest of the pipeline can
 * work on it off the search thread.
 *
 * @param indices         index expressions of the search, as given by the user
 * @param field           the dense vector field searched
 * @param queryVector     a copy of the query vector
 * @param k               number of nearest neighbours requested
 * @param numCandidates   candidates considered per shard
 * @param visitPercentage visit percentage for IVF-style indices, if set
 * @param oversample      rescore oversampling factor, if set
 * @param filters         the kNN pre-filters, empty for an unfiltered search
 * @param opaqueId        the request's {@code X-Opaque-Id}; a diagnostic label only, it must never
 *                        influence sampling decisions
 */
public record CapturedQuery(
    String[] indices,
    String field,
    float[] queryVector,
    int k,
    int numCandidates,
    @Nullable Float visitPercentage,
    @Nullable Float oversample,
    List<QueryBuilder> filters,
    @Nullable String opaqueId
) {}
