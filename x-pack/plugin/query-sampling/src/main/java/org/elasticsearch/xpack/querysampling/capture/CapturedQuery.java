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
 * A kNN search copied out of the request so it can be processed off the search thread.
 *
 * @param k        neighbours returned: the search's {@code k}, or fewer if it asks for fewer hits
 * @param opaqueId the request's {@code X-Opaque-Id}; a diagnostic label that must never influence sampling
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
