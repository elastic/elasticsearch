/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * A kNN search copied out of the request so it can be processed off the search thread.
 *
 * @param k        neighbours returned: the search's {@code k}, or fewer if it asks for fewer hits
 * @param filters  the filters of the kNN search, as JSON: written on the search thread because the query builders belong to the
 *                 caller, who may change them after the search, while the capture is read later on another thread
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
    List<String> filters,
    @Nullable String opaqueId
) {}
