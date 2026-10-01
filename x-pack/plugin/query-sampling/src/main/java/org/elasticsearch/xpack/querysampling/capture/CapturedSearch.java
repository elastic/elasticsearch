/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import java.util.List;

/**
 * A captured kNN search together with what the cluster answered, in rank order. The answer is what
 * lets recall of the live system be observed later, without running the query again.
 *
 * @param query      the search as it was received
 * @param hits       the hits returned to the user
 * @param tookMillis time the search took on the coordinating node
 */
public record CapturedSearch(CapturedQuery query, List<Hit> hits, long tookMillis) {

    /**
     * A returned document. The id alone is not enough because the same id can exist in several indices.
     */
    public record Hit(String index, String id, float score) {}
}
