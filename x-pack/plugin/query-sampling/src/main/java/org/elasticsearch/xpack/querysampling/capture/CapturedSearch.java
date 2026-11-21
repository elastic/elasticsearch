/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import java.util.List;

/**
 * A captured kNN search with the hits returned, in rank order.
 *
 * @param captureRate probability this search had of being captured; it is dynamic, so it travels with the search
 */
public record CapturedSearch(CapturedQuery query, List<Hit> hits, long tookMillis, double captureRate) {

    public record Hit(String index, String id, float score) {}
}
