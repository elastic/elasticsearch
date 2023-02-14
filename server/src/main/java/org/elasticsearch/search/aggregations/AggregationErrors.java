/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

/**
 * Collection of helper methods for what to throw in common aggregation error scenarios.
 */
public class AggregationErrors {
    private AggregationErrors() {}

    /**
     * This error indicates that the aggregations path the user specified cannot be parsed.
     * It is a 400 class error and should not be retried.
     */
    public static IllegalArgumentException invalidPathElement(String element, String path) {
        return new IllegalArgumentException("Invalid path element [" + element + "] in path [" + path + "]");
    }
}
