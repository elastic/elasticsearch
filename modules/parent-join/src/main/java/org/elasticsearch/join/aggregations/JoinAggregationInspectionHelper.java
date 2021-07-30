/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.aggregations;

/**
 * Counterpart to {@link org.elasticsearch.search.aggregations.support.AggregationInspectionHelper}, providing
 * helpers for some aggs in the Join package
 */
public class JoinAggregationInspectionHelper {

    public static boolean hasValue(InternalParent agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalChildren agg) {
        return agg.getDocCount() > 0;
    }
}
