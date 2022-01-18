/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

/**
 * Counterpart to {@link org.elasticsearch.search.aggregations.support.AggregationInspectionHelper}, providing
 * helpers for some aggs in the MatrixStats package
 */
public class MatrixAggregationInspectionHelper {
    public static boolean hasValue(InternalMatrixStats agg) {
        return agg.getResults() != null;
    }
}
