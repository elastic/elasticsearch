/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.PointValues;
import org.elasticsearch.lucene.search.cost.FixedBitSetCost;

/**
 * The per-leaf execution RAM of a Lucene {@code LatLonPointDistanceQuery}. When every document of the leaf has exactly one point
 * and the circle matches more than half of them, the query builds a {@code FixedBitSet(maxDoc)} of the documents that do not
 * match instead of collecting the matches; otherwise it collects them like any other points query.
 */
final class LatLonPointDistanceExecutionEstimator extends PointsExecutionEstimator {

    LatLonPointDistanceExecutionEstimator(String field) {
        super(field);
    }

    @Override
    long bytesForMatches(PointValues values, int maxDoc, long cost) {
        if (values.getDocCount() == maxDoc && values.getDocCount() == values.size() && cost > maxDoc / 2) {
            return FixedBitSetCost.scorerBytes(maxDoc);
        }
        return super.bytesForMatches(values, maxDoc, cost);
    }
}
