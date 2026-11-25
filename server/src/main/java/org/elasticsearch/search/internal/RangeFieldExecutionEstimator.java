/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScorerSupplier;

/**
 * The per-leaf execution RAM of a {@link RangeFieldQuery}. When every document of a leaf has a value and the leaf's whole value range
 * is inside the query, Lucene matches all documents without building anything. This charges nothing on the single-valued leaves of
 * that case and is sized like any other points query otherwise.
 * <p>
 * The query keeps its ranges and relation private, so the case is told apart by {@link ScorerSupplier#cost()}: {@code maxDoc} on the
 * match-all path. On a dense single-valued leaf the intersecting path only estimates {@code maxDoc} when every point lies in a BKD
 * cell inside the query, since a crossing leaf cell of {@code n > 1} points is counted as {@code (n + 1) / 2}. For the intersects,
 * within and contains relations a cell is inside the query whenever all its children are, so the whole leaf is too and Lucene would
 * have taken the match-all path. That does not hold for the crosses relation, which Elasticsearch never builds. A multivalued leaf is
 * always charged: its doc count estimate can round up to {@code maxDoc} while some documents do not match. So is a single-document
 * leaf, where the one crossing point is counted in full.
 */
final class RangeFieldExecutionEstimator extends PointsExecutionEstimator {

    RangeFieldExecutionEstimator(String field) {
        super(field);
    }

    @Override
    long bytesForMatches(PointValues values, int maxDoc, long cost) {
        if (maxDoc > 1 && values.getDocCount() == maxDoc && values.size() == maxDoc && cost == maxDoc) {
            return 0L;
        }
        return super.bytesForMatches(values, maxDoc, cost);
    }
}
