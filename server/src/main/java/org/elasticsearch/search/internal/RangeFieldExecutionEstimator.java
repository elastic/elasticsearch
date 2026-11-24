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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScorerSupplier;

import java.io.IOException;

/**
 * The per-leaf execution RAM of a {@link RangeFieldQuery}. When every document of a leaf has a value and the leaf's whole value range
 * is inside the query, Lucene matches all documents without building anything. This charges nothing on the single-valued leaves of
 * that case and is a {@link PointsExecutionEstimator} otherwise.
 * <p>
 * The query keeps its ranges and relation private, so the case is told apart by {@link ScorerSupplier#cost()}: {@code maxDoc} on the
 * match-all path. On a dense single-valued leaf the intersecting path only estimates {@code maxDoc} when every point lies in a BKD
 * cell inside the query, since a crossing leaf cell of {@code n > 1} points is counted as {@code (n + 1) / 2}. For the intersects,
 * within and contains relations a cell is inside the query whenever all its children are, so the whole leaf is too and Lucene would
 * have taken the match-all path. That does not hold for the crosses relation, which Elasticsearch never builds. A multivalued leaf is
 * always charged: its doc count estimate can round up to {@code maxDoc} while some documents do not match. So is a single-document
 * leaf, where the one crossing point is counted in full.
 */
final class RangeFieldExecutionEstimator implements LeafExecutionEstimator {

    private final String field;
    private final PointsExecutionEstimator points;

    RangeFieldExecutionEstimator(String field) {
        this.field = field;
        this.points = new PointsExecutionEstimator(field);
    }

    @Override
    public long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException {
        if (matchesAllDocs(ctx.reader(), inner)) {
            return 0L;
        }
        return points.bytesForLeaf(ctx, inner, leadCost);
    }

    private boolean matchesAllDocs(LeafReader reader, ScorerSupplier inner) throws IOException {
        final PointValues values = reader.getPointValues(field);
        if (values == null) {
            return false;
        }
        final int maxDoc = reader.maxDoc();
        return maxDoc > 1 && values.getDocCount() == maxDoc && values.size() == maxDoc && inner.cost() == maxDoc;
    }
}
