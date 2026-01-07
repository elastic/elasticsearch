/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;

/**
 * Base interface for result diversification.
 */
public abstract class ResultDiversification<C extends ResultDiversificationContext> {

    protected final C context;

    protected ResultDiversification(C context) {
        this.context = context;
    }

    public abstract RankDoc[] diversify(RankDoc[] docs) throws IOException;

    protected float getFloatVectorComparisonScore(
        VectorSimilarityFunction similarityFunction,
        VectorData thisDocVector,
        VectorData comparisonVector
    ) {
        return similarityFunction.compare(thisDocVector.floatVector(), comparisonVector.floatVector());
    }

    protected float getByteVectorComparisonScore(
        VectorSimilarityFunction similarityFunction,
        VectorData thisDocVector,
        VectorData comparisonVector
    ) {
        return similarityFunction.compare(thisDocVector.byteVector(), comparisonVector.byteVector());
    }
}
