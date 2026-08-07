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

    public static float getVectorComparisonScore(
        VectorSimilarityFunction similarityFunction,
        VectorData thisDocVector,
        VectorData comparisonVector
    ) {
        if (thisDocVector.isStringVector() || comparisonVector.isStringVector()) {
            throw new IllegalStateException("string vectors should be decoded before comparison");
        }

        return thisDocVector.isFloat()
            ? similarityFunction.compare(thisDocVector.asFloatVector(), comparisonVector.asFloatVector())
            : similarityFunction.compare(thisDocVector.asByteVector(), comparisonVector.asByteVector());
    }
}
