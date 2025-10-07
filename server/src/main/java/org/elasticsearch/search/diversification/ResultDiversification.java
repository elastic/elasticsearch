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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Base interface for result diversification.
 */
public abstract class ResultDiversification {

    public abstract TopDocs diversify(TopDocs hits, ResultDiversificationContext diversificationContext) throws IOException;

    protected Map<Integer, VectorData> getFieldVectorsForHits(
        ScoreDoc[] docs,
        ResultDiversificationContext context,
        Map<Integer, Integer> docIdIndexMapping
    ) {
        Map<Integer, VectorData> fieldVectors = new HashMap<>();
        for (int i = 0; i < docs.length; i++) {
            ScoreDoc hit = docs[i];
            int docId = hit.doc;
            docIdIndexMapping.put(docId, i);
            // hit.Object collapseValue = hit.field(context.getField()).getValue();
            // if (collapseValue instanceof float[] vecData) {
            // fieldVectors.put(docId, new VectorData(vecData));
            // } else if (collapseValue instanceof byte[] byteVecData) {
            // fieldVectors.put(docId, new VectorData(byteVecData));
            // }
        }
        return fieldVectors;
    }

    protected float getVectorComparisonScore(
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        VectorData thisDocVector,
        VectorData comparisonVector
    ) {
        return useFloat
            ? similarityFunction.compare(thisDocVector.floatVector(), comparisonVector.floatVector())
            : similarityFunction.compare(thisDocVector.byteVector(), comparisonVector.byteVector());
    }
}
