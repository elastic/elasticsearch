/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.diversification.ResultDiversification;
import org.elasticsearch.search.diversification.ResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMRResultDiversification extends ResultDiversification {

    @Override
    public RankDoc[] diversify(RankDoc[] docs, ResultDiversificationContext diversificationContext) throws IOException {
        if (docs == null || docs.length == 0 || ((diversificationContext instanceof MMRResultDiversificationContext) == false)) {
            return docs;
        }

        MMRResultDiversificationContext context = (MMRResultDiversificationContext) diversificationContext;

        Map<Integer, Integer> docIdIndexMapping = new HashMap<>();
        for (int i = 0; i < docs.length; i++) {
            docIdIndexMapping.put(docs[i].doc, i);
        }

        VectorSimilarityFunction similarityFunction = DenseVectorFieldMapper.VectorSimilarity.MAX_INNER_PRODUCT.vectorSimilarityFunction(
            context.getIndexVersion(),
            diversificationContext.getElementType()
        );

        // our chosen DocIDs to keep
        List<Integer> selectedDocIds = new ArrayList<>();

        // always add the highest scoring doc to the list
        int highestScoreDocId = -1;
        float highestScore = Float.MIN_VALUE;
        for (ScoreDoc doc : docs) {
            if (doc.score > highestScore) {
                highestScoreDocId = doc.doc;
                highestScore = doc.score;
            }
        }
        selectedDocIds.add(highestScoreDocId);

        // test the vector to see if we are using floats or bytes
        VectorData firstVec = context.getFieldVector(highestScoreDocId);
        boolean useFloat = firstVec.isFloat();

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(docs, similarityFunction, useFloat, context);

        Map<Integer, Map<Integer, Float>> cachedSimilarities = new HashMap<>();
        int numCandidates = context.getNumCandidates();

        for (int x = 0; x < numCandidates && selectedDocIds.size() < numCandidates && selectedDocIds.size() < docs.length; x++) {
            int thisMaxMMRDocId = -1;
            float thisMaxMMRScore = Float.NEGATIVE_INFINITY;
            for (ScoreDoc doc : docs) {
                int docId = doc.doc;

                if (selectedDocIds.contains(docId)) {
                    continue;
                }

                var thisDocVector = context.getFieldVector(docId);
                if (thisDocVector == null) {
                    continue;
                }

                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docId, new HashMap<>());

                // compute MMR scores for remaining searchHits
                float highestMMRScore = getHighestScoreForSelectedVectors(
                    docId,
                    context,
                    similarityFunction,
                    useFloat,
                    thisDocVector,
                    cachedScoresForDoc
                );

                // compute MMR
                float querySimilarityScore = querySimilarity.getOrDefault(doc.doc, 0.0f);
                float mmr = (context.getLambda() * querySimilarityScore) - ((1 - context.getLambda()) * highestMMRScore);
                if (mmr > thisMaxMMRScore) {
                    thisMaxMMRScore = mmr;
                    thisMaxMMRDocId = docId;
                }

                // cache these scores
                cachedSimilarities.put(docId, cachedScoresForDoc);
            }

            if (thisMaxMMRDocId >= 0) {
                selectedDocIds.add(thisMaxMMRDocId);
            }
        }

        // our return should be only those searchHits that are selected
        // and return in the same order as we got them
        List<Integer> returnDocIndices = new ArrayList<>();
        for (Integer docId : selectedDocIds) {
            returnDocIndices.add(docIdIndexMapping.get(docId));
        }
        returnDocIndices.sort(Integer::compareTo);

        RankDoc[] ret = new RankDoc[returnDocIndices.size()];
        for (int i = 0; i < returnDocIndices.size(); i++) {
            ret[i] = docs[returnDocIndices.get(i)];
        }

        return ret;
    }

    private float getHighestScoreForSelectedVectors(
        int docId,
        MMRResultDiversificationContext context,
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        VectorData thisDocVector,
        Map<Integer, Float> cachedScoresForDoc
    ) {
        float highestScore = Float.MIN_VALUE;
        for (var vec : context.getFieldVectorsEntrySet()) {
            if (vec.getKey().equals(docId)) {
                continue;
            }

            if (cachedScoresForDoc.containsKey(vec.getKey())) {
                float score = cachedScoresForDoc.get(vec.getKey());
                if (score > highestScore) {
                    highestScore = score;
                }
            } else {
                VectorData comparisonVector = vec.getValue();
                float score = getVectorComparisonScore(similarityFunction, useFloat, thisDocVector, comparisonVector);
                cachedScoresForDoc.put(vec.getKey(), score);
                if (score > highestScore) {
                    highestScore = score;
                }
            }
        }
        return highestScore;
    }

    protected Map<Integer, Float> getQuerySimilarityForDocs(
        ScoreDoc[] docs,
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        ResultDiversificationContext context
    ) {
        Map<Integer, Float> querySimilarity = new HashMap<>();
        for (ScoreDoc doc : docs) {
            VectorData vectorData = context.getFieldVector(doc.doc);
            if (vectorData != null) {
                float querySimilarityScore = getVectorComparisonScore(similarityFunction, useFloat, vectorData, context.getQueryVector());
                querySimilarity.put(doc.doc, querySimilarityScore);
            }
        }
        return querySimilarity;
    }
}
