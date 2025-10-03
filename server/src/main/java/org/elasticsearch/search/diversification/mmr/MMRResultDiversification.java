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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.diversification.ResultDiversification;
import org.elasticsearch.search.diversification.ResultDiversificationContext;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMRResultDiversification extends ResultDiversification {

    @Override
    public SearchHits diversify(SearchHits hits, ResultDiversificationContext diversificationContext) throws IOException {
        if (hits == null || ((diversificationContext instanceof MMRResultDiversificationContext) == false)) {
            return hits;
        }

        MMRResultDiversificationContext context = (MMRResultDiversificationContext) diversificationContext;
        SearchHit[] searchHits = hits.getHits();  // NOTE: by reference, not new array

        if (searchHits.length == 0) {
            return hits;
        }

        Map<Integer, Integer> docIdIndexMapping = new HashMap<>();
        Map<Integer, VectorData> fieldVectors = getFieldVectorsForHits(searchHits, context, docIdIndexMapping);

        VectorSimilarityFunction similarityFunction = DenseVectorFieldMapper.VectorSimilarity.MAX_INNER_PRODUCT.vectorSimilarityFunction(
            context.getIndexVersion(),
            diversificationContext.getElementType()
        );

        // our chosen DocIDs to keep
        List<Integer> selectedDocIds = new ArrayList<>();

        // always add the highest scoring doc to the list
        int highestScoreDocId = -1;
        float highestScore = Float.MIN_VALUE;
        for (SearchHit hit : searchHits) {
            if (hit.getScore() > highestScore) {
                highestScoreDocId = hit.docId();
                highestScore = hit.getScore();
            }
        }
        selectedDocIds.add(highestScoreDocId);

        // test the vector to see if we are using floats or bytes
        VectorData firstVec = fieldVectors.get(highestScoreDocId);
        boolean useFloat = firstVec.isFloat();

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(searchHits, fieldVectors, similarityFunction, useFloat, context);

        Map<Integer, Map<Integer, Float>> cachedSimilarities = new HashMap<>();
        int numCandidates = context.getNumCandidates();

        for (int x = 0; x < numCandidates && selectedDocIds.size() < numCandidates && selectedDocIds.size() < searchHits.length; x++) {
            int thisMaxMMRDocId = -1;
            float thisMaxMMRScore = Float.NEGATIVE_INFINITY;
            for (SearchHit thisHit : searchHits) {
                int docId = thisHit.docId();

                if (selectedDocIds.contains(docId)) {
                    continue;
                }

                var thisDocVector = fieldVectors.get(docId);
                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docId, new HashMap<>());

                // compute MMR scores for remaining searchHits
                float highestMMRScore = getHighestScoreForSelectedVectors(
                    fieldVectors,
                    similarityFunction,
                    useFloat,
                    thisDocVector,
                    cachedScoresForDoc
                );

                // compute MMR
                float querySimilarityScore = querySimilarity.getOrDefault(thisHit.docId(), 0.0f);
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
        SearchHit[] ret = new SearchHit[selectedDocIds.size()];
        for (int i = 0; i < selectedDocIds.size(); i++) {
            int scoredDocIndex = docIdIndexMapping.get(selectedDocIds.get(i));
            ret[i] = searchHits[scoredDocIndex];
        }

        return new SearchHits(
            ret,
            hits.getTotalHits(),
            hits.getMaxScore(),
            hits.getSortFields(),
            hits.getCollapseField(),
            hits.getCollapseValues()
        );
    }

    private float getHighestScoreForSelectedVectors(
        Map<Integer, VectorData> selectedVectors,
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        VectorData thisDocVector,
        Map<Integer, Float> cachedScoresForDoc
    ) {
        float highestScore = Float.MIN_VALUE;
        for (var vec : selectedVectors.entrySet()) {
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
        SearchHit[] searchHits,
        Map<Integer, VectorData> fieldVectors,
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        ResultDiversificationContext context
    ) {
        Map<Integer, Float> querySimilarity = new HashMap<>();
        for (SearchHit searchHit : searchHits) {
            int docId = searchHit.docId();
            VectorData vectorData = fieldVectors.get(docId);
            if (vectorData != null) {
                float querySimilarityScore = getVectorComparisonScore(similarityFunction, useFloat, vectorData, context.getQueryVector());
                querySimilarity.put(docId, querySimilarityScore);
            }
        }
        return querySimilarity;
    }
}
