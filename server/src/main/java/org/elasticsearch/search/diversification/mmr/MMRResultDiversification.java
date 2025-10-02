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
import org.apache.lucene.search.Explanation;
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
        SearchHit[] docs = hits.getHits();  // NOTE: by reference, not new array

        if (docs.length == 0) {
            return hits;
        }

        Map<Integer, Integer> docIdIndexMapping = new HashMap<>();
        Map<Integer, VectorData> fieldVectors = getFieldVectorsForHits(docs, context, docIdIndexMapping);

        VectorSimilarityFunction similarityFunction = DenseVectorFieldMapper.VectorSimilarity.MAX_INNER_PRODUCT.vectorSimilarityFunction(
            context.getIndexVersion(),
            diversificationContext.getElementType()
        );

        List<Integer> rerankedDocIds = new ArrayList<>();
        Map<Integer, VectorData> selectedVectors = new HashMap<>();

        // always add the highest scoring doc to the list
        int highestDocIdIndex = -1;
        float highestScore = Float.MIN_VALUE;
        for (int i = 0; i < docs.length; i++) {
            if (docs[i].getScore() > highestScore) {
                highestDocIdIndex = i;
                highestScore = docs[i].getScore();
            }
        }
        int firstDocId = docs[highestDocIdIndex].docId();
        rerankedDocIds.add(firstDocId);

        // and add the vector for the first items
        VectorData firstVec = fieldVectors.get(firstDocId);
        selectedVectors.put(firstDocId, firstVec);
        boolean useFloat = firstVec.isFloat();

        // cache the similarity scores for the query vector vs. docs
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(docs, fieldVectors, similarityFunction, useFloat, context);

        Map<Integer, Map<Integer, Float>> cachedSimilarities = new HashMap<>();
        int numCandidates = context.getNumCandidates();

        for (int x = 0; x < numCandidates && rerankedDocIds.size() < numCandidates && rerankedDocIds.size() < docs.length; x++) {
            int thisMaxMMRDocId = -1;
            float thisMaxMMRScore = Float.MIN_VALUE;
            for (SearchHit thisHit : docs) {
                int docId = thisHit.docId();

                if (rerankedDocIds.contains(docId)) {
                    continue;
                }

                var thisDocVector = fieldVectors.get(docId);

                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docId, new HashMap<>());

                // compute MMR scores for remaining docs
                float highestMMRScore = getHighestScoreForSelectedVectors(
                    selectedVectors,
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

            rerankedDocIds.add(thisMaxMMRDocId);
            selectedVectors.put(thisMaxMMRDocId, fieldVectors.get(thisMaxMMRDocId));
        }

        // our return should be only those docs that are selected
        SearchHit[] ret = new SearchHit[rerankedDocIds.size()];
        for (int i = 0; i < rerankedDocIds.size(); i++) {
            int scoredDocIndex = docIdIndexMapping.get(rerankedDocIds.get(i));
            ret[i] = docs[scoredDocIndex];
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

    @Override
    public Explanation explain(int topLevelDocId, ResultDiversificationContext diversificationContext, Explanation sourceExplanation)
        throws IOException {
        // TODO
        return null;
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
        SearchHit[] docs,
        Map<Integer, VectorData> fieldVectors,
        VectorSimilarityFunction similarityFunction,
        boolean useFloat,
        ResultDiversificationContext context
    ) {
        Map<Integer, Float> querySimilarity = new HashMap<>();
        for (int i = 0; i < docs.length; i++) {
            int docId = docs[i].docId();
            VectorData vectorData = fieldVectors.get(docId);
            if (vectorData != null) {
                float querySimilarityScore = getVectorComparisonScore(similarityFunction, useFloat, vectorData, context.getQueryVector());
                querySimilarity.put(docId, querySimilarityScore);
            }
        }
        return querySimilarity;
    }
}
