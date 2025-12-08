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
import org.elasticsearch.search.diversification.ResultDiversification;
import org.elasticsearch.search.diversification.ResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMRResultDiversification extends ResultDiversification<MMRResultDiversificationContext> {

    private static final VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

    public MMRResultDiversification(MMRResultDiversificationContext context) {
        super(context);
    }

    @Override
    public RankDoc[] diversify(RankDoc[] docs) throws IOException {
        if (docs == null || docs.length == 0) {
            return docs;
        }

        Map<Integer, Integer> docIdIndexMapping = new HashMap<>();
        for (int i = 0; i < docs.length; i++) {
            docIdIndexMapping.put(docs[i].rank, i);
        }

        // our chosen DocIDs to keep
        List<Integer> selectedDocRanks = new ArrayList<>();

        // always add the highest scoring doc to the list
        RankDoc highestScoreDoc = Arrays.stream(docs).max(Comparator.comparingDouble(doc -> doc.score)).orElse(docs[0]);
        int highestScoreDocRank = highestScoreDoc.rank;
        selectedDocRanks.add(highestScoreDocRank);

        // test the vector to see if we are using floats or bytes
        VectorData firstVec = context.getFieldVector(highestScoreDocRank);
        boolean useFloat = firstVec.isFloat();

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(docs, useFloat, context);

        Map<Integer, Map<Integer, Float>> cachedSimilarities = new HashMap<>();
        int topDocsSize = context.getSize();

        for (int x = 0; x < topDocsSize && selectedDocRanks.size() < topDocsSize && selectedDocRanks.size() < docs.length; x++) {
            int thisMaxMMRDocRank = -1;
            float thisMaxMMRScore = Float.NEGATIVE_INFINITY;
            for (RankDoc doc : docs) {
                int docRank = doc.rank;

                if (selectedDocRanks.contains(docRank)) {
                    continue;
                }

                var thisDocVector = context.getFieldVector(docRank);
                if (thisDocVector == null) {
                    continue;
                }

                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docRank, new HashMap<>());

                // compute MMR scores for remaining searchHits
                float highestMMRScore = getHighestScoreForSelectedVectors(docRank, context, useFloat, thisDocVector, cachedScoresForDoc);

                // compute MMR
                float querySimilarityScore = querySimilarity.getOrDefault(doc.rank, 0.0f);
                float mmr = (context.getLambda() * querySimilarityScore) - ((1 - context.getLambda()) * highestMMRScore);
                if (mmr > thisMaxMMRScore) {
                    thisMaxMMRScore = mmr;
                    thisMaxMMRDocRank = docRank;
                }

                // cache these scores
                cachedSimilarities.put(docRank, cachedScoresForDoc);
            }

            if (thisMaxMMRDocRank >= 0) {
                selectedDocRanks.add(thisMaxMMRDocRank);
            }
        }

        // our return should be only those searchHits that are selected
        // and return in the same order as we got them
        List<Integer> returnDocIndices = new ArrayList<>();
        for (Integer docRank : selectedDocRanks) {
            returnDocIndices.add(docIdIndexMapping.get(docRank));
        }
        returnDocIndices.sort(Integer::compareTo);

        RankDoc[] ret = new RankDoc[returnDocIndices.size()];
        for (int i = 0; i < returnDocIndices.size(); i++) {
            ret[i] = docs[returnDocIndices.get(i)];
        }

        return ret;
    }

    private float getHighestScoreForSelectedVectors(
        int docRank,
        MMRResultDiversificationContext context,
        boolean useFloat,
        VectorData thisDocVector,
        Map<Integer, Float> cachedScoresForDoc
    ) {
        float highestScore = Float.MIN_VALUE;
        for (var vec : context.getFieldVectorsEntrySet()) {
            if (vec.getKey().equals(docRank)) {
                continue;
            }

            if (cachedScoresForDoc.containsKey(vec.getKey())) {
                float score = cachedScoresForDoc.get(vec.getKey());
                if (score > highestScore) {
                    highestScore = score;
                }
            } else {
                VectorData comparisonVector = vec.getValue();
                float score = useFloat
                    ? getFloatVectorComparisonScore(similarityFunction, thisDocVector, comparisonVector)
                    : getByteVectorComparisonScore(similarityFunction, thisDocVector, comparisonVector);
                cachedScoresForDoc.put(vec.getKey(), score);
                if (score > highestScore) {
                    highestScore = score;
                }
            }
        }
        return highestScore;
    }

    protected Map<Integer, Float> getQuerySimilarityForDocs(RankDoc[] docs, boolean useFloat, ResultDiversificationContext context) {
        Map<Integer, Float> querySimilarity = new HashMap<>();

        VectorData queryVector = context.getQueryVector();
        if (queryVector == null) {
            return querySimilarity;
        }

        for (RankDoc doc : docs) {
            VectorData vectorData = context.getFieldVector(doc.rank);
            if (vectorData != null) {
                float querySimilarityScore = useFloat
                    ? getFloatVectorComparisonScore(similarityFunction, vectorData, queryVector)
                    : getByteVectorComparisonScore(similarityFunction, vectorData, queryVector);
                querySimilarity.put(doc.rank, querySimilarityScore);
            }
        }
        return querySimilarity;
    }
}
