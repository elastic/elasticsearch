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
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

        // keep the original ranking so we can output in the same order
        Map<Integer, Integer> docIdIndexMapping = new LinkedHashMap<>();
        for (int i = 0; i < docs.length; i++) {
            docIdIndexMapping.put(docs[i].rank, i);
        }

        // our chosen DocIDs to keep
        List<Integer> selectedDocRanks = new ArrayList<>();

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(docs);

        // always add the highest relevant doc to the list
        selectedDocRanks.add(getHighestRelevantDocRank(docs, querySimilarity));

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
                if (thisDocVector == null || thisDocVector.size() == 0) {
                    continue;
                }

                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docRank, new LinkedHashMap<>());

                // compute MMR scores for remaining searchHits
                float highestSimilarityScoreToSelected = getHighestSimilarityScoreToSelectedVectors(
                    selectedDocRanks,
                    thisDocVector,
                    cachedScoresForDoc
                );

                // compute MMR
                float querySimilarityScore = querySimilarity.getOrDefault(doc.rank, 0.0f);
                float mmr = (context.getLambda() * querySimilarityScore) - ((1 - context.getLambda()) * highestSimilarityScoreToSelected);
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

    private Integer getHighestRelevantDocRank(RankDoc[] docs, Map<Integer, Float> querySimilarity) {
        Map.Entry<Integer, Float> highestRelevantDoc = querySimilarity.entrySet()
            .stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElse(null);

        if (highestRelevantDoc != null) {
            return highestRelevantDoc.getKey();
        }

        // no query vectors? Just use the first document in the order
        return docs[0].rank;
    }

    private float getHighestSimilarityScoreToSelectedVectors(
        List<Integer> selectedDocRanks,
        VectorData thisDocVector,
        Map<Integer, Float> cachedScoresForDoc
    ) {
        float highestScore = Float.NEGATIVE_INFINITY;
        for (Integer compareToDocRank : selectedDocRanks) {
            Float similarityScore = cachedScoresForDoc.getOrDefault(compareToDocRank, null);
            if (similarityScore == null) {
                VectorData comparisonVector = context.getFieldVector(compareToDocRank);
                if (comparisonVector != null) {
                    if (comparisonVector.size() == 0) {
                        cachedScoresForDoc.put(compareToDocRank, Float.NEGATIVE_INFINITY);
                        continue;
                    }

                    similarityScore = getVectorComparisonScore(similarityFunction, thisDocVector, comparisonVector);
                    cachedScoresForDoc.put(compareToDocRank, similarityScore);
                }
            }
            if (similarityScore != null && similarityScore > highestScore) {
                highestScore = similarityScore;
            }
        }
        return highestScore == Float.NEGATIVE_INFINITY ? 0.0f : highestScore;
    }

    protected Map<Integer, Float> getQuerySimilarityForDocs(RankDoc[] docs) {
        Map<Integer, Float> querySimilarity = new HashMap<>();

        VectorData queryVector = context.getQueryVector();
        if (queryVector == null) {
            return querySimilarity;
        }

        for (RankDoc doc : docs) {
            VectorData vectorData = context.getFieldVector(doc.rank);
            if (vectorData != null) {
                float querySimilarityScore = getVectorComparisonScore(similarityFunction, vectorData, queryVector);
                querySimilarity.put(doc.rank, querySimilarityScore);
            }
        }
        return querySimilarity;
    }
}
