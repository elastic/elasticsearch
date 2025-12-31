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
import org.elasticsearch.core.Nullable;
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

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, List<Float>> querySimilarity = getQuerySimilarityForDocs(docs, context);

        // always add the highest relevant doc to the list
        selectedDocRanks.add(getHighestRelevantDocRank(docs, querySimilarity));

        Map<Integer, Map<Integer, List<Float>>> cachedSimilarities = new HashMap<>();
        int topDocsSize = context.getSize();

        for (int x = 0; x < topDocsSize && selectedDocRanks.size() < topDocsSize && selectedDocRanks.size() < docs.length; x++) {
            int thisMaxMMRDocRank = -1;
            float thisMaxMMRScore = Float.NEGATIVE_INFINITY;
            for (RankDoc doc : docs) {
                int docRank = doc.rank;

                if (selectedDocRanks.contains(docRank)) {
                    continue;
                }

                List<VectorData> fieldVectorData = context.getFieldVectorData(docRank);
                if (fieldVectorData == null || fieldVectorData.isEmpty()) {
                    continue;
                }

                var cachedScoresForDocsComparison = cachedSimilarities.getOrDefault(docRank, new HashMap<>());

                // compute MMR
                float docMMRScore = getMMRScoreForDoc(
                    context,
                    selectedDocRanks,
                    fieldVectorData,
                    cachedScoresForDocsComparison,
                    querySimilarity.getOrDefault(docRank, null)
                );

                if (docMMRScore > thisMaxMMRScore) {
                    thisMaxMMRScore = docMMRScore;
                    thisMaxMMRDocRank = docRank;
                }

                // cache these scores
                cachedSimilarities.put(docRank, cachedScoresForDocsComparison);
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

    private float getMMRScoreForDoc(
        MMRResultDiversificationContext context,
        List<Integer> selectedDocRanks,
        List<VectorData> fieldVectorData,
        Map<Integer, List<Float>> cachedScoresForDocsComparison,
        @Nullable List<Float> querySimilarity
    ) {
        var thisDocVectorsSize = fieldVectorData.size();
        if (thisDocVectorsSize == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        float highestSimilarityScore = Float.NEGATIVE_INFINITY;
        int chunkWithHighestSimilarityScore = 0;
        for (Integer docRank : selectedDocRanks) {
            var compareDocVectors = context.getFieldVectorData(docRank);
            if (compareDocVectors == null || compareDocVectors.isEmpty()) {
                // nothing to compare
                continue;
            }

            int compareVectorSize = compareDocVectors.size();
            var cachedChunkScores = cachedScoresForDocsComparison.getOrDefault(docRank, null);
            if (cachedChunkScores == null) {
                cachedChunkScores = new ArrayList<>(thisDocVectorsSize);

                for (int chunk = 0; chunk < thisDocVectorsSize; chunk++) {
                    float maxChunkScore = Float.NEGATIVE_INFINITY;
                    for (int compareChunk = 0; compareChunk < compareVectorSize; compareChunk++) {
                        float score = getVectorComparisonScore(
                            similarityFunction,
                            fieldVectorData.get(chunk),
                            compareDocVectors.get(compareChunk)
                        );
                        if (score > maxChunkScore) {
                            maxChunkScore = score;
                        }
                    }
                    cachedChunkScores.add(maxChunkScore);
                }

                cachedScoresForDocsComparison.put(docRank, cachedChunkScores);
            }

            int chunkWithHighestSimilarity = 0;
            for (int chunk = 0; chunk < thisDocVectorsSize; chunk++) {
                chunkWithHighestSimilarity = cachedChunkScores.get(chunk) > chunkWithHighestSimilarity ? chunk : chunkWithHighestSimilarity;
            }

            if (cachedChunkScores.get(chunkWithHighestSimilarity) > highestSimilarityScore) {
                highestSimilarityScore = cachedChunkScores.get(chunkWithHighestSimilarity);
                chunkWithHighestSimilarityScore = chunkWithHighestSimilarity;
            }
        }

        if (querySimilarity == null) {
            return 0.0f - (1.0f - context.getLambda()) * highestSimilarityScore;
        }

        return (context.getLambda() * querySimilarity.get(chunkWithHighestSimilarityScore)) - ((1.0f - context.getLambda())
            * highestSimilarityScore);
    }

    private Integer getHighestRelevantDocRank(RankDoc[] docs, Map<Integer, List<Float>> querySimilarity) {
        if (querySimilarity == null || querySimilarity.isEmpty()) {
            RankDoc highestScoreDoc = Arrays.stream(docs).max(Comparator.comparingDouble(doc -> doc.score)).orElse(docs[0]);
            return highestScoreDoc.rank;
        }

        Integer highestDocRank = docs[0].rank;
        Float highestDocScore = Float.NEGATIVE_INFINITY;
        for (RankDoc doc : docs) {
            List<Float> docQuerySimilarity = querySimilarity.getOrDefault(doc.rank, null);
            if (docQuerySimilarity == null) {
                continue;
            }

            for (Float querySimilarityScore : docQuerySimilarity) {
                if (querySimilarityScore > highestDocScore) {
                    highestDocRank = doc.rank;
                    highestDocScore = querySimilarityScore;
                }
            }
        }

        return highestDocRank;
    }

    protected Map<Integer, List<Float>> getQuerySimilarityForDocs(RankDoc[] docs, ResultDiversificationContext context) {
        Map<Integer, List<Float>> querySimilarity = new HashMap<>();

        VectorData queryVector = context.getQueryVector();
        if (queryVector == null) {
            return querySimilarity;
        }

        for (RankDoc doc : docs) {
            var fieldVectorData = context.getFieldVectorData(doc.rank);
            if (fieldVectorData == null || fieldVectorData.isEmpty()) {
                continue;
            }

            List<Float> docQueryVectorSimilarities = new ArrayList<>();
            for (VectorData vec : fieldVectorData) {
                docQueryVectorSimilarities.add(getVectorComparisonScore(similarityFunction, vec, queryVector));
            }
            querySimilarity.put(doc.rank, docQueryVectorSimilarities);
        }
        return querySimilarity;
    }
}
