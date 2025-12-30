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
import org.elasticsearch.core.Tuple;
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

        // test the vector to see if we are using floats or bytes
        // TODO - verify a vector actually exists here...
        List<Tuple<Integer, VectorData>> firstVecData = context.getFieldVectorData(docs[0].rank);
        boolean useFloat = firstVecData.getFirst().v2().isFloat();

        // cache the similarity scores for the query vector vs. searchHits
        Map<Integer, Float> querySimilarity = getQuerySimilarityForDocs(docs, useFloat, context);

        // always add the highest relevant doc to the list
        selectedDocRanks.add(getHighestRelevantDocRank(docs, querySimilarity));

        Map<Integer, Map<Integer, Float[]>> cachedSimilarities = new HashMap<>();
        int topDocsSize = context.getSize();

        for (int x = 0; x < topDocsSize && selectedDocRanks.size() < topDocsSize && selectedDocRanks.size() < docs.length; x++) {
            int thisMaxMMRDocRank = -1;
            float thisMaxMMRScore = Float.NEGATIVE_INFINITY;
            for (RankDoc doc : docs) {
                int docRank = doc.rank;

                if (selectedDocRanks.contains(docRank)) {
                    continue;
                }

                var fieldVectorData = context.getFieldVectorData(docRank);
                if (fieldVectorData == null || fieldVectorData.isEmpty()) {
                    continue;
                }

                var cachedScoresForDoc = cachedSimilarities.getOrDefault(docRank, new HashMap<>());

                // compute MMR scores for this doc
                float highestMMRScore = getBestMmrScoreForDoc(context, useFloat, selectedDocRanks, fieldVectorData, cachedScoresForDoc);

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

    private float getBestMmrScoreForDoc(
        MMRResultDiversificationContext context,
        boolean useFloat,
        List<Integer> selectedDocRanks,
        List<Tuple<Integer, VectorData>> fieldVectorData,
        Map<Integer, Float[]> cachedScoresForDoc
    ) {
        var thisDocVectorsSize = fieldVectorData.size();
        if (thisDocVectorsSize == 0) {
            return Float.MIN_VALUE;
        }

        float highestMMRScore = Float.MIN_VALUE;
        for (Integer docRank : selectedDocRanks) {
            var compareDocVectors = context.getFieldVectorData(docRank);
            if (compareDocVectors == null || compareDocVectors.isEmpty()) {
                continue;
            }

            int compareVectorSize = compareDocVectors.size();

            var cachedScores = cachedScoresForDoc.getOrDefault(docRank, new Float[thisDocVectorsSize * compareVectorSize]);
            boolean oneScoreWasSet = false;
            for (int r = 0; r < thisDocVectorsSize; r++) {
                for (int c = 0; c < compareVectorSize; c++) {
                    int position = (r * compareVectorSize) + c;
                    if (cachedScores[position] != null) {
                        if (highestMMRScore < cachedScores[position]) {
                            highestMMRScore = cachedScores[position];
                        }
                    } else {
                        float score = useFloat
                            ? getFloatVectorComparisonScore(similarityFunction, fieldVectorData.get(r).v2(), compareDocVectors.get(c).v2())
                            : getByteVectorComparisonScore(similarityFunction, fieldVectorData.get(r).v2(), compareDocVectors.get(c).v2());
                        cachedScores[position] = score;
                        if (highestMMRScore < score) {
                            highestMMRScore = score;
                        }
                        oneScoreWasSet = true;
                    }
                }
            }

            if (oneScoreWasSet) {
                cachedScoresForDoc.put(docRank, cachedScores);
            }
        }

        return highestMMRScore;
    }

    private Integer getHighestRelevantDocRank(RankDoc[] docs, Map<Integer, Float> querySimilarity) {
        Map.Entry<Integer, Float> highestRelevantDoc = querySimilarity.entrySet()
            .stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElse(null);

        if (highestRelevantDoc != null) {
            return highestRelevantDoc.getKey();
        }

        RankDoc highestScoreDoc = Arrays.stream(docs).max(Comparator.comparingDouble(doc -> doc.score)).orElse(docs[0]);
        return highestScoreDoc.rank;
    }

    protected Map<Integer, Float> getQuerySimilarityForDocs(RankDoc[] docs, boolean useFloat, ResultDiversificationContext context) {
        Map<Integer, Float> querySimilarity = new HashMap<>();

        VectorData queryVector = context.getQueryVector();
        if (queryVector == null) {
            return querySimilarity;
        }

        for (RankDoc doc : docs) {
            var fieldVectorData = context.getFieldVectorData(doc.rank);
            if (fieldVectorData == null || fieldVectorData.isEmpty()) {
                return querySimilarity;
            }

            // find highest rated similarity
            float highestScore = Float.MIN_VALUE;
            for (Tuple<Integer, VectorData> vec : fieldVectorData) {
                float querySimilarityScore = useFloat
                    ? getFloatVectorComparisonScore(similarityFunction, vec.v2(), queryVector)
                    : getByteVectorComparisonScore(similarityFunction, vec.v2(), queryVector);
                if (querySimilarityScore > highestScore) {
                    highestScore = querySimilarityScore;
                }
            }
            querySimilarity.put(doc.rank, highestScore);
        }
        return querySimilarity;
    }
}
