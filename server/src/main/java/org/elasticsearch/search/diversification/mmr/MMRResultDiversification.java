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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;

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
        float[] querySimilarities = getQuerySimilarityForDocs(docs);
        OptionalInt bestDocIdx = IntStream.range(0, querySimilarities.length)
            .filter(i -> context.getFieldVector(i + 1) != null)
            .reduce((a, b) -> querySimilarities[a] >= querySimilarities[b] ? a : b);

        if (bestDocIdx.isEmpty()) {
            return new RankDoc[0];
        }

        int prevSelectedDocRank = 1 + bestDocIdx.getAsInt();

        selectedDocRanks.add(prevSelectedDocRank);
        int topDocsSize = context.getSize();

        float[] maxSimilarityToSelected = new float[docs.length];
        Arrays.fill(maxSimilarityToSelected, Float.NEGATIVE_INFINITY);

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

                float similarityToLastSelected = getVectorComparisonScore(
                    similarityFunction,
                    thisDocVector,
                    context.getFieldVector(prevSelectedDocRank)
                );
                maxSimilarityToSelected[docRank - 1] = Float.max(similarityToLastSelected, maxSimilarityToSelected[docRank - 1]);

                // compute MMR
                float mmr = context.getLambda() * querySimilarities[docRank - 1] - (1 - context.getLambda())
                    * maxSimilarityToSelected[docRank - 1];

                if (mmr > thisMaxMMRScore) {
                    thisMaxMMRScore = mmr;
                    thisMaxMMRDocRank = docRank;
                }
            }

            if (thisMaxMMRDocRank >= 0) {
                selectedDocRanks.add(thisMaxMMRDocRank);
                prevSelectedDocRank = thisMaxMMRDocRank;
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

    protected float[] getQuerySimilarityForDocs(RankDoc[] docs) {
        float[] querySimilarity = new float[docs.length];
        Arrays.fill(querySimilarity, 0.0f);
        VectorData queryVector = context.getQueryVector();
        if (queryVector == null) {
            return querySimilarity;
        }

        for (RankDoc doc : docs) {
            VectorData vectorData = context.getFieldVector(doc.rank);
            if (vectorData != null) {
                querySimilarity[doc.rank - 1] = getVectorComparisonScore(similarityFunction, vectorData, queryVector);
            }
        }
        return querySimilarity;
    }
}
