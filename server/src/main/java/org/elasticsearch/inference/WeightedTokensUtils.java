/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;

import java.io.IOException;
import java.util.List;

public final class WeightedTokensUtils {

    private WeightedTokensUtils() {}

    public static Query queryBuilderWithAllTokens(
        String fieldName,
        List<WeightedToken> tokens,
        MappedFieldType ft,
        SearchExecutionContext context
    ) {
        var qb = new BooleanQuery.Builder();

        for (var token : tokens) {
            qb.add(new BoostQuery(ft.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
        }
        return new SparseVectorQueryWrapper(fieldName, qb.setMinimumNumberShouldMatch(1).build());
    }

    public static Query queryBuilderWithPrunedTokens(
        String fieldName,
        TokenPruningConfig tokenPruningConfig,
        List<WeightedToken> tokens,
        MappedFieldType ft,
        SearchExecutionContext context
    ) throws IOException {
        var qb = new BooleanQuery.Builder();
        int fieldDocCount = context.getIndexReader().getDocCount(fieldName);
        float bestWeight = tokens.stream().map(WeightedToken::weight).reduce(0f, Math::max);
        float averageTokenFreqRatio = getAverageTokenFreqRatio(fieldName, context.getIndexReader(), fieldDocCount);
        if (averageTokenFreqRatio == 0) {
            return new MatchNoDocsQuery("query is against an empty field");
        }

        for (var token : tokens) {
            boolean keep = shouldKeepToken(
                fieldName,
                tokenPruningConfig,
                context.getIndexReader(),
                token,
                fieldDocCount,
                averageTokenFreqRatio,
                bestWeight
            );
            keep ^= tokenPruningConfig != null && tokenPruningConfig.isOnlyScorePrunedTokens();
            if (keep) {
                qb.add(new BoostQuery(ft.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
            }
        }

        return new SparseVectorQueryWrapper(fieldName, qb.setMinimumNumberShouldMatch(1).build());
    }

    /**
     * We calculate the maximum number of unique tokens for any shard of data. The maximum is used to compute
     * average token frequency since we don't have a unique inter-segment token count.
     * Once we have the maximum number of unique tokens, we use the total count of tokens in the index to calculate
     * the average frequency ratio.
     *
     * @param reader
     * @param fieldDocCount
     * @return float
     * @throws IOException
     */
    private static float getAverageTokenFreqRatio(String fieldName, IndexReader reader, int fieldDocCount) throws IOException {
        int numUniqueTokens = 0;
        for (var leaf : reader.getContext().leaves()) {
            var terms = leaf.reader().terms(fieldName);
            if (terms != null) {
                numUniqueTokens = (int) Math.max(terms.size(), numUniqueTokens);
            }
        }
        if (numUniqueTokens == 0) {
            return 0;
        }
        return (float) reader.getSumDocFreq(fieldName) / fieldDocCount / numUniqueTokens;
    }

    /**
     * Returns true if the token should be queried based on the {@code tokensFreqRatioThreshold} and {@code tokensWeightThreshold}
     * set on the query.
     */
    private static boolean shouldKeepToken(
        String fieldName,
        TokenPruningConfig tokenPruningConfig,
        IndexReader reader,
        WeightedToken token,
        int fieldDocCount,
        float averageTokenFreqRatio,
        float bestWeight
    ) throws IOException {
        if (tokenPruningConfig == null) {
            return true;
        }
        int docFreq = reader.docFreq(new Term(fieldName, token.token()));
        if (docFreq == 0) {
            return false;
        }
        float tokenFreqRatio = (float) docFreq / fieldDocCount;
        return tokenFreqRatio < tokenPruningConfig.getTokensFreqRatioThreshold() * averageTokenFreqRatio
            || token.weight() > tokenPruningConfig.getTokensWeightThreshold() * bestWeight;
    }

}
