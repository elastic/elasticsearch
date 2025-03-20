/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.io.IOException;
import java.util.List;

public final class WeightedTokensUtils {

    private WeightedTokensUtils() {}

    public static QueryBuilder queryBuilderWithAllTokens(
        String fieldName,
        List<WeightedToken> tokens,
        MappedFieldType ft,
        SearchExecutionContext context
    ) {
        var boolQuery = new BoolQueryBuilder();

        for (var token : tokens) {
            var termQuery = new TermQueryBuilder(fieldName, token.token());
            boolQuery.should(termQuery.boost(token.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    public static QueryBuilder queryBuilderWithPrunedTokens(
        String fieldName,
        TokenPruningConfig tokenPruningConfig,
        List<WeightedToken> tokens,
        MappedFieldType ft,
        SearchExecutionContext context
    ) throws IOException {
        var boolQuery = new BoolQueryBuilder();
        int fieldDocCount = context.getIndexReader().getDocCount(fieldName);
        float bestWeight = tokens.stream().map(WeightedToken::weight).reduce(0f, Math::max);
        float averageTokenFreqRatio = getAverageTokenFreqRatio(fieldName, context.getIndexReader(), fieldDocCount);
        if (averageTokenFreqRatio == 0) {
            return new BoolQueryBuilder().must(new TermQueryBuilder(fieldName, ""));
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
                var termQuery = new TermQueryBuilder(fieldName, token.token());
                boolQuery.should(termQuery.boost(token.weight()));
            }
        }

        boolQuery.minimumShouldMatch(1);
        return boolQuery;
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
