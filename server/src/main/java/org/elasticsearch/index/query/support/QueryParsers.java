/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ParseField;

public final class QueryParsers {

    public static final ParseField CONSTANT_SCORE = new ParseField("constant_score");
    public static final ParseField SCORING_BOOLEAN = new ParseField("scoring_boolean");
    public static final ParseField CONSTANT_SCORE_BOOLEAN = new ParseField("constant_score_boolean");
    public static final ParseField TOP_TERMS = new ParseField("top_terms_");
    public static final ParseField TOP_TERMS_BOOST = new ParseField("top_terms_boost_");
    public static final ParseField TOP_TERMS_BLENDED_FREQS = new ParseField("top_terms_blended_freqs_");

    private QueryParsers() {

    }

    public static void setRewriteMethod(MultiTermQuery query, @Nullable MultiTermQuery.RewriteMethod rewriteMethod) {
        if (rewriteMethod == null) {
            return;
        }
        query.setRewriteMethod(rewriteMethod);
    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(@Nullable String rewriteMethod, DeprecationHandler deprecationHandler) {
        return parseRewriteMethod(rewriteMethod, MultiTermQuery.CONSTANT_SCORE_REWRITE, deprecationHandler);
    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(
        @Nullable String rewriteMethod,
        @Nullable MultiTermQuery.RewriteMethod defaultRewriteMethod,
        DeprecationHandler deprecationHandler
    ) {
        if (rewriteMethod == null) {
            return defaultRewriteMethod;
        }
        if (CONSTANT_SCORE.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.CONSTANT_SCORE_REWRITE;
        }
        if (SCORING_BOOLEAN.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.SCORING_BOOLEAN_REWRITE;
        }
        if (CONSTANT_SCORE_BOOLEAN.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE;
        }

        int firstDigit = -1;
        for (int i = 0; i < rewriteMethod.length(); ++i) {
            if (Character.isDigit(rewriteMethod.charAt(i))) {
                firstDigit = i;
                break;
            }
        }

        if (firstDigit >= 0) {
            final int size = Integer.parseInt(rewriteMethod.substring(firstDigit));
            String rewriteMethodName = rewriteMethod.substring(0, firstDigit);

            if (TOP_TERMS.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(size);
            }
            if (TOP_TERMS_BOOST.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(size);
            }
            if (TOP_TERMS_BLENDED_FREQS.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsBlendedFreqScoringRewrite(size);
            }
        }

        throw new IllegalArgumentException("Failed to parse rewrite_method [" + rewriteMethod + "]");
    }
}
