/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query.support;

import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;

/**
 *
 */
public final class QueryParsers {

    private static final ParseField CONSTANT_SCORE = new ParseField("constant_score", "constant_score_auto", "constant_score_filter");
    private static final ParseField SCORING_BOOLEAN = new ParseField("scoring_boolean");
    private static final ParseField CONSTANT_SCORE_BOOLEAN = new ParseField("constant_score_boolean");
    private static final ParseField TOP_TERMS = new ParseField("top_terms_");
    private static final ParseField TOP_TERMS_BOOST = new ParseField("top_terms_boost_");
    private static final ParseField TOP_TERMS_BLENDED_FREQS = new ParseField("top_terms_blended_freqs_");

    private QueryParsers() {

    }

    public static void setRewriteMethod(MultiTermQuery query, @Nullable MultiTermQuery.RewriteMethod rewriteMethod) {
        if (rewriteMethod == null) {
            return;
        }
        query.setRewriteMethod(rewriteMethod);
    }

    public static void setRewriteMethod(MultiTermQuery query, ParseFieldMatcher matcher, @Nullable String rewriteMethod) {
        if (rewriteMethod == null) {
            return;
        }
        query.setRewriteMethod(parseRewriteMethod(matcher, rewriteMethod));
    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(ParseFieldMatcher matcher, @Nullable String rewriteMethod) {
        return parseRewriteMethod(matcher, rewriteMethod, MultiTermQuery.CONSTANT_SCORE_REWRITE);
    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(ParseFieldMatcher matcher, @Nullable String rewriteMethod, @Nullable MultiTermQuery.RewriteMethod defaultRewriteMethod) {
        if (rewriteMethod == null) {
            return defaultRewriteMethod;
        }
        if (matcher.match(rewriteMethod, CONSTANT_SCORE)) {
            return MultiTermQuery.CONSTANT_SCORE_REWRITE;
        }
        if (matcher.match(rewriteMethod, SCORING_BOOLEAN)) {
            return MultiTermQuery.SCORING_BOOLEAN_REWRITE;
        }
        if (matcher.match(rewriteMethod, CONSTANT_SCORE_BOOLEAN)) {
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

            if (matcher.match(rewriteMethodName, TOP_TERMS)) {
                return new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(size);
            }
            if (matcher.match(rewriteMethodName, TOP_TERMS_BOOST)) {
                return new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(size);
            }
            if (matcher.match(rewriteMethodName, TOP_TERMS_BLENDED_FREQS)) {
                return new MultiTermQuery.TopTermsBlendedFreqScoringRewrite(size);
            }
        }

        throw new IllegalArgumentException("Failed to parse rewrite_method [" + rewriteMethod + "]");
    }

}
