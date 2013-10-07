/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.regex.Pattern;

/**
 *
 */
public class Queries {

    /* In general we should never us a static query instance and share it.
     * In this case the instance is immutable so that's ok.*/
    public final static Query NO_MATCH_QUERY = MatchNoDocsQuery.INSTANCE;

    /**
     * A match all docs filter. Note, requires no caching!.
     */
    public final static Filter MATCH_ALL_FILTER = new MatchAllDocsFilter();
    public final static Filter MATCH_NO_FILTER = new MatchNoDocsFilter();

    public static Query newMatchAllQuery() {
        // We don't use MatchAllDocsQuery, its slower than the one below ... (much slower)
        // NEVER cache this XConstantScore Query it's not immutable and based on #3521
        // some code might set a boost on this query.
        return new XConstantScoreQuery(MATCH_ALL_FILTER);
    }

    /**
     * Optimizes the given query and returns the optimized version of it.
     */
    public static Query optimizeQuery(Query q) {
        if (q instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) q;
            BooleanClause[] clauses = booleanQuery.getClauses();
            if (clauses.length == 1) {
                BooleanClause clause = clauses[0];
                if (clause.getOccur() == BooleanClause.Occur.MUST) {
                    Query query = clause.getQuery();
                    query.setBoost(booleanQuery.getBoost() * query.getBoost());
                    return optimizeQuery(query);
                }
                if (clause.getOccur() == BooleanClause.Occur.SHOULD && booleanQuery.getMinimumNumberShouldMatch() > 0) {
                    Query query = clause.getQuery();
                    query.setBoost(booleanQuery.getBoost() * query.getBoost());
                    return optimizeQuery(query);
                }
            }
        }
        return q;
    }

    public static boolean isNegativeQuery(Query q) {
        if (!(q instanceof BooleanQuery)) {
            return false;
        }
        List<BooleanClause> clauses = ((BooleanQuery) q).clauses();
        if (clauses.isEmpty()) {
            return false;
        }
        for (BooleanClause clause : clauses) {
            if (!clause.isProhibited()) return false;
        }
        return true;
    }

    public static Query fixNegativeQueryIfNeeded(Query q) {
        if (isNegativeQuery(q)) {
            BooleanQuery newBq = (BooleanQuery) q.clone();
            newBq.add(newMatchAllQuery(), BooleanClause.Occur.MUST);
            return newBq;
        }
        return q;
    }

    public static boolean isConstantMatchAllQuery(Query query) {
        if (query instanceof XConstantScoreQuery) {
            XConstantScoreQuery scoreQuery = (XConstantScoreQuery) query;
            if (scoreQuery.getFilter() instanceof MatchAllDocsFilter) {
                return true;
            }
        }
        return false;
    }

    public static void applyMinimumShouldMatch(BooleanQuery query, @Nullable String minimumShouldMatch) {
        if (minimumShouldMatch == null) {
            return;
        }
        int optionalClauses = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                optionalClauses++;
            }
        }

        int msm = calculateMinShouldMatch(optionalClauses, minimumShouldMatch);
        if (0 < msm) {
            query.setMinimumNumberShouldMatch(msm);
        }
    }

    private static Pattern spaceAroundLessThanPattern = Pattern.compile("(\\s+<\\s*)|(\\s*<\\s+)");
    private static Pattern spacePattern = Pattern.compile(" ");
    private static Pattern lessThanPattern = Pattern.compile("<");

    public static int calculateMinShouldMatch(int optionalClauseCount, String spec) {
        int result = optionalClauseCount;
        spec = spec.trim();

        if (-1 < spec.indexOf("<")) {
            /* we have conditional spec(s) */
            spec = spaceAroundLessThanPattern.matcher(spec).replaceAll("<");
            for (String s : spacePattern.split(spec)) {
                String[] parts = lessThanPattern.split(s, 0);
                int upperBound = Integer.parseInt(parts[0]);
                if (optionalClauseCount <= upperBound) {
                    return result;
                } else {
                    result = calculateMinShouldMatch
                            (optionalClauseCount, parts[1]);
                }
            }
            return result;
        }

        /* otherwise, simple expresion */

        if (-1 < spec.indexOf('%')) {
            /* percentage - assume the % was the last char.  If not, let Integer.parseInt fail. */
            spec = spec.substring(0, spec.length() - 1);
            int percent = Integer.parseInt(spec);
            float calc = (result * percent) * (1 / 100f);
            result = calc < 0 ? result + (int) calc : (int) calc;
        } else {
            int calc = Integer.parseInt(spec);
            result = calc < 0 ? result + calc : calc;
        }

        return (optionalClauseCount < result ?
                optionalClauseCount : (result < 0 ? 0 : result));

    }
}
