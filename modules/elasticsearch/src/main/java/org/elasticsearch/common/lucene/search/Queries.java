/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.apache.lucene.search.DeletionAwareConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class Queries {

    // We don't use MatchAllDocsQuery, its slower than the one below ... (much slower)
    public final static Query MATCH_ALL_QUERY = new DeletionAwareConstantScoreQuery(new MatchAllDocsFilter());

    /**
     * A match all docs filter. Note, requires no caching!.
     */
    public final static Filter MATCH_ALL_FILTER = new MatchAllDocsFilter();

    private final static Field disjuncts;

    static {
        Field disjunctsX;
        try {
            disjunctsX = DisjunctionMaxQuery.class.getDeclaredField("disjuncts");
            disjunctsX.setAccessible(true);
        } catch (Exception e) {
            disjunctsX = null;
        }
        disjuncts = disjunctsX;
    }

    public static List<Query> disMaxClauses(DisjunctionMaxQuery query) {
        try {
            return (List<Query>) disjuncts.get(query);
        } catch (IllegalAccessException e) {
            return null;
        }
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
            newBq.add(MATCH_ALL_QUERY, BooleanClause.Occur.MUST);
            return newBq;
        }
        return q;
    }

    public static boolean isMatchAllQuery(Query query) {
        if (query instanceof MatchAllDocsQuery) {
            return true;
        }
        if (query instanceof DeletionAwareConstantScoreQuery) {
            DeletionAwareConstantScoreQuery scoreQuery = (DeletionAwareConstantScoreQuery) query;
            if (scoreQuery.getFilter() instanceof MatchAllDocsFilter) {
                return true;
            }
        }
        return false;
    }
}
