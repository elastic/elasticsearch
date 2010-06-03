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

package org.elasticsearch.util.lucene.search;

import org.apache.lucene.search.*;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class Queries {

    public final static MatchAllDocsQuery MATCH_ALL_QUERY = new MatchAllDocsQuery();

    public final static QueryWrapperFilter MATCH_ALL_FILTER = new QueryWrapperFilter(MATCH_ALL_QUERY);

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
            newBq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            return newBq;
        }
        return q;
    }
}
