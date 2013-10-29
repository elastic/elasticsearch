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

package org.elasticsearch.index.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.Map;

public class MultiMatchQuery extends MatchQuery {

    private boolean useDisMax = true;
    private float tieBreaker;

    public void setUseDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    public void setTieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public MultiMatchQuery(QueryParseContext parseContext) {
        super(parseContext);
    }
    
    private Query parseAndApply(Type type, String fieldName, Object value, String minimumShouldMatch, Float boostValue) throws IOException {
        Query query = parse(type, fieldName, value);
        if (query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        if (boostValue != null && query != null) {
            query.setBoost(boostValue);
        }
        return query;
    }

    public Query parse(Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException {
        if (fieldNames.size() == 1) {
            Map.Entry<String, Float> fieldBoost = fieldNames.entrySet().iterator().next();
            Float boostValue = fieldBoost.getValue();
            return parseAndApply(type, fieldBoost.getKey(), value, minimumShouldMatch, boostValue);
        }

        if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean clauseAdded = false;
            for (String fieldName : fieldNames.keySet()) {
                Float boostValue = fieldNames.get(fieldName);
                Query query = parseAndApply(type, fieldName, value, minimumShouldMatch, boostValue);
                if (query != null) {
                    clauseAdded = true;
                    disMaxQuery.add(query);
                }
            }
            return clauseAdded ? disMaxQuery : null;
        } else {
            BooleanQuery booleanQuery = new BooleanQuery();
            for (String fieldName : fieldNames.keySet()) {
                Float boostValue = fieldNames.get(fieldName);
                Query query = parseAndApply(type, fieldName, value, minimumShouldMatch, boostValue);
                if (query != null) {
                    booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                }
            }
            return !booleanQuery.clauses().isEmpty() ? booleanQuery : null;
        }
    }

}