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

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiMatchQuery extends MatchQuery {

    private Float groupTieBreaker = null;

    public void setTieBreaker(float tieBreaker) {
        this.groupTieBreaker = tieBreaker;
    }

    public MultiMatchQuery(QueryParseContext parseContext) {
        super(parseContext);
    }
    
    private Query parseAndApply(Type type, String fieldName, Object value, String minimumShouldMatch, Float boostValue) throws IOException {
        Query query = parse(type, fieldName, value);
        if (query instanceof BooleanQuery) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        if (boostValue != null && query != null) {
            query.setBoost(boostValue);
        }
        return query;
    }

    public Query parse(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException {
        if (fieldNames.size() == 1) {
            Map.Entry<String, Float> fieldBoost = fieldNames.entrySet().iterator().next();
            Float boostValue = fieldBoost.getValue();
            return parseAndApply(type.matchQueryType(), fieldBoost.getKey(), value, minimumShouldMatch, boostValue);
        }

        final float tieBreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
        switch (type) {
            case PHRASE:
            case PHRASE_PREFIX:
            case BEST_FIELDS:
            case MOST_FIELDS:
                queryBuilder = new QueryBuilder(tieBreaker);
                break;
            case CROSS_FIELDS:
                queryBuilder = new CrossFieldsQueryBuilder(tieBreaker);
                break;
            default:
                throw new IllegalStateException("No such type: " + type);
        }
        final List<? extends Query> queries = queryBuilder.buildGroupedQueries(type, fieldNames, value, minimumShouldMatch);
        return queryBuilder.combineGrouped(queries);
    }

    private QueryBuilder queryBuilder;

    public class QueryBuilder {
        protected final boolean groupDismax;
        protected final float tieBreaker;

        public QueryBuilder(float tieBreaker) {
            this(tieBreaker != 1.0f, tieBreaker);
        }

        public QueryBuilder(boolean groupDismax, float tieBreaker) {
            this.groupDismax = groupDismax;
            this.tieBreaker = tieBreaker;
        }

        public  List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException{
            List<Query> queries = new ArrayList<>();
            for (String fieldName : fieldNames.keySet()) {
                Float boostValue = fieldNames.get(fieldName);
                Query query = parseGroup(type.matchQueryType(), fieldName, boostValue, value, minimumShouldMatch);
                if (query != null) {
                    queries.add(query);
                }
            }
            return queries;
        }

        public Query parseGroup(Type type, String field, Float boostValue, Object value, String minimumShouldMatch) throws IOException {
            return parseAndApply(type, field, value, minimumShouldMatch, boostValue);
        }

        public Query combineGrouped(List<? extends Query> groupQuery) {
            if (groupQuery == null || groupQuery.isEmpty()) {
                return null;
            }
            if (groupQuery.size() == 1) {
                return groupQuery.get(0);
            }
            if (groupDismax) {
                DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
                for (Query query : groupQuery) {
                    disMaxQuery.add(query);
                }
                return disMaxQuery;
            } else {
                final BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                for (Query query : groupQuery) {
                    booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                }
                return booleanQuery.build();
            }
        }

        public Query blendTerm(Term term, MappedFieldType fieldType) {
            return MultiMatchQuery.super.blendTermQuery(term, fieldType);
        }

        public boolean forceAnalyzeQueryString() {
            return false;
        }
    }

    public class CrossFieldsQueryBuilder extends QueryBuilder {
        private FieldAndFieldType[] blendedFields;

        public CrossFieldsQueryBuilder(float tieBreaker) {
            super(false, tieBreaker);
        }

        @Override
        public List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException {
            Map<Analyzer, List<FieldAndFieldType>> groups = new HashMap<>();
            List<Tuple<String, Float>> missing = new ArrayList<>();
            for (Map.Entry<String, Float> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                MappedFieldType fieldType = parseContext.fieldMapper(name);
                if (fieldType != null) {
                    Analyzer actualAnalyzer = getAnalyzer(fieldType);
                    name = fieldType.names().indexName();
                    if (!groups.containsKey(actualAnalyzer)) {
                       groups.put(actualAnalyzer, new ArrayList<FieldAndFieldType>());
                    }
                    Float boost = entry.getValue();
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new FieldAndFieldType(name, fieldType, boost));
                } else {
                    missing.add(new Tuple(name, entry.getValue()));
                }

            }
            List<Query> queries = new ArrayList<>();
            for (Tuple<String, Float> tuple : missing) {
                Query q = parseGroup(type.matchQueryType(), tuple.v1(), tuple.v2(), value, minimumShouldMatch);
                if (q != null) {
                    queries.add(q);
                }
            }
            for (List<FieldAndFieldType> group : groups.values()) {
                if (group.size() > 1) {
                    blendedFields = new FieldAndFieldType[group.size()];
                    int i = 0;
                    for (FieldAndFieldType fieldAndFieldType : group) {
                        blendedFields[i++] = fieldAndFieldType;
                    }
                } else {
                    blendedFields = null;
                }
                final FieldAndFieldType fieldAndFieldType = group.get(0);
                Query q = parseGroup(type.matchQueryType(), fieldAndFieldType.field, 1f, value, minimumShouldMatch);
                if (q != null) {
                    queries.add(q);
                }
            }

            return queries.isEmpty() ? null : queries;
        }

        @Override
        public boolean forceAnalyzeQueryString() {
            return blendedFields != null;
        }

        @Override
        public Query blendTerm(Term term, MappedFieldType fieldType) {
            if (blendedFields == null) {
                return super.blendTerm(term, fieldType);
            }
            final Term[] terms = new Term[blendedFields.length];
            float[] blendedBoost = new float[blendedFields.length];
            for (int i = 0; i < blendedFields.length; i++) {
                terms[i] = blendedFields[i].newTerm(term.text());
                blendedBoost[i] = blendedFields[i].boost;
            }
            if (commonTermsCutoff != null) {
                return BlendedTermQuery.commonTermsBlendedQuery(terms, blendedBoost, false, commonTermsCutoff);
            }

            if (tieBreaker == 1.0f) {
                return BlendedTermQuery.booleanBlendedQuery(terms, blendedBoost, false);
            }
            return BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker);
        }
    }

    @Override
    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermQuery(term, fieldType);
        }
        return queryBuilder.blendTerm(term, fieldType);
    }

    private static final class FieldAndFieldType {
        final String field;
        final MappedFieldType fieldType;
        final float boost;


        private FieldAndFieldType(String field, MappedFieldType fieldType, float boost) {
            this.field = field;
            this.fieldType = fieldType;
            this.boost = boost;
        }

        public Term newTerm(String value) {
            try {
                final BytesRef bytesRef = fieldType.indexedValueForSearch(value);
                return new Term(field, bytesRef);
            } catch (Exception ex) {
                // we can't parse it just use the incoming value -- it will
                // just have a DF of 0 at the end of the day and will be ignored
            }
            return new Term(field, value);
        }
    }

    @Override
    protected boolean forceAnalyzeQueryString() {
        return this.queryBuilder == null ? super.forceAnalyzeQueryString() : this.queryBuilder.forceAnalyzeQueryString();
    }
}
