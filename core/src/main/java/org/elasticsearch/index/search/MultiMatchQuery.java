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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MultiMatchQuery extends MatchQuery {

    private Float groupTieBreaker = null;

    public void setTieBreaker(float tieBreaker) {
        this.groupTieBreaker = tieBreaker;
    }

    public MultiMatchQuery(QueryShardContext context) {
        super(context);
    }

    private Query parseAndApply(Type type, String fieldName, Object value, String minimumShouldMatch, Float boostValue) throws IOException {
        Query query = parse(type, fieldName, value);
        // If the coordination factor is disabled on a boolean query we don't apply the minimum should match.
        // This is done to make sure that the minimum_should_match doesn't get applied when there is only one word
        // and multiple variations of the same word in the query (synonyms for instance).
        if (query instanceof BooleanQuery && !((BooleanQuery) query).isCoordDisabled()) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        if (query != null && boostValue != null && boostValue != AbstractQueryBuilder.DEFAULT_BOOST) {
            query = new BoostQuery(query, boostValue);
        }
        return query;
    }

    public Query parse(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException {
        Query result;
        if (fieldNames.size() == 1) {
            Map.Entry<String, Float> fieldBoost = fieldNames.entrySet().iterator().next();
            Float boostValue = fieldBoost.getValue();
            result = parseAndApply(type.matchQueryType(), fieldBoost.getKey(), value, minimumShouldMatch, boostValue);
        } else {
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
            result = queryBuilder.combineGrouped(queries);
        }
        assert result != null;
        return result;
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

        public List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException{
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

        private Query combineGrouped(List<? extends Query> groupQuery) {
            if (groupQuery == null || groupQuery.isEmpty()) {
                return  new MatchNoDocsQuery("[multi_match] list of group queries was empty");
            }
            if (groupQuery.size() == 1) {
                return groupQuery.get(0);
            }
            if (groupDismax) {
                List<Query> queries = new ArrayList<>();
                for (Query query : groupQuery) {
                    queries.add(query);
                }
                return new DisjunctionMaxQuery(queries, tieBreaker);
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

        public Query termQuery(MappedFieldType fieldType, Object value) {
            return MultiMatchQuery.this.termQuery(fieldType, value, lenient);
        }
    }

    final class CrossFieldsQueryBuilder extends QueryBuilder {
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
                MappedFieldType fieldType = context.fieldMapper(name);
                if (fieldType != null) {
                    Analyzer actualAnalyzer = getAnalyzer(fieldType);
                    name = fieldType.name();
                    if (!groups.containsKey(actualAnalyzer)) {
                       groups.put(actualAnalyzer, new ArrayList<>());
                    }
                    Float boost = entry.getValue();
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new FieldAndFieldType(fieldType, boost));
                } else {
                    missing.add(new Tuple<>(name, entry.getValue()));
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
                /*
                 * We have to pick some field to pass through the superclass so
                 * we just pick the first field. It shouldn't matter because
                 * fields are already grouped by their analyzers/types.
                 */
                String representativeField = group.get(0).fieldType.name();
                Query q = parseGroup(type.matchQueryType(), representativeField, 1f, value, minimumShouldMatch);
                if (q != null) {
                    queries.add(q);
                }
            }

            return queries.isEmpty() ? null : queries;
        }

        @Override
        public Query blendTerm(Term term, MappedFieldType fieldType) {
            if (blendedFields == null) {
                return super.blendTerm(term, fieldType);
            }
            return MultiMatchQuery.blendTerm(term.bytes(), commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query termQuery(MappedFieldType fieldType, Object value) {
            /*
             * Use the string value of the term because we're reusing the
             * portion of the query is usually after the analyzer has run on
             * each term. We just skip that analyzer phase.
             */
            return blendTerm(new Term(fieldType.name(), value.toString()), fieldType);
        }
    }

    static Query blendTerm(BytesRef value, Float commonTermsCutoff, float tieBreaker, FieldAndFieldType... blendedFields) {
        List<Query> queries = new ArrayList<>();
        Term[] terms = new Term[blendedFields.length];
        float[] blendedBoost = new float[blendedFields.length];
        int i = 0;
        for (FieldAndFieldType ft : blendedFields) {
            Query query;
            try {
                query = ft.fieldType.termQuery(value, null);
            } catch (IllegalArgumentException e) {
                // the query expects a certain class of values such as numbers
                // of ip addresses and the value can't be parsed, so ignore this
                // field
                continue;
            }
            float boost = ft.boost;
            while (query instanceof BoostQuery) {
                BoostQuery bq = (BoostQuery) query;
                query = bq.getQuery();
                boost *= bq.getBoost();
            }
            if (query.getClass() == TermQuery.class) {
                terms[i] = ((TermQuery) query).getTerm();
                blendedBoost[i] = boost;
                i++;
            } else {
                if (boost != 1f) {
                    query = new BoostQuery(query, boost);
                }
                queries.add(query);
            }
        }
        if (i > 0) {
            terms = Arrays.copyOf(terms, i);
            blendedBoost = Arrays.copyOf(blendedBoost, i);
            if (commonTermsCutoff != null) {
                queries.add(BlendedTermQuery.commonTermsBlendedQuery(terms, blendedBoost, false, commonTermsCutoff));
            } else if (tieBreaker == 1.0f) {
                queries.add(BlendedTermQuery.booleanBlendedQuery(terms, blendedBoost, false));
            } else {
                queries.add(BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker));
            }
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            // best effort: add clauses that are not term queries so that they have an opportunity to match
            // however their score contribution will be different
            // TODO: can we improve this?
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            bq.setDisableCoord(true);
            for (Query query : queries) {
                bq.add(query, Occur.SHOULD);
            }
            return bq.build();
        }
    }

    @Override
    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermQuery(term, fieldType);
        }
        return queryBuilder.blendTerm(term, fieldType);
    }

    static final class FieldAndFieldType {
        final MappedFieldType fieldType;
        final float boost;

        FieldAndFieldType(MappedFieldType fieldType, float boost) {
            this.fieldType = Objects.requireNonNull(fieldType);
            this.boost = boost;
        }
    }
}
