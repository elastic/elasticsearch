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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
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
        query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
        if (query != null && boostValue != null && boostValue != AbstractQueryBuilder.DEFAULT_BOOST && query instanceof MatchNoDocsQuery == false) {
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
                    queryBuilder = new CrossFieldsQueryBuilder();
                    break;
                default:
                    throw new IllegalStateException("No such type: " + type);
            }
            final List<? extends Query> queries = queryBuilder.buildGroupedQueries(type, fieldNames, value, minimumShouldMatch);
            result = queryBuilder.combineGrouped(queries);
        }
        return result;
    }

    private QueryBuilder queryBuilder;

    public class QueryBuilder {
        protected final float tieBreaker;

        public QueryBuilder(float tieBreaker) {
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
            List<Query> queries = new ArrayList<>();
            for (Query query : groupQuery) {
                queries.add(query);
            }
            return new DisjunctionMaxQuery(queries, tieBreaker);
        }

        public Query blendTerm(Term term, MappedFieldType fieldType) {
            return MultiMatchQuery.super.blendTermQuery(term, fieldType);
        }

        public Query blendTerms(Term[] terms, MappedFieldType fieldType) {
            return MultiMatchQuery.super.blendTermsQuery(terms, fieldType);
        }

        public Query termQuery(MappedFieldType fieldType, BytesRef value) {
            return MultiMatchQuery.this.termQuery(fieldType, value, lenient);
        }
    }

    final class CrossFieldsQueryBuilder extends QueryBuilder {
        private FieldAndFieldType[] blendedFields;

        CrossFieldsQueryBuilder() {
            super(0.0f);
        }

        @Override
        public List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) throws IOException {
            Map<Analyzer, List<FieldAndFieldType>> groups = new HashMap<>();
            List<Query> queries = new ArrayList<>();
            for (Map.Entry<String, Float> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                MappedFieldType fieldType = context.fieldMapper(name);
                if (fieldType != null) {
                    Analyzer actualAnalyzer = getAnalyzer(fieldType, type == MultiMatchQueryBuilder.Type.PHRASE);
                    name = fieldType.name();
                    if (!groups.containsKey(actualAnalyzer)) {
                       groups.put(actualAnalyzer, new ArrayList<>());
                    }
                    Float boost = entry.getValue();
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new FieldAndFieldType(fieldType, boost));
                } else {
                    queries.add(new MatchNoDocsQuery("unknown field " + name));
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
        public Query blendTerms(Term[] terms, MappedFieldType fieldType) {
            if (blendedFields == null || blendedFields.length == 1) {
                return super.blendTerms(terms, fieldType);
            }
            BytesRef[] values = new BytesRef[terms.length];
            for (int i = 0; i < terms.length; i++) {
                values[i] = terms[i].bytes();
            }
            return MultiMatchQuery.blendTerms(context, values, commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query blendTerm(Term term, MappedFieldType fieldType) {
            if (blendedFields == null) {
                return super.blendTerm(term, fieldType);
            }
            return MultiMatchQuery.blendTerm(context, term.bytes(), commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query termQuery(MappedFieldType fieldType, BytesRef value) {
            /*
             * Use the string value of the term because we're reusing the
             * portion of the query is usually after the analyzer has run on
             * each term. We just skip that analyzer phase.
             */
            return blendTerm(new Term(fieldType.name(), value.utf8ToString()), fieldType);
        }
    }

    static Query blendTerm(QueryShardContext context, BytesRef value, Float commonTermsCutoff, float tieBreaker,
                           FieldAndFieldType... blendedFields) {
        return blendTerms(context, new BytesRef[] {value}, commonTermsCutoff, tieBreaker, blendedFields);
    }

    static Query blendTerms(QueryShardContext context, BytesRef[] values, Float commonTermsCutoff, float tieBreaker,
                            FieldAndFieldType... blendedFields) {
        List<Query> queries = new ArrayList<>();
        Term[] terms = new Term[blendedFields.length * values.length];
        float[] blendedBoost = new float[blendedFields.length * values.length];
        int i = 0;
        for (FieldAndFieldType ft : blendedFields) {
            for (BytesRef term : values) {
                Query query;
                try {
                    query = ft.fieldType.termQuery(term, context);
                } catch (IllegalArgumentException e) {
                    // the query expects a certain class of values such as numbers
                    // of ip addresses and the value can't be parsed, so ignore this
                    // field
                    continue;
                } catch (ElasticsearchParseException parseException) {
                    // date fields throw an ElasticsearchParseException with the
                    // underlying IAE as the cause, ignore this field if that is
                    // the case
                    if (parseException.getCause() instanceof IllegalArgumentException) {
                        continue;
                    }
                    throw parseException;
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
                    if (boost != 1f && query instanceof MatchNoDocsQuery == false) {
                        query = new BoostQuery(query, boost);
                    }
                    queries.add(query);
                }
            }
        }
        if (i > 0) {
            terms = Arrays.copyOf(terms, i);
            blendedBoost = Arrays.copyOf(blendedBoost, i);
            if (commonTermsCutoff != null) {
                queries.add(BlendedTermQuery.commonTermsBlendedQuery(terms, blendedBoost, commonTermsCutoff));
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
            return new DisjunctionMaxQuery(queries, 1.0f);
        }
    }

    @Override
    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermQuery(term, fieldType);
        }
        return queryBuilder.blendTerm(term, fieldType);
    }

    @Override
    protected Query blendTermsQuery(Term[] terms, MappedFieldType fieldType) {
        if (queryBuilder == null) {
            return super.blendTermsQuery(terms, fieldType);
        }
        return queryBuilder.blendTerms(terms, fieldType);
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
