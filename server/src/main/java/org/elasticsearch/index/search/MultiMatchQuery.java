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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
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

import static org.elasticsearch.common.lucene.search.Queries.newLenientFieldQuery;

public class MultiMatchQuery extends MatchQuery {

    private Float groupTieBreaker = null;

    public MultiMatchQuery(QueryShardContext context) {
        super(context);
    }

    public void setTieBreaker(float tieBreaker) {
        this.groupTieBreaker = tieBreaker;
    }

    public Query parse(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames,
                       Object value, String minimumShouldMatch) throws IOException {
        boolean hasMappedField = fieldNames.keySet().stream()
            .anyMatch(k -> context.fieldMapper(k) != null);
        if (hasMappedField == false) {
            // all query fields are unmapped
            return Queries.newUnmappedFieldsQuery(fieldNames.keySet());
        }
        final float tieBreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
        final List<Query> queries;
        switch (type) {
            case PHRASE:
            case PHRASE_PREFIX:
            case BEST_FIELDS:
            case MOST_FIELDS:
            case BOOL_PREFIX:
                queries = buildFieldQueries(type, fieldNames, value, minimumShouldMatch);
                break;

            case CROSS_FIELDS:
                queries = buildCrossFieldQuery(type, fieldNames, value, minimumShouldMatch, tieBreaker);
                break;

            default:
                throw new IllegalStateException("No such type: " + type);
        }
        return combineGrouped(queries, tieBreaker);
    }

    private Query combineGrouped(List<Query> groupQuery, float tieBreaker) {
        if (groupQuery.isEmpty()) {
            return zeroTermsQuery();
        }
        if (groupQuery.size() == 1) {
            return groupQuery.get(0);
        }
        return new DisjunctionMaxQuery(groupQuery, tieBreaker);
    }

    private List<Query> buildFieldQueries(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames,
                                          Object value, String minimumShouldMatch) throws IOException {
        List<Query> queries = new ArrayList<>();
        for (String fieldName : fieldNames.keySet()) {
            if (context.fieldMapper(fieldName) == null) {
                // ignore unmapped fields
                continue;
            }
            float boostValue = fieldNames.getOrDefault(fieldName, 1.0f);
            Query query = parse(type.matchQueryType(), fieldName, value);
            query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
            if (query != null
                    && boostValue != AbstractQueryBuilder.DEFAULT_BOOST
                    && query instanceof MatchNoDocsQuery == false) {
                query = new BoostQuery(query, boostValue);
            }
            if (query != null) {
                queries.add(query);
            }
        }
        return queries;
    }

    private List<Query> buildCrossFieldQuery(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames,
                                            Object value, String minimumShouldMatch, float tieBreaker) throws IOException {
        Map<Analyzer, List<FieldAndBoost>> groups = new HashMap<>();
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fieldNames.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.fieldMapper(name);
            if (fieldType != null) {
                Analyzer actualAnalyzer = getAnalyzer(fieldType, type == MultiMatchQueryBuilder.Type.PHRASE);
                if (!groups.containsKey(actualAnalyzer)) {
                    groups.put(actualAnalyzer, new ArrayList<>());
                }
                float boost = entry.getValue() == null ? 1.0f : entry.getValue();
                groups.get(actualAnalyzer).add(new FieldAndBoost(fieldType, boost));
            }
        }
        for (Map.Entry<Analyzer, List<FieldAndBoost>> group : groups.entrySet()) {
            final MatchQueryBuilder builder;
            if (group.getValue().size() == 1) {
                builder = new MatchQueryBuilder(group.getKey(), group.getValue().get(0).fieldType,
                    enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            } else {
                builder = new BlendedQueryBuilder(group.getKey(), group.getValue(), tieBreaker,
                    enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            }

            /*
             * We have to pick some field to pass through the superclass so
             * we just pick the first field. It shouldn't matter because
             * fields are already grouped by their analyzers/types.
             */
            String representativeField = group.getValue().get(0).fieldType.name();
            Query query = parseInternal(type.matchQueryType(), representativeField, builder, value);
            query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
            if (query != null) {
                if (group.getValue().size() == 1) {
                    // apply the field boost to groups that contain a single field
                    float boost = group.getValue().get(0).boost;
                    if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
                        query = new BoostQuery(query, boost);
                    }
                }
                queries.add(query);
            }
        }

        return queries;
    }

    private class BlendedQueryBuilder extends MatchQueryBuilder {
        private final List<FieldAndBoost> blendedFields;
        private final float tieBreaker;

        BlendedQueryBuilder(Analyzer analyzer, List<FieldAndBoost> blendedFields, float tieBreaker,
                                boolean enablePositionIncrements, boolean autoGenerateSynonymsPhraseQuery) {
            super(analyzer, blendedFields.get(0).fieldType, enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            this.blendedFields = blendedFields;
            this.tieBreaker = tieBreaker;
        }

        @Override
        protected Query newSynonymQuery(TermAndBoost[] terms) {
            BytesRef[] values = new BytesRef[terms.length];
            for (int i = 0; i < terms.length; i++) {
                values[i] = terms[i].term.bytes();
            }
            return blendTerms(context, values, tieBreaker, lenient, blendedFields);
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            return blendTerm(context, term.bytes(), tieBreaker, lenient, blendedFields);
        }

        @Override
        protected Query newPrefixQuery(Term term) {
            List<Query> disjunctions = new ArrayList<>();
            for (FieldAndBoost fieldType : blendedFields) {
                Query query = fieldType.fieldType.prefixQuery(term.text(), null, context);
                if (fieldType.boost != 1f) {
                    query = new BoostQuery(query, fieldType.boost);
                }
                disjunctions.add(query);
            }
            return new DisjunctionMaxQuery(disjunctions, tieBreaker);
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            List<Query> disjunctions = new ArrayList<>();
            for (FieldAndBoost fieldType : blendedFields) {
                Query query = fieldType.fieldType.phraseQuery(stream, slop, enablePositionIncrements);
                if (fieldType.boost != 1f) {
                    query = new BoostQuery(query, fieldType.boost);
                }
                disjunctions.add(query);
            }
            return new DisjunctionMaxQuery(disjunctions, tieBreaker);
        }

        @Override
        protected Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
            List<Query> disjunctions = new ArrayList<>();
            for (FieldAndBoost fieldType : blendedFields) {
                Query query = fieldType.fieldType.multiPhraseQuery(stream, slop, enablePositionIncrements);
                if (fieldType.boost != 1f) {
                    query = new BoostQuery(query, fieldType.boost);
                }
                disjunctions.add(query);
            }
            return new DisjunctionMaxQuery(disjunctions, tieBreaker);
        }
    }

    static Query blendTerm(QueryShardContext context, BytesRef value, float tieBreaker,
                           boolean lenient, List<FieldAndBoost> blendedFields) {

        return blendTerms(context, new BytesRef[] {value}, tieBreaker, lenient, blendedFields);
    }

    static Query blendTerms(QueryShardContext context, BytesRef[] values, float tieBreaker,
                            boolean lenient, List<FieldAndBoost> blendedFields) {

        List<Query> queries = new ArrayList<>();
        Term[] terms = new Term[blendedFields.size() * values.length];
        float[] blendedBoost = new float[blendedFields.size() * values.length];
        int i = 0;
        for (FieldAndBoost ft : blendedFields) {
            for (BytesRef term : values) {
                Query query;
                try {
                    query = ft.fieldType.termQuery(term, context);
                } catch (RuntimeException e) {
                    if (lenient) {
                        query = newLenientFieldQuery(ft.fieldType.name(), e);
                    } else {
                        throw e;
                    }
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
            queries.add(BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker));
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            // best effort: add clauses that are not term queries so that they have an opportunity to match
            // however their score contribution will be different
            // TODO: can we improve this?
            return new DisjunctionMaxQuery(queries, tieBreaker);
        }
    }

    static final class FieldAndBoost {
        final MappedFieldType fieldType;
        final float boost;

        FieldAndBoost(MappedFieldType fieldType, float boost) {
            this.fieldType = Objects.requireNonNull(fieldType);
            this.boost = boost;
        }
    }
}
