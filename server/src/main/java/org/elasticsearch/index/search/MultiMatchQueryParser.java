/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BooleanClause;
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
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.lucene.search.Queries.newLenientFieldQuery;

public class MultiMatchQueryParser extends MatchQueryParser {

    private Float groupTieBreaker = null;

    public MultiMatchQueryParser(SearchExecutionContext context) {
        super(context);
    }

    public void setTieBreaker(float tieBreaker) {
        this.groupTieBreaker = tieBreaker;
    }

    public Query parse(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames,
                       Object value, String minimumShouldMatch) throws IOException {
        boolean hasMappedField = fieldNames.keySet().stream()
            .anyMatch(k -> context.getFieldType(k) != null);
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
                queries = buildCrossFieldQuery(fieldNames, value, minimumShouldMatch, tieBreaker);
                break;

            default:
                throw new IllegalStateException("No such type: " + type);
        }
        return combineGrouped(queries, tieBreaker);
    }

    private Query combineGrouped(List<Query> groupQuery, float tieBreaker) {
        if (groupQuery.isEmpty()) {
            return zeroTermsQuery.asQuery();
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
            if (context.getFieldType(fieldName) == null) {
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

    private List<Query> buildCrossFieldQuery(Map<String, Float> fieldNames,
                                             Object value, String minimumShouldMatch, float tieBreaker) {

        Map<Analyzer, List<FieldAndBoost>> groups = new HashMap<>();
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fieldNames.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType != null) {
                Analyzer actualAnalyzer = getAnalyzer(fieldType, false);
                if (groups.containsKey(actualAnalyzer) == false) {
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
                builder = new CrossFieldsQueryBuilder(group.getKey(), group.getValue(), tieBreaker,
                    enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            }

            /*
             * We have to pick some field to pass through the superclass so
             * we just pick the first field. It shouldn't matter because
             * fields are already grouped by their analyzers/types.
             */
            String representativeField = group.getValue().get(0).fieldType.name();
            Query query = builder.createBooleanQuery(representativeField, value.toString(), occur);
            if (query == null) {
                query = zeroTermsQuery.asQuery();
            }

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

    private class CrossFieldsQueryBuilder extends MatchQueryBuilder {
        private final List<FieldAndBoost> blendedFields;
        private final float tieBreaker;

        CrossFieldsQueryBuilder(Analyzer analyzer, List<FieldAndBoost> blendedFields, float tieBreaker,
                                boolean enablePositionIncrements, boolean autoGenerateSynonymsPhraseQuery) {
            super(analyzer, blendedFields.get(0).fieldType, enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            this.blendedFields = blendedFields;
            this.tieBreaker = tieBreaker;
        }

        @Override
        public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
            throw new IllegalArgumentException("[multi_match] queries in [cross_fields] mode don't support phrases");
        }

        @Override
        protected Query createPhrasePrefixQuery(String field, String queryText, int slop) {
            throw new IllegalArgumentException("[multi_match] queries in [cross_fields] mode don't support phrase prefix");
        }

        @Override
        protected Query createBooleanPrefixQuery(String field, String queryText, BooleanClause.Occur occur) {
            throw new IllegalArgumentException("[multi_match] queries in [cross_fields] mode don't support boolean prefix");
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
            throw new IllegalArgumentException("[multi_match] queries in [cross_fields] mode don't support prefix");
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            List<Query> disjunctions = new ArrayList<>();
            for (FieldAndBoost fieldType : blendedFields) {
                Query query = fieldType.fieldType.phraseQuery(stream, slop, enablePositionIncrements, context);
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
                Query query = fieldType.fieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, context);
                if (fieldType.boost != 1f) {
                    query = new BoostQuery(query, fieldType.boost);
                }
                disjunctions.add(query);
            }
            return new DisjunctionMaxQuery(disjunctions, tieBreaker);
        }
    }

    static Query blendTerm(SearchExecutionContext context, BytesRef value, float tieBreaker,
                           boolean lenient, List<FieldAndBoost> blendedFields) {

        return blendTerms(context, new BytesRef[] {value}, tieBreaker, lenient, blendedFields);
    }

    static Query blendTerms(SearchExecutionContext context, BytesRef[] values, float tieBreaker,
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
