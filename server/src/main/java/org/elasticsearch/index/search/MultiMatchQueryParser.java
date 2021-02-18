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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.XCombinedFieldQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
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
        switch (type) {
            case PHRASE:
            case PHRASE_PREFIX:
            case BEST_FIELDS:
            case MOST_FIELDS:
            case BOOL_PREFIX:
                return buildFieldQuery(type, fieldNames, value, minimumShouldMatch, tieBreaker);
            case COMBINED_FIELDS:
                return buildCombinedFieldQuery(fieldNames, value, minimumShouldMatch);
            case CROSS_FIELDS:
                return buildCrossFieldQuery(fieldNames, value, minimumShouldMatch, tieBreaker);
            default:
                throw new IllegalStateException("No such type: " + type);
        }
    }

    private Query buildFieldQuery(MultiMatchQueryBuilder.Type type, Map<String, Float> fieldNames,
                                  Object value, String minimumShouldMatch, float tieBreaker) throws IOException {
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
        return combineWithDisMax(queries, tieBreaker);
    }

    private Query combineWithDisMax(List<Query> groupQuery, float tieBreaker) {
        if (groupQuery.isEmpty()) {
            return zeroTermsQuery();
        }
        if (groupQuery.size() == 1) {
            return groupQuery.get(0);
        }
        return new DisjunctionMaxQuery(groupQuery, tieBreaker);
    }

    private Query buildCombinedFieldQuery(Map<String, Float> fields, Object value, String minimumShouldMatch) {
        validateSimilarity(fields);

        List<Query> queries = new ArrayList<>();
        Map<Analyzer, List<FieldAndBoost>> groups = new HashMap<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType == null) {
                continue;
            }

            if (fieldType.getTextSearchInfo() != TextSearchInfo.NONE) {
                float boost = entry.getValue() == null ? 1.0f : entry.getValue();
                Analyzer actualAnalyzer = getAnalyzer(fieldType, false);
                if (groups.containsKey(actualAnalyzer) == false) {
                    groups.put(actualAnalyzer, new ArrayList<>());
                }
                groups.get(actualAnalyzer).add(new FieldAndBoost(fieldType, boost));
            } else {
                IllegalArgumentException exception = new IllegalArgumentException("Field [" + fieldType.name() +
                    "] of type [" + fieldType.typeName() + "] does not support match queries");
                if (lenient) {
                    queries.add(newLenientFieldQuery(name, exception));
                } else {
                    throw exception;
                }
            }
        }

        for (Map.Entry<Analyzer, List<FieldAndBoost>> group : groups.entrySet()) {
            // Pick an arbitrary field name to pass through the builder.
            String field = group.getValue().get(0).fieldType.name();
            MatchQueryBuilder builder = new CombinedFieldsQueryBuilder(group.getKey(), group.getValue(),
                enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            Query query = builder.createBooleanQuery(field, value.toString(), occur);
            if (query == null) {
                query = zeroTermsQuery();
            }
            query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
            if (query != null) {
                queries.add(query);
            }
        }

        return combineWithBoolean(queries);
    }

    private void validateSimilarity(Map<String, Float> fields) {
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType != null && fieldType.getTextSearchInfo().getSimilarity() != null) {
                throw new IllegalArgumentException("[multi_match] queries in [combined_fields] mode cannot be used " +
                    "with per-field similarities");
            }
        }

        Similarity defaultSimilarity = context.getDefaultSimilarity();
        if ((defaultSimilarity instanceof LegacyBM25Similarity
            || defaultSimilarity instanceof BM25Similarity) == false) {
            throw new IllegalArgumentException("[multi_match] queries in [combined_fields] mode can only be used " +
                "with the [BM25] similarity");
        }
    }

    private Query combineWithBoolean(List<Query> groupQuery) {
        if (groupQuery.isEmpty()) {
            return zeroTermsQuery();
        }
        BooleanQuery.Builder combinedQuery = new BooleanQuery.Builder();
        for (Query query : groupQuery) {
            combinedQuery.add(query, BooleanClause.Occur.SHOULD);
        }
        return combinedQuery.build();
    }

    private class CombinedFieldsQueryBuilder extends MatchQueryBuilder {
        private final List<FieldAndBoost> fields;

        CombinedFieldsQueryBuilder(Analyzer analyzer, List<FieldAndBoost> fields,
                                   boolean enablePositionIncrements, boolean autoGenerateSynonymsPhraseQuery) {
            super(analyzer, fields.get(0).fieldType, enablePositionIncrements, autoGenerateSynonymsPhraseQuery);
            this.fields = fields;
        }

        @Override
        public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
            throw new IllegalArgumentException("[multi_match] queries in [combined_fields] mode don't support phrases");
        }

        @Override
        protected Query createPhrasePrefixQuery(String field, String queryText, int slop) {
            throw new IllegalArgumentException("[multi_match] queries in [combined_fields] mode don't support phrase prefix");
        }

        @Override
        protected Query createBooleanPrefixQuery(String field, String queryText, BooleanClause.Occur occur) {
            throw new IllegalArgumentException("[multi_match] queries in [combined_fields] mode don't support boolean prefix");
        }

        @Override
        protected Query newSynonymQuery(TermAndBoost[] terms) {
            List<FieldAndBoost> textFields = new ArrayList<>();
            List<FieldAndBoost> nonTextFields = new ArrayList<>();

            for (FieldAndBoost fieldAndBoost : fields) {
                MappedFieldType fieldType = fieldAndBoost.fieldType;
                if (fieldType.getTextSearchInfo().hasTerms()) {
                    textFields.add(fieldAndBoost);
                } else {
                    nonTextFields.add(fieldAndBoost);
                }
            }

            Query textQuery = createTextQuery(terms, textFields);
            if (nonTextFields.isEmpty()) {
                return textQuery;
            } else {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(textQuery, BooleanClause.Occur.SHOULD);
                addNonTextQueries(terms, nonTextFields, builder);
                return builder.build();
            }
        }

        private XCombinedFieldQuery createTextQuery(TermAndBoost[] terms, List<FieldAndBoost> fields) {
            XCombinedFieldQuery.Builder query = new XCombinedFieldQuery.Builder();
            for (TermAndBoost termAndBoost : terms) {
                assert termAndBoost.boost == BoostAttribute.DEFAULT_BOOST;
                BytesRef bytes = termAndBoost.term.bytes();
                query.addTerm(bytes);
            }
            for (FieldAndBoost fieldAndBoost : fields) {
                MappedFieldType fieldType = fieldAndBoost.fieldType;
                float fieldBoost = fieldAndBoost.boost;
                if (fieldType.getTextSearchInfo().hasTerms()) {
                    query.addField(fieldType.name(), fieldBoost);
                }
            }
            return query.build();
        }

        private void addNonTextQueries(TermAndBoost[] terms, List<FieldAndBoost> fields, BooleanQuery.Builder builder) {
            for (FieldAndBoost fieldAndBoost : fields) {
                MappedFieldType fieldType = fieldAndBoost.fieldType;
                float fieldBoost = fieldAndBoost.boost;

                for (TermAndBoost termAndBoost : terms) {
                    assert termAndBoost.boost == BoostAttribute.DEFAULT_BOOST;
                    BytesRef bytes = termAndBoost.term.bytes();
                    Query nonTextQuery;
                    try {
                        nonTextQuery = fieldType.termQuery(bytes, context);
                    } catch (RuntimeException e) {
                        if (lenient) {
                            nonTextQuery = newLenientFieldQuery(fieldType.name(), e);
                        } else {
                            throw e;
                        }
                    }

                    if (fieldBoost != AbstractQueryBuilder.DEFAULT_BOOST) {
                        nonTextQuery = new BoostQuery(nonTextQuery, fieldBoost);
                    }
                    builder.add(nonTextQuery, BooleanClause.Occur.SHOULD);
                }
            }
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            TermAndBoost termAndBoost = new TermAndBoost(term, boost);
            return newSynonymQuery(new TermAndBoost[]{termAndBoost});
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (FieldAndBoost fieldType : fields) {
                Query query = fieldType.fieldType.phraseQuery(stream, slop, enablePositionIncrements);
                if (fieldType.boost != 1f) {
                    query = new BoostQuery(query, fieldType.boost);
                }
                builder.add(query, BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        }
    }

    private Query buildCrossFieldQuery(Map<String, Float> fields, Object value, String minimumShouldMatch, float tieBreaker) {
        Map<Analyzer, List<FieldAndBoost>> groups = new HashMap<>();
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
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
                query = zeroTermsQuery();
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
        return combineWithDisMax(queries, tieBreaker);
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
