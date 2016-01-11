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
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.List;

public class MatchQuery {

    public static enum Type {
        BOOLEAN,
        PHRASE,
        PHRASE_PREFIX
    }

    public static enum ZeroTermsQuery {
        NONE,
        ALL
    }

    protected final QueryParseContext parseContext;

    protected String analyzer;

    protected BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    protected boolean enablePositionIncrements = true;

    protected int phraseSlop = 0;

    protected Fuzziness fuzziness = null;

    protected int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;

    protected int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    protected boolean transpositions = FuzzyQuery.defaultTranspositions;

    protected MultiTermQuery.RewriteMethod fuzzyRewriteMethod;

    protected boolean lenient;

    protected ZeroTermsQuery zeroTermsQuery = ZeroTermsQuery.NONE;

    protected Float commonTermsCutoff = null;

    public MatchQuery(QueryParseContext parseContext) {
        this.parseContext = parseContext;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public void setOccur(BooleanClause.Occur occur) {
        this.occur = occur;
    }

    public void setCommonTermsCutoff(float cutoff) {
        this.commonTermsCutoff = Float.valueOf(cutoff);
    }

    public void setEnablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
    }

    public void setPhraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
    }

    public void setFuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
    }

    public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
    }

    public void setMaxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
    }

    public void setTranspositions(boolean transpositions) {
        this.transpositions = transpositions;
    }

    public void setFuzzyRewriteMethod(MultiTermQuery.RewriteMethod fuzzyRewriteMethod) {
        this.fuzzyRewriteMethod = fuzzyRewriteMethod;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public void setZeroTermsQuery(ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
    }

    protected Analyzer getAnalyzer(MappedFieldType fieldType) {
        if (this.analyzer == null) {
            if (fieldType != null) {
                return parseContext.getSearchAnalyzer(fieldType);
            }
            return parseContext.mapperService().searchAnalyzer();
        } else {
            Analyzer analyzer = parseContext.mapperService().analysisService().analyzer(this.analyzer);
            if (analyzer == null) {
                throw new IllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
            }
            return analyzer;
        }
    }

    public Query parse(Type type, String fieldName, Object value) throws IOException {
        final String field;
        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType != null) {
            field = fieldType.names().indexName();
        } else {
            field = fieldName;
        }

        /*
         * If the user forced an analyzer we really don't care if they are
         * searching a type that wants term queries to be used with query string
         * because the QueryBuilder will take care of it. If they haven't forced
         * an analyzer then types like NumberFieldType that want terms with
         * query string will blow up because their analyzer isn't capable of
         * passing through QueryBuilder.
         */
        boolean noForcedAnalyzer = this.analyzer == null;
        if (fieldType != null && fieldType.useTermQueryWithQueryString() && noForcedAnalyzer) {
            return termQuery(fieldType, value);
        }

        Analyzer analyzer = getAnalyzer(fieldType);
        assert analyzer != null;
        MatchQueryBuilder builder = new MatchQueryBuilder(analyzer, fieldType);
        builder.setEnablePositionIncrements(this.enablePositionIncrements);

        Query query = null;
        switch (type) {
            case BOOLEAN:
                if (commonTermsCutoff == null) {
                    query = builder.createBooleanQuery(field, value.toString(), occur);
                } else {
                    query = builder.createCommonTermsQuery(field, value.toString(), occur, occur, commonTermsCutoff, fieldType);
                }
                break;
            case PHRASE:
                query = builder.createPhraseQuery(field, value.toString(), phraseSlop);
                break;
            case PHRASE_PREFIX:
                query = builder.createPhrasePrefixQuery(field, value.toString(), phraseSlop, maxExpansions);
                break;
            default:
                throw new IllegalStateException("No type found for [" + type + "]");
        }

        if (query == null) {
            return zeroTermsQuery();
        } else {
            return query;
        }
    }

    /**
     * Creates a TermQuery-like-query for MappedFieldTypes that don't support
     * QueryBuilder which is very string-ish. Just delegates to the
     * MappedFieldType for MatchQuery but gets more complex for blended queries.
     */
    protected Query termQuery(MappedFieldType fieldType, Object value) {
        return termQuery(fieldType, value, lenient);
    }

    protected final Query termQuery(MappedFieldType fieldType, Object value, boolean lenient) {
        try {
            return fieldType.termQuery(value, parseContext);
        } catch (RuntimeException e) {
            if (lenient) {
                return null;
            }
            throw e;
        }
    }

    protected Query zeroTermsQuery() {
        return zeroTermsQuery == ZeroTermsQuery.NONE ? Queries.newMatchNoDocsQuery() : Queries.newMatchAllQuery();
    }

    private class MatchQueryBuilder extends QueryBuilder {

        private final MappedFieldType mapper;

        /**
         * Creates a new QueryBuilder using the given analyzer.
         */
        public MatchQueryBuilder(Analyzer analyzer, @Nullable MappedFieldType mapper) {
            super(analyzer);
            this.mapper = mapper;
        }

        @Override
        protected Query newTermQuery(Term term) {
            return blendTermQuery(term, mapper);
        }

        public Query createPhrasePrefixQuery(String field, String queryText, int phraseSlop, int maxExpansions) {
            final Query query = createFieldQuery(getAnalyzer(), Occur.MUST, field, queryText, true, phraseSlop);
            final MultiPhrasePrefixQuery prefixQuery = new MultiPhrasePrefixQuery();
            prefixQuery.setMaxExpansions(maxExpansions);
            prefixQuery.setSlop(phraseSlop);
            if (query instanceof PhraseQuery) {
                PhraseQuery pq = (PhraseQuery)query;
                Term[] terms = pq.getTerms();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(new Term[] {terms[i]}, positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof MultiPhraseQuery) {
                MultiPhraseQuery pq = (MultiPhraseQuery)query;
                List<Term[]> terms = pq.getTermArrays();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.size(); i++) {
                    prefixQuery.add(terms.get(i), positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof TermQuery) {
                prefixQuery.add(((TermQuery) query).getTerm());
                return prefixQuery;
            }
            return query;
        }

        public Query createCommonTermsQuery(String field, String queryText, Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency, MappedFieldType fieldType) {
            Query booleanQuery = createBooleanQuery(field, queryText, lowFreqOccur);
            if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) booleanQuery;
                ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency, ((BooleanQuery)booleanQuery).isCoordDisabled(), fieldType);
                for (BooleanClause clause : bq.clauses()) {
                    if (!(clause.getQuery() instanceof TermQuery)) {
                        return booleanQuery;
                    }
                    query.add(((TermQuery) clause.getQuery()).getTerm());
                }
                return query;
            }
            return booleanQuery;

        }
    }

    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (fuzziness != null) {
            if (fieldType != null) {
                try {
                    Query query = fieldType.fuzzyQuery(term.text(), fuzziness, fuzzyPrefixLength, maxExpansions, transpositions);
                    if (query instanceof FuzzyQuery) {
                        QueryParsers.setRewriteMethod((FuzzyQuery) query, fuzzyRewriteMethod);
                    }
                    return query;
                } catch (RuntimeException e) {
                    return new TermQuery(term);
                    // See long comment below about why we're lenient here.
                }
            }
            int edits = fuzziness.asDistance(term.text());
            FuzzyQuery query = new FuzzyQuery(term, edits, fuzzyPrefixLength, maxExpansions, transpositions);
            QueryParsers.setRewriteMethod(query, fuzzyRewriteMethod);
            return query;
        }
        if (fieldType != null) {
            /*
             * Its a bit weird to default to lenient here but its the backwards
             * compatible. It makes some sense when you think about what we are
             * doing here: at this point the user has forced an analyzer and
             * passed some string to the match query. We cut it up using the
             * analyzer and then tried to cram whatever we get into the field.
             * lenient=true here means that we try the terms in the query and on
             * the off chance that they are actually valid terms then we
             * actually try them. lenient=false would mean that we blow up the
             * query if they aren't valid terms. "valid" in this context means
             * "parses properly to something of the type being queried." So "1"
             * is a valid number, etc.
             *
             * We use the text form here because we we've received the term from
             * an analyzer that cut some string into text.
             */
            Query query = termQuery(fieldType, term.bytes(), true);
            if (query != null) {
                return query;
            }
        }
        return new TermQuery(term);
    }

}
