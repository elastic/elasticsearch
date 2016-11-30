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

import static org.apache.lucene.analysis.synonym.SynonymGraphFilter.GRAPH_FLAG;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.GraphTokenStreamFiniteStrings;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.GraphQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MatchQuery {

    public static enum Type implements Writeable {
        /**
         * The text is analyzed and terms are added to a boolean query.
         */
        BOOLEAN(0),
        /**
         * The text is analyzed and used as a phrase query.
         */
        PHRASE(1),
        /**
         * The text is analyzed and used in a phrase query, with the last term acting as a prefix.
         */
        PHRASE_PREFIX(2);

        private final int ordinal;

        private Type(int ordinal) {
            this.ordinal = ordinal;
        }

        public static Type readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (Type type : Type.values()) {
                if (type.ordinal == ord) {
                    return type;
                }
            }
            throw new ElasticsearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    public static enum ZeroTermsQuery implements Writeable {
        NONE(0),
        ALL(1);

        private final int ordinal;

        private ZeroTermsQuery(int ordinal) {
            this.ordinal = ordinal;
        }

        public static ZeroTermsQuery readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (ZeroTermsQuery zeroTermsQuery : ZeroTermsQuery.values()) {
                if (zeroTermsQuery.ordinal == ord) {
                    return zeroTermsQuery;
                }
            }
            throw new ElasticsearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    /**
     * the default phrase slop
     */
    public static final int DEFAULT_PHRASE_SLOP = 0;

    /**
     * the default leniency setting
     */
    public static final boolean DEFAULT_LENIENCY = false;

    /**
     * the default zero terms query
     */
    public static final ZeroTermsQuery DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQuery.NONE;

    protected final QueryShardContext context;

    protected String analyzer;

    protected BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    protected boolean enablePositionIncrements = true;

    protected int phraseSlop = DEFAULT_PHRASE_SLOP;

    protected Fuzziness fuzziness = null;

    protected int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;

    protected int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    protected boolean transpositions = FuzzyQuery.defaultTranspositions;

    protected MultiTermQuery.RewriteMethod fuzzyRewriteMethod;

    protected boolean lenient = DEFAULT_LENIENCY;

    protected ZeroTermsQuery zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;

    protected Float commonTermsCutoff = null;

    public MatchQuery(QueryShardContext context) {
        this.context = context;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public void setOccur(BooleanClause.Occur occur) {
        this.occur = occur;
    }

    public void setCommonTermsCutoff(Float cutoff) {
        this.commonTermsCutoff = cutoff;
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
                return context.getSearchAnalyzer(fieldType);
            }
            return context.getMapperService().searchAnalyzer();
        } else {
            Analyzer analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            if (analyzer == null) {
                throw new IllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
            }
            return analyzer;
        }
    }

    public Query parse(Type type, String fieldName, Object value) throws IOException {
        final String field;
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            field = fieldType.name();
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
        if (fieldType != null && fieldType.tokenized() == false && noForcedAnalyzer) {
            return blendTermQuery(new Term(fieldName, value.toString()), fieldType);
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

    protected final Query termQuery(MappedFieldType fieldType, Object value, boolean lenient) {
        try {
            return fieldType.termQuery(value, context);
        } catch (RuntimeException e) {
            if (lenient) {
                return null;
            }
            throw e;
        }
    }

    protected Query zeroTermsQuery() {
        if (zeroTermsQuery == DEFAULT_ZERO_TERMS_QUERY) {
            return Queries.newMatchNoDocsQuery("Matching no documents because no terms present.");
        }
        return Queries.newMatchAllQuery();
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

        /**
         * Creates a query from the analysis chain.  Overrides original so all it does is create the token stream and pass that into the
         * new {@link #createFieldQuery(TokenStream, Occur, String, boolean, int)} method which has all the original query generation logic.
         *
         * @param analyzer   analyzer used for this query
         * @param operator   default boolean operator used for this query
         * @param field      field to create queries against
         * @param queryText  text to be passed to the analysis chain
         * @param quoted     true if phrases should be generated when terms occur at more than one position
         * @param phraseSlop slop factor for phrase/multiphrase queries
         */
        @Override
        protected final Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText,
                                               boolean quoted, int phraseSlop) {
            assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

            // Use the analyzer to get all the tokens, and then build an appropriate
            // query based on the analysis chain.
            try (TokenStream source = analyzer.tokenStream(field, queryText)) {
                return createFieldQuery(source, operator, field, quoted, phraseSlop);
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        /**
         * Creates a query from a token stream.  Same logic as {@link #createFieldQuery(Analyzer, Occur, String, String, boolean, int)}
         * with additional graph token stream detection.
         *
         * @param source     the token stream to create the query from
         * @param operator   default boolean operator used for this query
         * @param field      field to create queries against
         * @param quoted     true if phrases should be generated when terms occur at more than one position
         * @param phraseSlop slop factor for phrase/multiphrase queries
         */
        protected final Query createFieldQuery(TokenStream source, BooleanClause.Occur operator, String field, boolean quoted,
                                               int phraseSlop) {
            assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

            // Build an appropriate query based on the analysis chain.
            try (CachingTokenFilter stream = new CachingTokenFilter(source)) {

                TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
                PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
                PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);
                FlagsAttribute flagsAtt = stream.addAttribute(FlagsAttribute.class);

                if (termAtt == null) {
                    return null;
                }

                // phase 1: read through the stream and assess the situation:
                // counting the number of tokens/positions and marking if we have any synonyms.

                int numTokens = 0;
                int positionCount = 0;
                boolean hasSynonyms = false;
                boolean isGraph = false;

                stream.reset();
                while (stream.incrementToken()) {
                    numTokens++;
                    int positionIncrement = posIncAtt.getPositionIncrement();
                    if (positionIncrement != 0) {
                        positionCount += positionIncrement;
                    } else {
                        hasSynonyms = true;
                    }

                    int positionLength = posLenAtt.getPositionLength();
                    if (!isGraph && positionLength > 1 && ((flagsAtt.getFlags() & GRAPH_FLAG) == GRAPH_FLAG)) {
                        isGraph = true;
                    }
                }

                // phase 2: based on token count, presence of synonyms, and options
                // formulate a single term, boolean, or phrase.

                if (numTokens == 0) {
                    return null;
                } else if (numTokens == 1) {
                    // single term
                    return analyzeTerm(field, stream);
                } else if (isGraph) {
                    // graph
                    return analyzeGraph(stream, operator, field, quoted, phraseSlop);
                } else if (quoted && positionCount > 1) {
                    // phrase
                    if (hasSynonyms) {
                        // complex phrase with synonyms
                        return analyzeMultiPhrase(field, stream, phraseSlop);
                    } else {
                        // simple phrase
                        return analyzePhrase(field, stream, phraseSlop);
                    }
                } else {
                    // boolean
                    if (positionCount == 1) {
                        // only one position, with synonyms
                        return analyzeBoolean(field, stream);
                    } else {
                        // complex case: multiple positions
                        return analyzeMultiBoolean(field, stream, operator);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        @Override
        protected Query newTermQuery(Term term) {
            return blendTermQuery(term, mapper);
        }

        @Override
        protected Query newSynonymQuery(Term[] terms) {
            return blendTermsQuery(terms, mapper);
        }

        public Query createPhrasePrefixQuery(String field, String queryText, int phraseSlop, int maxExpansions) {
            final Query query = createFieldQuery(getAnalyzer(), Occur.MUST, field, queryText, true, phraseSlop);
            float boost = 1;
            Query innerQuery = query;
            while (innerQuery instanceof BoostQuery) {
                BoostQuery bq = (BoostQuery) innerQuery;
                boost *= bq.getBoost();
                innerQuery = bq.getQuery();
            }
            final MultiPhrasePrefixQuery prefixQuery = new MultiPhrasePrefixQuery();
            prefixQuery.setMaxExpansions(maxExpansions);
            prefixQuery.setSlop(phraseSlop);
            if (innerQuery instanceof PhraseQuery) {
                PhraseQuery pq = (PhraseQuery) innerQuery;
                Term[] terms = pq.getTerms();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(new Term[]{terms[i]}, positions[i]);
                }
                return boost == 1 ? prefixQuery : new BoostQuery(prefixQuery, boost);
            } else if (innerQuery instanceof MultiPhraseQuery) {
                MultiPhraseQuery pq = (MultiPhraseQuery) innerQuery;
                Term[][] terms = pq.getTermArrays();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(terms[i], positions[i]);
                }
                return boost == 1 ? prefixQuery : new BoostQuery(prefixQuery, boost);
            } else if (innerQuery instanceof TermQuery) {
                prefixQuery.add(((TermQuery) innerQuery).getTerm());
                return boost == 1 ? prefixQuery : new BoostQuery(prefixQuery, boost);
            } else if (innerQuery instanceof AllTermQuery) {
                prefixQuery.add(((AllTermQuery) innerQuery).getTerm());
                return boost == 1 ? prefixQuery : new BoostQuery(prefixQuery, boost);
            }
            return query;
        }

        public Query createCommonTermsQuery(String field, String queryText, Occur highFreqOccur, Occur lowFreqOccur, float
            maxTermFrequency, MappedFieldType fieldType) {
            Query booleanQuery = createBooleanQuery(field, queryText, lowFreqOccur);
            if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) booleanQuery;
                ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency, (
                    (BooleanQuery) booleanQuery).isCoordDisabled(), fieldType);
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

        /**
         * Creates a query from a graph token stream by extracting all the finite strings from the graph and using them to create the query.
         */
        protected Query analyzeGraph(TokenStream source, BooleanClause.Occur operator, String field, boolean quoted, int phraseSlop)
            throws IOException {
            source.reset();
            GraphTokenStreamFiniteStrings graphTokenStreams = new GraphTokenStreamFiniteStrings();
            List<TokenStream> tokenStreams = graphTokenStreams.getTokenStreams(source);

            if (tokenStreams.isEmpty()) {
                return null;
            }

            List<Query> queries = new ArrayList<>(tokenStreams.size());
            for (TokenStream ts : tokenStreams) {
                Query query = createFieldQuery(ts, operator, field, quoted, phraseSlop);
                if (query != null) {
                    queries.add(query);
                }
            }

            return new GraphQuery(queries.toArray(new Query[0]));
        }
    }

    protected Query blendTermsQuery(Term[] terms, MappedFieldType fieldType) {
        return new SynonymQuery(terms);
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
                    if (lenient) {
                        return new TermQuery(term);
                    } else {
                        throw e;
                    }
                }
            }
            int edits = fuzziness.asDistance(term.text());
            FuzzyQuery query = new FuzzyQuery(term, edits, fuzzyPrefixLength, maxExpansions, transpositions);
            QueryParsers.setRewriteMethod(query, fuzzyRewriteMethod);
            return query;
        }
        if (fieldType != null) {
            Query query = termQuery(fieldType, term.bytes(), lenient);
            if (query != null) {
                return query;
            }
        }
        return new TermQuery(term);
    }

}
