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
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.elasticsearch.common.lucene.search.Queries.newLenientFieldQuery;
import static org.elasticsearch.common.lucene.search.Queries.newUnmappedFieldQuery;

public class MatchQuery {

    public enum Type implements Writeable {
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

        Type(int ordinal) {
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

    public enum ZeroTermsQuery implements Writeable {
        NONE(0),
        ALL(1),
        // this is used internally to make sure that query_string and simple_query_string
        // ignores query part that removes all tokens.
        NULL(2);

        private final int ordinal;

        ZeroTermsQuery(int ordinal) {
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

    protected Analyzer analyzer;

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

    protected boolean autoGenerateSynonymsPhraseQuery = true;

    public MatchQuery(QueryShardContext context) {
        this.context = context;
    }

    public void setAnalyzer(String analyzerName) {
        this.analyzer = context.getMapperService().getIndexAnalyzers().get(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }
    }

    public void setAnalyzer(Analyzer analyzer) {
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

    public void setAutoGenerateSynonymsPhraseQuery(boolean enabled) {
        this.autoGenerateSynonymsPhraseQuery = enabled;
    }

    public Query parse(Type type, String fieldName, Object value) throws IOException {
        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            return newUnmappedFieldQuery(fieldName);
        }
        Analyzer analyzer = getAnalyzer(fieldType, type == Type.PHRASE || type == Type.PHRASE_PREFIX);
        assert analyzer != null;

        MatchQueryBuilder builder = new MatchQueryBuilder(analyzer, fieldType);

        /*
         * If a keyword analyzer is used, we know that further analysis isn't
         * needed and can immediately return a term query.
         */
        if (analyzer == Lucene.KEYWORD_ANALYZER
                && type != Type.PHRASE_PREFIX) {
            return builder.newTermQuery(new Term(fieldName, value.toString()));
        }

        return parseInternal(type, fieldName, builder, value);
    }

    protected final Query parseInternal(Type type, String fieldName, MatchQueryBuilder builder, Object value) throws IOException {
        final Query query;
        switch (type) {
            case BOOLEAN:
                if (commonTermsCutoff == null) {
                    query = builder.createBooleanQuery(fieldName, value.toString(), occur);
                } else {
                    query = createCommonTermsQuery(builder, fieldName, value.toString(), occur, occur, commonTermsCutoff);
                }
                break;

            case PHRASE:
                query = builder.createPhraseQuery(fieldName, value.toString(), phraseSlop);
                break;

            case PHRASE_PREFIX:
                query = builder.createPhrasePrefixQuery(fieldName, value.toString(), phraseSlop);
                break;

            default:
                throw new IllegalStateException("No type found for [" + type + "]");
        }

        return query == null ? zeroTermsQuery() : query;
    }

    private Query createCommonTermsQuery(MatchQueryBuilder builder, String field, String queryText,
                                         Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency) {
        Query booleanQuery = builder.createBooleanQuery(field, queryText, lowFreqOccur);
        if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) booleanQuery;
            return boolToExtendedCommonTermsQuery(bq, highFreqOccur, lowFreqOccur, maxTermFrequency);
        }
        return booleanQuery;
    }

    private Query boolToExtendedCommonTermsQuery(BooleanQuery bq,
                                                 Occur highFreqOccur,
                                                 Occur lowFreqOccur,
                                                 float maxTermFrequency) {
        ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency);
        for (BooleanClause clause : bq.clauses()) {
            if ((clause.getQuery() instanceof TermQuery) == false) {
                return bq;
            }
            query.add(((TermQuery) clause.getQuery()).getTerm());
        }
        return query;
    }

    protected Analyzer getAnalyzer(MappedFieldType fieldType, boolean quoted) {
        if (analyzer == null) {
            return quoted ? context.getSearchQuoteAnalyzer(fieldType) : context.getSearchAnalyzer(fieldType);
        } else {
            return analyzer;
        }
    }

    protected Query zeroTermsQuery() {
        switch (zeroTermsQuery) {
            case NULL:
                return null;
            case NONE:
                return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
            case ALL:
                return Queries.newMatchAllQuery();
            default:
                throw new IllegalStateException("unknown zeroTermsQuery " + zeroTermsQuery);
        }
    }

    private boolean hasPositions(MappedFieldType fieldType) {
        return fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    class MatchQueryBuilder extends QueryBuilder {
        private final MappedFieldType fieldType;

        /**
         * Creates a new QueryBuilder using the given analyzer.
         */
        MatchQueryBuilder(Analyzer analyzer, MappedFieldType fieldType) {
            super(analyzer);
            this.fieldType = fieldType;
            if (hasPositions(fieldType)) {
                setAutoGenerateMultiTermSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
            } else {
                setAutoGenerateMultiTermSynonymsPhraseQuery(false);
            }
            setEnablePositionIncrements(enablePositionIncrements);
        }

        /**
         * Checks if graph analysis should be enabled for the field depending
         * on the provided {@link Analyzer}
         */
        @Override
        protected Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field,
                                         String queryText, boolean quoted, int slop) {
            assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
            return createQuery(field, queryText, source -> createFieldQuery(source, operator, field, quoted, slop));
        }

        public Query createPhrasePrefixQuery(String field, String queryText, int slop) {
            return createQuery(field, queryText, source -> createPhrasePrefixQuery(source, field, slop));
        }

        private Query createQuery(String field, String queryText, CheckedFunction<TokenStream, Query, IOException> queryFunc) {
            // Use the analyzer to get all the tokens, and then build an appropriate
            // query based on the analysis chain.
            try (TokenStream source = analyzer.tokenStream(field, queryText)) {
                if (source.hasAttribute(DisableGraphAttribute.class)) {
                    /*
                     * A {@link TokenFilter} in this {@link TokenStream} disabled the graph analysis to avoid
                     * paths explosion. See {@link org.elasticsearch.index.analysis.ShingleTokenFilterFactory} for details.
                     */
                    setEnableGraphQueries(false);
                }
                try {
                    return queryFunc.apply(source);
                } finally {
                    setEnableGraphQueries(true);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        private Query createPhrasePrefixQuery(TokenStream source, String field, int slop) {
            // Build an appropriate phrase prefix query based on the analysis chain.
            try (CachingTokenFilter stream = new CachingTokenFilter(source)) {

                TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
                PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
                PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

                if (termAtt == null) {
                    return null;
                }

                int numTokens = 0;
                int positionCount = 0;
                boolean isGraph = false;

                stream.reset();
                while (stream.incrementToken()) {
                    numTokens++;
                    int positionIncrement = posIncAtt.getPositionIncrement();
                    if (positionIncrement != 0) {
                        positionCount += positionIncrement;
                    }

                    int positionLength = posLenAtt.getPositionLength();
                    if (enableGraphQueries && positionLength > 1) {
                        isGraph = true;
                    }
                }

                // phase 2: based on token count, presence of synonyms, and options
                // formulate a single term, boolean, or phrase.

                if (numTokens == 0) {
                    return null;
                } else if (isGraph) {
                    // graph
                    return analyzeGraphPhrasePrefix(stream, field, slop);
                } else {
                    // single position
                    return analyzePhrasePrefix(field, stream, slop, positionCount);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        @Override
        protected Query newTermQuery(Term term) {
            Supplier<Query> querySupplier;
            if (fuzziness != null) {
                querySupplier = () -> {
                    Query query = fieldType.fuzzyQuery(term.text(), fuzziness, fuzzyPrefixLength, maxExpansions, transpositions);
                    if (query instanceof FuzzyQuery) {
                        QueryParsers.setRewriteMethod((FuzzyQuery) query, fuzzyRewriteMethod);
                    }
                    return query;
                };
            } else {
                querySupplier = () -> fieldType.termQuery(term.bytes(), context);
            }
            try {
                Query query = querySupplier.get();
                return query;
            } catch (RuntimeException e) {
                if (lenient) {
                    return newLenientFieldQuery(fieldType.name(), e);
                } else {
                    throw e;
                }
            }
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            try {
                checkForPositions(field);
                return fieldType.phraseQuery(stream, slop, enablePositionIncrements);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        @Override
        protected Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
            try {
                checkForPositions(field);
                return fieldType.multiPhraseQuery(stream, slop, enablePositionIncrements);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        private Query analyzePhrasePrefix(String field, TokenStream stream, int slop, int positionCount) throws IOException {
            try {
                if (positionCount > 1) {
                    checkForPositions(field);
                }
                return fieldType.phrasePrefixQuery(stream, slop, maxExpansions, enablePositionIncrements);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        private Query analyzeGraphPhrasePrefix(TokenStream source, String field, int slop) throws IOException {
            source.reset();
            GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);
            /*
             * Creates a boolean query from the graph token stream by extracting all the finite strings from the graph
             * and using them to create phrase prefix queries with the appropriate slop.
             */
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            Iterator<TokenStream> it = graph.getFiniteStrings();
            while (it.hasNext()) {
                Query query = createPhrasePrefixQuery(it.next(), field, slop);
                if (query != null) {
                    builder.add(query, BooleanClause.Occur.SHOULD);
                }
            }
            return builder.build();
        }

        private void checkForPositions(String field) {
            if (hasPositions(fieldType) == false) {
                throw new IllegalStateException("field:[" + field + "] was indexed without position data; cannot run PhraseQuery");
            }
        }
    }
}
