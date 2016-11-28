/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;

/**
 * Creates queries from the {@link Analyzer} chain.
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   QueryBuilder builder = new QueryBuilder(analyzer);
 *   Query a = builder.createBooleanQuery("body", "just a test");
 *   Query b = builder.createPhraseQuery("body", "another test");
 *   Query c = builder.createMinShouldMatchQuery("body", "another test", 0.5f);
 * </pre>
 * <p>
 * This can also be used as a subclass for query parsers to make it easier
 * to interact with the analysis chain. Factory methods such as {@code newTermQuery}
 * are provided so that the generated queries can be customized.
 *
 * TODO: un-fork once we are on Lucene 6.4.0
 * This is only forked due to `createFieldQuery` being final and the analyze* methods being private.  Lucene 6.4.0 removes final and will
 * make the private methods protected allowing us to override it.
 */
public class XQueryBuilder {
    private Analyzer analyzer;
    private boolean enablePositionIncrements = true;

    /** Creates a new QueryBuilder using the given analyzer. */
    public XQueryBuilder(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    /**
     * Creates a boolean query from the query text.
     * <p>
     * This is equivalent to {@code createBooleanQuery(field, queryText, Occur.SHOULD)}
     * @param field field name
     * @param queryText text to be passed to the analyzer
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis
     *         of {@code queryText}
     */
    public Query createBooleanQuery(String field, String queryText) {
        return createBooleanQuery(field, queryText, BooleanClause.Occur.SHOULD);
    }

    /**
     * Creates a boolean query from the query text.
     * <p>
     * @param field field name
     * @param queryText text to be passed to the analyzer
     * @param operator operator used for clauses between analyzer tokens.
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis
     *         of {@code queryText}
     */
    public Query createBooleanQuery(String field, String queryText, BooleanClause.Occur operator) {
        if (operator != BooleanClause.Occur.SHOULD && operator != BooleanClause.Occur.MUST) {
            throw new IllegalArgumentException("invalid operator: only SHOULD or MUST are allowed");
        }
        return createFieldQuery(analyzer, operator, field, queryText, false, 0);
    }

    /**
     * Creates a phrase query from the query text.
     * <p>
     * This is equivalent to {@code createPhraseQuery(field, queryText, 0)}
     * @param field field name
     * @param queryText text to be passed to the analyzer
     * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
     *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
     */
    public Query createPhraseQuery(String field, String queryText) {
        return createPhraseQuery(field, queryText, 0);
    }

    /**
     * Creates a phrase query from the query text.
     * <p>
     * @param field field name
     * @param queryText text to be passed to the analyzer
     * @param phraseSlop number of other words permitted between words in query phrase
     * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or
     *         {@code MultiPhraseQuery}, based on the analysis of {@code queryText}
     */
    public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
        return createFieldQuery(analyzer, BooleanClause.Occur.MUST, field, queryText, true, phraseSlop);
    }

    /**
     * Creates a minimum-should-match query from the query text.
     * <p>
     * @param field field name
     * @param queryText text to be passed to the analyzer
     * @param fraction of query terms {@code [0..1]} that should match
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis
     *         of {@code queryText}
     */
    public Query createMinShouldMatchQuery(String field, String queryText, float fraction) {
        if (Float.isNaN(fraction) || fraction < 0 || fraction > 1) {
            throw new IllegalArgumentException("fraction should be >= 0 and <= 1");
        }

        // TODO: wierd that BQ equals/rewrite/scorer doesn't handle this?
        if (fraction == 1) {
            return createBooleanQuery(field, queryText, BooleanClause.Occur.MUST);
        }

        Query query = createFieldQuery(analyzer, BooleanClause.Occur.SHOULD, field, queryText, false, 0);
        if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setDisableCoord(bq.isCoordDisabled());
            builder.setMinimumNumberShouldMatch((int) (fraction * bq.clauses().size()));
            for (BooleanClause clause : bq) {
                builder.add(clause);
            }
            query = builder.build();
        }
        return query;
    }

    /**
     * Returns the analyzer.
     * @see #setAnalyzer(Analyzer)
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Sets the analyzer used to tokenize text.
     */
    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    /**
     * Returns true if position increments are enabled.
     * @see #setEnablePositionIncrements(boolean)
     */
    public boolean getEnablePositionIncrements() {
        return enablePositionIncrements;
    }

    /**
     * Set to <code>true</code> to enable position increments in result query.
     * <p>
     * When set, result phrase and multi-phrase queries will
     * be aware of position increments.
     * Useful when e.g. a StopFilter increases the position increment of
     * the token that follows an omitted token.
     * <p>
     * Default: true.
     */
    public void setEnablePositionIncrements(boolean enable) {
        this.enablePositionIncrements = enable;
    }

    /**
     * Creates a query from the analysis chain.
     * <p>
     * Expert: this is more useful for subclasses such as queryparsers.
     * If using this class directly, just use {@link #createBooleanQuery(String, String)}
     * and {@link #createPhraseQuery(String, String)}
     * @param analyzer analyzer used for this query
     * @param operator default boolean operator used for this query
     * @param field field to create queries against
     * @param queryText text to be passed to the analysis chain
     * @param quoted true if phrases should be generated when terms occur at more than one position
     * @param phraseSlop slop factor for phrase/multiphrase queries
     */
    protected Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText, boolean quoted,
                                     int phraseSlop) {
        assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

        // Use the analyzer to get all the tokens, and then build an appropriate
        // query based on the analysis chain.

        try (TokenStream source = analyzer.tokenStream(field, queryText);
             CachingTokenFilter stream = new CachingTokenFilter(source)) {

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);

            if (termAtt == null) {
                return null;
            }

            // phase 1: read through the stream and assess the situation:
            // counting the number of tokens/positions and marking if we have any synonyms.

            int numTokens = 0;
            int positionCount = 0;
            boolean hasSynonyms = false;

            stream.reset();
            while (stream.incrementToken()) {
                numTokens++;
                int positionIncrement = posIncAtt.getPositionIncrement();
                if (positionIncrement != 0) {
                    positionCount += positionIncrement;
                } else {
                    hasSynonyms = true;
                }
            }

            // phase 2: based on token count, presence of synonyms, and options
            // formulate a single term, boolean, or phrase.

            if (numTokens == 0) {
                return null;
            } else if (numTokens == 1) {
                // single term
                return analyzeTerm(field, stream);
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

    /**
     * Creates simple term query from the cached tokenstream contents
     */
    protected Query analyzeTerm(String field, TokenStream stream) throws IOException {
        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        stream.reset();
        if (!stream.incrementToken()) {
            throw new AssertionError();
        }

        return newTermQuery(new Term(field, termAtt.getBytesRef()));
    }

    /**
     * Creates simple boolean query from the cached tokenstream contents
     */
    protected Query analyzeBoolean(String field, TokenStream stream) throws IOException {
        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        stream.reset();
        List<Term> terms = new ArrayList<>();
        while (stream.incrementToken()) {
            terms.add(new Term(field, termAtt.getBytesRef()));
        }

        return newSynonymQuery(terms.toArray(new Term[terms.size()]));
    }

    protected void add(BooleanQuery.Builder q, List<Term> current, BooleanClause.Occur operator) {
        if (current.isEmpty()) {
            return;
        }
        if (current.size() == 1) {
            q.add(newTermQuery(current.get(0)), operator);
        } else {
            q.add(newSynonymQuery(current.toArray(new Term[current.size()])), operator);
        }
    }

    /**
     * Creates complex boolean query from the cached tokenstream contents
     */
    protected Query analyzeMultiBoolean(String field, TokenStream stream, BooleanClause.Occur operator) throws IOException {
        BooleanQuery.Builder q = newBooleanQuery();
        List<Term> currentQuery = new ArrayList<>();

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);

        stream.reset();
        while (stream.incrementToken()) {
            if (posIncrAtt.getPositionIncrement() != 0) {
                add(q, currentQuery, operator);
                currentQuery.clear();
            }
            currentQuery.add(new Term(field, termAtt.getBytesRef()));
        }
        add(q, currentQuery, operator);

        return q.build();
    }

    /**
     * Creates simple phrase query from the cached tokenstream contents
     */
    protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        stream.reset();
        while (stream.incrementToken()) {
            if (enablePositionIncrements) {
                position += posIncrAtt.getPositionIncrement();
            } else {
                position += 1;
            }
            builder.add(new Term(field, termAtt.getBytesRef()), position);
        }

        return builder.build();
    }

    /**
     * Creates complex phrase query from the cached tokenstream contents
     */
    protected Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
        MultiPhraseQuery.Builder mpqb = newMultiPhraseQueryBuilder();
        mpqb.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        List<Term> multiTerms = new ArrayList<>();
        stream.reset();
        while (stream.incrementToken()) {
            int positionIncrement = posIncrAtt.getPositionIncrement();

            if (positionIncrement > 0 && multiTerms.size() > 0) {
                if (enablePositionIncrements) {
                    mpqb.add(multiTerms.toArray(new Term[0]), position);
                } else {
                    mpqb.add(multiTerms.toArray(new Term[0]));
                }
                multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, termAtt.getBytesRef()));
        }

        if (enablePositionIncrements) {
            mpqb.add(multiTerms.toArray(new Term[0]), position);
        } else {
            mpqb.add(multiTerms.toArray(new Term[0]));
        }
        return mpqb.build();
    }

    /**
     * Builds a new BooleanQuery instance.
     * <p>
     * This is intended for subclasses that wish to customize the generated queries.
     * @return new BooleanQuery instance
     */
    protected BooleanQuery.Builder newBooleanQuery() {
        return new BooleanQuery.Builder();
    }

    /**
     * Builds a new SynonymQuery instance.
     * <p>
     * This is intended for subclasses that wish to customize the generated queries.
     * @return new Query instance
     */
    protected Query newSynonymQuery(Term terms[]) {
        return new SynonymQuery(terms);
    }

    /**
     * Builds a new TermQuery instance.
     * <p>
     * This is intended for subclasses that wish to customize the generated queries.
     * @param term term
     * @return new TermQuery instance
     */
    protected Query newTermQuery(Term term) {
        return new TermQuery(term);
    }

    /**
     * Builds a new MultiPhraseQuery instance.
     * <p>
     * This is intended for subclasses that wish to customize the generated queries.
     * @return new MultiPhraseQuery instance
     */
    protected MultiPhraseQuery.Builder newMultiPhraseQueryBuilder() {
        return new MultiPhraseQuery.Builder();
    }
}
