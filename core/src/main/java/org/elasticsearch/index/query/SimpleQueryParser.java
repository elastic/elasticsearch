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
package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Wrapper class for Lucene's SimpleQueryParser that allows us to redefine
 * different types of queries.
 */
public class SimpleQueryParser extends org.apache.lucene.queryparser.simple.SimpleQueryParser {

    private final Settings settings;

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags, Settings settings) {
        super(analyzer, weights, flags);
        this.settings = settings;
    }

    /**
     * Rethrow the runtime exception, unless the lenient flag has been set, returns null
     */
    private Query rethrowUnlessLenient(RuntimeException e) {
        if (settings.lenient()) {
            return null;
        }
        throw e;
    }

    @Override
    public Query newDefaultQuery(String text) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.setDisableCoord(true);
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            try {
                Query q = createBooleanQuery(entry.getKey(), text, super.getDefaultOperator());
                if (q != null) {
                    q.setBoost(entry.getValue());
                    bq.add(q, BooleanClause.Occur.SHOULD);
                }
            } catch (RuntimeException e) {
                rethrowUnlessLenient(e);
            }
        }
        return super.simplify(bq.build());
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newFuzzyQuery, optionally
     * lowercasing the term first
     */
    @Override
    public Query newFuzzyQuery(String text, int fuzziness) {
        if (settings.lowercaseExpandedTerms()) {
            text = text.toLowerCase(settings.locale());
        }
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.setDisableCoord(true);
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            try {
                Query q = new FuzzyQuery(new Term(entry.getKey(), text), fuzziness);
                q.setBoost(entry.getValue());
                bq.add(q, BooleanClause.Occur.SHOULD);
            } catch (RuntimeException e) {
                rethrowUnlessLenient(e);
            }
        }
        return super.simplify(bq.build());
    }

    @Override
    public Query newPhraseQuery(String text, int slop) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.setDisableCoord(true);
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            try {
                Query q = createPhraseQuery(entry.getKey(), text, slop);
                if (q != null) {
                    q.setBoost(entry.getValue());
                    bq.add(q, BooleanClause.Occur.SHOULD);
                }
            } catch (RuntimeException e) {
                rethrowUnlessLenient(e);
            }
        }
        return super.simplify(bq.build());
    }

    /**
     * Dispatches to Lucene's SimpleQueryParser's newPrefixQuery, optionally
     * lowercasing the term first or trying to analyze terms
     */
    @Override
    public Query newPrefixQuery(String text) {
        if (settings.lowercaseExpandedTerms()) {
            text = text.toLowerCase(settings.locale());
        }
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.setDisableCoord(true);
        for (Map.Entry<String,Float> entry : weights.entrySet()) {
            try {
                if (settings.analyzeWildcard()) {
                    Query analyzedQuery = newPossiblyAnalyzedQuery(entry.getKey(), text);
                    analyzedQuery.setBoost(entry.getValue());
                    bq.add(analyzedQuery, BooleanClause.Occur.SHOULD);
                } else {
                    PrefixQuery prefix = new PrefixQuery(new Term(entry.getKey(), text));
                    prefix.setBoost(entry.getValue());
                    bq.add(prefix, BooleanClause.Occur.SHOULD);
                }
            } catch (RuntimeException e) {
                return rethrowUnlessLenient(e);
            }
        }
        return super.simplify(bq.build());
    }

    /**
     * Analyze the given string using its analyzer, constructing either a
     * {@code PrefixQuery} or a {@code BooleanQuery} made up
     * of {@code PrefixQuery}s
     */
    private Query newPossiblyAnalyzedQuery(String field, String termStr) {
        try (TokenStream source = getAnalyzer().tokenStream(field, termStr)) {
            // Use the analyzer to get all the tokens, and then build a TermQuery,
            // PhraseQuery, or nothing based on the term count
            CachingTokenFilter buffer = new CachingTokenFilter(source);
            buffer.reset();

            TermToBytesRefAttribute termAtt = null;
            int numTokens = 0;
            boolean hasMoreTokens = false;
            termAtt = buffer.getAttribute(TermToBytesRefAttribute.class);
            if (termAtt != null) {
                try {
                    hasMoreTokens = buffer.incrementToken();
                    while (hasMoreTokens) {
                        numTokens++;
                        hasMoreTokens = buffer.incrementToken();
                    }
                } catch (IOException e) {
                    // ignore
                }
            }

            // rewind buffer
            buffer.reset();

            if (numTokens == 0) {
                return null;
            } else if (numTokens == 1) {
                try {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                } catch (IOException e) {
                    // safe to ignore, because we know the number of tokens
                }
                return new PrefixQuery(new Term(field, BytesRef.deepCopyOf(termAtt.getBytesRef())));
            } else {
                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                for (int i = 0; i < numTokens; i++) {
                    try {
                        boolean hasNext = buffer.incrementToken();
                        assert hasNext == true;
                    } catch (IOException e) {
                        // safe to ignore, because we know the number of tokens
                    }
                    bq.add(new BooleanClause(new PrefixQuery(new Term(field, BytesRef.deepCopyOf(termAtt.getBytesRef()))), BooleanClause.Occur.SHOULD));
                }
                return bq.build();
            }
        } catch (IOException e) {
            // Bail on any exceptions, going with a regular prefix query
            return new PrefixQuery(new Term(field, termStr));
        }
    }

    /**
     * Class encapsulating the settings for the SimpleQueryString query, with
     * their default values
     */
    public static class Settings {
        private Locale locale = Locale.ROOT;
        private boolean lowercaseExpandedTerms = true;
        private boolean lenient = false;
        private boolean analyzeWildcard = false;

        public Settings() {

        }

        public void locale(Locale locale) {
            this.locale = locale;
        }

        public Locale locale() {
            return this.locale;
        }

        public void lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
            this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        }

        public boolean lowercaseExpandedTerms() {
            return this.lowercaseExpandedTerms;
        }

        public void lenient(boolean lenient) {
            this.lenient = lenient;
        }

        public boolean lenient() {
            return this.lenient;
        }

        public void analyzeWildcard(boolean analyzeWildcard) {
            this.analyzeWildcard = analyzeWildcard;
        }

        public boolean analyzeWildcard() {
            return analyzeWildcard;
        }
    }
}
