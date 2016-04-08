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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
                    bq.add(wrapWithBoost(q, entry.getValue()), BooleanClause.Occur.SHOULD);
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
                Query query = new FuzzyQuery(new Term(entry.getKey(), text), fuzziness);
                bq.add(wrapWithBoost(query, entry.getValue()), BooleanClause.Occur.SHOULD);
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
                    bq.add(wrapWithBoost(q, entry.getValue()), BooleanClause.Occur.SHOULD);
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
                    if (analyzedQuery != null) {
                        bq.add(wrapWithBoost(analyzedQuery, entry.getValue()), BooleanClause.Occur.SHOULD);
                    }
                } else {
                    Query query = new PrefixQuery(new Term(entry.getKey(), text));
                    bq.add(wrapWithBoost(query, entry.getValue()), BooleanClause.Occur.SHOULD);
                }
            } catch (RuntimeException e) {
                return rethrowUnlessLenient(e);
            }
        }
        return super.simplify(bq.build());
    }

    private static Query wrapWithBoost(Query query, float boost) {
        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            return new BoostQuery(query, boost);
        }
        return query;
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
    static class Settings {
        /** Locale to use for parsing. */
        private Locale locale = SimpleQueryStringBuilder.DEFAULT_LOCALE;
        /** Specifies whether parsed terms should be lowercased. */
        private boolean lowercaseExpandedTerms = SimpleQueryStringBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS;
        /** Specifies whether lenient query parsing should be used. */
        private boolean lenient = SimpleQueryStringBuilder.DEFAULT_LENIENT;
        /** Specifies whether wildcards should be analyzed. */
        private boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;

        /**
         * Generates default {@link Settings} object (uses ROOT locale, does
         * lowercase terms, no lenient parsing, no wildcard analysis).
         * */
        public Settings() {
        }

        public Settings(Locale locale, Boolean lowercaseExpandedTerms, Boolean lenient, Boolean analyzeWildcard) {
            this.locale = locale;
            this.lowercaseExpandedTerms = lowercaseExpandedTerms;
            this.lenient = lenient;
            this.analyzeWildcard = analyzeWildcard;
        }

        /** Specifies the locale to use for parsing, Locale.ROOT by default. */
        public void locale(Locale locale) {
            this.locale = (locale != null) ? locale : SimpleQueryStringBuilder.DEFAULT_LOCALE;
        }

        /** Returns the locale to use for parsing. */
        public Locale locale() {
            return this.locale;
        }

        /**
         * Specifies whether to lowercase parse terms, defaults to true if
         * unset.
         */
        public void lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
            this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        }

        /** Returns whether to lowercase parse terms. */
        public boolean lowercaseExpandedTerms() {
            return this.lowercaseExpandedTerms;
        }

        /** Specifies whether to use lenient parsing, defaults to false. */
        public void lenient(boolean lenient) {
            this.lenient = lenient;
        }

        /** Returns whether to use lenient parsing. */
        public boolean lenient() {
            return this.lenient;
        }

        /** Specifies whether to analyze wildcards. Defaults to false if unset. */
        public void analyzeWildcard(boolean analyzeWildcard) {
            this.analyzeWildcard = analyzeWildcard;
        }

        /** Returns whether to analyze wildcards. */
        public boolean analyzeWildcard() {
            return analyzeWildcard;
        }

        @Override
        public int hashCode() {
            // checking the return value of toLanguageTag() for locales only.
            // For further reasoning see
            // https://issues.apache.org/jira/browse/LUCENE-4021
            return Objects.hash(locale.toLanguageTag(), lowercaseExpandedTerms, lenient, analyzeWildcard);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Settings other = (Settings) obj;

            // checking the return value of toLanguageTag() for locales only.
            // For further reasoning see
            // https://issues.apache.org/jira/browse/LUCENE-4021
            return (Objects.equals(locale.toLanguageTag(), other.locale.toLanguageTag())
                    && Objects.equals(lowercaseExpandedTerms, other.lowercaseExpandedTerms)
                    && Objects.equals(lenient, other.lenient)
                    && Objects.equals(analyzeWildcard, other.analyzeWildcard));
        }
    }
}
