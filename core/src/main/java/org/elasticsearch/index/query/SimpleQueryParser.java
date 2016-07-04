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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;

/**
 * Wrapper class for Lucene's SimpleQueryParser that allows us to redefine
 * different types of queries.
 */
public class SimpleQueryParser extends org.apache.lucene.queryparser.simple.SimpleQueryParser {

    private final Settings settings;
    private QueryShardContext context;

    /** Creates a new parser with custom flags used to enable/disable certain features. */
    public SimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags,
                             Settings settings, QueryShardContext context) {
        super(analyzer, weights, flags);
        this.settings = settings;
        this.context = context;
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
    protected Query newTermQuery(Term term) {
        MappedFieldType currentFieldType = context.fieldMapper(term.field());
        if (currentFieldType == null || currentFieldType.tokenized()) {
            return super.newTermQuery(term);
        }
        return currentFieldType.termQuery(term.bytes(), context);
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
     * of {@code TermQuery}s and {@code PrefixQuery}s
     */
    private Query newPossiblyAnalyzedQuery(String field, String termStr) {
        List<List<String>> tlist = new ArrayList<> ();
        // get Analyzer from superclass and tokenize the term
        try (TokenStream source = getAnalyzer().tokenStream(field, termStr)) {
            source.reset();
            List<String> currentPos = new ArrayList<>();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posAtt = source.addAttribute(PositionIncrementAttribute.class);

            try {
                boolean hasMoreTokens = source.incrementToken();
                while (hasMoreTokens) {
                    if (currentPos.isEmpty() == false && posAtt.getPositionIncrement() > 0) {
                        tlist.add(currentPos);
                        currentPos = new ArrayList<>();
                    }
                    currentPos.add(termAtt.toString());
                    hasMoreTokens = source.incrementToken();
                }
                if (currentPos.isEmpty() == false) {
                    tlist.add(currentPos);
                }
            } catch (IOException e) {
                // ignore
                // TODO: we should not ignore the exception and return a prefix query with the original term ?
            }
        } catch (IOException e) {
            // Bail on any exceptions, going with a regular prefix query
            return new PrefixQuery(new Term(field, termStr));
        }

        if (tlist.size() == 0) {
            return null;
        }

        if (tlist.size() == 1 && tlist.get(0).size() == 1) {
            return new PrefixQuery(new Term(field, tlist.get(0).get(0)));
        }

        // build a boolean query with prefix on the last position only.
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int pos = 0; pos < tlist.size(); pos++) {
            List<String> plist = tlist.get(pos);
            boolean isLastPos = (pos == tlist.size()-1);
            Query posQuery;
            if (plist.size() == 1) {
                if (isLastPos) {
                    posQuery = new PrefixQuery(new Term(field, plist.get(0)));
                } else {
                    posQuery = newTermQuery(new Term(field, plist.get(0)));
                }
            } else if (isLastPos == false) {
                // build a synonym query for terms in the same position.
                Term[] terms = new Term[plist.size()];
                for (int i = 0; i < plist.size(); i++) {
                    terms[i] = new Term(field, plist.get(i));
                }
                posQuery = new SynonymQuery(terms);
            } else {
                BooleanQuery.Builder innerBuilder = new BooleanQuery.Builder();
                for (String token : plist) {
                    innerBuilder.add(new BooleanClause(new PrefixQuery(new Term(field, token)),
                        BooleanClause.Occur.SHOULD));
                }
                posQuery = innerBuilder.setDisableCoord(true).build();
            }
            builder.add(new BooleanClause(posQuery, getDefaultOperator()));
        }
        return builder.build();
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
