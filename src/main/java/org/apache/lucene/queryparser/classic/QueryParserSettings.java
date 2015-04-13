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

package org.apache.lucene.queryparser.classic;

import com.carrotsearch.hppc.ObjectFloatOpenHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.automaton.Operations;
import org.joda.time.DateTimeZone;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 *
 */
public class QueryParserSettings {

    public static final boolean DEFAULT_ALLOW_LEADING_WILDCARD = true;
    public static final boolean DEFAULT_ANALYZE_WILDCARD = false;
    public static final float DEFAULT_BOOST = 1.f;

    private String queryString;
    private String defaultField;
    private float boost = DEFAULT_BOOST;
    private MapperQueryParser.Operator defaultOperator = QueryParser.Operator.OR;
    private boolean autoGeneratePhraseQueries = false;
    private boolean allowLeadingWildcard = DEFAULT_ALLOW_LEADING_WILDCARD;
    private boolean lowercaseExpandedTerms = true;
    private boolean enablePositionIncrements = true;
    private int phraseSlop = 0;
    private float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
    private int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
    private int fuzzyMaxExpansions = FuzzyQuery.defaultMaxExpansions;
    private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
    private MultiTermQuery.RewriteMethod fuzzyRewriteMethod = null;
    private boolean analyzeWildcard = DEFAULT_ANALYZE_WILDCARD;
    private boolean escape = false;
    private Analyzer defaultAnalyzer = null;
    private Analyzer defaultQuoteAnalyzer = null;
    private Analyzer forcedAnalyzer = null;
    private Analyzer forcedQuoteAnalyzer = null;
    private String quoteFieldSuffix = null;
    private MultiTermQuery.RewriteMethod rewriteMethod = MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE;
    private String minimumShouldMatch;
    private boolean lenient;
    private Locale locale;
    private DateTimeZone timeZone;

    List<String> fields = null;
    Collection<String> queryTypes = null;
    ObjectFloatOpenHashMap<String> boosts = null;
    float tieBreaker = 0.0f;
    boolean useDisMax = true;

    public boolean isCacheable() {
        // a hack for now :) to determine if a query string is cacheable
        return !queryString.contains("now");
    }

    public String queryString() {
        return queryString;
    }

    public void queryString(String queryString) {
        this.queryString = queryString;
    }

    public String defaultField() {
        return defaultField;
    }

    public void defaultField(String defaultField) {
        this.defaultField = defaultField;
    }

    public float boost() {
        return boost;
    }

    public void boost(float boost) {
        this.boost = boost;
    }

    public QueryParser.Operator defaultOperator() {
        return defaultOperator;
    }

    public void defaultOperator(QueryParser.Operator defaultOperator) {
        this.defaultOperator = defaultOperator;
    }

    public boolean autoGeneratePhraseQueries() {
        return autoGeneratePhraseQueries;
    }

    public void autoGeneratePhraseQueries(boolean autoGeneratePhraseQueries) {
        this.autoGeneratePhraseQueries = autoGeneratePhraseQueries;
    }

    public int maxDeterminizedStates() {
        return maxDeterminizedStates;
    }

    public void maxDeterminizedStates(int maxDeterminizedStates) {
        this.maxDeterminizedStates = maxDeterminizedStates;
    }

    public boolean allowLeadingWildcard() {
        return allowLeadingWildcard;
    }

    public void allowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
    }

    public boolean lowercaseExpandedTerms() {
        return lowercaseExpandedTerms;
    }

    public void lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
    }

    public boolean enablePositionIncrements() {
        return enablePositionIncrements;
    }

    public void enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
    }

    public int phraseSlop() {
        return phraseSlop;
    }

    public void phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
    }

    public float fuzzyMinSim() {
        return fuzzyMinSim;
    }

    public void fuzzyMinSim(float fuzzyMinSim) {
        this.fuzzyMinSim = fuzzyMinSim;
    }

    public int fuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    public void fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
    }

    public int fuzzyMaxExpansions() {
        return fuzzyMaxExpansions;
    }

    public void fuzzyMaxExpansions(int fuzzyMaxExpansions) {
        this.fuzzyMaxExpansions = fuzzyMaxExpansions;
    }

    public MultiTermQuery.RewriteMethod fuzzyRewriteMethod() {
        return fuzzyRewriteMethod;
    }

    public void fuzzyRewriteMethod(MultiTermQuery.RewriteMethod fuzzyRewriteMethod) {
        this.fuzzyRewriteMethod = fuzzyRewriteMethod;
    }

    public boolean escape() {
        return escape;
    }

    public void escape(boolean escape) {
        this.escape = escape;
    }

    public Analyzer defaultAnalyzer() {
        return defaultAnalyzer;
    }

    public void defaultAnalyzer(Analyzer defaultAnalyzer) {
        this.defaultAnalyzer = defaultAnalyzer;
    }

    public Analyzer defaultQuoteAnalyzer() {
        return defaultQuoteAnalyzer;
    }

    public void defaultQuoteAnalyzer(Analyzer defaultAnalyzer) {
        this.defaultQuoteAnalyzer = defaultAnalyzer;
    }

    public Analyzer forcedAnalyzer() {
        return forcedAnalyzer;
    }

    public void forcedAnalyzer(Analyzer forcedAnalyzer) {
        this.forcedAnalyzer = forcedAnalyzer;
    }

    public Analyzer forcedQuoteAnalyzer() {
        return forcedQuoteAnalyzer;
    }

    public void forcedQuoteAnalyzer(Analyzer forcedAnalyzer) {
        this.forcedQuoteAnalyzer = forcedAnalyzer;
    }

    public boolean analyzeWildcard() {
        return this.analyzeWildcard;
    }

    public void analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
    }

    public MultiTermQuery.RewriteMethod rewriteMethod() {
        return this.rewriteMethod;
    }

    public void rewriteMethod(MultiTermQuery.RewriteMethod rewriteMethod) {
        this.rewriteMethod = rewriteMethod;
    }

    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    public void minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public void quoteFieldSuffix(String quoteFieldSuffix) {
        this.quoteFieldSuffix = quoteFieldSuffix;
    }

    public String quoteFieldSuffix() {
        return this.quoteFieldSuffix;
    }

    public void lenient(boolean lenient) {
        this.lenient = lenient;
    }

    public boolean lenient() {
        return this.lenient;
    }

    public List<String> fields() {
        return fields;
    }

    public void fields(List<String> fields) {
        this.fields = fields;
    }

    public Collection<String> queryTypes() {
        return queryTypes;
    }

    public void queryTypes(Collection<String> queryTypes) {
        this.queryTypes = queryTypes;
    }

    public ObjectFloatOpenHashMap<String> boosts() {
        return boosts;
    }

    public void boosts(ObjectFloatOpenHashMap<String> boosts) {
        this.boosts = boosts;
    }

    public float tieBreaker() {
        return tieBreaker;
    }

    public void tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public boolean useDisMax() {
        return useDisMax;
    }

    public void useDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    public void locale(Locale locale) {
        this.locale = locale;
    }

    public Locale locale() {
        return this.locale;
    }

    public void timeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public DateTimeZone timeZone() {
        return this.timeZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryParserSettings that = (QueryParserSettings) o;

        if (autoGeneratePhraseQueries != that.autoGeneratePhraseQueries()) return false;
        if (maxDeterminizedStates != that.maxDeterminizedStates()) return false;
        if (allowLeadingWildcard != that.allowLeadingWildcard) return false;
        if (Float.compare(that.boost, boost) != 0) return false;
        if (enablePositionIncrements != that.enablePositionIncrements) return false;
        if (escape != that.escape) return false;
        if (analyzeWildcard != that.analyzeWildcard) return false;
        if (Float.compare(that.fuzzyMinSim, fuzzyMinSim) != 0) return false;
        if (fuzzyPrefixLength != that.fuzzyPrefixLength) return false;
        if (fuzzyMaxExpansions != that.fuzzyMaxExpansions) return false;
        if (fuzzyRewriteMethod != null ? !fuzzyRewriteMethod.equals(that.fuzzyRewriteMethod) : that.fuzzyRewriteMethod != null)
            return false;
        if (lowercaseExpandedTerms != that.lowercaseExpandedTerms) return false;
        if (phraseSlop != that.phraseSlop) return false;
        if (defaultAnalyzer != null ? !defaultAnalyzer.equals(that.defaultAnalyzer) : that.defaultAnalyzer != null)
            return false;
        if (defaultQuoteAnalyzer != null ? !defaultQuoteAnalyzer.equals(that.defaultQuoteAnalyzer) : that.defaultQuoteAnalyzer != null)
            return false;
        if (forcedAnalyzer != null ? !forcedAnalyzer.equals(that.forcedAnalyzer) : that.forcedAnalyzer != null)
            return false;
        if (forcedQuoteAnalyzer != null ? !forcedQuoteAnalyzer.equals(that.forcedQuoteAnalyzer) : that.forcedQuoteAnalyzer != null)
            return false;
        if (defaultField != null ? !defaultField.equals(that.defaultField) : that.defaultField != null) return false;
        if (defaultOperator != that.defaultOperator) return false;
        if (queryString != null ? !queryString.equals(that.queryString) : that.queryString != null) return false;
        if (rewriteMethod != null ? !rewriteMethod.equals(that.rewriteMethod) : that.rewriteMethod != null)
            return false;
        if (minimumShouldMatch != null ? !minimumShouldMatch.equals(that.minimumShouldMatch) : that.minimumShouldMatch != null)
            return false;
        if (quoteFieldSuffix != null ? !quoteFieldSuffix.equals(that.quoteFieldSuffix) : that.quoteFieldSuffix != null)
            return false;
        if (lenient != that.lenient) {
            return false;
        }
        if (locale != null ? !locale.equals(that.locale) : that.locale != null) {
            return false;
        }
        if (timeZone != null ? !timeZone.equals(that.timeZone) : that.timeZone != null) {
            return false;
        }

        if (Float.compare(that.tieBreaker, tieBreaker) != 0) return false;
        if (useDisMax != that.useDisMax) return false;
        if (boosts != null ? !boosts.equals(that.boosts) : that.boosts != null) return false;
        if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
        if (queryTypes != null ? !queryTypes.equals(that.queryTypes) : that.queryTypes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = queryString != null ? queryString.hashCode() : 0;
        result = 31 * result + (defaultField != null ? defaultField.hashCode() : 0);
        result = 31 * result + (boost != +0.0f ? Float.floatToIntBits(boost) : 0);
        result = 31 * result + (defaultOperator != null ? defaultOperator.hashCode() : 0);
        result = 31 * result + (autoGeneratePhraseQueries ? 1 : 0);
        result = 31 * result + maxDeterminizedStates;
        result = 31 * result + (allowLeadingWildcard ? 1 : 0);
        result = 31 * result + (lowercaseExpandedTerms ? 1 : 0);
        result = 31 * result + (enablePositionIncrements ? 1 : 0);
        result = 31 * result + phraseSlop;
        result = 31 * result + (fuzzyMinSim != +0.0f ? Float.floatToIntBits(fuzzyMinSim) : 0);
        result = 31 * result + fuzzyPrefixLength;
        result = 31 * result + (escape ? 1 : 0);
        result = 31 * result + (defaultAnalyzer != null ? defaultAnalyzer.hashCode() : 0);
        result = 31 * result + (defaultQuoteAnalyzer != null ? defaultQuoteAnalyzer.hashCode() : 0);
        result = 31 * result + (forcedAnalyzer != null ? forcedAnalyzer.hashCode() : 0);
        result = 31 * result + (forcedQuoteAnalyzer != null ? forcedQuoteAnalyzer.hashCode() : 0);
        result = 31 * result + (analyzeWildcard ? 1 : 0);

        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (queryTypes != null ? queryTypes.hashCode() : 0);
        result = 31 * result + (boosts != null ? boosts.hashCode() : 0);
        result = 31 * result + (tieBreaker != +0.0f ? Float.floatToIntBits(tieBreaker) : 0);
        result = 31 * result + (useDisMax ? 1 : 0);
        result = 31 * result + (locale != null ? locale.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        return result;
    }
}
