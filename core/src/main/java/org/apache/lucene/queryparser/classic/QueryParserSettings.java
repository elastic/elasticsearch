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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.unit.Fuzziness;
import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.Map;

/**
 * Encapsulates settings that affect query_string parsing via {@link MapperQueryParser}
 */
public class QueryParserSettings {

    private String defaultField;

    private Map<String, Float> fieldsAndWeights = Collections.emptyMap();

    private QueryParser.Operator defaultOperator;

    private Analyzer analyzer;
    private Analyzer multiTermAnalyzer;
    private Analyzer quoteAnalyzer;

    private String quoteFieldSuffix;

    private boolean autoGeneratePhraseQueries;

    private boolean allowLeadingWildcard;

    private boolean analyzeWildcard;

    private boolean enablePositionIncrements;

    private Fuzziness fuzziness = Fuzziness.AUTO;
    private int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
    private int fuzzyMaxExpansions = FuzzyQuery.defaultMaxExpansions;
    private MultiTermQuery.RewriteMethod fuzzyRewriteMethod;

    private int phraseSlop;

    private boolean useDisMax;

    private float tieBreaker;

    private MultiTermQuery.RewriteMethod rewriteMethod;

    private boolean lenient;

    private DateTimeZone timeZone;

    /** To limit effort spent determinizing regexp queries. */
    private int maxDeterminizedStates = Operations.DEFAULT_MAX_DETERMINIZED_STATES;

    public String defaultField() {
        return defaultField;
    }

    public void defaultField(String defaultField) {
        this.defaultField = defaultField;
    }

    public Map<String, Float> fieldsAndWeights() {
        return fieldsAndWeights;
    }

    public void fieldsAndWeights(Map<String, Float> fieldsAndWeights) {
        this.fieldsAndWeights = fieldsAndWeights;
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

    public void analyzer(Analyzer analyzer, Analyzer multiTermAnalyzer) {
        this.analyzer = analyzer;
        this.multiTermAnalyzer = multiTermAnalyzer;
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    public Analyzer multiTermAnalyzer() {
        return multiTermAnalyzer;
    }

    public void quoteAnalyzer(Analyzer quoteAnalyzer) {
        this.quoteAnalyzer = quoteAnalyzer;
    }

    public Analyzer quoteAnalyzer() {
        return quoteAnalyzer;
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

    public void timeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public DateTimeZone timeZone() {
        return this.timeZone;
    }

    public void fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
    }

    public Fuzziness fuzziness() {
        return fuzziness;
    }
}
