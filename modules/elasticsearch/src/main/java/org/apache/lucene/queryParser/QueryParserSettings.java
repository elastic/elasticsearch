/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.queryParser;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.FuzzyQuery;

/**
 * @author kimchy (shay.banon)
 */
public class QueryParserSettings {

    private String queryString;
    private String defaultField;
    private float boost = 1.0f;
    private MapperQueryParser.Operator defaultOperator = QueryParser.Operator.OR;
    private boolean allowLeadingWildcard = true;
    private boolean lowercaseExpandedTerms = true;
    private boolean enablePositionIncrements = true;
    private int phraseSlop = 0;
    private float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
    private int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
    private boolean analyzeWildcard = false;
    private boolean escape = false;
    private Analyzer analyzer = null;

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

    public boolean escape() {
        return escape;
    }

    public void escape(boolean escape) {
        this.escape = escape;
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    public void analyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public boolean analyzeWildcard() {
        return this.analyzeWildcard;
    }

    public void analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryParserSettings that = (QueryParserSettings) o;

        if (allowLeadingWildcard != that.allowLeadingWildcard) return false;
        if (Float.compare(that.boost, boost) != 0) return false;
        if (enablePositionIncrements != that.enablePositionIncrements) return false;
        if (escape != that.escape) return false;
        if (analyzeWildcard != that.analyzeWildcard) return false;
        if (Float.compare(that.fuzzyMinSim, fuzzyMinSim) != 0) return false;
        if (fuzzyPrefixLength != that.fuzzyPrefixLength) return false;
        if (lowercaseExpandedTerms != that.lowercaseExpandedTerms) return false;
        if (phraseSlop != that.phraseSlop) return false;
        if (analyzer != null ? !analyzer.equals(that.analyzer) : that.analyzer != null) return false;
        if (defaultField != null ? !defaultField.equals(that.defaultField) : that.defaultField != null) return false;
        if (defaultOperator != that.defaultOperator) return false;
        if (queryString != null ? !queryString.equals(that.queryString) : that.queryString != null) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = queryString != null ? queryString.hashCode() : 0;
        result = 31 * result + (defaultField != null ? defaultField.hashCode() : 0);
        result = 31 * result + (boost != +0.0f ? Float.floatToIntBits(boost) : 0);
        result = 31 * result + (defaultOperator != null ? defaultOperator.hashCode() : 0);
        result = 31 * result + (allowLeadingWildcard ? 1 : 0);
        result = 31 * result + (lowercaseExpandedTerms ? 1 : 0);
        result = 31 * result + (enablePositionIncrements ? 1 : 0);
        result = 31 * result + phraseSlop;
        result = 31 * result + (fuzzyMinSim != +0.0f ? Float.floatToIntBits(fuzzyMinSim) : 0);
        result = 31 * result + fuzzyPrefixLength;
        result = 31 * result + (escape ? 1 : 0);
        result = 31 * result + (analyzer != null ? analyzer.hashCode() : 0);
        result = 31 * result + (analyzeWildcard ? 1 : 0);
        return result;
    }
}
