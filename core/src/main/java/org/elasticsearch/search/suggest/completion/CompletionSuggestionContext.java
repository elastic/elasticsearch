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
package org.elasticsearch.search.suggest.completion;

import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextQuery;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CompletionSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    private CompletionFieldMapper mapper;
    private int fuzzyEditDistance = XFuzzySuggester.DEFAULT_MAX_EDITS;
    private boolean fuzzyTranspositions = XFuzzySuggester.DEFAULT_TRANSPOSITIONS;
    private int fuzzyMinLength = XFuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH;
    private int fuzzyPrefixLength = XFuzzySuggester.DEFAULT_NON_FUZZY_PREFIX;
    private boolean fuzzy = false;
    private boolean fuzzyUnicodeAware = XFuzzySuggester.DEFAULT_UNICODE_AWARE;
    private List<ContextQuery> contextQueries = Collections.emptyList();
    
    public CompletionSuggestionContext(Suggester suggester) {
        super(suggester);
    }

    public CompletionFieldMapper mapper() {
        return this.mapper;
    }

    public void mapper(CompletionFieldMapper mapper) {
        this.mapper = mapper;
    }

    public void setFuzzyEditDistance(int fuzzyEditDistance) {
        this.fuzzyEditDistance = fuzzyEditDistance;
    }

    public int getFuzzyEditDistance() {
        return fuzzyEditDistance;
    }

    public void setFuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
    }

    public boolean isFuzzyTranspositions() {
        return fuzzyTranspositions;
    }

    public void setFuzzyMinLength(int fuzzyMinPrefixLength) {
        this.fuzzyMinLength = fuzzyMinPrefixLength;
    }

    public int getFuzzyMinLength() {
        return fuzzyMinLength;
    }

    public void setFuzzyPrefixLength(int fuzzyNonPrefixLength) {
        this.fuzzyPrefixLength = fuzzyNonPrefixLength;
    }

    public int getFuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    public void setFuzzy(boolean fuzzy) {
        this.fuzzy = fuzzy;
    }

    public boolean isFuzzy() {
        return fuzzy;
    }

    public void setFuzzyUnicodeAware(boolean fuzzyUnicodeAware) {
        this.fuzzyUnicodeAware = fuzzyUnicodeAware;
    }

    public boolean isFuzzyUnicodeAware() {
        return fuzzyUnicodeAware;
    }
    
    public void setContextQuery(List<ContextQuery> queries) {
        this.contextQueries = queries;
    }

    public List<ContextQuery> getContextQueries() {   
        return this.contextQueries;
    }
}
