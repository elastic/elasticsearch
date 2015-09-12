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
package org.elasticsearch.search.suggest.phrase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.search.suggest.DirectSpellcheckerSettings;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

class PhraseSuggestionContext extends SuggestionContext {
    private final BytesRef SEPARATOR = new BytesRef(" ");
    private IndexQueryParserService queryParserService;
    private float maxErrors = 0.5f;
    private BytesRef separator = SEPARATOR;
    private float realworldErrorLikelihood = 0.95f;
    private List<DirectCandidateGenerator> generators = new ArrayList<>();
    private int gramSize = 1;
    private float confidence = 1.0f;
    private int tokenLimit = NoisyChannelSpellChecker.DEFAULT_TOKEN_LIMIT;
    private BytesRef preTag;
    private BytesRef postTag;
    private CompiledScript collateQueryScript;
    private CompiledScript collateFilterScript;
    private Map<String, Object> collateScriptParams = new HashMap<>(1);

    private WordScorer.WordScorerFactory scorer;

    private boolean requireUnigram = true;
    private boolean prune = false;

    public PhraseSuggestionContext(Suggester<? extends PhraseSuggestionContext> suggester) {
        super(suggester);
    }

    public float maxErrors() {
        return maxErrors;
    }

    public void setMaxErrors(Float maxErrors) {
        this.maxErrors = maxErrors;
    }

    public BytesRef separator() {
        return separator;
    }

    public void setSeparator(BytesRef separator) {
        this.separator = separator;
    }

    public Float realworldErrorLikelyhood() {
        return realworldErrorLikelihood;
    }

    public void setRealWordErrorLikelihood(Float realworldErrorLikelihood) {
        this.realworldErrorLikelihood = realworldErrorLikelihood;
    }

    public void addGenerator(DirectCandidateGenerator generator) {
        this.generators.add(generator);
    }
    
    public List<DirectCandidateGenerator> generators() {
        return this.generators ;
    }
    
    public void setGramSize(int gramSize) {
        this.gramSize = gramSize;
    }
    
    public int gramSize() {
        return gramSize;
    }
    
    public float confidence() {
        return confidence;
    }
    
    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }
    
    public void setModel(WordScorer.WordScorerFactory scorer) {
        this.scorer = scorer;
    }

    public WordScorer.WordScorerFactory model() {
        return scorer;
    }

    public void setQueryParserService(IndexQueryParserService queryParserService) {
        this.queryParserService = queryParserService;
    }

    public IndexQueryParserService getQueryParserService() {
        return queryParserService;
    }

    static class DirectCandidateGenerator extends DirectSpellcheckerSettings {
        private Analyzer preFilter;
        private Analyzer postFilter;
        private String field;
        private int size = 5;

        public String field() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public int size() {
            return size;
        }

        public void size(int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("Size must be positive");
            }
            this.size = size;
        }
        
        public Analyzer preFilter() {
            return preFilter;
        }

        public void preFilter(Analyzer preFilter) {
            this.preFilter = preFilter;
        }

        public Analyzer postFilter() {
            return postFilter;
        }

        public void postFilter(Analyzer postFilter) {
            this.postFilter = postFilter;
        }
        
        
    }

    public void setRequireUnigram(boolean requireUnigram) {
        this.requireUnigram  = requireUnigram;
    }
    
    public boolean getRequireUnigram() {
        return requireUnigram;
    }
    
    public void setTokenLimit(int tokenLimit) {
        this.tokenLimit = tokenLimit;
    }
   
    public int getTokenLimit() {
        return tokenLimit;
    }

    public void setPreTag(BytesRef preTag) {
        this.preTag = preTag;
    }

    public BytesRef getPreTag() {
        return preTag;
    }

    public void setPostTag(BytesRef postTag) {
        this.postTag = postTag;
    }

    public BytesRef getPostTag() {
        return postTag;
    }

    CompiledScript getCollateQueryScript() {
        return collateQueryScript;
    }

    void setCollateQueryScript(CompiledScript collateQueryScript) {
        this.collateQueryScript = collateQueryScript;
    }

    Map<String, Object> getCollateScriptParams() {
        return collateScriptParams;
    }

    void setCollateScriptParams(Map<String, Object> collateScriptParams) {
        this.collateScriptParams = collateScriptParams;
    }

    void setCollatePrune(boolean prune) {
        this.prune = prune;
    }

    boolean collatePrune() {
        return prune;
    }

}
