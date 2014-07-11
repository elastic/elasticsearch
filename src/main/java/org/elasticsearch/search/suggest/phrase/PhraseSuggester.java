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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.*;
import org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class PhraseSuggester extends Suggester<PhraseSuggestionContext> {
    private final BytesRef SEPARATOR = new BytesRef(" ");
    private static final String SUGGESTION_TEMPLATE_VAR_NAME = "suggestion";
    private final Client client;
    private final ScriptService scriptService;

    @Inject
    public PhraseSuggester(Client client, ScriptService scriptService) {
        this.client = client;
        this.scriptService = scriptService;
    }

    /*
     * More Ideas:
     *   - add ability to find whitespace problems -> we can build a poor mans decompounder with our index based on a automaton?
     *   - add ability to build different error models maybe based on a confusion matrix?   
     *   - try to combine a token with its subsequent token to find / detect word splits (optional)
     *      - for this to work we need some way to defined the position length of a candidate
     *   - phonetic filters could be interesting here too for candidate selection
     */
    @Override
    public Suggestion<? extends Entry<? extends Option>> innerExecute(String name, PhraseSuggestionContext suggestion,
            IndexReader indexReader, CharsRef spare) throws IOException {
        double realWordErrorLikelihood = suggestion.realworldErrorLikelyhood();
        final PhraseSuggestion response = new PhraseSuggestion(name, suggestion.getSize());

        List<PhraseSuggestionContext.DirectCandidateGenerator>  generators = suggestion.generators();
        final int numGenerators = generators.size();
        final List<CandidateGenerator> gens = new ArrayList<>(generators.size());
        for (int i = 0; i < numGenerators; i++) {
            PhraseSuggestionContext.DirectCandidateGenerator generator = generators.get(i);
            DirectSpellChecker directSpellChecker = SuggestUtils.getDirectSpellChecker(generator);
            Terms terms = MultiFields.getTerms(indexReader, generator.field());
            if (terms !=  null) {
                gens.add(new DirectCandidateGenerator(directSpellChecker, generator.field(), generator.suggestMode(), 
                        indexReader, realWordErrorLikelihood, generator.size(), generator.preFilter(), generator.postFilter(), terms));    
            }
        }
        final String suggestField = suggestion.getField();
        final Terms suggestTerms = MultiFields.getTerms(indexReader, suggestField);
        if (gens.size() > 0 && suggestTerms != null) {
            final NoisyChannelSpellChecker checker = new NoisyChannelSpellChecker(realWordErrorLikelihood, suggestion.getRequireUnigram(), suggestion.getTokenLimit());
            final BytesRef separator = suggestion.separator();
            TokenStream stream = checker.tokenStream(suggestion.getAnalyzer(), suggestion.getText(), spare, suggestion.getField());
            
            WordScorer wordScorer = suggestion.model().newScorer(indexReader, suggestTerms, suggestField, realWordErrorLikelihood, separator);
            Result checkerResult = checker.getCorrections(stream, new MultiCandidateGeneratorWrapper(suggestion.getShardSize(),
                    gens.toArray(new CandidateGenerator[gens.size()])), suggestion.maxErrors(),
                    suggestion.getShardSize(), indexReader,wordScorer , separator, suggestion.confidence(), suggestion.gramSize());

            PhraseSuggestion.Entry resultEntry = buildResultEntry(suggestion, spare, checkerResult.cutoffScore);
            response.addTerm(resultEntry);

            BytesRef byteSpare = new BytesRef();

            MultiSearchResponse multiSearchResponse = collate(suggestion, checkerResult, byteSpare, spare);

            for (int i = 0; i < checkerResult.corrections.length; i++) {
                if (!hasMatchingDocs(multiSearchResponse, i)) {
                    continue;
                }
                Correction correction = checkerResult.corrections[i];
                UnicodeUtil.UTF8toUTF16(correction.join(SEPARATOR, byteSpare, null, null), spare);
                Text phrase = new StringText(spare.toString());
                Text highlighted = null;
                if (suggestion.getPreTag() != null) {
                    UnicodeUtil.UTF8toUTF16(correction.join(SEPARATOR, byteSpare, suggestion.getPreTag(), suggestion.getPostTag()), spare);
                    highlighted = new StringText(spare.toString());
                }
                resultEntry.addOption(new Suggestion.Entry.Option(phrase, highlighted, (float) (correction.score)));
            }
        } else {
            response.addTerm(buildResultEntry(suggestion, spare, Double.MIN_VALUE));
        }
        return response;
    }

    private PhraseSuggestion.Entry buildResultEntry(PhraseSuggestionContext suggestion, CharsRef spare, double cutoffScore) {
        UnicodeUtil.UTF8toUTF16(suggestion.getText(), spare);
        return new PhraseSuggestion.Entry(new StringText(spare.toString()), 0, spare.length, cutoffScore);
    }

    private MultiSearchResponse collate(PhraseSuggestionContext suggestion, Result checkerResult, BytesRef byteSpare, CharsRef spare) throws IOException {
        CompiledScript collateQueryScript = suggestion.getCollateQueryScript();
        CompiledScript collateFilterScript = suggestion.getCollateFilterScript();
        MultiSearchResponse multiSearchResponse = null;
        if (collateQueryScript != null) {
            multiSearchResponse = fetchMatchingDocCountResponses(checkerResult.corrections, collateQueryScript, false, suggestion, byteSpare, spare);
        } else if (collateFilterScript != null) {
            multiSearchResponse = fetchMatchingDocCountResponses(checkerResult.corrections, collateFilterScript, true, suggestion, byteSpare, spare);
        }
        return multiSearchResponse;
    }

    private MultiSearchResponse fetchMatchingDocCountResponses(Correction[] corrections, CompiledScript collateScript,
                                                               boolean isFilter, PhraseSuggestionContext suggestions,
                                                               BytesRef byteSpare, CharsRef spare) throws IOException {
        Map<String, Object> vars = suggestions.getCollateScriptParams();
        MultiSearchResponse multiSearchResponse = null;
        MultiSearchRequestBuilder multiSearchRequestBuilder = client.prepareMultiSearch();
        boolean requestAdded = false;
        SearchRequestBuilder req;
        for (Correction correction : corrections) {
            UnicodeUtil.UTF8toUTF16(correction.join(SEPARATOR, byteSpare, null, null), spare);
            vars.put(SUGGESTION_TEMPLATE_VAR_NAME, spare.toString());
            ExecutableScript executable = scriptService.executable(collateScript, vars);
            BytesReference querySource = (BytesReference) executable.run();
            requestAdded = true;
            if (isFilter) {
                req = client.prepareSearch()
                        .setPreference(suggestions.getPreference())
                        .setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.bytesFilter(querySource)))
                        .setSearchType(SearchType.COUNT);
            } else {
                req = client.prepareSearch()
                        .setPreference(suggestions.getPreference())
                        .setQuery(querySource)
                        .setSearchType(SearchType.COUNT);
            }
            multiSearchRequestBuilder.add(req);
        }
        if (requestAdded) {
            multiSearchResponse = multiSearchRequestBuilder.get();
        }

        return multiSearchResponse;
    }

    private static boolean hasMatchingDocs(MultiSearchResponse multiSearchResponse, int index) {
        if (multiSearchResponse == null) {
            return true;
        }
        MultiSearchResponse.Item item = multiSearchResponse.getResponses()[index];
        if (!item.isFailure()) {
            SearchResponse resp = item.getResponse();
            return resp.getHits().totalHits() > 0;
        } else {
            throw new ElasticsearchException("Collate request failed: " + item.getFailureMessage());
        }
    }

    ScriptService scriptService() {
        return scriptService;
    }
    
    @Override
    public String[] names() {
        return new String[] {"phrase"};
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new PhraseSuggestParser(this);
    }

}
