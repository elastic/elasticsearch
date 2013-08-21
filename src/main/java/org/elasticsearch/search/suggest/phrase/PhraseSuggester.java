/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.search.suggest.phrase;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.*;
import org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PhraseSuggester extends Suggester<PhraseSuggestionContext> {
    private final BytesRef SEPARATOR = new BytesRef(" ");
    
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
        final List<CandidateGenerator> gens = new ArrayList<CandidateGenerator>(generators.size());
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
            for (Correction correction : checkerResult.corrections) {
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
    
    @Override
    public String[] names() {
        return new String[] {"phrase"};
    }

    @Override
    public SuggestContextParser getContextParser() {
        return new PhraseSuggestParser();
    }

}
