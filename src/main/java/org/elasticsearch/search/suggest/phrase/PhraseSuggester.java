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


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option.Change;
import org.elasticsearch.search.suggest.*;
import org.elasticsearch.search.suggest.SuggestUtils.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PhraseSuggester implements Suggester<PhraseSuggestionContext> {
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
    public Suggestion<? extends Entry<? extends Option>> execute(String name, PhraseSuggestionContext suggestion,
            IndexReader indexReader, CharsRef spare) throws IOException {
        double realWordErrorLikelihood = suggestion.realworldErrorLikelyhood();
        List<PhraseSuggestionContext.DirectCandidateGenerator>  generators = suggestion.generators();
        CandidateGenerator[] gens = new CandidateGenerator[generators.size()];
        for (int i = 0; i < gens.length; i++) {
            PhraseSuggestionContext.DirectCandidateGenerator generator = generators.get(i);
            DirectSpellChecker directSpellChecker = SuggestUtils.getDirectSpellChecker(generator);
            gens[i] = new DirectCandidateGenerator(directSpellChecker, generator.field(), generator.suggestMode(), indexReader, realWordErrorLikelihood, generator.size(), generator.preFilter(), generator.postFilter());
        }
        
        
        final NoisyChannelSpellChecker checker = new NoisyChannelSpellChecker(realWordErrorLikelihood, suggestion.getRequireUnigram(), suggestion.getTokenLimit());
        final BytesRef separator = suggestion.separator();
        TokenStream stream = checker.tokenStream(suggestion.getAnalyzer(), suggestion.getText(), spare, suggestion.getField());
        WordScorer wordScorer = suggestion.model().newScorer(indexReader, suggestion.getField(), realWordErrorLikelihood, separator);
        Correction[] corrections = checker.getCorrections(stream, new MultiCandidateGeneratorWrapper(suggestion.getShardSize(), gens), suggestion.maxErrors(),
                suggestion.getShardSize(), indexReader,wordScorer , separator, suggestion.confidence(), suggestion.gramSize());
        
        UnicodeUtil.UTF8toUTF16(suggestion.getText(), spare);

        Analyzer noShinglesAnalyzer = SuggestUtils.copyWithoutShingleFilterFactory(suggestion.getAnalyzer());
        BytesRef byteSpare = new BytesRef();
        List<Token> textTokensForDiff = SuggestUtils.collectTokenList(
                noShinglesAnalyzer.tokenStream(suggestion.getField(), new FastCharArrayReader(spare.chars, spare.offset, spare.length)),
                byteSpare, spare);
        
        Suggestion.Entry<Option> resultEntry = new Suggestion.Entry<Option>(new StringText(spare.toString()), 0, spare.length);
        for (Correction correction : corrections) {
            UnicodeUtil.UTF8toUTF16(correction.join(SEPARATOR, byteSpare), spare);
            Text phrase = new StringText(spare.toString());
            List<Token> suggestionTokensForDiff = SuggestUtils.collectTokenList(
                    noShinglesAnalyzer.tokenStream(suggestion.getField(), new FastCharArrayReader(spare.chars, spare.offset, spare.length)),
                    byteSpare, spare);
            List<Change> changes = new ArrayList<Change>();
            // The length of the lists should be the same size but lets just make sure.
            for (int i = 0; i < Math.min(textTokensForDiff.size(), suggestionTokensForDiff.size()); i++) {
                Token textToken = textTokensForDiff.get(i);
                Token suggestionToken = suggestionTokensForDiff.get(i);
                if (!textToken.getText().equals(suggestionToken.getText())) {
                    changes.add(new Change(textToken.getText(), suggestionToken.getText(), textToken.getFrom(), textToken.getTo()));
                }
            }
            resultEntry.addOption(new Suggestion.Entry.Option(phrase, (float) (correction.score), changes));
        }
        final Suggestion<Entry<Option>> response = new Suggestion<Entry<Option>>(name, suggestion.getSize());
        response.addTerm(resultEntry);
        return response;
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
