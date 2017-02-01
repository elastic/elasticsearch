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
package org.elasticsearch.search.suggest.term;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TermSuggester extends Suggester<TermSuggestionContext> {

    public static final TermSuggester INSTANCE = new TermSuggester();

    private TermSuggester() {}

    @Override
    public TermSuggestion innerExecute(String name, TermSuggestionContext suggestion, IndexSearcher searcher, CharsRefBuilder spare)
            throws IOException {
        DirectSpellChecker directSpellChecker = suggestion.getDirectSpellCheckerSettings().createDirectSpellChecker();
        final IndexReader indexReader = searcher.getIndexReader();
        TermSuggestion response = new TermSuggestion(
                name, suggestion.getSize(), suggestion.getDirectSpellCheckerSettings().sort()
        );
        List<Token> tokens = queryTerms(suggestion, spare);
        for (Token token : tokens) {
            // TODO: Extend DirectSpellChecker in 4.1, to get the raw suggested words as BytesRef
            SuggestWord[] suggestedWords = directSpellChecker.suggestSimilar(
                    token.term, suggestion.getShardSize(), indexReader, suggestion.getDirectSpellCheckerSettings().suggestMode()
            );
            Text key = new Text(new BytesArray(token.term.bytes()));
            TermSuggestion.Entry resultEntry = new TermSuggestion.Entry(key, token.startOffset, token.endOffset - token.startOffset);
            if (suggestion.getDirectSpellCheckerSettings().exactMatch()){
                final TermsEnum termsEnum = MultiFields.getTerms(indexReader, token.term.field()).iterator();
                if (termsEnum.seekExact(token.term.bytes())) {
				    Text word = new Text(token.term.text());
				    resultEntry.addOption(new TermSuggestion.Entry.Option(word, termsEnum.docFreq(), 1f));
				}
            }
            for (SuggestWord suggestWord : suggestedWords) {
                Text word = new Text(suggestWord.string);
                resultEntry.addOption(new TermSuggestion.Entry.Option(word, suggestWord.freq, suggestWord.score));
            }
            response.addTerm(resultEntry);
        }
        return response;
    }

    private static List<Token> queryTerms(SuggestionContext suggestion, CharsRefBuilder spare) throws IOException {
        final List<Token> result = new ArrayList<>();
        final String field = suggestion.getField();
        DirectCandidateGenerator.analyze(suggestion.getAnalyzer(), suggestion.getText(), field,
                new DirectCandidateGenerator.TokenConsumer() {
            @Override
            public void nextToken() {
                Term term = new Term(field, BytesRef.deepCopyOf(fillBytesRef(new BytesRefBuilder())));
                result.add(new Token(term, offsetAttr.startOffset(), offsetAttr.endOffset()));
            }
        }, spare);
       return result;
    }

    private static class Token {

        public final Term term;
        public final int startOffset;
        public final int endOffset;

        private Token(Term term, int startOffset, int endOffset) {
            this.term = term;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

    }
	
	private void addExactMatch(TermSuggestionContext suggestion, IndexReader indexReader, Token token, TermSuggestion.Entry resultEntry) throws IOException{
         //If token is spelled correctly, add to results with a score of 1
         Terms terms = MultiFields.getTerms(indexReader, token.term.field());
         TermsEnum e = terms.iterator();
         CharsRefBuilder crb = new CharsRefBuilder();
         BytesRef candidateTerm;
         String termAsString;
         //token that will be compared
         String tokenAsString = token.term.text();
         boolean foundToken = false;
         //iterate through terms in index
         while ((candidateTerm = e.next()) != null && !foundToken) {
             crb.copyUTF8Bytes(candidateTerm);
             //get the term
             termAsString = crb.toString();
             //exact match found
             if(termAsString.equals(tokenAsString)){
                 SuggestWord exactMatch = new SuggestWord();
                 exactMatch.string = termAsString;
                 //as it is an exact match its score is 1, differentiating it from suggestions
                 exactMatch.score = 1;
                 exactMatch.freq = e.docFreq();
                 //we found it
                 foundToken = true;
                 //add to results
                 Text word = new StringText(exactMatch.string);
                 resultEntry.addOption(new TermSuggestion.Entry.Option(word, exactMatch.freq, exactMatch.score));
             }
         }
    }
}
