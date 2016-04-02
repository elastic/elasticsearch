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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TermSuggester extends Suggester<TermSuggestionContext> {

    public static final TermSuggester INSTANCE = new TermSuggester();

    private TermSuggester() {}

    @Override
    public TermSuggestion innerExecute(String name, TermSuggestionContext suggestion, IndexSearcher searcher, CharsRefBuilder spare)
            throws IOException {
        DirectSpellChecker directSpellChecker = SuggestUtils.getDirectSpellChecker(suggestion.getDirectSpellCheckerSettings());
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
            for (SuggestWord suggestWord : suggestedWords) {
                Text word = new Text(suggestWord.string);
                resultEntry.addOption(new TermSuggestion.Entry.Option(word, suggestWord.freq, suggestWord.score));
            }
            response.addTerm(resultEntry);
        }
        return response;
    }

    private List<Token> queryTerms(SuggestionContext suggestion, CharsRefBuilder spare) throws IOException {
        final List<Token> result = new ArrayList<>();
        final String field = suggestion.getField();
        SuggestUtils.analyze(suggestion.getAnalyzer(), suggestion.getText(), field, new SuggestUtils.TokenConsumer() {
            @Override
            public void nextToken() {
                Term term = new Term(field, BytesRef.deepCopyOf(fillBytesRef(new BytesRefBuilder())));
                result.add(new Token(term, offsetAttr.startOffset(), offsetAttr.endOffset()));
            }
        }, spare);
       return result;
    }

    @Override
    public SuggestionBuilder<?> innerFromXContent(QueryParseContext context) throws IOException {
        return TermSuggestionBuilder.innerFromXContent(context);
    }

    @Override
    public SuggestionBuilder<?> read(StreamInput in) throws IOException {
        return new TermSuggestionBuilder(in);
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
}
