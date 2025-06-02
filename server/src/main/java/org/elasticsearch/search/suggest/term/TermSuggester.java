/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

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
        TermSuggestion response = new TermSuggestion(name, suggestion.getSize(), suggestion.getDirectSpellCheckerSettings().sort());
        List<Token> tokens = queryTerms(suggestion, spare);
        for (Token token : tokens) {
            // TODO: Extend DirectSpellChecker in 4.1, to get the raw suggested words as BytesRef
            SuggestWord[] suggestedWords = directSpellChecker.suggestSimilar(
                token.term,
                suggestion.getShardSize(),
                indexReader,
                suggestion.getDirectSpellCheckerSettings().suggestMode()
            );
            var termBytes = token.term.bytes();
            var termEncoded = new XContentString.EncodedBytes(termBytes.bytes, termBytes.offset, termBytes.length);
            Text key = new Text(termEncoded);
            TermSuggestion.Entry resultEntry = new TermSuggestion.Entry(key, token.startOffset, token.endOffset - token.startOffset);
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
        DirectCandidateGenerator.analyze(
            suggestion.getAnalyzer(),
            suggestion.getText(),
            field,
            new DirectCandidateGenerator.TokenConsumer() {
                @Override
                public void nextToken() {
                    Term term = new Term(field, BytesRef.deepCopyOf(fillBytesRef(new BytesRefBuilder())));
                    result.add(new Token(term, offsetAttr.startOffset(), offsetAttr.endOffset()));
                }
            },
            spare
        );
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

    @Override
    protected TermSuggestion emptySuggestion(String name, TermSuggestionContext suggestion, CharsRefBuilder spare) throws IOException {
        TermSuggestion termSuggestion = new TermSuggestion(name, suggestion.getSize(), suggestion.getDirectSpellCheckerSettings().sort());
        List<Token> tokens = queryTerms(suggestion, spare);
        for (Token token : tokens) {
            var termBytes = token.term.bytes();
            var termEncoded = new XContentString.EncodedBytes(termBytes.bytes, termBytes.offset, termBytes.length);
            Text key = new Text(termEncoded);
            TermSuggestion.Entry resultEntry = new TermSuggestion.Entry(key, token.startOffset, token.endOffset - token.startOffset);
            termSuggestion.addTerm(resultEntry);
        }
        return termSuggestion;
    }
}
