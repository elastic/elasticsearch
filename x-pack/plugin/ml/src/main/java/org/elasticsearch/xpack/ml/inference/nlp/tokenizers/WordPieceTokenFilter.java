/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public final class WordPieceTokenFilter extends TokenFilter {
    private final LinkedList<WordPieceToken> tokens;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private static final CharSequence CONTINUATION = "##";

    private State current;
    private final CharArraySet neverSplit;
    private final CharArrayMap<Integer> vocabulary;
    private final List<WordPieceToken> tokenizedValues;
    private final int maxInputCharsPerWord;
    private final int tokenizedUnknown;
    private final CharSequence unknownToken;

    public static WordPieceTokenFilter build(
        boolean isLowerCase,
        boolean isTokenizeCjkChars,
        boolean isStripAccents,
        List<String> neverSplit,
        List<String> dictionary,
        String unknownToken,
        int maxInputCharsPerWord,
        TokenStream input
    ) throws IOException {
        CharArrayMap<Integer> vocabMap = new CharArrayMap<>(dictionary.size(), isLowerCase);
        int i = 0;
        for (var word : dictionary) {
            vocabMap.put(word, i++);
        }
        input = BasicTokenFilter.build(isTokenizeCjkChars, isStripAccents, neverSplit, input);
        return new WordPieceTokenFilter(input, new CharArraySet(neverSplit, isLowerCase), vocabMap, unknownToken, maxInputCharsPerWord);
    }

    public WordPieceTokenFilter(
        TokenStream input,
        CharArraySet neverSplit,
        CharArrayMap<Integer> vocabulary,
        CharSequence unknownToken,
        int maxInputCharsPerWord
    ) {
        super(input);
        this.tokens = new LinkedList<>();
        this.neverSplit = neverSplit;
        this.vocabulary = vocabulary;
        this.tokenizedValues = new ArrayList<>();
        if (vocabulary.containsKey(unknownToken) == false) {
            throw new IllegalArgumentException(
                "provided vocabulary does not contain the unknown token of [" + unknownToken.toString() + "]"
            );
        }
        this.unknownToken = unknownToken;
        this.tokenizedUnknown = vocabulary.get(unknownToken);
        this.maxInputCharsPerWord = maxInputCharsPerWord;
    }

    public List<WordPieceToken> getTokenizedValues() {
        return tokenizedValues;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens.clear();
        tokenizedValues.clear();
        current = null;
    }

    @Override
    public boolean incrementToken() throws IOException {
        // TODO seems like this + lowercase + tokenize cjk + wordpiece could all be the same thing....
        if (tokens.isEmpty() == false) {
            assert current != null;
            WordPieceToken token = tokens.removeFirst();
            restoreState(current); // keep all other attributes untouched
            termAtt.setEmpty().append(token.charSequence());
            offsetAtt.setOffset(token.startOffset(), token.endOffset());
            posIncAtt.setPositionIncrement(0);
            return true;
        }

        current = null; // not really needed, but for safety
        if (input.incrementToken()) {
            if (neverSplit.contains(termAtt)) {
                Integer maybeTokenized = vocabulary.get(termAtt);
                tokenizedValues.add(
                    new WordPieceToken(
                        termAtt.toString(),
                        Objects.requireNonNullElse(maybeTokenized, tokenizedUnknown),
                        offsetAtt.startOffset(),
                        offsetAtt.endOffset()
                    )
                );
                return true;
            }
            if (termAtt.length() > maxInputCharsPerWord) {
                tokenizedValues.add(new WordPieceToken(unknownToken, tokenizedUnknown, offsetAtt.startOffset(), offsetAtt.endOffset()));
                termAtt.setEmpty().append(unknownToken);
                return true;
            }

            boolean isBad = false;
            int start = 0;
            int length = termAtt.length();
            while (start < length) {
                int end = length;
                CharSequence currentValidSubStr = null;

                while (start < end) {
                    CharSequence subStr;
                    if (start > 0) {
                        subStr = new MultiCharSequence(List.of(CONTINUATION, termAtt.subSequence(start, end)));
                    } else {
                        subStr = termAtt.subSequence(start, end);
                    }

                    if (vocabulary.containsKey(subStr)) {
                        currentValidSubStr = subStr;
                        break;
                    }
                    end--;
                }

                if (currentValidSubStr == null) {
                    isBad = true;
                    break;
                }
                int encoding = vocabulary.get(currentValidSubStr);
                WordPieceToken t = new WordPieceToken(currentValidSubStr, encoding, offsetAtt.startOffset(), offsetAtt.endOffset());
                tokenizedValues.add(t);
                tokens.add(t);
                start = end;
            }

            if (isBad) {
                tokens.clear();
                WordPieceToken t = new WordPieceToken(unknownToken, tokenizedUnknown, offsetAtt.startOffset(), offsetAtt.endOffset());
                tokenizedValues.add(t);
                termAtt.setEmpty().append(unknownToken);
            } else {
                current = captureState();
                WordPieceToken token = tokens.removeFirst();
                termAtt.setEmpty().append(token.charSequence());
                offsetAtt.setOffset(token.startOffset(), token.endOffset());
            }
            return true;
        }
        return false;
    }

    public static class WordPieceToken extends DelimitedToken.Encoded implements CharSequence {

        WordPieceToken(CharSequence sequence, int encoding, int startOffset, int endOffset) {
            super(sequence, encoding, startOffset, endOffset);
        }

        @Override
        public int length() {
            return charSequence().length();
        }

        @Override
        public char charAt(int index) {
            return charSequence().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return charSequence().subSequence(start, end);
        }

        @Override
        public String toString() {
            return charSequence().toString();
        }
    }

}
