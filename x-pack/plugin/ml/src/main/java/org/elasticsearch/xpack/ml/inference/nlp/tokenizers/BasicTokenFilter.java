/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Assumes that the text is already whitespace tokenized
 */
public final class BasicTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    protected final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private final CharSeqTokenTrieNode neverSplit;
    protected final LinkedList<DelimitedToken> tokens;
    private final CharArraySet neverSplitSet;

    private State current;

    public static BasicTokenFilter buildFromSettings(
        boolean isLowerCase,
        boolean isTokenizeCjkChars,
        boolean isStripAccents,
        List<String> neverSplit,
        TokenStream input
    ) throws IOException {
        if (isStripAccents) {
            input = new StripAccentTokenFilter(input);
        }
        if (isTokenizeCjkChars) {
            input = new CJKSplitTokenFilter(input);
        }
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(512);
                TokenStream stream = tokenizer;
                if (isStripAccents) {
                    stream = new StripAccentTokenFilter(stream);
                }
                if (isTokenizeCjkChars) {
                    stream = new CJKSplitTokenFilter(stream);
                }
                stream = new BasicTokenFilter(stream, CharSeqTokenTrieNode.EMPTY, new CharArraySet(0, false));
                return new TokenStreamComponents(tokenizer, stream);
            }

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ControlCharFilter(reader);
            }
        };
        CharArraySet neverSplitSet = new CharArraySet(neverSplit, isLowerCase);
        CharSeqTokenTrieNode neverSplitTree;
        try (analyzer) {
            neverSplitTree = CharSeqTokenTrieNode.build(neverSplit, c -> {
                try (TokenStream ts = analyzer.tokenStream("never_split", c)) {
                    ts.reset();
                    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
                    List<String> tokens = new ArrayList<>();
                    while (ts.incrementToken()) {
                        tokens.add(term.toString());
                    }
                    return tokens;
                }
            }, false);
        }
        return new BasicTokenFilter(input, neverSplitTree, neverSplitSet);
    }

    public BasicTokenFilter(TokenStream input, CharSeqTokenTrieNode neverSplit, CharArraySet neverSplitSet) {
        super(input);
        this.neverSplit = neverSplit;
        this.neverSplitSet = neverSplitSet;
        this.tokens = new LinkedList<>();
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens.clear();
        current = null;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (tokens.isEmpty() == false) {
            assert current != null;
            DelimitedToken token = tokens.removeFirst();
            restoreState(current); // keep all other attributes untouched
            termAtt.setEmpty().append(token.charSequence());
            offsetAtt.setOffset(token.startOffset(), token.endOffset());
            return true;
        }
        current = null; // not really needed, but for safety
        if (input.incrementToken()) {
            if (neverSplitSet.contains(termAtt)) {
                return true;
            }
            int startOffset = offsetAtt.startOffset();
            // split punctuation!!!
            LinkedList<DelimitedToken> splits = new LinkedList<>();
            int charIndex = 0;
            int lastCharSplit = 0;
            for (PrimitiveIterator.OfInt it = termAtt.codePoints().iterator(); it.hasNext();) {
                int cp = it.next();
                if (isPunctuationMark(cp)) {
                    int charCount = charIndex - lastCharSplit;
                    if (charCount > 0) {
                        splits.add(
                            new DelimitedToken(
                                termAtt.subSequence(lastCharSplit, charIndex),
                                lastCharSplit + startOffset,
                                charIndex + startOffset
                            )
                        );
                    }
                    splits.add(
                        new DelimitedToken(
                            termAtt.subSequence(charIndex, charIndex + 1),
                            charIndex + startOffset,
                            charIndex + 1 + startOffset
                        )
                    );
                    lastCharSplit = charIndex + 1;
                }
                charIndex += Character.charCount(cp);
            }
            if (lastCharSplit < termAtt.length()) {
                splits.add(
                    new DelimitedToken(
                        termAtt.subSequence(lastCharSplit, termAtt.length()),
                        lastCharSplit + startOffset,
                        offsetAtt.endOffset()
                    )
                );
            }
            List<DelimitedToken> matchingTokens = new ArrayList<>();
            CharSeqTokenTrieNode current = neverSplit;
            for (DelimitedToken token : splits) {
                CharSeqTokenTrieNode childNode = current.getChild(token.charSequence());
                if (childNode == null) {
                    if (current != neverSplit) {
                        tokens.addAll(matchingTokens);
                        matchingTokens = new ArrayList<>();
                        current = neverSplit;
                    }
                    tokens.add(token);
                } else if (childNode.isLeaf()) {
                    matchingTokens.add(token);
                    DelimitedToken mergedToken = DelimitedToken.mergeTokens(matchingTokens);
                    if (neverSplitSet.contains(mergedToken.charSequence())) {
                        tokens.add(mergedToken);
                    } else {
                        tokens.addAll(matchingTokens);
                    }
                    matchingTokens = new ArrayList<>();
                    current = neverSplit;
                } else {
                    matchingTokens.add(token);
                    current = childNode;
                }
            }
            if (matchingTokens.isEmpty() == false) {
                tokens.addAll(matchingTokens);
            }
            this.current = captureState();
            DelimitedToken token = tokens.removeFirst();
            termAtt.setEmpty().append(token.charSequence());
            offsetAtt.setOffset(token.startOffset(), token.endOffset());
            return true;
        }
        current = null; // not really needed, but for safety
        return false;
    }

    static boolean isPunctuationMark(int codePoint) {
        if ((codePoint >= 33 && codePoint <= 47)
            || (codePoint >= 58 && codePoint <= 64)
            || (codePoint >= 91 && codePoint <= 96)
            || (codePoint >= 123 && codePoint <= 126)) {
            return true;
        }

        int category = Character.getType(codePoint);
        return (category >= Character.DASH_PUNCTUATION && category <= Character.OTHER_PUNCTUATION)
            || (category >= Character.INITIAL_QUOTE_PUNCTUATION && category <= Character.FINAL_QUOTE_PUNCTUATION);
    }

}
