/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import com.ibm.icu.text.Normalizer;
import com.ibm.icu.text.Normalizer2;

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
import java.util.function.IntPredicate;

/**
 * Assumes that the text is already whitespace tokenized
 */
public final class BasicTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private final CharSeqTokenTrieNode neverSplit;
    private final LinkedList<DelimitedToken> tokens;
    private final boolean isStripAccents;
    private final CharArraySet neverSplitSet;
    private final Normalizer2 normalizer;
    private final StringBuilder accentBuffer = new StringBuilder();
    private final IntPredicate splitOn;

    private State current;

    public static BasicTokenFilter build(boolean isTokenizeCjkChars, boolean isStripAccents, List<String> neverSplit, TokenStream input)
        throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = new BasicTokenFilter(
                    tokenizer,
                    CharSeqTokenTrieNode.EMPTY,
                    CharArraySet.EMPTY_SET,
                    isStripAccents,
                    isTokenizeCjkChars
                );
                return new TokenStreamComponents(tokenizer, stream);
            }

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ControlCharFilter(reader);
            }
        };
        CharArraySet neverSplitSet = new CharArraySet(neverSplit, false);
        CharSeqTokenTrieNode neverSplitTree;
        try (analyzer) {
            neverSplitTree = CharSeqTokenTrieNode.build(neverSplit, c -> {
                try (TokenStream ts = analyzer.tokenStream("never_split", c)) {
                    CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
                    ts.reset();
                    List<String> tokens = new ArrayList<>();
                    while (ts.incrementToken()) {
                        tokens.add(term.toString());
                    }
                    return tokens;
                }
            });
        }
        return new BasicTokenFilter(input, neverSplitTree, neverSplitSet, isStripAccents, isTokenizeCjkChars);
    }

    public BasicTokenFilter(
        TokenStream input,
        CharSeqTokenTrieNode neverSplit,
        CharArraySet neverSplitSet,
        boolean isStripAccents,
        boolean isTokenizeCjkChars
    ) {
        super(input);
        this.neverSplit = neverSplit;
        this.neverSplitSet = neverSplitSet;
        this.tokens = new LinkedList<>();
        this.isStripAccents = isStripAccents;
        this.normalizer = Normalizer2.getNFDInstance();
        this.splitOn = cp -> (isTokenizeCjkChars && isCjkChar(cp)) || isPunctuationMark(cp);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens.clear();
        accentBuffer.setLength(0);
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
        while (input.incrementToken()) {
            if (neverSplitSet.contains(termAtt)) {
                return true;
            }
            // split punctuation and maybe cjk chars!!!
            LinkedList<DelimitedToken> splits = split();
            LinkedList<DelimitedToken> delimitedTokens = mergeSplits(splits);
            if (isStripAccents) {
                for (DelimitedToken token : delimitedTokens) {
                    // stripping accents may result in an empty string
                    var stripped = stripAccent(token);
                    if (stripped.charSequence().isEmpty() == false) {
                        tokens.add(stripped);
                    }
                }
            } else {
                tokens.addAll(delimitedTokens);
            }
            this.current = captureState();

            if (tokens.isEmpty()) {
                // keep going until we have token(s) with non-empty strings
                continue;
            }

            DelimitedToken token = tokens.removeFirst();
            termAtt.setEmpty().append(token.charSequence());
            offsetAtt.setOffset(token.startOffset(), token.endOffset());
            return true;
        }
        return false;
    }

    private DelimitedToken stripAccent(DelimitedToken token) {
        accentBuffer.setLength(0);
        boolean changed = false;
        if (normalizer.quickCheck(token.charSequence()) != Normalizer.YES) {
            normalizer.normalize(token.charSequence(), accentBuffer);
            changed = true;
        } else {
            accentBuffer.append(token.charSequence());
        }
        List<Integer> badIndices = new ArrayList<>();
        List<Integer> charCount = new ArrayList<>();
        int index = 0;
        int deletedIndices = 0;
        for (PrimitiveIterator.OfInt it = accentBuffer.codePoints().iterator(); it.hasNext();) {
            int cp = it.next();
            if (Character.getType(cp) == Character.NON_SPACING_MARK) {
                // When we iterate to delete accents, we need to account for previously deleted ones
                badIndices.add(index - deletedIndices);
                charCount.add(Character.charCount(cp));
                deletedIndices++;
                changed = true;
            }
            index++;
        }
        for (int i = 0; i < badIndices.size(); i++) {
            int badIndex = badIndices.get(i);
            int count = charCount.get(i);
            for (int j = 0; j < count && badIndex < accentBuffer.length(); j++) {
                accentBuffer.deleteCharAt(badIndex);
            }
        }
        if (changed) {
            return new DelimitedToken(accentBuffer.toString(), token.startOffset(), token.endOffset());
        }
        return token;
    }

    private LinkedList<DelimitedToken> split() {
        LinkedList<DelimitedToken> splits = new LinkedList<>();
        final int startOffset = offsetAtt.startOffset();
        int charIndex = 0;
        int lastCharSplit = 0;
        for (PrimitiveIterator.OfInt it = termAtt.codePoints().iterator(); it.hasNext();) {
            int cp = it.next();
            if (splitOn.test(cp)) {
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
                    new DelimitedToken(termAtt.subSequence(charIndex, charIndex + 1), charIndex + startOffset, charIndex + 1 + startOffset)
                );
                lastCharSplit = charIndex + 1;
            }
            charIndex += Character.charCount(cp);
        }
        if (lastCharSplit < termAtt.length()) {
            splits.add(
                new DelimitedToken(termAtt.subSequence(lastCharSplit, termAtt.length()), lastCharSplit + startOffset, offsetAtt.endOffset())
            );
        }
        return splits;
    }

    private LinkedList<DelimitedToken> mergeSplits(LinkedList<DelimitedToken> splits) {
        if (splits.size() == 1) {
            return splits;
        }
        LinkedList<DelimitedToken> mergedTokens = new LinkedList<>();
        List<DelimitedToken> matchingTokens = new ArrayList<>();
        CharSeqTokenTrieNode current = neverSplit;
        for (DelimitedToken token : splits) {
            CharSeqTokenTrieNode childNode = current.getChild(token.charSequence());
            if (childNode == null) {
                if (current != neverSplit) {
                    mergedTokens.addAll(matchingTokens);
                    matchingTokens = new ArrayList<>();
                    current = neverSplit;
                }
                childNode = current.getChild(token.charSequence());
                if (childNode == null) {
                    mergedTokens.add(token);
                } else {
                    matchingTokens.add(token);
                    current = childNode;
                }
            } else if (childNode.isLeaf()) {
                matchingTokens.add(token);
                DelimitedToken mergedToken = DelimitedToken.mergeTokens(matchingTokens);
                if (neverSplitSet.contains(mergedToken.charSequence())) {
                    mergedTokens.add(mergedToken);
                } else {
                    mergedTokens.addAll(matchingTokens);
                }
                matchingTokens = new ArrayList<>();
                current = neverSplit;
            } else {
                matchingTokens.add(token);
                current = childNode;
            }
        }
        if (matchingTokens.isEmpty() == false) {
            mergedTokens.addAll(matchingTokens);
        }
        return mergedTokens;
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

    private static boolean isCjkChar(int codePoint) {
        // https://en.wikipedia.org/wiki/CJK_Unified_Ideographs_(Unicode_block)
        Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
        return Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_C.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_D.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_E.equals(block)
            || Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT.equals(block);
    }

}
