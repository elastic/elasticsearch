/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import joptsimple.internal.Strings;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Basic tokenization of text by whitespace with optional extras:
 * 1. Lower case the input
 * 2. Convert to Unicode NFD
 * 3. Stip accents
 * 4. Surround CJK characters with ' '
 *
 * Derived from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/src/transformers/tokenization_bert.py
 */
public class BasicTokenizer {

    private final boolean isLowerCase;
    private final boolean isTokenizeCjkChars;
    private final boolean isStripAccents;
    private final Set<String> neverSplitTokens;
    private final TokenTrieNode neverSplitTokenTrieRoot;

    /**
     * Tokenizer behaviour is controlled by the options passed here.
     *
     * @param isLowerCase  If true convert the input to lowercase
     * @param isTokenizeCjkChars Should CJK ideographs be tokenized
     * @param isStripAccents Strip all accents
     * @param neverSplit The set of tokens that should not be split
     */
    public BasicTokenizer(boolean isLowerCase, boolean isTokenizeCjkChars, boolean isStripAccents, Set<String> neverSplit) {
        this.isLowerCase = isLowerCase;
        this.isTokenizeCjkChars = isTokenizeCjkChars;
        this.isStripAccents = isStripAccents;
        this.neverSplitTokens = neverSplit;
        this.neverSplitTokenTrieRoot = TokenTrieNode.build(neverSplit, this::doTokenizeString);
    }

    public BasicTokenizer(boolean isLowerCase, boolean isTokenizeCjkChars, boolean isStripAccents) {
        this(isLowerCase, isTokenizeCjkChars, isStripAccents, Collections.emptySet());
    }

    /**
     * Tokenize CJK chars defaults to the value of {@code isLowerCase}
     * when not explicitly set
     * @param isLowerCase  If true convert the input to lowercase
     * @param isTokenizeCjkChars Should CJK ideographs be tokenized
     */
    public BasicTokenizer(boolean isLowerCase, boolean isTokenizeCjkChars) {
        this(isLowerCase, isTokenizeCjkChars, isLowerCase);
    }

    BasicTokenizer() {
        this(true, true, true);
    }

    /**
     * Clean the text and whitespace tokenize then process depending
     * on the values of {@code lowerCase}, {@code tokenizeCjkChars},
     * {@code stripAccents} and the contents of {@code neverSplit}
     *
     * @param text The input text to tokenize
     * @return List of tokens
     */
    public List<DelimitedToken> tokenize(String text) {
        return mergeNeverSplitTokens(text, doTokenize(text));
    }

    private List<String> doTokenizeString(String text) {
        return doTokenize(text).stream().map(DelimitedToken::getToken).collect(Collectors.toList());
    }

    private List<DelimitedToken> doTokenize(String text) {
        text = cleanText(text);
        if (isTokenizeCjkChars) {
            text = tokenizeCjkChars(text);
        }

        List<DelimitedToken> tokens = whiteSpaceTokenize(text);

        List<DelimitedToken> processedTokens = new ArrayList<>(tokens.size());
        for (DelimitedToken tokenRecord : tokens) {

            String tokenStr = tokenRecord.getToken();
            if (Strings.EMPTY.equals(tokenStr)) {
                continue;
            }

            if (isLowerCase) {
                tokenStr = tokenStr.toLowerCase(Locale.ROOT);
            }
            if (isStripAccents) {
                tokenStr = stripAccents(tokenStr);
            }
            processedTokens.addAll(splitOnPunctuation(new DelimitedToken(tokenRecord.getStartPos(), tokenRecord.getEndPos(), tokenStr)));
        }

        return processedTokens;
    }

    private List<DelimitedToken> mergeNeverSplitTokens(String originalText, List<DelimitedToken> tokens) {
        if (neverSplitTokenTrieRoot.isLeaf()) {
            return tokens;
        }
        List<DelimitedToken> mergedTokens = new ArrayList<>(tokens.size());
        List<DelimitedToken> matchingTokens = new ArrayList<>();
        TokenTrieNode current = neverSplitTokenTrieRoot;
        for (DelimitedToken token : tokens) {
            TokenTrieNode childNode = current.getChild(token.getToken());
            if (childNode == null) {
                if (current != neverSplitTokenTrieRoot) {
                    mergedTokens.addAll(matchingTokens);
                    matchingTokens = new ArrayList<>();
                    current = neverSplitTokenTrieRoot;
                }
                mergedTokens.add(token);
            } else if (childNode.isLeaf()) {
                matchingTokens.add(token);
                DelimitedToken mergedToken = DelimitedToken.mergeTokens(matchingTokens);
                String originalTokenText = originalText.substring(mergedToken.getStartPos(), mergedToken.getEndPos());
                if (neverSplitTokens.contains(originalTokenText)) {
                    mergedTokens.add(new DelimitedToken(mergedToken.getStartPos(), mergedToken.getEndPos(), originalTokenText));
                } else {
                    mergedTokens.addAll(matchingTokens);
                }
                matchingTokens = new ArrayList<>();
                current = neverSplitTokenTrieRoot;
            } else {
                matchingTokens.add(token);
                current = childNode;
            }
        }
        return mergedTokens;
    }

    public boolean isLowerCase() {
        return isLowerCase;
    }

    public boolean isStripAccents() {
        return isStripAccents;
    }

    public boolean isTokenizeCjkChars() {
        return isTokenizeCjkChars;
    }

    /**
     * Split the input text by whitespace.
     * For the returned objects {@link DelimitedToken#getStartPos()} is the
     * start character index inclusive and {@link DelimitedToken#getEndPos()}
     * the index exclusive. The number of whitespace characters between 2 consecutive
     * {@link DelimitedToken}s is the difference between the first's {@code endPos}
     * and the second's {@code startPos}.
     *
     * The input should be normalized via a call to {@link #cleanText(String)}
     * before it is passed to this function.
     *
     * @param text to tokenize
     * @return White space separated strings
     */
    static List<DelimitedToken> whiteSpaceTokenize(String text) {
        var tokens = new ArrayList<DelimitedToken>();

        // whitespace at beginning
        int index = 0;
        while (index < text.length() && text.charAt(index) == ' ') {
            index++;
        }

        int tokenStart = index;

        while (index < text.length()) {
            if (text.charAt(index) == ' ') {
                int tokenEnd = index;
                index++;
                // consume trail whitespace before the next word
                // or end of text
                while (index < text.length() && text.charAt(index) == ' ') {
                    index++;
                }

                tokens.add(new DelimitedToken(tokenStart, tokenEnd, text.substring(tokenStart, tokenEnd)));
                tokenStart = index;
            }
            index++;
        }

        // trailing whitespace
        if (tokenStart != text.length()) {
            tokens.add(new DelimitedToken(tokenStart, text.length(), text.substring(tokenStart)));
        }

        return tokens;
    }

    /**
     * Normalize unicode text to NFD form
     * "Characters are decomposed by canonical equivalence, and multiple
     * combining characters are arranged in a specific order"
     * from https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms
     *
     * And remove non-spacing marks https://www.compart.com/en/unicode/category/Mn
     *
     * @param word Word to strip
     * @return {@code word} normalized and stripped.
     */
    static String stripAccents(String word) {
        String normalizedString = Normalizer.normalize(word, Normalizer.Form.NFD);

        int[] codePoints = normalizedString.codePoints()
            .filter(codePoint -> Character.getType(codePoint) != Character.NON_SPACING_MARK)
            .toArray();

        return new String(codePoints, 0, codePoints.length);
    }

    static List<DelimitedToken> splitOnPunctuation(DelimitedToken word) {
        List<DelimitedToken> splits = new ArrayList<>();
        int[] codePoints = word.getToken().codePoints().toArray();

        int lastSplit = 0;
        for (int i = 0; i < codePoints.length; i++) {
            if (isPunctuationMark(codePoints[i])) {
                int charCount = i - lastSplit;
                if (charCount > 0) {
                    // add a new string for what has gone before
                    splits.add(
                        new DelimitedToken(
                            word.getStartPos() + lastSplit,
                            word.getStartPos() + i,
                            new String(codePoints, lastSplit, i - lastSplit)
                        )
                    );
                }
                splits.add(new DelimitedToken(word.getStartPos() + i, word.getStartPos() + i + 1, new String(codePoints, i, 1)));
                lastSplit = i + 1;
            }
        }

        if (lastSplit < codePoints.length) {
            splits.add(
                new DelimitedToken(
                    word.getStartPos() + lastSplit,
                    word.getStartPos() + codePoints.length,
                    new String(codePoints, lastSplit, codePoints.length - lastSplit)
                )
            );
        }

        return splits;
    }

    /**
     * Surrounds any CJK character with whitespace
     * @param text To tokenize
     * @return tokenized text
     */
    static String tokenizeCjkChars(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        AtomicBoolean cjkCharFound = new AtomicBoolean(false);

        text.codePoints().forEach(cp -> {
            if (isCjkChar(cp)) {
                sb.append(' ');
                sb.appendCodePoint(cp);
                sb.append(' ');
                cjkCharFound.set(true);
            } else {
                sb.appendCodePoint(cp);
            }
        });

        // no change
        if (cjkCharFound.get() == false) {
            return text;
        }

        return sb.toString();
    }

    /**
     * Remove control chars and normalize white space to ' '
     * @param text Text to clean
     * @return Cleaned text
     */
    static String cleanText(String text) {
        int[] codePoints = text.codePoints()
            .filter(codePoint -> (codePoint == 0x00 || codePoint == 0xFFFD || isControlChar(codePoint)) == false)
            .map(codePoint -> isWhiteSpace(codePoint) ? ' ' : codePoint)
            .toArray();

        return new String(codePoints, 0, codePoints.length);
    }

    static boolean isCjkChar(int codePoint) {
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

    /**
     * newline, carriage return and tab are control chars but for
     * tokenization purposes they are treated as whitespace.
     *
     * @param codePoint code point
     * @return is control char
     */
    static boolean isControlChar(int codePoint) {
        if (codePoint == '\n' || codePoint == '\r' || codePoint == '\t') {
            return false;
        }
        int category = Character.getType(codePoint);

        return category >= Character.CONTROL && category <= Character.SURROGATE;
    }

    /**
     * newline, carriage return and tab are technically control chars
     * but are not part of the Unicode Space Separator (Zs) group.
     * For tokenization purposes they are treated as whitespace
     *
     * @param codePoint code point
     * @return is white space
     */
    static boolean isWhiteSpace(int codePoint) {
        if (codePoint == '\n' || codePoint == '\r' || codePoint == '\t') {
            return true;
        }
        return Character.getType(codePoint) == Character.SPACE_SEPARATOR;
    }

    /**
     * We treat all non-letter/number ASCII as punctuation.
     * Characters such as "^", "$", and "`" are not in the Unicode
     * Punctuation class but are treated as punctuation for consistency.
     *
     * @param codePoint code point
     * @return true if is punctuation
     */
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
