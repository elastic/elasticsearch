/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs basic tokenization and normalization of input text
 * then tokenizes with the WordPiece algorithm using the given
 * vocabulary.
 * <p>
 * Derived from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/src/transformers/tokenization_bert.py
 */
public class BertTokenizer {

    public static final String UNKNOWN_TOKEN = "[UNK]";
    public static final String SEPARATOR_TOKEN = "[SEP]";
    public static final String PAD_TOKEN = "[PAD]";
    public static final String CLASS_TOKEN = "[CLS]";
    public static final String MASK_TOKEN = "[MASK]";

    public static final int DEFAULT_MAX_INPUT_CHARS_PER_WORD = 100;

    private final WordPieceTokenizer wordPieceTokenizer;
    private final Map<String, Integer> vocab;
    private final boolean doLowerCase;
    private final boolean doTokenizeCjKChars;
    private final boolean doStripAccents;
    private final Set<String> neverSplit;

    private BertTokenizer(Map<String, Integer> vocab,
                          boolean doLowerCase,
                          boolean doTokenizeCjKChars,
                          boolean doStripAccents,
                          Set<String> neverSplit) {
        wordPieceTokenizer = new WordPieceTokenizer(vocab, UNKNOWN_TOKEN, DEFAULT_MAX_INPUT_CHARS_PER_WORD);
        this.vocab = vocab;
        this.doLowerCase = doLowerCase;
        this.doTokenizeCjKChars = doTokenizeCjKChars;
        this.doStripAccents = doStripAccents;
        this.neverSplit = neverSplit;
    }

    /**
     * Tokenize the input according to the basic tokenization options
     * then perform Word Piece tokenization with the given vocabulary.
     *
     * The result is the Word Piece tokens, a map of the Word Piece
     * token position to the position of the token in the source
     * @param text Text to tokenize
     * @return Tokenized text, token Ids and map
     */
    public TokenizationResult tokenize(String text) {
        BasicTokenizer basicTokenizer = new BasicTokenizer(doLowerCase, doTokenizeCjKChars, doStripAccents, neverSplit);

        List<String> delineatedTokens = basicTokenizer.tokenize(text);
        List<WordPieceTokenizer.TokenAndId> wordPieceTokens = new ArrayList<>();
        List<Integer> tokenPositionMap = new ArrayList<>();

        for (int sourceIndex = 0; sourceIndex < delineatedTokens.size(); sourceIndex++) {
            String token = delineatedTokens.get(sourceIndex);
            if (neverSplit.contains(token)) {
                wordPieceTokens.add(new WordPieceTokenizer.TokenAndId(token, vocab.getOrDefault(token, vocab.get(UNKNOWN_TOKEN))));
                tokenPositionMap.add(sourceIndex);
            } else {
                List<WordPieceTokenizer.TokenAndId> tokens = wordPieceTokenizer.tokenize(token);
                for (int tokenCount = 0; tokenCount < tokens.size(); tokenCount++) {
                    tokenPositionMap.add(sourceIndex);
                }
                wordPieceTokens.addAll(tokens);
            }
        }

        assert tokenPositionMap.size() == wordPieceTokens.size();

        List<String> tokens = new ArrayList<>(wordPieceTokens.size());
        int [] tokenIds = new int[wordPieceTokens.size()];
        int [] tokenMap = new int[wordPieceTokens.size()];
        for (int i = 0; i < wordPieceTokens.size(); i++) {
            tokens.add(wordPieceTokens.get(i).getToken());
            tokenIds[i] = wordPieceTokens.get(i).getId();
            tokenMap[i] = tokenPositionMap.get(i);
        }

        return new TokenizationResult(tokens, tokenIds, tokenMap);
    }

    public class TokenizationResult {
        private final List<String> tokens;
        private final int [] tokenIds;
        private final int [] tokenMap;

        TokenizationResult(List<String> tokens, int [] tokenIds, int [] tokenMap) {
            assert tokens.size() == tokenIds.length;
            assert tokenIds.length == tokenMap.length;
            this.tokens = tokens;
            this.tokenIds = tokenIds;
            this.tokenMap = tokenMap;
        }

        /**
         * The token strings from the tokenization process
         * @return A list of tokens
         */
        public List<String> getTokens() {
            return tokens;
        }

        /**
         * The integer values of the tokens in {@link #getTokens()}
         * @return A list of token Ids
         */
        public int[] getTokenIds() {
            return tokenIds;
        }

        /**
         * Maps the token position to the position in the source text.
         * Source words may be divided into more than one token so more
         * than one token can map back to the source token
         * @return Map of source token to
         */
        public int[] getTokenMap() {
            return tokenMap;
        }
    }

    public static Builder builder(Map<String, Integer> vocab) {
        return new Builder(vocab);
    }

    public static class Builder {

        private final Map<String, Integer> vocab;
        private boolean doLowerCase = true;
        private boolean doTokenizeCjKChars = true;
        private Boolean doStripAccents = null;
        private Set<String> neverSplit;

        public Builder(Map<String, Integer> vocab) {
            this.vocab = vocab;
        }

        public Builder setDoLowerCase(boolean doLowerCase) {
            this.doLowerCase = doLowerCase;
            return this;
        }

        public Builder setDoTokenizeCjKChars(boolean doTokenizeCjKChars) {
            this.doTokenizeCjKChars = doTokenizeCjKChars;
            return this;
        }

        public Builder setDoStripAccents(Boolean doStripAccents) {
            this.doStripAccents = doStripAccents;
            return this;
        }

        public Builder setNeverSplit(Set<String> neverSplit) {
            this.neverSplit = neverSplit;
            return this;
        }

        public BertTokenizer build() {
            // if not set strip accents defaults to the value of doLowerCase
            if (doStripAccents == null) {
                doStripAccents = doLowerCase;
            }

            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new BertTokenizer(vocab, doLowerCase, doTokenizeCjKChars, doStripAccents, neverSplit);
        }
    }
}
