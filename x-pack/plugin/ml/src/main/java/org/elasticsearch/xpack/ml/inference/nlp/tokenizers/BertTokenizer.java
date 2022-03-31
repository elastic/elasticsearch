/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Performs basic tokenization and normalization of input text
 * then tokenizes with the WordPiece algorithm using the given
 * vocabulary.
 * <p>
 * Derived from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/src/transformers/tokenization_bert.py
 */
public class BertTokenizer extends NlpTokenizer {

    public static final String UNKNOWN_TOKEN = "[UNK]";
    public static final String SEPARATOR_TOKEN = "[SEP]";
    public static final String PAD_TOKEN = "[PAD]";
    public static final String CLASS_TOKEN = "[CLS]";
    public static final String MASK_TOKEN = "[MASK]";

    private static final Set<String> NEVER_SPLIT = Set.of(MASK_TOKEN);

    private final WordPieceAnalyzer wordPieceAnalyzer;
    protected final List<String> originalVocab;
    // TODO Not sure this needs to be a sorted map
    private final SortedMap<String, Integer> vocab;
    protected final boolean withSpecialTokens;
    private final int maxSequenceLength;
    protected final int sepTokenId;
    private final int clsTokenId;
    private final String padToken;
    protected final int padTokenId;
    private final String maskToken;
    private final String unknownToken;

    protected BertTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Set<String> neverSplit
    ) {
        this(
            originalVocab,
            vocab,
            doLowerCase,
            doTokenizeCjKChars,
            doStripAccents,
            withSpecialTokens,
            maxSequenceLength,
            Sets.union(neverSplit, NEVER_SPLIT),
            SEPARATOR_TOKEN,
            CLASS_TOKEN,
            PAD_TOKEN,
            MASK_TOKEN,
            UNKNOWN_TOKEN
        );
    }

    protected BertTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Set<String> neverSplit,
        String sepToken,
        String clsToken,
        String padToken,
        String maskToken,
        String unknownToken
    ) {
        wordPieceAnalyzer = new WordPieceAnalyzer(
            originalVocab,
            new ArrayList<>(neverSplit),
            doLowerCase,
            doTokenizeCjKChars,
            doStripAccents,
            unknownToken
        );
        this.originalVocab = originalVocab;
        this.vocab = vocab;
        this.withSpecialTokens = withSpecialTokens;
        this.maxSequenceLength = maxSequenceLength;
        if (vocab.containsKey(unknownToken) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", unknownToken);
        }
        if (vocab.containsKey(padToken) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", padToken);
        }
        this.padTokenId = vocab.get(padToken);

        if (withSpecialTokens) {
            Set<String> missingSpecialTokens = Sets.difference(Set.of(sepToken, clsToken), vocab.keySet());
            if (missingSpecialTokens.isEmpty() == false) {
                throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required {} token(s)", missingSpecialTokens);
            }
            this.sepTokenId = vocab.get(sepToken);
            this.clsTokenId = vocab.get(clsToken);
        } else {
            this.sepTokenId = -1;
            this.clsTokenId = -1;
        }
        this.padToken = padToken;
        this.maskToken = maskToken;
        this.unknownToken = unknownToken;
    }

    @Override
    int sepTokenId() {
        return sepTokenId;
    }

    @Override
    int maxSequenceLength() {
        return maxSequenceLength;
    }

    @Override
    boolean isWithSpecialTokens() {
        return withSpecialTokens;
    }

    @Override
    int clsTokenId() {
        return clsTokenId;
    }

    public String getPadToken() {
        return padToken;
    }

    public String getUnknownToken() {
        return unknownToken;
    }

    @Override
    public OptionalInt getPadTokenId() {
        Integer pad = vocab.get(this.padToken);
        if (pad != null) {
            return OptionalInt.of(pad);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public OptionalInt getMaskTokenId() {
        Integer pad = vocab.get(this.maskToken);
        if (pad != null) {
            return OptionalInt.of(pad);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public String getMaskToken() {
        return maskToken;
    }

    @Override
    public TokenizationResult buildTokenizationResult(List<TokenizationResult.Tokens> tokenizations) {
        return new BertTokenizationResult(originalVocab, tokenizations, vocab.get(this.padToken));
    }

    TokenizationResult.TokensBuilder createTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens) {
        return new BertTokenizationResult.BertTokensBuilder(withSpecialTokens, clsTokenId, sepTokenId);
    }

    @Override
    public NlpTask.RequestBuilder requestBuilder() {
        return (inputs, requestId, truncate, span) -> buildTokenizationResult(
            IntStream.range(0, inputs.size())
                .boxed()
                .flatMap(seqId -> tokenize(inputs.get(seqId), truncate, span, seqId).stream())
                .collect(Collectors.toList())
        ).buildRequest(requestId, truncate);
    }

    @Override
    int getNumExtraTokensForSeqPair() {
        return 3;
    }

    @Override
    public InnerTokenization innerTokenize(String seq) {
        List<Integer> tokenPositionMap = new ArrayList<>();
        try (TokenStream ts = wordPieceAnalyzer.tokenStream("input", seq)) {
            ts.reset();
            PositionIncrementAttribute tokenPos = ts.addAttribute(PositionIncrementAttribute.class);
            int currPos = -1;
            while (ts.incrementToken()) {
                currPos += tokenPos.getPositionIncrement();
                tokenPositionMap.add(currPos);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return new InnerTokenization(new ArrayList<>(wordPieceAnalyzer.getTokens()), tokenPositionMap);
    }

    @Override
    public void close() {
        wordPieceAnalyzer.close();
    }

    public int getMaxSequenceLength() {
        return maxSequenceLength;
    }

    public static Builder builder(List<String> vocab, Tokenization tokenization) {
        return new Builder(vocab, tokenization);
    }

    public static class Builder {

        protected final List<String> originalVocab;
        protected final SortedMap<String, Integer> vocab;
        protected boolean doLowerCase;
        protected boolean doTokenizeCjKChars = true;
        protected boolean withSpecialTokens;
        protected int span = -1;
        protected int maxSequenceLength;
        protected Boolean doStripAccents = null;
        protected Set<String> neverSplit;

        protected Builder(List<String> vocab, Tokenization tokenization) {
            this.originalVocab = vocab;
            this.vocab = buildSortedVocab(vocab);
            this.doLowerCase = tokenization.doLowerCase();
            this.withSpecialTokens = tokenization.withSpecialTokens();
            this.maxSequenceLength = tokenization.maxSequenceLength();
            this.span = tokenization.getSpan();
        }

        private static SortedMap<String, Integer> buildSortedVocab(List<String> vocab) {
            SortedMap<String, Integer> sortedVocab = new TreeMap<>();
            for (int i = 0; i < vocab.size(); i++) {
                sortedVocab.put(vocab.get(i), i);
            }
            return sortedVocab;
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

        public Builder setMaxSequenceLength(int maxSequenceLength) {
            this.maxSequenceLength = maxSequenceLength;
            return this;
        }

        /**
         * Include CLS and SEP tokens
         * @param withSpecialTokens if true include CLS and SEP tokens
         * @return this
         */
        public Builder setWithSpecialTokens(boolean withSpecialTokens) {
            this.withSpecialTokens = withSpecialTokens;
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

            return new BertTokenizer(
                originalVocab,
                vocab,
                doLowerCase,
                doTokenizeCjKChars,
                doStripAccents,
                withSpecialTokens,
                maxSequenceLength,
                neverSplit
            );
        }
    }
}
