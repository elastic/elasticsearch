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
import java.util.Objects;
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
public class BertTokenizer implements NlpTokenizer {

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
    private final String sepToken;
    protected final int sepTokenId;
    private final String clsToken;
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
        this.sepToken = sepToken;
        this.clsToken = clsToken;
        this.padToken = padToken;
        this.maskToken = maskToken;
        this.unknownToken = unknownToken;
    }

    public String getSepToken() {
        return sepToken;
    }

    public String getClsToken() {
        return clsToken;
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

    /**
     * Tokenize the input according to the basic tokenization
     * options then perform Word Piece tokenization with the given vocabulary.
     *
     * The result is the Word Piece tokens, a map of the Word Piece
     * token position to the position of the token in the source for
     * each input string grouped into a {@link Tokenization}.
     *
     * @param seq Text to tokenize
     * @return A list of {@link Tokenization}
     */
    @Override
    public List<TokenizationResult.Tokens> tokenize(String seq, Tokenization.Truncate truncate, int span, int sequenceId) {
        var innerResult = innerTokenize(seq);
        List<WordPieceTokenFilter.WordPieceToken> wordPieceTokenIds = innerResult.tokens;
        List<Integer> tokenPositionMap = innerResult.tokenPositionMap;
        int numTokens = withSpecialTokens ? wordPieceTokenIds.size() + 2 : wordPieceTokenIds.size();
        boolean isTruncated = false;

        if (numTokens > maxSequenceLength) {
            switch (truncate) {
                case FIRST, SECOND -> {
                    isTruncated = true;
                    wordPieceTokenIds = wordPieceTokenIds.subList(0, withSpecialTokens ? maxSequenceLength - 2 : maxSequenceLength);
                    tokenPositionMap = tokenPositionMap.subList(0, withSpecialTokens ? maxSequenceLength - 2 : maxSequenceLength);
                }
                case NONE -> {
                    if (span == -1) {
                        throw ExceptionsHelper.badRequestException(
                            "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                            numTokens,
                            maxSequenceLength
                        );
                    }
                }
            }
        }

        if (numTokens <= maxSequenceLength || span == -1) {
            return List.of(
                createTokensBuilder(clsTokenId, sepTokenId, withSpecialTokens).addSequence(
                    wordPieceTokenIds.stream().map(WordPieceTokenFilter.WordPieceToken::getEncoding).collect(Collectors.toList()),
                    tokenPositionMap
                ).build(seq, isTruncated, innerResult.tokens, -1, sequenceId)
            );
        }

        List<TokenizationResult.Tokens> toReturn = new ArrayList<>();
        int splitEndPos = 0;
        int splitStartPos = 0;
        int spanPrev = -1;
        while (splitEndPos < wordPieceTokenIds.size()) {
            splitEndPos = Math.min(
                splitStartPos + (withSpecialTokens ? maxSequenceLength - 2 : maxSequenceLength),
                wordPieceTokenIds.size()
            );
            // Make sure we do not end on a word
            if (splitEndPos != wordPieceTokenIds.size()) {
                while (Objects.equals(tokenPositionMap.get(splitEndPos), tokenPositionMap.get(splitEndPos - 1))) {
                    splitEndPos--;
                }
            }

            toReturn.add(
                createTokensBuilder(clsTokenId, sepTokenId, withSpecialTokens).addSequence(
                    wordPieceTokenIds.subList(splitStartPos, splitEndPos)
                        .stream()
                        .map(WordPieceTokenFilter.WordPieceToken::getEncoding)
                        .collect(Collectors.toList()),
                    tokenPositionMap.subList(splitStartPos, splitEndPos)
                ).build(seq, false, innerResult.tokens, spanPrev, sequenceId)
            );
            spanPrev = span;
            int prevSplitStart = splitStartPos;
            splitStartPos = splitEndPos - span;
            // try to back up our split so that it starts at the first whole word
            if (splitStartPos < wordPieceTokenIds.size()) {
                while (splitStartPos > (prevSplitStart + 1)
                    && Objects.equals(tokenPositionMap.get(splitStartPos), tokenPositionMap.get(splitStartPos - 1))) {
                    splitStartPos--;
                    spanPrev++;
                }
            }
        }
        return toReturn;
    }

    @Override
    public TokenizationResult.Tokens tokenize(String seq1, String seq2, Tokenization.Truncate truncate, int sequenceId) {
        var innerResultSeq1 = innerTokenize(seq1);
        List<WordPieceTokenFilter.WordPieceToken> wordPieceTokenIdsSeq1 = innerResultSeq1.tokens;
        List<Integer> tokenPositionMapSeq1 = innerResultSeq1.tokenPositionMap;
        var innerResultSeq2 = innerTokenize(seq2);
        List<WordPieceTokenFilter.WordPieceToken> wordPieceTokenIdsSeq2 = innerResultSeq2.tokens;
        List<Integer> tokenPositionMapSeq2 = innerResultSeq2.tokenPositionMap;
        if (withSpecialTokens == false) {
            throw new IllegalArgumentException("Unable to do sequence pair tokenization without special tokens");
        }
        int extraTokens = getNumExtraTokensForSeqPair();
        int numTokens = wordPieceTokenIdsSeq1.size() + wordPieceTokenIdsSeq2.size() + extraTokens;

        boolean isTruncated = false;
        if (numTokens > maxSequenceLength) {
            switch (truncate) {
                case FIRST -> {
                    isTruncated = true;
                    if (wordPieceTokenIdsSeq2.size() > maxSequenceLength - extraTokens) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the second sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            wordPieceTokenIdsSeq2.size(),
                            maxSequenceLength - extraTokens
                        );
                    }
                    wordPieceTokenIdsSeq1 = wordPieceTokenIdsSeq1.subList(
                        0,
                        maxSequenceLength - extraTokens - wordPieceTokenIdsSeq2.size()
                    );
                    tokenPositionMapSeq1 = tokenPositionMapSeq1.subList(0, maxSequenceLength - extraTokens - wordPieceTokenIdsSeq2.size());
                }
                case SECOND -> {
                    isTruncated = true;
                    if (wordPieceTokenIdsSeq1.size() > maxSequenceLength - extraTokens) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the first sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            wordPieceTokenIdsSeq1.size(),
                            maxSequenceLength - extraTokens
                        );
                    }
                    wordPieceTokenIdsSeq2 = wordPieceTokenIdsSeq2.subList(
                        0,
                        maxSequenceLength - extraTokens - wordPieceTokenIdsSeq1.size()
                    );
                    tokenPositionMapSeq2 = tokenPositionMapSeq2.subList(0, maxSequenceLength - extraTokens - wordPieceTokenIdsSeq1.size());
                }
                case NONE -> throw ExceptionsHelper.badRequestException(
                    "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                    numTokens,
                    maxSequenceLength
                );
            }
        }
        List<WordPieceTokenFilter.WordPieceToken> tokens = new ArrayList<>(innerResultSeq1.tokens);
        tokens.addAll(innerResultSeq2.tokens);
        return createTokensBuilder(clsTokenId, sepTokenId, withSpecialTokens).addSequencePair(
            wordPieceTokenIdsSeq1.stream().map(WordPieceTokenFilter.WordPieceToken::getEncoding).collect(Collectors.toList()),
            tokenPositionMapSeq1,
            wordPieceTokenIdsSeq2.stream().map(WordPieceTokenFilter.WordPieceToken::getEncoding).collect(Collectors.toList()),
            tokenPositionMapSeq2
        ).build(seq1 + seq2, isTruncated, tokens, -1, sequenceId);
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

    protected int getNumExtraTokensForSeqPair() {
        return 3;
    }

    private InnerTokenization innerTokenize(String seq) {
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

    private static class InnerTokenization {
        List<WordPieceTokenFilter.WordPieceToken> tokens;
        List<Integer> tokenPositionMap;

        InnerTokenization(List<WordPieceTokenFilter.WordPieceToken> tokens, List<Integer> tokenPositionMap) {
            this.tokens = tokens;
            this.tokenPositionMap = tokenPositionMap;
        }
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
