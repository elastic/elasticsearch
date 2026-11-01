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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ByteLevelBpeTokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Byte-level BPE tokenizer backed by {@link BpeTokenizer} and {@link BpeAnalyzer}, using configurable
 * special token strings from {@link ByteLevelBpeTokenization}.
 *
 * <p>Results are built with {@link RobertaTokenizationResult} on purpose: the packed token layout
 * (CLS/BOS, SEP/EOS, padding) matches what RoBERTa-style BPE models expect, independent of the
 * configurable BOS/EOS string values.
 */
public class ByteLevelBpeTokenizer extends NlpTokenizer {

    private final BpeAnalyzer bpeAnalyzer;
    private final List<String> originalVocab;
    private final SortedMap<String, Integer> vocab;
    private final boolean withSpecialTokens;
    private final int sepTokenId;
    private final int clsTokenId;
    private final int padTokenId;
    private final int maxSequenceLength;
    private final String unkToken;
    private final String padToken;
    private final String bosToken;
    private final String eosToken;
    private final String maskToken;

    private ByteLevelBpeTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        List<String> merges,
        ByteLevelBpeTokenization tokenization,
        Set<String> neverSplit
    ) {
        this.originalVocab = originalVocab;
        this.unkToken = tokenization.getUnkToken();
        this.padToken = tokenization.getPadToken();
        this.bosToken = tokenization.getBosToken();
        this.eosToken = tokenization.getEosToken();
        this.maskToken = tokenization.getMaskToken();
        this.bpeAnalyzer = new BpeAnalyzer(
            originalVocab,
            merges,
            new ArrayList<>(Sets.union(Set.of(maskToken), neverSplit)),
            tokenization.isAddPrefixSpace(),
            unkToken
        );
        this.vocab = vocab;
        this.withSpecialTokens = tokenization.withSpecialTokens();
        this.maxSequenceLength = tokenization.maxSequenceLength();
        if (vocab.containsKey(unkToken) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", unkToken);
        }
        if (vocab.containsKey(padToken) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", padToken);
        }
        this.padTokenId = vocab.get(padToken);
        if (withSpecialTokens) {
            // Use a mutable set: bos/eos strings are user-configurable and may match; Set.of would throw on duplicates.
            Set<String> requiredSpecialTokens = new HashSet<>();
            requiredSpecialTokens.add(eosToken);
            requiredSpecialTokens.add(bosToken);
            Set<String> missingSpecialTokens = Sets.difference(requiredSpecialTokens, vocab.keySet());
            if (missingSpecialTokens.isEmpty() == false) {
                throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required {} token(s)", missingSpecialTokens);
            }
            this.sepTokenId = vocab.get(eosToken);
            this.clsTokenId = vocab.get(bosToken);
        } else {
            this.sepTokenId = -1;
            this.clsTokenId = -1;
        }
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
    int getNumExtraTokensForSeqPair() {
        return 4;
    }

    @Override
    int numExtraTokensForSingleSequence() {
        return 2;
    }

    @Override
    int clsTokenId() {
        return clsTokenId;
    }

    @Override
    public String getPadToken() {
        return padToken;
    }

    public String getUnknownToken() {
        return unkToken;
    }

    @Override
    public void close() {
        this.bpeAnalyzer.close();
    }

    @Override
    public TokenizationResult buildTokenizationResult(List<TokenizationResult.Tokens> tokenizations) {
        return new RobertaTokenizationResult(originalVocab, tokenizations, padTokenId);
    }

    @Override
    public NlpTask.RequestBuilder requestBuilder() {
        return (inputs, requestId, truncate, span, windowSize) -> buildTokenizationResult(
            IntStream.range(0, inputs.size())
                .boxed()
                .flatMap(seqId -> tokenize(inputs.get(seqId), truncate, span, seqId, windowSize).stream())
                .collect(Collectors.toList())
        ).buildRequest(requestId, truncate);
    }

    @Override
    public OptionalInt getPadTokenId() {
        return OptionalInt.of(padTokenId);
    }

    @Override
    public OptionalInt getMaskTokenId() {
        Integer maskId = vocab.get(maskToken);
        if (maskId == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(maskId);
    }

    @Override
    public String getMaskToken() {
        return maskToken;
    }

    @Override
    public List<String> getVocabulary() {
        return originalVocab;
    }

    @Override
    TokenizationResult.TokensBuilder createTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens) {
        return new RobertaTokenizationResult.RobertaTokensBuilder(withSpecialTokens, clsTokenId, sepTokenId);
    }

    @Override
    public InnerTokenization innerTokenize(String seq) {
        List<Integer> tokenPositionMap = new ArrayList<>();
        try (TokenStream ts = bpeAnalyzer.tokenStream("input", seq)) {
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
        return new InnerTokenization(new ArrayList<>(bpeAnalyzer.getTokens()), tokenPositionMap);
    }

    public static Builder builder(List<String> vocab, List<String> merges, ByteLevelBpeTokenization tokenization) {
        return new Builder(vocab, merges, tokenization);
    }

    public static class Builder {

        private final List<String> originalVocab;
        private final List<String> merges;
        private final SortedMap<String, Integer> vocab;
        private final ByteLevelBpeTokenization tokenization;
        private boolean withSpecialTokens;
        private int maxSequenceLength;
        private Set<String> neverSplit;

        private Builder(List<String> vocab, List<String> merges, ByteLevelBpeTokenization tokenization) {
            this.originalVocab = vocab;
            this.vocab = buildSortedVocab(vocab);
            this.merges = merges;
            this.tokenization = tokenization;
            this.withSpecialTokens = tokenization.withSpecialTokens();
            this.maxSequenceLength = tokenization.maxSequenceLength();
        }

        private static SortedMap<String, Integer> buildSortedVocab(List<String> vocab) {
            SortedMap<String, Integer> sortedVocab = new TreeMap<>();
            for (int i = 0; i < vocab.size(); i++) {
                sortedVocab.put(vocab.get(i), i);
            }
            return sortedVocab;
        }

        public Builder setNeverSplit(Set<String> neverSplit) {
            this.neverSplit = neverSplit;
            return this;
        }

        public Builder setMaxSequenceLength(int maxSequenceLength) {
            this.maxSequenceLength = maxSequenceLength;
            return this;
        }

        public Builder setWithSpecialTokens(boolean withSpecialTokens) {
            this.withSpecialTokens = withSpecialTokens;
            return this;
        }

        public ByteLevelBpeTokenizer build() {
            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }
            ByteLevelBpeTokenization effectiveTokenization = new ByteLevelBpeTokenization(
                false,
                withSpecialTokens,
                maxSequenceLength,
                tokenization.getTruncate(),
                tokenization.getSpan(),
                tokenization.isAddPrefixSpace(),
                tokenization.getUnkToken(),
                tokenization.getPadToken(),
                tokenization.getBosToken(),
                tokenization.getEosToken(),
                tokenization.getMaskToken()
            );
            return new ByteLevelBpeTokenizer(originalVocab, vocab, merges, effectiveTokenization, neverSplit);
        }
    }
}
