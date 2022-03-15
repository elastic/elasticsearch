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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
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

public class RobertaTokenizer extends NlpTokenizer {

    public static final String UNKNOWN_TOKEN = "<unk>";
    public static final String SEPARATOR_TOKEN = "</s>";
    public static final String PAD_TOKEN = "<pad>";
    public static final String CLASS_TOKEN = "<s>";
    public static final String MASK_TOKEN = "<mask>";

    private static final Set<String> NEVER_SPLIT = Set.of(MASK_TOKEN);

    private final BpeAnalyzer bpeAnalyzer;
    protected final List<String> originalVocab;
    // TODO Not sure this needs to be a sorted map
    private final SortedMap<String, Integer> vocab;
    protected final boolean withSpecialTokens;
    protected final int sepTokenId;
    private final int clsTokenId;
    protected final int padTokenId;
    private final int maxSequenceLength;

    protected RobertaTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        List<String> merges,
        boolean isPrefixSpace,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Set<String> neverSplit
    ) {
        this.originalVocab = originalVocab;
        this.bpeAnalyzer = new BpeAnalyzer(
            originalVocab,
            merges,
            new ArrayList<>(Sets.union(NEVER_SPLIT, neverSplit)),
            isPrefixSpace,
            UNKNOWN_TOKEN
        );
        this.vocab = vocab;
        this.withSpecialTokens = withSpecialTokens;
        this.maxSequenceLength = maxSequenceLength;
        if (vocab.containsKey(UNKNOWN_TOKEN) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", UNKNOWN_TOKEN);
        }
        if (vocab.containsKey(PAD_TOKEN) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", PAD_TOKEN);
        }
        this.padTokenId = vocab.get(PAD_TOKEN);
        if (withSpecialTokens) {
            Set<String> missingSpecialTokens = Sets.difference(Set.of(SEPARATOR_TOKEN, CLASS_TOKEN), vocab.keySet());
            if (missingSpecialTokens.isEmpty() == false) {
                throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required {} token(s)", missingSpecialTokens);
            }
            this.sepTokenId = vocab.get(SEPARATOR_TOKEN);
            this.clsTokenId = vocab.get(CLASS_TOKEN);
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
    int clsTokenId() {
        return clsTokenId;
    }

    public String getPadToken() {
        return PAD_TOKEN;
    }

    public String getUnknownToken() {
        return UNKNOWN_TOKEN;
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
        return (inputs, requestId, truncate, span) -> buildTokenizationResult(
            IntStream.range(0, inputs.size())
                .boxed()
                .flatMap(seqId -> tokenize(inputs.get(seqId), truncate, span, seqId).stream())
                .collect(Collectors.toList())
        ).buildRequest(requestId, truncate);
    }

    @Override
    public OptionalInt getPadTokenId() {
        return OptionalInt.of(padTokenId);
    }

    @Override
    public OptionalInt getMaskTokenId() {
        Integer maskId = vocab.get(MASK_TOKEN);
        if (maskId == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(maskId);
    }

    @Override
    public String getMaskToken() {
        return MASK_TOKEN;
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

    public static Builder builder(List<String> vocab, List<String> merges, RobertaTokenization tokenization) {
        return new Builder(vocab, merges, tokenization);
    }

    public static class Builder {

        protected final List<String> originalVocab;
        protected final List<String> merges;
        protected final SortedMap<String, Integer> vocab;
        protected boolean withSpecialTokens;
        protected boolean prefixSpace;
        protected int maxSequenceLength;
        protected Set<String> neverSplit;

        protected Builder(List<String> vocab, List<String> merges, RobertaTokenization tokenization) {
            this.originalVocab = vocab;
            this.vocab = buildSortedVocab(vocab);
            this.merges = merges;
            this.prefixSpace = tokenization.isAddPrefixSpace();
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

        /**
         * Include CLS and SEP tokens
         * @param withSpecialTokens if true include CLS and SEP tokens
         * @return this
         */
        public Builder setWithSpecialTokens(boolean withSpecialTokens) {
            this.withSpecialTokens = withSpecialTokens;
            return this;
        }

        public RobertaTokenizer build() {
            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new RobertaTokenizer(originalVocab, vocab, merges, prefixSpace, withSpecialTokens, maxSequenceLength, neverSplit);
        }
    }
}
