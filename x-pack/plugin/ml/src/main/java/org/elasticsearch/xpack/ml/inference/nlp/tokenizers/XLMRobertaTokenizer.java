/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.XLMRobertaTokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.io.Reader;
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

public class XLMRobertaTokenizer extends NlpTokenizer {

    public static final String UNKNOWN_TOKEN = "<unk>";
    public static final String SEPARATOR_TOKEN = "</s>";
    public static final String PAD_TOKEN = "<pad>";
    public static final String CLASS_TOKEN = "<s>";
    public static final String MASK_TOKEN = XLMRobertaTokenization.MASK_TOKEN;

    private static final Set<String> NEVER_SPLIT = Set.of(MASK_TOKEN);

    private final XLMAnalyzer xlmAnalyzer;
    protected final List<String> originalVocab;
    // TODO Not sure this needs to be a sorted map
    private final SortedMap<String, Integer> vocab;
    protected final boolean withSpecialTokens;
    protected final int sepTokenId;
    private final int clsTokenId;
    protected final int padTokenId;
    private final int maxSequenceLength;

    protected XLMRobertaTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        List<Double> scores,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Set<String> neverSplit
    ) throws IOException {
        this.originalVocab = originalVocab;
        this.xlmAnalyzer = new XLMAnalyzer(originalVocab, scores, new ArrayList<>(Sets.union(NEVER_SPLIT, neverSplit)), UNKNOWN_TOKEN);
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
    int numExtraTokensForSingleSequence() {
        return 2;
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
        this.xlmAnalyzer.close();
    }

    @Override
    public TokenizationResult buildTokenizationResult(List<TokenizationResult.Tokens> tokenizations) {
        return new XLMRobertaTokenizationResult(originalVocab, tokenizations, padTokenId);
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
    public List<String> getVocabulary() {
        return originalVocab;
    }

    @Override
    TokenizationResult.TokensBuilder createTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens) {
        return new XLMRobertaTokenizationResult.XLMRobertaTokensBuilder(withSpecialTokens, clsTokenId, sepTokenId);
    }

    /**
     * @param seq cannot be null
     * @return InnerTokenization
     */
    @Override
    public InnerTokenization innerTokenize(String seq) {
        List<Integer> tokenPositionMap = new ArrayList<>();
        try (TokenStream ts = xlmAnalyzer.tokenStream("input", seq)) {
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
        return new InnerTokenization(new ArrayList<>(xlmAnalyzer.getTokens()), tokenPositionMap);
    }

    public static Builder builder(List<String> vocab, List<Double> scores, XLMRobertaTokenization tokenization) {
        return new Builder(vocab, scores, tokenization);
    }

    public static class Builder {

        protected final List<String> originalVocab;
        protected final List<Double> scores;
        protected final SortedMap<String, Integer> vocab;
        protected boolean withSpecialTokens;
        protected int maxSequenceLength;
        protected Set<String> neverSplit;

        protected Builder(List<String> vocab, List<Double> scores, XLMRobertaTokenization tokenization) {
            this.originalVocab = vocab;
            this.vocab = buildSortedVocab(vocab);
            this.scores = scores;
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

        public XLMRobertaTokenizer build() throws IOException {
            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new XLMRobertaTokenizer(originalVocab, vocab, scores, withSpecialTokens, maxSequenceLength, neverSplit);
        }
    }

    static class XLMAnalyzer extends Analyzer {
        private final List<String> vocabulary;
        private final List<String> neverSplit;
        private final double[] scores;
        private UnigramTokenizer innerTokenizer;
        private final String unknownToken;
        private final PrecompiledCharMapNormalizer.Config normalizer;

        XLMAnalyzer(List<String> vocabulary, List<Double> scores, List<String> neverSplit, String unknownToken) throws IOException {
            this.vocabulary = vocabulary;
            this.neverSplit = neverSplit;
            this.unknownToken = unknownToken;
            this.scores = new double[scores.size()];
            int i = 0;
            for (Double s : scores) {
                this.scores[i++] = s;
            }
            normalizer = PrecompiledCharMapNormalizer.fromBase64EncodedResource(
                "/org/elasticsearch/xpack/ml/inference.nlp.tokenizers/spm_precompiled_normalizer.txt"
            );
        }

        @Override
        protected Reader initReader(String fieldName, Reader reader) {
            if (normalizer.offsets().length > 0) {
                return new PrecompiledCharMapNormalizer(normalizer.offsets(), normalizer.utf8str(), reader);
            }
            return reader;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            this.innerTokenizer = UnigramTokenizer.build(neverSplit, vocabulary, scores, unknownToken, false);
            return new TokenStreamComponents(this.innerTokenizer);
        }

        public List<DelimitedToken.Encoded> getTokens() {
            if (innerTokenizer != null) {
                return innerTokenizer.getTokenizedValues();
            } else {
                return List.of();
            }
        }
    }
}
