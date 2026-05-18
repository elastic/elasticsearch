/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;

import java.util.List;

public class BpeAnalyzer extends Analyzer {

    private final List<String> vocabulary;
    private final List<String> merges;
    private final List<String> neverSplit;
    private final boolean isPrefixSpace;
    private BpeTokenizer innerTokenizer;
    private final String unknownToken;

    public BpeAnalyzer(List<String> vocabulary, List<String> merges, List<String> neverSplit, boolean isPrefixSpace, String unknownToken) {
        this.vocabulary = vocabulary;
        this.merges = merges;
        this.neverSplit = neverSplit;
        this.isPrefixSpace = isPrefixSpace;
        this.unknownToken = unknownToken;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        this.innerTokenizer = BpeTokenizer.build(neverSplit, vocabulary, merges, unknownToken, isPrefixSpace);
        return new TokenStreamComponents(this.innerTokenizer);
    }

    public List<BpeTokenizer.BpeToken> getTokens() {
        if (innerTokenizer != null) {
            return innerTokenizer.getTokenizedValues();
        } else {
            return List.of();
        }
    }
}
