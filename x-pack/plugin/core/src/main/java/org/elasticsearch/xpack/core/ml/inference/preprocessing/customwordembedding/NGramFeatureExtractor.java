/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import org.apache.lucene.util.Counter;

import java.util.Map;
import java.util.TreeMap;

/**
 * This provides an array of {@link FeatureValue} for the given nGram size and dimensionId
 *
 * Each feature value contains the average occurrence of an nGram and its "id". This id is determined via a custom hash ({@link Hash32})
 * and the provided dimensionId
 */
public class NGramFeatureExtractor implements FeatureExtractor {

    private static final Hash32 hashing = new Hash32();

    private final int nGrams;
    private final int dimensionId;

    public NGramFeatureExtractor(int nGrams, int dimensionId) {
        this.nGrams = nGrams;
        this.dimensionId = dimensionId;
    }

    @Override
    public FeatureValue[] extractFeatures(String text) {
        // First add terminators:
        // Split the text based on spaces to get tokens, adds "^"
        // to the beginning of each token, and adds "$" to the end of each token.
        // e.g.
        // " this text is written in english" goes to
        // "^$ ^this$ ^text$ ^is$ ^written$ ^in$ ^english$ ^$"
        StringBuilder newText = new StringBuilder("^");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == ' ') {
                newText.append("$ ^");
            } else {
                newText.append(c);
            }
        }
        newText.append("$");

        // Find the char ngrams
        // ^$ ^this$ ^text$ ^is$ ^written$ ^in$ ^english$ ^$"
        // nGramSize = 2
        // [{h$},{sh},{li},{gl},{in},{en},{^$},...]
        Map<String, Counter> charNGrams = new TreeMap<>();

        int countSum = 0;
        String textWithTerminators = newText.toString();
        int end = textWithTerminators.length() - nGrams;
        for (int start = 0; start <= end; ++start) {
            StringBuilder charNGram = new StringBuilder();

            int index;
            for (index = 0; index < nGrams; ++index) {
                char currentChar = textWithTerminators.charAt(start + index);
                if (currentChar == ' ') {
                    break;
                }
                charNGram.append(currentChar);
            }

            if (index == nGrams) {
                charNGrams.computeIfAbsent(charNGram.toString(), ngram -> Counter.newCounter()).addAndGet(1);
                ++countSum;
            }
        }

        FeatureValue[] results = new FeatureValue[charNGrams.size()];
        int index = 0;
        for (Map.Entry<String, Counter> entry : charNGrams.entrySet()) {
            String key = entry.getKey();
            long value = entry.getValue().get();

            double weight = (double) value / (double) countSum;
            // We need to use the special hashing so that we choose the appropriate weight+ quantile
            // when building the feature vector.
            int id = (int)(hashing.hash(key) % dimensionId);

            results[index++] = new ContinuousFeatureValue(id, weight);
        }
        return results;
    }
}
