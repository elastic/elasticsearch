/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import java.util.Map;
import java.util.TreeMap;

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
        Map<String, Integer> charNGrams = new TreeMap<>();

        //TODO use lucene tokenizer ?
        int countSum = 0;
        for (int start = 0; start <= (newText.toString().length()) - nGrams; ++start) {
            StringBuilder charNGram = new StringBuilder();

            int index;
            for (index = 0; index < nGrams; ++index) {
                char currentChar = newText.toString().charAt(start + index);
                if (currentChar == ' ') {
                    break;
                }
                charNGram.append(currentChar);
            }

            if (index == nGrams) {
                charNGrams.put(charNGram.toString(),
                    charNGrams.getOrDefault(charNGram.toString(), 0) + 1);
                ++countSum;
            }
        }

        FeatureValue[] results = new FeatureValue[charNGrams.size()];
        int index = 0;
        for (Map.Entry<String, Integer> entry : charNGrams.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();

            double weight = (double) value / (double) countSum;
            int id = Integer.remainderUnsigned(hashing.hash(key), dimensionId);

            results[index++] = new ContinuousFeatureValue(id, weight);
        }
        return results;
    }
}
