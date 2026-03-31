/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.analysis.miscellaneous.DisableGraphAttribute;

public class ShingleTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Factory factory;

    public ShingleTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        int maxAllowedShingleDiff = indexSettings.getMaxShingleDiff();
        Integer maxShingleSize = settings.getAsInt("max_shingle_size", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
        Integer minShingleSize = settings.getAsInt("min_shingle_size", ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE);
        Boolean outputUnigrams = settings.getAsBoolean("output_unigrams", true);

        int shingleDiff = maxShingleSize - minShingleSize + (outputUnigrams ? 1 : 0);
        if (shingleDiff > maxAllowedShingleDiff) {
            throw new IllegalArgumentException(
                "In Shingle TokenFilter the difference between max_shingle_size and min_shingle_size (and +1 if outputting unigrams)"
                    + " must be less than or equal to: ["
                    + maxAllowedShingleDiff
                    + "] but was ["
                    + shingleDiff
                    + "]. This limit"
                    + " can be set by changing the ["
                    + IndexSettings.MAX_SHINGLE_DIFF_SETTING.getKey()
                    + "] index level setting."
            );
        }

        Boolean outputUnigramsIfNoShingles = settings.getAsBoolean("output_unigrams_if_no_shingles", false);
        String tokenSeparator = settings.get("token_separator", ShingleFilter.DEFAULT_TOKEN_SEPARATOR);
        String fillerToken = settings.get("filler_token", ShingleFilter.DEFAULT_FILLER_TOKEN);
        factory = new Factory(
            "shingle",
            minShingleSize,
            maxShingleSize,
            outputUnigrams,
            outputUnigramsIfNoShingles,
            tokenSeparator,
            fillerToken
        );
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return factory.create(tokenStream);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }

    public Factory getInnerFactory() {
        return this.factory;
    }

    public static final class Factory implements TokenFilterFactory {
        private final int maxShingleSize;

        private final boolean outputUnigrams;

        private final boolean outputUnigramsIfNoShingles;

        private final String tokenSeparator;
        private final String fillerToken;

        private final int minShingleSize;

        private final String name;

        public Factory(String name) {
            this(
                name,
                ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE,
                ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE,
                true,
                false,
                ShingleFilter.DEFAULT_TOKEN_SEPARATOR,
                ShingleFilter.DEFAULT_FILLER_TOKEN
            );
        }

        Factory(
            String name,
            int minShingleSize,
            int maxShingleSize,
            boolean outputUnigrams,
            boolean outputUnigramsIfNoShingles,
            String tokenSeparator,
            String fillerToken
        ) {
            this.maxShingleSize = maxShingleSize;
            this.outputUnigrams = outputUnigrams;
            this.outputUnigramsIfNoShingles = outputUnigramsIfNoShingles;
            this.tokenSeparator = tokenSeparator;
            this.minShingleSize = minShingleSize;
            this.fillerToken = fillerToken;
            this.name = name;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            ShingleFilter filter = new ShingleFilter(tokenStream, minShingleSize, maxShingleSize);
            filter.setOutputUnigrams(outputUnigrams);
            filter.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
            filter.setTokenSeparator(tokenSeparator);
            filter.setFillerToken(fillerToken);
            if (outputUnigrams || (minShingleSize != maxShingleSize)) {
                /**
                 * We disable the graph analysis on this token stream
                 * because it produces shingles of different size.
                 * Graph analysis on such token stream is useless and dangerous as it may create too many paths
                 * since shingles of different size are not aligned in terms of positions.
                 */
                filter.addAttribute(DisableGraphAttribute.class);
            }
            return filter;
        }

        public int getMaxShingleSize() {
            return maxShingleSize;
        }

        public int getMinShingleSize() {
            return minShingleSize;
        }

        public boolean getOutputUnigrams() {
            return outputUnigrams;
        }

        public boolean getOutputUnigramsIfNoShingles() {
            return outputUnigramsIfNoShingles;
        }

        @Override
        public String name() {
            return name;
        }
    }
}
