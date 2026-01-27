/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class NGramTokenFilterFactory extends AbstractTokenFilterFactory {
    private final int minGram;
    private final int maxGram;
    private final boolean preserveOriginal;
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    NGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        int maxAllowedNgramDiff = indexSettings.getMaxNgramDiff();
        this.minGram = settings.getAsInt("min_gram", 1);
        this.maxGram = settings.getAsInt("max_gram", 2);
        int ngramDiff = maxGram - minGram;
        if (ngramDiff > maxAllowedNgramDiff) {
            throw new IllegalArgumentException(
                "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: ["
                    + maxAllowedNgramDiff
                    + "] but was ["
                    + ngramDiff
                    + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey()
                    + "] index level setting."
            );
        }
        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new NGramTokenFilter(tokenStream, minGram, maxGram, preserveOriginal);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }
}
