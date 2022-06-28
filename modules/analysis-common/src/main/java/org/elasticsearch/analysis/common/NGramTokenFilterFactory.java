/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class NGramTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(NGramTokenFilterFactory.class);

    private final int minGram;
    private final int maxGram;
    private final boolean preserveOriginal;
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    NGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        int maxAllowedNgramDiff = indexSettings.getMaxNgramDiff();
        this.minGram = settings.getAsInt("min_gram", 1);
        this.maxGram = settings.getAsInt("max_gram", 2);
        int ngramDiff = maxGram - minGram;
        if (ngramDiff > maxAllowedNgramDiff) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
                throw new IllegalArgumentException(
                    "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: ["
                        + maxAllowedNgramDiff
                        + "] but was ["
                        + ngramDiff
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey()
                        + "] index level setting."
                );
            } else {
                deprecationLogger.critical(
                    DeprecationCategory.ANALYSIS,
                    "ngram_big_difference",
                    "Deprecated big difference between max_gram and min_gram in NGram Tokenizer,"
                        + "expected difference must be less than or equal to: ["
                        + maxAllowedNgramDiff
                        + "]"
                );
            }
        }
        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new NGramTokenFilter(tokenStream, minGram, maxGram, preserveOriginal);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
        } else {
            DEPRECATION_LOGGER.critical(
                DeprecationCategory.ANALYSIS,
                "synonym_tokenfilters",
                "Token filter [" + name() + "] will not be usable to parse synonyms after v7.0"
            );
            return this;
        }
    }
}
