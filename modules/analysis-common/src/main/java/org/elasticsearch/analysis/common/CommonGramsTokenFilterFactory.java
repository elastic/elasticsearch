/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsQueryFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class CommonGramsTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(CommonGramsTokenFilterFactory.class);

    private final CharArraySet words;

    private final boolean ignoreCase;

    private final boolean queryMode;

    CommonGramsTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.queryMode = settings.getAsBoolean("query_mode", false);
        this.words = Analysis.parseCommonWords(env, settings, null, ignoreCase);

        if (this.words == null) {
            throw new IllegalArgumentException(
                "missing or empty [common_words] or [common_words_path] configuration for common_grams token filter"
            );
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        CommonGramsFilter filter = new CommonGramsFilter(tokenStream, words);
        if (queryMode) {
            return new CommonGramsQueryFilter(filter);
        } else {
            return filter;
        }
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
        }

        return this;
    }
}
