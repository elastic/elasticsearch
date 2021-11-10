/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import static org.elasticsearch.analysis.common.FingerprintAnalyzerProvider.DEFAULT_MAX_OUTPUT_SIZE;
import static org.elasticsearch.analysis.common.FingerprintAnalyzerProvider.MAX_OUTPUT_SIZE;

public class FingerprintTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(FingerprintTokenFilterFactory.class);

    private final char separator;
    private final int maxOutputSize;

    FingerprintTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.separator = FingerprintAnalyzerProvider.parseSeparator(settings);
        this.maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(), DEFAULT_MAX_OUTPUT_SIZE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;
        result = new FingerprintFilter(result, maxOutputSize, separator);
        return result;
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
