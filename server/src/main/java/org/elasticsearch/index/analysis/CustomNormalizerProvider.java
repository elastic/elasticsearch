/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A custom normalizer that is built out of a char and token filters. On the
 * contrary to analyzers, it does not support tokenizers and only supports a
 * subset of char and token filters.
 */
public final class CustomNormalizerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    private final Settings analyzerSettings;

    private CustomAnalyzer customAnalyzer;

    public CustomNormalizerProvider(IndexSettings indexSettings, String name, Settings settings) {
        super(name);
        this.analyzerSettings = settings;
    }

    public void build(
        final TokenizerFactory tokenizerFactory,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters
    ) {
        if (analyzerSettings.get("tokenizer") != null) {
            throw new IllegalArgumentException("Custom normalizer [" + name() + "] cannot configure a tokenizer");
        }

        List<String> charFilterNames = analyzerSettings.getAsList("char_filter");
        List<CharFilterFactory> charFiltersList = new ArrayList<>(charFilterNames.size());
        for (String charFilterName : charFilterNames) {
            CharFilterFactory charFilter = charFilters.get(charFilterName);
            if (charFilter == null) {
                throw new IllegalArgumentException(
                    "Custom normalizer [" + name() + "] failed to find char_filter under name [" + charFilterName + "]"
                );
            }
            if (charFilter instanceof NormalizingCharFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer [" + name() + "] may not use char filter [" + charFilterName + "]");
            }
            charFiltersList.add(charFilter);
        }

        List<String> tokenFilterNames = analyzerSettings.getAsList("filter");
        List<TokenFilterFactory> tokenFilterList = new ArrayList<>(tokenFilterNames.size());
        for (String tokenFilterName : tokenFilterNames) {
            TokenFilterFactory tokenFilter = tokenFilters.get(tokenFilterName);
            if (tokenFilter == null) {
                throw new IllegalArgumentException(
                    "Custom Analyzer [" + name() + "] failed to find filter under name [" + tokenFilterName + "]"
                );
            }
            if (tokenFilter instanceof NormalizingTokenFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer [" + name() + "] may not use filter [" + tokenFilterName + "]");
            }
            tokenFilterList.add(tokenFilter);
        }

        this.customAnalyzer = new CustomAnalyzer(
            tokenizerFactory,
            charFiltersList.toArray(new CharFilterFactory[charFiltersList.size()]),
            tokenFilterList.toArray(new TokenFilterFactory[tokenFilterList.size()])
        );
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
