/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService.IndexCreationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A class that groups analysis components necessary to produce a custom analyzer.
 * See {@link ReloadableCustomAnalyzer} for an example usage.
 */
public final class AnalyzerComponents {

    private final TokenizerFactory tokenizerFactory;
    private final CharFilterFactory[] charFilters;
    private final TokenFilterFactory[] tokenFilters;
    private final AnalysisMode analysisMode;

    AnalyzerComponents(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters, TokenFilterFactory[] tokenFilters) {

        this.tokenizerFactory = tokenizerFactory;
        this.charFilters = charFilters;
        this.tokenFilters = tokenFilters;
        AnalysisMode mode = AnalysisMode.ALL;
        for (TokenFilterFactory f : tokenFilters) {
            mode = mode.merge(f.getAnalysisMode());
        }
        this.analysisMode = mode;
    }

    static AnalyzerComponents createComponents(
        IndexCreationContext context,
        String name,
        Settings analyzerSettings,
        final Map<String, TokenizerFactory> tokenizers,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters
    ) {
        String tokenizerName = analyzerSettings.get("tokenizer");
        if (tokenizerName == null) {
            throw new IllegalArgumentException("Custom Analyzer [" + name + "] must be configured with a tokenizer");
        }

        TokenizerFactory tokenizer = tokenizers.get(tokenizerName);
        if (tokenizer == null) {
            throw new IllegalArgumentException(
                "Custom Analyzer [" + name + "] failed to find tokenizer under name " + "[" + tokenizerName + "]"
            );
        }

        List<String> charFilterNames = analyzerSettings.getAsList("char_filter");
        List<CharFilterFactory> charFiltersList = new ArrayList<>(charFilterNames.size());
        for (String charFilterName : charFilterNames) {
            CharFilterFactory charFilter = charFilters.get(charFilterName);
            if (charFilter == null) {
                throw new IllegalArgumentException(
                    "Custom Analyzer [" + name + "] failed to find char_filter under name " + "[" + charFilterName + "]"
                );
            }
            charFiltersList.add(charFilter);
        }

        List<String> tokenFilterNames = analyzerSettings.getAsList("filter");
        List<TokenFilterFactory> tokenFilterList = new ArrayList<>(tokenFilterNames.size());
        for (String tokenFilterName : tokenFilterNames) {
            TokenFilterFactory tokenFilter = tokenFilters.get(tokenFilterName);
            if (tokenFilter == null) {
                throw new IllegalArgumentException(
                    "Custom Analyzer [" + name + "] failed to find filter under name " + "[" + tokenFilterName + "]"
                );
            }
            tokenFilter = tokenFilter.getChainAwareTokenFilterFactory(
                context,
                tokenizer,
                charFiltersList,
                tokenFilterList,
                tokenFilters::get
            );
            tokenFilterList.add(tokenFilter);
        }

        return new AnalyzerComponents(
            tokenizer,
            charFiltersList.toArray(new CharFilterFactory[charFiltersList.size()]),
            tokenFilterList.toArray(new TokenFilterFactory[tokenFilterList.size()])
        );
    }

    public TokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    public TokenFilterFactory[] getTokenFilters() {
        return tokenFilters;
    }

    public CharFilterFactory[] getCharFilters() {
        return charFilters;
    }

    public AnalysisMode analysisMode() {
        return this.analysisMode;
    }
}
