/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.TextFieldMapper;

import java.util.Map;

import static org.elasticsearch.index.analysis.AnalyzerComponents.createComponents;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 */
public class CustomAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Settings analyzerSettings;

    private Analyzer customAnalyzer;

    public CustomAnalyzerProvider(IndexSettings indexSettings, String name, Settings settings) {
        super(name);
        this.analyzerSettings = settings;
    }

    void build(
        final IndexCreationContext context,
        final Map<String, TokenizerFactory> tokenizers,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters
    ) {
        customAnalyzer = create(context, name(), analyzerSettings, tokenizers, charFilters, tokenFilters);
    }

    /**
     * Factory method that either returns a plain {@link ReloadableCustomAnalyzer} if the components used for creation are supporting index
     * and search time use, or a {@link ReloadableCustomAnalyzer} if the components are intended for search time use only.
     */
    private static Analyzer create(
        IndexCreationContext context,
        String name,
        Settings analyzerSettings,
        Map<String, TokenizerFactory> tokenizers,
        Map<String, CharFilterFactory> charFilters,
        Map<String, TokenFilterFactory> tokenFilters
    ) {
        int positionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        positionIncrementGap = analyzerSettings.getAsInt("position_increment_gap", positionIncrementGap);
        int offsetGap = analyzerSettings.getAsInt("offset_gap", -1);
        AnalyzerComponents components = createComponents(context, name, analyzerSettings, tokenizers, charFilters, tokenFilters);
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME)) {
            return new ReloadableCustomAnalyzer(components, positionIncrementGap, offsetGap);
        } else {
            return new CustomAnalyzer(
                components.getTokenizerFactory(),
                components.getCharFilters(),
                components.getTokenFilters(),
                positionIncrementGap,
                offsetGap
            );
        }
    }

    @Override
    public Analyzer get() {
        return this.customAnalyzer;
    }
}
