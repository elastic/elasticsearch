/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.smartcn;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides Smart Chinese analysis components.
 * Smart Chinese analyzer performs Chinese text segmentation using a probabilistic
 * Hidden Markov Model with Viterbi algorithm for word breaking.
 */
public class AnalysisSmartChinesePlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides Smart Chinese token filters for Chinese text processing.
     * Includes Chinese-specific stop word filtering.
     *
     * @return a map of token filter names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_smartcn_stop": {
     *     "type": "smartcn_stop"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>();
        tokenFilters.put("smartcn_stop", SmartChineseStopTokenFilterFactory::new);
        // TODO: deprecate and remove, this is a noop token filter; it's here for backwards compat before we had "smartcn_tokenizer"
        tokenFilters.put("smartcn_word", SmartChineseNoOpTokenFilterFactory::new);
        return tokenFilters;
    }

    /**
     * Provides Smart Chinese tokenizers for Chinese text segmentation.
     *
     * @return a map of tokenizer names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "tokenizer": {
     *   "my_smartcn": {
     *     "type": "smartcn_tokenizer"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisProvider<TokenizerFactory>> extra = new HashMap<>();
        extra.put("smartcn_tokenizer", SmartChineseTokenizerTokenizerFactory::new);
        // TODO: deprecate and remove, this is an alias to "smartcn_tokenizer"; it's here for backwards compat
        extra.put("smartcn_sentence", SmartChineseTokenizerTokenizerFactory::new);
        return extra;
    }

    /**
     * Provides the Smart Chinese analyzer for complete Chinese text analysis.
     *
     * @return a map containing the "smartcn" analyzer provider
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_smartcn": {
     *     "type": "smartcn"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("smartcn", SmartChineseAnalyzerProvider::new);
    }
}
