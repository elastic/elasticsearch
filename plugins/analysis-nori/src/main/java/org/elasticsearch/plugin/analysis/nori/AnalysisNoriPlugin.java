/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.nori;

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
 * Elasticsearch plugin that provides Nori-based analysis components for Korean text.
 * Nori is a Korean morphological analyzer that performs tokenization and linguistic
 * transformations specific to the Korean language.
 */
public class AnalysisNoriPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides Nori token filters for Korean text analysis.
     * Includes filters for part-of-speech filtering, reading form extraction, and number handling.
     *
     * @return a map of token filter names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_pos_filter": {
     *     "type": "nori_part_of_speech",
     *     "stoptags": ["E", "IC", "J"]
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("nori_part_of_speech", NoriPartOfSpeechStopFilterFactory::new);
        extra.put("nori_readingform", NoriReadingFormFilterFactory::new);
        extra.put("nori_number", NoriNumberFilterFactory::new);
        return extra;
    }

    /**
     * Provides the Nori tokenizer for Korean text segmentation.
     *
     * @return a map containing the "nori_tokenizer" tokenizer factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "tokenizer": {
     *   "my_nori_tokenizer": {
     *     "type": "nori_tokenizer",
     *     "decompound_mode": "mixed"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("nori_tokenizer", NoriTokenizerFactory::new);
    }

    /**
     * Provides the Nori analyzer for complete Korean text analysis.
     *
     * @return a map containing the "nori" analyzer provider
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_nori": {
     *     "type": "nori"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("nori", NoriAnalyzerProvider::new);
    }
}
