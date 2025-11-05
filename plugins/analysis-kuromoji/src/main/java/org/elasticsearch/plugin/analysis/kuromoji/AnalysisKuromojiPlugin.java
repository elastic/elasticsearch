/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides Kuromoji-based analysis components for Japanese text.
 * Kuromoji is a Japanese morphological analyzer that performs tokenization and various
 * linguistic transformations specific to the Japanese language.
 */
public class AnalysisKuromojiPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides Kuromoji character filters for Japanese text preprocessing.
     *
     * @return a map containing the "kuromoji_iteration_mark" character filter factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "char_filter": {
     *   "my_iteration_mark_filter": {
     *     "type": "kuromoji_iteration_mark"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return singletonMap("kuromoji_iteration_mark", KuromojiIterationMarkCharFilterFactory::new);
    }

    /**
     * Provides Kuromoji token filters for Japanese text analysis.
     * Includes filters for base form conversion, part-of-speech filtering, reading form extraction,
     * stemming, stop words, number handling, and case conversion.
     *
     * @return a map of token filter names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_baseform": {
     *     "type": "kuromoji_baseform"
     *   },
     *   "my_pos_filter": {
     *     "type": "kuromoji_part_of_speech",
     *     "stoptags": ["助詞-格助詞-一般"]
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("kuromoji_baseform", KuromojiBaseFormFilterFactory::new);
        extra.put("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory::new);
        extra.put("kuromoji_readingform", KuromojiReadingFormFilterFactory::new);
        extra.put("kuromoji_stemmer", KuromojiKatakanaStemmerFactory::new);
        extra.put("ja_stop", JapaneseStopTokenFilterFactory::new);
        extra.put("kuromoji_number", KuromojiNumberFilterFactory::new);
        extra.put("kuromoji_completion", KuromojiCompletionFilterFactory::new);
        extra.put("hiragana_uppercase", HiraganaUppercaseFilterFactory::new);
        extra.put("katakana_uppercase", KatakanaUppercaseFilterFactory::new);
        return extra;
    }

    /**
     * Provides the Kuromoji tokenizer for Japanese text segmentation.
     *
     * @return a map containing the "kuromoji_tokenizer" tokenizer factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "tokenizer": {
     *   "my_kuromoji_tokenizer": {
     *     "type": "kuromoji_tokenizer",
     *     "mode": "search"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("kuromoji_tokenizer", KuromojiTokenizerFactory::new);
    }

    /**
     * Provides Kuromoji analyzers for complete Japanese text analysis workflows.
     *
     * @return a map of analyzer names to their corresponding provider factories
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_kuromoji": {
     *     "type": "kuromoji",
     *     "mode": "search"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> extra = new HashMap<>();
        extra.put("kuromoji", KuromojiAnalyzerProvider::new);
        extra.put("kuromoji_completion", KuromojiCompletionAnalyzerProvider::new);
        return extra;
    }
}
