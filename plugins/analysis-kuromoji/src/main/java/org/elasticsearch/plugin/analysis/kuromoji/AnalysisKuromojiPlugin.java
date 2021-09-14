/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.JapaneseStopTokenFilterFactory;
import org.elasticsearch.index.analysis.KuromojiAnalyzerProvider;
import org.elasticsearch.index.analysis.KuromojiBaseFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiIterationMarkCharFilterFactory;
import org.elasticsearch.index.analysis.KuromojiKatakanaStemmerFactory;
import org.elasticsearch.index.analysis.KuromojiNumberFilterFactory;
import org.elasticsearch.index.analysis.KuromojiPartOfSpeechFilterFactory;
import org.elasticsearch.index.analysis.KuromojiReadingFormFilterFactory;
import org.elasticsearch.index.analysis.KuromojiTokenizerFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class AnalysisKuromojiPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return singletonMap("kuromoji_iteration_mark", KuromojiIterationMarkCharFilterFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> extra = new HashMap<>();
        extra.put("kuromoji_baseform", KuromojiBaseFormFilterFactory::new);
        extra.put("kuromoji_part_of_speech", KuromojiPartOfSpeechFilterFactory::new);
        extra.put("kuromoji_readingform", KuromojiReadingFormFilterFactory::new);
        extra.put("kuromoji_stemmer", KuromojiKatakanaStemmerFactory::new);
        extra.put("ja_stop", JapaneseStopTokenFilterFactory::new);
        extra.put("kuromoji_number", KuromojiNumberFilterFactory::new);
        return extra;
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return singletonMap("kuromoji_tokenizer", KuromojiTokenizerFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("kuromoji", KuromojiAnalyzerProvider::new);
    }
}
