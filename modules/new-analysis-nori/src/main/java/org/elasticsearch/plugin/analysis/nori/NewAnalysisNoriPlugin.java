/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.elasticsearch.sp.api.analysis.AnalysisPlugin;
import org.elasticsearch.sp.api.analysis.TokenFilterFactory;
import org.elasticsearch.sp.api.analysis.TokenizerFactory;

import java.util.Map;

public class NewAnalysisNoriPlugin implements AnalysisPlugin {
    @Override
    public Map<String, Class<? extends TokenFilterFactory>> getTokenFilterFactories() {
        return Map.of(
            "nori_part_of_speech",
            NoriPartOfSpeechStopFilterFactory.class,
            "nori_readingform",
            NoriReadingFormFilterFactory.class,
            "nori_number",
            NoriNumberFilterFactory2.class
        );
    }

    @Override
    public Map<String, Class<? extends TokenizerFactory>> getTokenizerFactories() {
        return Map.of("nori_tokenizer", NoriTokenizerFactory.class);
    }

    // @Override
    // public Map<String, Class< ? extends Analyzer>> getAnalyzers() {
    // return null;// singletonMap("nori", NoriAnalyzerProvider::new);
    // }
}
