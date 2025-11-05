/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.stempel;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.pl.PolishAnalyzerProvider;
import org.elasticsearch.index.analysis.pl.PolishStemTokenFilterFactory;
import org.elasticsearch.index.analysis.pl.PolishStopTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides Stempel-based analysis components for Polish text.
 * Stempel is a Polish language stemmer that provides algorithmic stemming specifically
 * designed for the Polish language morphology.
 */
public class AnalysisStempelPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides Polish-specific token filters including stemming and stop word filtering.
     *
     * @return a map of Polish token filter names to their corresponding factory providers
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_polish_stem": {
     *     "type": "polish_stem"
     *   },
     *   "my_polish_stop": {
     *     "type": "polish_stop"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Map.of("polish_stem", PolishStemTokenFilterFactory::new, "polish_stop", PolishStopTokenFilterFactory::new);
    }

    /**
     * Provides the Polish analyzer for complete Polish text analysis.
     *
     * @return a map containing the "polish" analyzer provider
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_polish": {
     *     "type": "polish"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("polish", PolishAnalyzerProvider::new);
    }
}
