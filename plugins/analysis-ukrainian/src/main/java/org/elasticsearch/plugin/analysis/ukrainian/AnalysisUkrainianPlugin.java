/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.ukrainian;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides Ukrainian language analysis components.
 * This plugin provides linguistic analysis specifically designed for Ukrainian text,
 * including appropriate stemming and stop word filtering.
 */
public class AnalysisUkrainianPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides the Ukrainian analyzer for complete Ukrainian text analysis.
     *
     * @return a map containing the "ukrainian" analyzer provider
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_ukrainian": {
     *     "type": "ukrainian"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("ukrainian", UkrainianAnalyzerProvider::new);
    }
}
