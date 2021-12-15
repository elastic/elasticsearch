/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.stempel;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.pl.DemoIteratorFactory;
import org.elasticsearch.index.analysis.pl.DemoTokenFilterFactory;
import org.elasticsearch.index.analysis.pl.PolishAnalyzerProvider;
import org.elasticsearch.index.analysis.pl.PolishStemTokenFilterFactory;
import org.elasticsearch.index.analysis.pl.PolishStopTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.analysis.AnalysisIteratorFactory;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class AnalysisStempelPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return Map.of(
            "polish_stem", PolishStemTokenFilterFactory::new,
            "polish_stop", PolishStopTokenFilterFactory::new,
            "demo_legacy", DemoTokenFilterFactory::new);
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("polish", PolishAnalyzerProvider::new);
    }

    @Override
    public Map<String, AnalysisProvider<AnalysisIteratorFactory>> getIterators() {
        return singletonMap("demo", DemoIteratorFactory::new);
    }

    @Override
    public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        return List.of(PreConfiguredTokenFilter.singleton("uppercase", true, UpperCaseFilter::new));
    }
}
